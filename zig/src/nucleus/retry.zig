// Nucleus retry — serialization-failure classification and a managed helper.
//
// SERIALIZABLE is real on the shipping engine, which makes SQLSTATE 40001
// something applications actually receive. A serializable transaction that is
// never retried is a transaction that randomly fails under concurrency, and no
// PostgreSQL driver retries for you — drivers surface the code, the application
// decides. This is the SDK's answer to that obligation, per
// FRAMEWORK_CONTRACT.md §3.14.
//
// No allocation: classification is a fixed-size string compare and the helper
// keeps its state on the stack, so this stays usable from the embedded target.

const std = @import("std");
const NucleusClient = @import("client.zig").NucleusClient;
const Transaction = @import("tx.zig").Transaction;

/// The transaction lost a conflict and MUST be retried from the beginning.
///
/// Raised by two mechanisms: strict 2PL wait-die on the disk engine (the
/// younger transaction is killed to break a potential deadlock) and SSI on the
/// MVCC engine (a dangerous structure detected at commit).
pub const SQLSTATE_SERIALIZATION_FAILURE = "40001";

/// `lock_timeout` elapsed waiting for a table lock. Deliberately NOT retryable:
/// the holder is still there, so retrying spins against a lock that is not
/// moving. Raise lock_timeout or find the transaction holding it.
pub const SQLSTATE_LOCK_NOT_AVAILABLE = "55P03";

/// A statement was issued after the transaction had already been aborted. Only
/// ROLLBACK is accepted, so the whole transaction must re-run — which is what
/// `withRetry` does.
pub const SQLSTATE_IN_FAILED_TRANSACTION = "25P02";

/// Whether a SQLSTATE is a conflict the caller should retry (40001, or 25P02
/// from a transaction already killed by one).
///
/// Classification is on the code, never on message text — `NucleusClient`
/// captures the SQLSTATE off the connection before it returns to the pool.
pub fn isSerializationFailure(code: []const u8) bool {
    return std.mem.eql(u8, code, SQLSTATE_SERIALIZATION_FAILURE) or
        std.mem.eql(u8, code, SQLSTATE_IN_FAILED_TRANSACTION);
}

/// Whether a SQLSTATE is a `lock_timeout` expiry (55P03).
///
/// Kept distinct from `isSerializationFailure` on purpose: the two call for
/// opposite responses. A serialization failure means "someone else won, try
/// again"; a lock timeout means "the lock is still held, retrying will not
/// help".
pub fn isLockNotAvailable(code: []const u8) bool {
    return std.mem.eql(u8, code, SQLSTATE_LOCK_NOT_AVAILABLE);
}

/// Retry policy for `withRetry`.
pub const RetryOptions = struct {
    /// Attempts including the first. Values below 1 are treated as 1.
    max_attempts: u32 = 5,
    /// Delay before the second attempt, doubled each subsequent attempt.
    base_delay_ns: u64 = 2 * std.time.ns_per_ms,
    /// Ceiling on the backoff.
    max_delay_ns: u64 = 250 * std.time.ns_per_ms,
    /// Isolation level, e.g. "SERIALIZABLE". Null leaves the server default.
    isolation_level: ?[]const u8 = null,
};

/// Full jitter. Without it two conflicting transactions retry in lockstep and
/// collide again on the same schedule — and under wait-die the younger one
/// loses every round, so a fixed backoff can starve it indefinitely.
fn jitter(rng: *std.Random.DefaultPrng, delay_ns: u64) u64 {
    if (delay_ns == 0) return 0;
    return rng.random().uintLessThan(u64, delay_ns + 1);
}

/// Run `func` inside a transaction, retrying it on serialization failure.
///
/// `func` MUST be idempotent with respect to anything outside the database: it
/// can run more than once. Everything it does through the transaction is rolled
/// back between attempts; anything it does elsewhere is not.
///
/// On success the transaction commits. On a serialization failure it is rolled
/// back and retried with jittered exponential backoff. On any other error it is
/// rolled back and the error returned unchanged — in particular a lock_timeout
/// (55P03) is NOT retried, because the lock is still held.
///
/// The client's last error message is what gets classified, so `func` should
/// surface server errors rather than swallowing them.
pub fn withRetry(
    client: *NucleusClient,
    opts: RetryOptions,
    comptime Ctx: type,
    ctx: Ctx,
    func: *const fn (Ctx, *Transaction) anyerror!void,
) anyerror!void {
    const attempts = if (opts.max_attempts < 1) 1 else opts.max_attempts;
    var delay = if (opts.base_delay_ns == 0) 2 * std.time.ns_per_ms else opts.base_delay_ns;
    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp()));

    var attempt: u32 = 1;
    while (attempt <= attempts) : (attempt += 1) {
        var tx = try client.begin();

        if (opts.isolation_level) |level| {
            // An engine that cannot honour the level refuses rather than
            // silently downgrading, so this surfaces the mismatch.
            var buf: [96]u8 = undefined;
            const stmt = std.fmt.bufPrint(
                &buf,
                "SET TRANSACTION ISOLATION LEVEL {s}",
                .{level},
            ) catch return error.IsolationLevelTooLong;
            _ = client.exec(stmt) catch |err| {
                tx.rollback() catch {};
                return err;
            };
        }

        if (func(ctx, &tx)) |_| {
            if (tx.commit()) |_| {
                return;
            } else |err| {
                // An abandoned transaction holds its locks, and on the disk
                // engine an abandoned exclusive lock blocks every other
                // serializable transaction on that table.
                tx.rollback() catch {};
                if (!retryable(client, attempt, attempts)) return err;
            }
        } else |err| {
            tx.rollback() catch {};
            if (!retryable(client, attempt, attempts)) return err;
        }

        if (attempt < attempts) {
            std.Thread.sleep(jitter(&rng, delay));
            delay = @min(delay * 2, opts.max_delay_ns);
        }
    }
    return error.SerializationFailure;
}

fn retryable(client: *const NucleusClient, attempt: u32, attempts: u32) bool {
    if (attempt >= attempts) return false;
    return isSerializationFailure(client.lastErrorCode());
}

// ── Tests ─────────────────────────────────────────────────────

test "lock timeout is never treated as retryable" {
    // The distinction that matters: retrying 55P03 spins against a lock that
    // is not moving, so it must never fold into the serialization case.
    try std.testing.expect(isLockNotAvailable(SQLSTATE_LOCK_NOT_AVAILABLE));
    try std.testing.expect(!isSerializationFailure(SQLSTATE_LOCK_NOT_AVAILABLE));
}

test "aborted transaction counts as retryable" {
    try std.testing.expect(isSerializationFailure(SQLSTATE_IN_FAILED_TRANSACTION));
    try std.testing.expect(isSerializationFailure(SQLSTATE_SERIALIZATION_FAILURE));
    try std.testing.expect(!isLockNotAvailable(SQLSTATE_IN_FAILED_TRANSACTION));
}

test "an empty or unrelated code is not retryable" {
    try std.testing.expect(!isSerializationFailure(""));
    try std.testing.expect(!isSerializationFailure("42601"));
    try std.testing.expect(!isLockNotAvailable(""));
}

test "jitter never exceeds the delay" {
    var rng = std.Random.DefaultPrng.init(7);
    var i: usize = 0;
    while (i < 128) : (i += 1) {
        try std.testing.expect(jitter(&rng, 1000) <= 1000);
    }
    try std.testing.expectEqual(@as(u64, 0), jitter(&rng, 0));
}
