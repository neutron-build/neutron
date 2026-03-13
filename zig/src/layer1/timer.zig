// Layer 1: Timer and deadline utilities
//
// Simple deadline/timeout helpers for connection management.
// Uses std.time for monotonic clock access.

const std = @import("std");

/// A deadline that expires after a given duration.
pub const Deadline = struct {
    expiry_ns: i128,

    /// Create a deadline that expires `timeout_ms` from now.
    pub fn fromNow(timeout_ms: u64) Deadline {
        const now = std.time.nanoTimestamp();
        return .{ .expiry_ns = now + @as(i128, timeout_ms) * std.time.ns_per_ms };
    }

    /// Create a deadline that never expires.
    pub fn never() Deadline {
        return .{ .expiry_ns = std.math.maxInt(i128) };
    }

    /// Check if the deadline has expired.
    pub fn isExpired(self: Deadline) bool {
        return std.time.nanoTimestamp() >= self.expiry_ns;
    }

    /// Remaining time in milliseconds (0 if expired).
    pub fn remainingMs(self: Deadline) u64 {
        const now = std.time.nanoTimestamp();
        if (now >= self.expiry_ns) return 0;
        const remaining_ns: u128 = @intCast(self.expiry_ns - now);
        return @intCast(remaining_ns / std.time.ns_per_ms);
    }
};

/// Simple elapsed timer for measuring durations.
pub const Timer = struct {
    start_ns: i128,

    pub fn start() Timer {
        return .{ .start_ns = std.time.nanoTimestamp() };
    }

    pub fn elapsedNs(self: Timer) u64 {
        const now = std.time.nanoTimestamp();
        const diff = now - self.start_ns;
        if (diff < 0) return 0;
        return @intCast(diff);
    }

    pub fn elapsedMs(self: Timer) u64 {
        return self.elapsedNs() / std.time.ns_per_ms;
    }

    pub fn elapsedUs(self: Timer) u64 {
        return self.elapsedNs() / std.time.ns_per_us;
    }
};

test "Deadline: fromNow" {
    const d = Deadline.fromNow(1000);
    try std.testing.expect(!d.isExpired());
    try std.testing.expect(d.remainingMs() > 0);
    try std.testing.expect(d.remainingMs() <= 1000);
}

test "Deadline: never" {
    const d = Deadline.never();
    try std.testing.expect(!d.isExpired());
}

test "Deadline: zero timeout is immediately expired" {
    const d = Deadline.fromNow(0);
    // May or may not be expired depending on timing, but remainingMs should be 0 or very small
    try std.testing.expect(d.remainingMs() == 0);
}

test "Timer: elapsed" {
    const t = Timer.start();
    // Just verify it returns a reasonable value (>= 0)
    try std.testing.expect(t.elapsedNs() < 1_000_000_000); // less than 1 second
}
