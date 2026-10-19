//! C-T0 §9: garbage collection for ts-in-key backends — the compaction
//! filter, the watermark publisher, the tombstone `DeleteRange` job and
//! retired-storage cleanup. Normative: `docs/C-T0-txn-protocol.md` §9;
//! §2.2 defines the version-key layout and the GC range, §3.1 the registry
//! the watermark is computed from, §7.3 the intent removal the retire path
//! reuses, §10 the `/sys/` rules.
//!
//! - [`TxnGcFilter`] is the §9.2 compaction filter. It shares an
//!   `Arc<AtomicU64>` **durable W** with [`GcJob`]: `begin` reads it once
//!   per stream, so a stream never acts on a `W` that is not already
//!   synced to `/sys/gc_w` (seed 42), and every drop decision's state
//!   lives in the stream, never on the filter object and never across
//!   streams (seed 33). It drops a version only when a newer version
//!   `<= W` of the same logical key passed by earlier in the same stream;
//!   the newest version `<= W` is never dropped, tombstone or not
//!   (seed 3). §10's scope is [`gc_scope`]: every `/sys/` key except the
//!   catalog's is never dropped, even when its bytes match the
//!   version-key pattern; the catalog (`/sys/catalog/`, versioned table
//!   data since C-T5) is GC-able like any table — the C-B1c carve-out
//!   the C-T5 card escalated.
//! - [`GcJob::install`] wires the filter into the KV. The durable W it
//!   hands out comes from `/sys/gc_w` itself (read through a registered
//!   view), never from `registry.published_w()`: a `publish_computed_w`
//!   run directly on the registry must not reach the filter before it is
//!   synced. At most one job is live per core; a second `install` while
//!   one is alive is an error, and the slot is released when the job
//!   drops.
//! - [`GcJob::publish`] converts the AS OF retention window to a ts floor
//!   through the `/sys/ts_clock` samples (§9.1), computes and publishes
//!   `W = max(old W, computed)` in one registry critical section (seed 21's
//!   atomicity, seed 22's monotonicity; `register_at` checks the published
//!   `W`, seed 31), then — only if `W` rose — writes `/sys/gc_w` with
//!   `Durability::Yes`, hands `W` to the KV's watermark, and only then
//!   stores it into the durable slot the filter reads. On any error the
//!   durable `W` stays unchanged (seed 42), and every GC step — the
//!   filter, `drop_tombstones`, `retire_storage` — acts on that durable
//!   slot only.
//! - [`GcJob::drop_tombstones_chunk`] (C-B1c) removes each logical key's
//!   newest-`<= W` tombstone or moved-tombstone `k@t` with `DeleteRange
//!   [version_key(k, t), end_key(k))`, start inclusive (seed 34), walking
//!   the keyspace through one registered view per chunk, at most
//!   `max_entries` stored entries at a time, ending only at a
//!   logical-key boundary (the next chunk resumes at the next key's
//!   `intent_key`). [`GcJob::drop_tombstones`] loops the chunks to the
//!   end of the keyspace. The walk covers [`gc_scope`] — data keys plus
//!   `/sys/catalog/` (§10), so dropped catalog rows lose their tombstones
//!   instead of accumulating versions forever. Intents are never touched.
//!   Each step charges the §9.3 I/O budget one unit per stored entry
//!   scanned and one per `DeleteRange` written.
//! - [`GcJob::retire_storage`] (§9.2) is gated on `durable W > retired_at`;
//!   through one registered view it plans every intent removal — the
//!   owner's status is looked up **inside the scan, while the view is
//!   open** (I-TRUNC guarantees the entry; a lookup after the view is
//!   dropped can hit the window where the async resolver removes the
//!   intent and truncates the owner, yielding a spurious fatal
//!   `Invariant`) — carrying `(key, prefix, owner, status)` per entry;
//!   `remove_intent` re-reads under the latch and no-ops if the intent is
//!   already gone. `Resolve` for a visible commit, `Discard` for an
//!   Aborted owner; any other owner is an invariant error
//!   (AccessExclusive excludes it). Only after every removal returned does
//!   it `DeleteRange` the prefix (seed 35).
//! - [`spawn_gc`] runs `publish` + `drop_tombstones`
//!   ([`GcJob::run_once`]) in a thread; an error is reported to the
//!   [`FailStop`](crate::commit::FailStop) hook and stops the thread, and
//!   so are a thread-spawn failure and a thread panic observed when the
//!   handle is dropped.
//!
//! One mutex inside the job serialises its steps, so two publishes (or a
//! publish and a tombstone pass) never interleave. Tests: `tests/gc_*.rs`
//! rebuild the G0-gc scenarios (seeds 3, 21, 22, 31, 33, 34, 35, 42, 55)
//! over `MemKv` in LSM mode, plus a seeded I-GC property; every test
//! names the mutant it kills.

mod filter;
mod job;

pub use filter::TxnGcFilter;
pub use job::{spawn_gc, GcConfig, GcHandle, GcJob, GcRunner};

/// §10's GC scope, one predicate for the compaction filter's
/// `drop_key` and the tombstone pass: data keys (everything outside
/// `/sys/`) plus the catalog (`/sys/catalog/`, the one `/sys/` family
/// §10 makes versioned table data). Every other `/sys/` key — `/sys/txn`,
/// `/sys/gc_w`, `/sys/ts_clock`, the id counters — is never dropped,
/// even when its bytes match the version-key pattern (§10).
pub(crate) fn gc_scope(key: &[u8]) -> bool {
    !key.starts_with(crate::encoding::SYS_PREFIX)
        || key.starts_with(crate::catalog::SYS_CATALOG_PREFIX)
}
