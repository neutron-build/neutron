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
//!   (seed 3). `/sys/` keys are never dropped (§10). **The
//!   `/sys/catalog/` carve-out of §10 (catalog rows are versioned,
//!   GC-able data) is not implemented**: `TxnGcFilter` lives in
//!   `gc/filter.rs`, which is outside card C-T5's Touch-only scope, so
//!   the filter still keeps every `/sys/` key, including the catalog's —
//!   catalog rows accumulate versions until the filter is extended (the
//!   C-T5 escalation; see `catalog`'s module docs).
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
//! - [`GcJob::drop_tombstones`] removes each logical key's newest-`<= W`
//!   tombstone or moved-tombstone `k@t` with `DeleteRange
//!   [version_key(k, t), end_key(k))`, start inclusive (seed 34), walking
//!   the keyspace through one registered view. Intents are never touched.
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
pub use job::{spawn_gc, GcConfig, GcHandle, GcJob};
