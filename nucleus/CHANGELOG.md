# Changelog

Notable changes to the Nucleus engine. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.4] - 2026-08-03

### Fixed

- Release plumbing only, no engine changes. The v0.1.3 image is good — it was
  the first to actually run on the runtime base — but its release job stopped
  before signing because the new smoke test started the server on `0.0.0.0`
  without `NUCLEUS_ALLOW_INSECURE_CLUSTER`, so Nucleus correctly refused a
  non-loopback bind with no cluster token. The test tripped over the product
  behaving properly. Fixed, so this release is signed and attested.

## [0.1.3] - 2026-08-03

### Fixed

- **The v0.1.2 release artefacts could not run.** Linux binaries were built on
  `ubuntu-latest`, which had moved to 24.04 (glibc 2.39), while the runtime
  image is `debian:bookworm-slim` (2.36) — so the published container exited
  immediately with `GLIBC_2.38 not found`, and the standalone tarballs were
  equally unusable on Debian 12. The Linux builders are now pinned to
  ubuntu-22.04, which keeps the binaries runnable on older hosts rather than
  only fixing the container.

  Nothing in the pipeline had ever *run* the image: clippy, tests, SBOM,
  signing and provenance all passed on a container that could not start,
  because each inspects source or metadata rather than behaviour. The release
  now smoke-tests the image — runs it, starts a server, waits for a status
  probe — before signing it.

  v0.1.2 is superseded; use this release. No engine changes.

## [0.1.2] - 2026-08-03

The theme of this release is memory: an instance that had grown could exceed its
limit, refuse writes, and not be recoverable by restarting. All five defects
below were found from one production incident and are fixed together, because
each was only visible once the one before it was.

### Fixed

- **KV memory was accounted in entries, not bytes.** `Pressurable::current_usage`
  returned `dbsize() * 128` — a flat 128 bytes per entry — so a store holding
  31,992 source maps averaging 150 KB reported 4 MB while actually holding
  4.8 GB. The allocator, choosing which subsystem to reclaim from, never picked
  KV no matter how much it held. Usage is now measured from real value sizes.

- **Eviction to the cold tier never triggered.** It fired only when the entry
  count passed `max_hot_entries` (100k). At 32k large keys that never happened,
  so the disk tier sat empty while the hot tier grew without bound. Eviction is
  now driven by a byte budget as well — whichever limit is reached first — and
  spills largest-first, so a target is met by moving a few big values rather
  than thousands of small ones. Configurable with `NUCLEUS_KV_MAX_HOT_MB`
  (default 1 GiB).

- **Memory pressure could not reclaim anything.** The pressure handler only
  swept *expired* entries, so a store whose entries carry no TTL — the normal
  case for anything durable — reported pressure forever while freeing precisely
  nothing, running eviction twice a second indefinitely. It now spills to the
  cold tier, where data stays readable and a cold hit is promoted back on
  access.

- **WAL replay read the entire log into memory before parsing.** A 4.8 GB KV WAL
  cost 4.8 GB of buffer on top of the map being built, so an instance that had
  grown past its memory limit could not be restarted within that limit —
  restarting being exactly what one reaches for. Replay now streams through a
  sliding window that grows only to the largest single item. A checkpointed log
  is a *single* snapshot record containing every live key, so snapshots are
  streamed item by item; record-level streaming alone would still have buffered
  the whole file.

- **Evicted keys could be lost on a crash.** Checkpoint snapshots the hot tier
  and truncates the WAL to it, so an evicted key's last WAL record disappears at
  that moment — while `LsmTree::put` only buffers into an in-memory memtable
  until a 1000-entry threshold. Between the two, an evicted key existed in
  neither the WAL nor on disk. Latent while eviction effectively never fired;
  routine once it did. The cold tier is now flushed before snapshotting.

- **A key asked to be temporary could come back permanent.** `SET` with an expiry
  logged the value and the deadline as two records; replaying only the first
  produced a permanent key. Now written as one atomic record.

### Changed

- **SSTable values live on disk rather than in memory.** An `SSTable` held every
  key *and value* resident and loaded each file in full at open, so the "cold"
  tier only persisted data — it did not offload it. Evicting 3.9 GB left
  resident memory unchanged. Tables now keep keys with a per-key value location
  and read values from the backing file per lookup; loading skips value payloads
  while indexing, writing streams and releases resident copies, and compaction
  materialises one value at a time. Measured on a live instance: **8.34 GB →
  1.16 GB resident**, same data, same query results.

- Removed `TieredKvStore`, a complete second hot/cold KV implementation with no
  callers anywhere in the tree. `KvStore` has its own inline cold tier and is
  what the server constructs. What remains of that module is the value codec it
  always was, now named accordingly.

### Known limitations

- The cold-tier codec carries six type tags and falls back to text for anything
  else — a tradeoff shared with the KV WAL, where a `Bytea` has always returned
  as text across a restart. Eviction now refuses to move values it cannot
  represent, so it never changes a value's type as a side effect of memory
  pressure, but the underlying WAL limitation is unchanged.
- Streaming replay bounds the sliding window, not the parsed map. A store whose
  live dataset genuinely exceeds memory still cannot be opened.

## [0.1.1]

Earlier releases predate this file.
