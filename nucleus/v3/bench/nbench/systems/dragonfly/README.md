# Dragonfly v1.27.1 ( RESP-compatible in-memory store )

Serves row: `kv-memtier-dragonfly`.

## Licence / benchmark-publication check (§5.2 rule 9 — done first)

The pinned image itself carries no Dragonfly licence file (its label is
`NOASSERTION`; `/usr/share/common-licenses/` holds only Ubuntu's stock
licence texts, and the binary embeds no licence text — all verified
2026-11-27 from inside the image). The licence that governs the release
is therefore upstream's: releases before v2.1, including v1.27.1, are
under **BSL 1.1 with Dragonfly's Additional Use Grant** (the project's
LICENSE.md). Read 2026-11-27. Neither the BSL 1.1 text nor the grant
restricts publishing benchmark results: BSL 1.1 grants non-production
use (which a benchmark run is) and the Additional Use Grant is a
field-of-use restriction on production/commercial use, not a
DeWitt clause. Verdict: permitted — see `../../LICENSES.md`.

## Durability knobs (verified, not assumed)

The pinned release's own `--help` (read from the image, 2026-11-27; 133
flags total) exposes **no AOF-equivalent fsync control**: no
appendfsync, no per-write fsync, nothing matching the AOF pairs of the
Redis/Valkey rows. Persistence is snapshots only — `--snapshot_cron`,
`--dbfilename`, `--dir`, `--df_snapshot_format` — and the journal flags
(`server/journal/`) serve replication, not durability.

Consequences, stated per PLAN-v3 §5.2 rule 3:

- `variants` is empty and the row's sweep is pipeline-only (a fixed
  durability configuration — `sweep_kv_fixed` in `src/rows.rs`);
- the Dragonfly number is **durability-unmatched** against the
  AOF-matched Redis/Valkey pairs, and the row's report line carries that
  note verbatim (the row description in `src/rows.rs`);
- whether an unmatched Dragonfly number may carry a §5.3 label at all
  is the C-9/D report's call, not this system's.

## Base args

`--logtostderr` (the image's documented run shape — its entrypoint
prepends `dragonfly --logtostderr` itself; stating it here keeps the
provenance explicit) and `--proactor_threads=16` (the same CPU budget
the other KV systems get: 16 `--cpus`), `--port=6382`.

## Client and probe

Dragonfly is RESP-compatible, so the pinned `memtiercli` client and the
`RespPing` readiness probe are reused unchanged. The image ships
`redis-cli` 6.0.16 (Ubuntu `redis-tools`), so the in-container probe and
the digest's verification-key protocol use it exactly like the `redis7`
row — the card's fallback of driving `redis-cli` from the redis7 image
was not needed.

## Limits

`--memory 12g --cpus 16`, `--network host` (port 6382), volume
`nbench-dragonfly-data`.
