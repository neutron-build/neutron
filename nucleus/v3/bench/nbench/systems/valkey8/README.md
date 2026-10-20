# Valkey 8 (valkey-server official image)

Serves row: `kv-memtier-valkey8`.

## Tuning and its sources

| Setting | Value | Source |
|---|---|---|
| `appendonly` | yes | Valkey docs "Persistence": AOF enabled for durability |
| `appendfsync` | always / everysec (run variants) | Valkey docs "Persistence" documents the fsync policies; the pair implements PLAN-v3 §5.2 rule 3 |
| `save` | "" (RDB snapshots disabled) | Valkey docs "Snapshotting": `save ""` disables RDB, isolating the AOF durability semantics being measured |
| `maxmemory` | 6gb | Valkey docs "Memory management" recommend an explicit limit |
| `maxmemory-policy` | noeviction | Valkey default; stated explicitly for provenance |

Variants: `aof-always` (`--appendfsync always`), `aof-everysec`
(`--appendfsync everysec`). Durability is fixed per container start and
never mixed within a series.

## Limits

`--memory 12g --cpus 16`, `--network host` (port 6379), volume
`nbench-valkey8-data`.

## Licence / benchmark-publication check

Valkey: BSD-3-Clause. No benchmark-publication restriction; see
`../../LICENSES.md`.
