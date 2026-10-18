# postgres 17 (official image)

Serves rows: `oltp-pgbench-pg17`, `oltp-sysbench-pg17`.

## Tuning and its sources

All settings follow the PostgreSQL documentation's production guidance;
nothing beyond vendor-documented knobs is applied:

| Setting | Value | Source |
|---|---|---|
| `shared_buffers` | 6 GB | PostgreSQL docs, §20.4 "shared_buffers ... 25% of machine RAM is a common value" (host: 38 GiB) |
| `max_wal_size` | 8 GB | PostgreSQL docs, §30.5: raise to reduce checkpoint frequency |
| `checkpoint_timeout` | 15 min | PostgreSQL docs, §30.5 |
| `synchronous_commit` | on/off per run variant | PostgreSQL docs, §20.5; the two variants implement the matched durability pair of PLAN-v3 §5.2 rule 3 |
| everything else | image defaults | — |

Variants (start-time args, never mid-run edits):
- `sync-on` — `-c synchronous_commit=on`
- `sync-off` — `-c synchronous_commit=off`

## Limits (recorded into every results file)

`--memory 24g --cpus 16`, `--network host`, volume `nbench-pg17-data`.

## Licence / benchmark-publication check

PostgreSQL Licence (BSD-style, permissive). No clause restricts
publishing benchmarks; see `../../LICENSES.md`.
