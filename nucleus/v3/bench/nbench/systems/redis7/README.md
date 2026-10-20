# Redis 7.x (redis-server official image)

Serves row: `kv-memtier-redis7`.

## Tuning and its sources

Identical guidance to `../valkey8/` (the configuration surface is the
same), from redis.io documentation:

| Setting | Value | Source |
|---|---|---|
| `appendonly` | yes | redis.io "Persistence" |
| `appendfsync` | always / everysec (run variants) | redis.io "Persistence"; the matched durability pair of PLAN-v3 §5.2 rule 3 |
| `save` | "" | redis.io "Snapshotting": `save ""` disables RDB |
| `maxmemory` | 6gb | redis.io "Memory management" |
| `maxmemory-policy` | noeviction | redis.io default, stated for provenance |
| port | 6380 | coexists with Valkey on the host network |

## Limits

`--memory 12g --cpus 16`, `--network host` (port 6380), volume
`nbench-redis7-data`.

## Licence / benchmark-publication check

Redis 7.x is dual-licensed RSALv2 / SSPLv1. Neither licence restricts
*publishing benchmark results* (they restrict offering the software as a
managed service). The trademark policy is respected by factual naming.
See `../../LICENSES.md`.
