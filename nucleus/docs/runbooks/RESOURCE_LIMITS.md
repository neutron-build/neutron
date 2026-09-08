# Resource limits — what the engine bounds itself, and what it cannot

Several long-lived structures that grow through client actions have in-process
caps, configurable in `nucleus.toml` under `[limits]` (or `NUCLEUS_LIMITS_*`
env vars). This is not a complete memory bound; the unique-key gate remains
uncapped (see Remaining Work). This page maps the caps: what each bounds,
what a client sees when one bites, and what an operator should do about it.

The honest posture first: **these limits bound logical growth, not total
process memory.** Rust's ownership plus session teardown reclaim memory on
disconnect; what the caps prevent is one pathological *connected* session —
or an unauthenticated connection storm precursor — growing a map, cache, or
lock table without bound while the server stays up. A cache eviction policy
bug, an allocator that never returns pages, or a bug outside these
structures can still grow RSS. **Run an external hard cap** — a container
memory limit or `systemd MemoryMax=` — as the backstop. The in-process
limits keep the server *serving* under abuse; the external cap is what turns
a true leak into a kill-and-restart instead of a machine-wide OOM. A restart
under an external cap is also the one real test of crash recovery: WAL
replay after an OOM kill is the durability path `DURABILITY.md` documents,
and it only gets exercised if the backstop can actually fire.

## The caps

| Structure | Default | Config key | On overrun |
|---|---|---|---|
| Sessions (connections) | 100 | `server.max_connections` | `FATAL 53300` at connect, pre-auth |
| Row locks per session (one transaction) | 100,000 | `limits.max_row_locks_per_session` | `ERROR 53200`, locks held until COMMIT/ROLLBACK |
| Prepared statements per session (SQL `PREPARE` + wire Parse) | 1024 | `limits.max_prepared_statements_per_session` | `ERROR 54000` |
| Named portals per connection (wire Bind) | 1024 | `limits.max_portals_per_session` | `ERROR 54000` |
| Cursors per session (SQL `DECLARE`) | 1024 | `limits.max_cursors_per_session` | `ERROR 54000` |
| LISTEN channels per connection | 1024 | `limits.max_listen_channels_per_session` | `ERROR 54000` |
| Open large-object descriptors per connection | 1024 | `limits.max_large_objects_per_session` | `ERROR 54000` |
| Failed-auth source IPs tracked | 10,000 | `limits.max_auth_failure_entries` | oldest entry shed (no client-visible change) |

Caches evict instead of refusing — a cache entry is an optimization, invisible
to the client; eviction is not an error:

| Cache | Cap | Notes |
|---|---|---|
| Query plan cache | 1024 entries | least-accessed evicted; cleared on DDL |
| AST cache | 4096 entries | least-accessed evicted; cleared on DDL |
| Global prepared-statement cache | 4096 entries | least-accessed evicted |
| Query result cache | 1000 entries, 1 MiB/result, 64 MiB total estimated bytes, 30 s TTL | `limits.max_query_cache_bytes`; oldest-inserted eviction, invalidated on writes |
| KV cache tier | `cache.max_memory_mb` | byte-budgeted LRU+TTL |

### Query-result byte budget

Implemented and verified 2026-09-07. `cargo test --lib query_cache_budget`
passes all 10 cache/config regressions (including four isolated env cases).
The full library run passes 4826 tests, with 8 ignored. The regression suite is
`executor::cache::query_cache_budget_tests`; config coverage is
`config::tests::query_cache_budget_*`. These gates verify logical accounting,
not an RSS ceiling or long-running workload memory behavior.

```toml
[limits]
max_query_cache_bytes = 67108864 # 64 MiB, shared across sessions per executor
```

`NUCLEUS_LIMITS_MAX_QUERY_CACHE_BYTES` overrides TOML. Zero is rejected at
startup, not interpreted as unlimited or disabled, matching the other `[limits]`
keys. Embedded callers use `Executor::with_query_cache_max_bytes`; it clears
existing cached results and rejects zero with a panic. A positive budget smaller
than a result simply makes that result bypass caching; the query still succeeds.
The existing 1 MiB/result and 1000-entry caps continue to apply independently.

Accounting uses `executor/cache.rs::estimate_result_size` for both byte caps:
column-name lengths, estimated value payload sizes (JSON uses serialized text
length), and fixed per-value/row/result overheads. Each entry retains its charge;
the total is derived from those charges under the cache lock, with replacement
removed before capacity eviction. It is **not serialized storage size, measured
heap allocation, or an upper bound on RSS**. Map keys/buckets, spare capacity,
allocator overhead, and result clones returned to queries are not included.

TTL is lazy: an expired entry is a miss, but remains retained and charged until
replacement, oldest-inserted eviction, or invalidation/clear actually removes it.
There is no background result-cache TTL sweep. Clear/invalidation resets both
entry and byte gauges; a rejected stale-generation insertion changes neither.
The budget is separate from the KV cache budget and is not reserved from or
included in the buffer-pool-plus-KV startup validation against
`server.max_memory_mb`. Allow for it when sizing the external memory cap.

## Why rejection for handles, eviction for caches

A prepared statement, portal, cursor, channel, or descriptor is
**client-addressable state**: the client names it and uses it later. Silently
dropping one would turn the client's next `EXECUTE`/`FETCH`/`lo_read` into a
confusing "not found". PostgreSQL's answer is the same shape — refuse the
*creation* (`54000 program_limit_exceeded`), keep what exists. A cache entry
has no client-visible name, so eviction is free of semantics.

Row locks refuse with `53200` (`out_of_memory`) — the class PostgreSQL uses
when `max_locks_per_transaction` is spent — because the failure is a resource
exhaustion, not a conflict a retry can win, and not a program defect in the
client's SQL. If you see `53200` with `too_many_row_locks` in the message,
the fix is committing or rolling back the holding transaction (or raising the
limit for a legitimate bulk-claim workload), not retrying.

## Watching the caps before they bite

Prometheus gauges (also in `SHOW METRICS`) expose occupancy of the bounded
structures:

```
nucleus_sessions_active            # live connections (bounded by max_connections)
nucleus_row_locks_held             # FOR UPDATE rows held across all sessions
nucleus_plan_cache_entries         # plan cache occupancy (cap 1024)
nucleus_ast_cache_entries          # AST cache occupancy (cap 4096)
nucleus_query_cache_entries        # result cache occupancy (cap 1000)
nucleus_query_cache_estimated_bytes # retained result estimate, including expired entries
nucleus_prepared_cache_entries     # global prepared cache occupancy (cap 4096)
```

A gauge pinned at its cap with healthy throughput means the workload is
living on eviction — that is the cache doing its job. A gauge pinned at its
cap with `54000`/`53200` errors in client logs means a client is hitting a
handle limit — find the holder (`pg_stat_activity`-equivalent surfaces: `SHOW
SESSIONS` / `SHOW SUBSYSTEM_HEALTH`) or raise the specific limit.

## Tuning guidance

- Raise a limit only when the message names it and the workload is
  legitimate. A queue worker claiming 200k rows in one transaction is a
  batch job, not a claim pattern; split it or raise
  `max_row_locks_per_session` knowingly.
- Lower the per-session handle limits (1024s) for multi-tenant deployments
  where no client should legitimately hold hundreds of named statements.
- `max_auth_failure_entries` exists so an internet-facing port cannot grow
  the failure table via source-IP rotation; at capacity the table sheds the
  stalest entries, and a live lockout (5 failures within 30 s from one IP)
  is never the one shed — lockouts are recent by definition.
- Every `[limits]` value validates `>= 1`; `0` is rejected at startup, not
  treated as an unlimited sentinel.

## Remaining Work

### Unique-key gate cap and workload analysis

Open, source-checked 2026-09-07: `src/executor/unique_gate.rs::Held` has
`owner` and `by_session` maps without an acquisition cap. The row-lock cap does
not cover these UNIQUE/PRIMARY KEY reservations. This is a logical growth risk,
not evidence of a leak: guards release statement slots, and transaction/session
teardown releases retained slots.

**Trigger:** large multi-row INSERT/UPDATE statements or long transactions
touching many distinct unique keys, especially with multiple unique constraints
or concurrent sessions. Also measure release latency: `release` retains a
session's list using `keys.contains`, a nested membership scan whose actual cost
depends on held/new key counts; no workload complexity estimate is asserted here.

**Current mitigation:** keep bulk transactions/batches small; commit/rollback
promptly, limit connections, and retain the external RSS backstop. The default
10 s conflict-wait timeout limits waiting, not the number of held slots. Do not
disable the gate: it protects check-then-write uniqueness on the paged engines.

**Acceptance:** measure retained slot counts, estimated/allocated bytes and
acquisition/release latency for single/multi-constraint bulk writes and concurrent
long transactions on the serving buffered-disk path. Use that evidence to select
a configurable cap and explicit resource-exhaustion response. Tests must prove
boundary/re-entrant acquisition, refusal without partial reservations or duplicate
writes, preservation of previously held transaction slots, and reclamation on
statement failure, COMMIT, ROLLBACK and disconnect. Include a concurrent duplicate
key control and a bounded-churn occupancy test. This work remains separate from
the implemented row-lock cap and query-result byte budget above.

## The external backstop, concretely

```systemd
# systemd unit slice
MemoryMax=1200M
```

```yaml
# k8s deployment (deploy/k3s manifests carry the same values)
resources:
  limits:
    memory: 1200Mi
```

Size it above `server.max_memory_mb` plus a margin for the allocator and
non-budgeted structures (~20–30%); the RSS watchdog (`nucleus_memory_rss_bytes`
vs `nucleus_memory_limit_bytes`, writes refused at critical pressure when
`server.reject_writes_on_memory_critical` is on) degrades gracefully first,
and the external cap catches what nothing in-process catches.

## Disk Admission and Validation (2026-09-08)

The compatibility validation run hit two independent resource gates: physical
volume free space was 2.78% (12.8 GiB), below the unchanged 3% read-only watermark,
so write/DDL admission returned SQLSTATE `53100`; build artifacts measured
28.8 GB against the 25 GB build-size ceiling. These are recorded observations,
not a fresh free-space measurement. Memory/cache caps above do not resolve them.

Actual unchanged application migrations stopped before application DDL; live
event-store, queue and lease setup were also blocked. A setup refusal proves the
admission guard fired, not migration compatibility, concurrent lease correctness
or constraint/rollback/user-isolation behavior. See `../../DATABASE_COMPLETION.md`
for the separate transactional catalog/DO blocker and local verification bounds.

Before retrying, obtain owner-approved safe capacity remediation and remeasure
both gates. Do not lower the disk watermark, raise the build ceiling or remove
data merely to pass validation. Identify ownership and active use before proposing
any artifact removal; preserve shared data and recovery material. Rerun unchanged
write/conformance suites only after adequate headroom is established. No cleanup
or threshold change was performed as part of this documentation update.
