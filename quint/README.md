# Neutron Quint

This directory is the Neutron protocol-verification suite within the broader Neutron ecosystem.

Formal specifications of the stateful, distributed protocols behind Nucleus and the Neutron frameworks, written in [Quint](https://quint-lang.org/) — a modern TLA+ alternative with TypeScript-like syntax, a REPL, executable tests, and bounded model checking via Apalache.

Lean 4 and Verus contain model propositions and planned implementation linkage. Quint models the concurrent, multi-node protocols: consensus, resharding, distributed transactions, replication, and the framework state machines that coordinate them.

## What it is

Each spec is an executable state machine plus a set of named safety invariants. The active gates run scenarios and seeded finite simulation. Exhaustive bounded model checking is an optional tool capability; it has not run in this campaign:

- **`quint test`** executes hand-written `run` scenarios — concrete traces that drive the state machine through a sequence of actions and assert the invariants hold at the end.
- **Optional `quint verify`** hands the spec to Apalache, which model-checks every invariant across all reachable states of a *bounded* instance (for example, 3 nodes across 2 Raft groups, or 4 shards over 100 keys). This is exhaustive within those bounds, not a proof for arbitrary cluster sizes.

Specs are grouped by domain: shared modeling primitives (`common/`), database protocols (`nucleus/`), framework middleware state machines (`framework/`), and real-time transports (`realtime/`).

## What is modeled

### Nucleus — database protocols (`specs/nucleus/`)

| Spec | Invariants |
|------|-----------|
| `multi_raft.qnt` | `election_safety`, `log_matching` — at most one leader per group per term across independent Raft groups |
| `resharding.qnt` | `no_data_loss`, `no_double_ownership`, `key_conservation` — keys are never lost or double-owned during shard migration |
| `distributed_tx.qnt` | `commit_validity`, `atomicity`, `no_committed_abort` — 2PC commit/abort correctness |
| `replication.qnt` | `replicas_behind`, `sync_durability` — replica lag bounds and synchronous durability |
| `membership.qnt` | `non_empty`, `no_duplicate_add`, `config_monotonic` — cluster membership changes |
| `snapshot_transfer.qnt` | `source_unchanged` — snapshot install never mutates the source |

### Framework — middleware state machines (`specs/framework/`)

| Spec | Invariants | Models |
|------|-----------|--------|
| `circuit_breaker.qnt` | `valid_state`, `closed_under_threshold`, `open_has_bounded_ticks`, `half_open_bounded` | `rust/` circuit breaker |
| `rate_limiter.qnt` | `rate_enforced`, `fair_capacity`, `no_undercount`, `offset_bounded` | `rust/` rate limiter |
| `csrf_lifecycle.qnt` | `no_replay`, `session_isolation`, `expired_not_active` | `rust/` CSRF middleware |
| `session_lifecycle.qnt` | `terminal_permanent`, `renewal_bounded`, `active_has_ttl` | `rust/` session management |

### Realtime — communication protocols (`specs/realtime/`)

| Spec | Invariants | Models |
|------|-----------|--------|
| `websocket_hub.qnt` | `members_connected`, `no_self_delivery`, `no_duplicate_delivery`, `broadcast_scoped` | `go/` realtime hub |
| `hot_reload.qnt` | `version_monotonic`, `delta_ordering`, `no_version_gaps`, `disconnected_no_pending` | mobile-preview hot reload |

### Common — shared modeling primitives (`specs/common/`)

`types.qnt` (node/group/shard/key identifiers), `network.qnt` (message delivery, partitions, reordering), and `crash.qnt` (crash and recovery). These are imported by the protocol specs and by the fault-injection tests; they are not protocols themselves.

## Layout

```
quint/
  specs/
    common/       # 3 shared modules: types, network, crash
    nucleus/      # 6 database protocol specs
    framework/    # 4 middleware state-machine specs
    realtime/     # 2 real-time transport specs
  tests/          # scenario modules — `run` scenarios per protocol,
                  #   incl. fault_injection_test (partitions + crashes)
  conformance/    # spec-as-oracle scaffolding (Quint + Rust crate)
  scripts/        # check.sh, simulate.sh, ci.sh
```

`manifest.json` classifies every `.qnt` file and each scenario module. `scripts/manifest.py` rejects missing files/modules and checks actual nonzero test result counts. These counts measure executed scenarios, not state-space coverage.

## Running

Install Quint (requires Node.js), then run against any spec or test file:

```bash
npm i -g @informalsystems/quint

# Type-check a spec
quint typecheck specs/nucleus/multi_raft.qnt

# Run the `run` test scenarios for a protocol
python3 scripts/manifest.py test

# Randomized simulation — sample many traces, check an invariant
quint run --invariant election_safety --max-samples=1000 --max-steps=50 \
  specs/nucleus/multi_raft.qnt

# Bounded model check via Apalache (exhaustive within the instance bounds)
quint verify --invariant election_safety specs/nucleus/multi_raft.qnt
```

Convenience scripts drive these across the whole suite:

```bash
bash scripts/check.sh      # typecheck every nucleus spec
bash scripts/simulate.sh   # randomized simulation on the core protocols
bash scripts/ci.sh         # typecheck + simulate + conformance (cargo test)
```

## Conformance

`conformance/` is the spec-to-implementation bridge — the design goal is to run a
Quint spec as an *oracle* alongside the live engine, flagging any divergence as a
test failure (the approach MongoDB uses for replication).

What ships today:

- `conformance/conformance_test.qnt` — Quint property tests that re-check the
  headline Nucleus invariants (distributed-tx serializability, Raft election
  safety, resharding data conservation) with extra conformance-level assertions.
- `conformance/` Rust crate (`nucleus-conformance`) — a self-contained Rust
  re-implementation of the same state machines and invariant checks, runnable
  with `cargo test`.

The `quint-connect` integration that would drive the *live* Nucleus engine
directly from a spec is stubbed (its crate dependency is commented out in
`Cargo.toml`, pending a crates.io release). Until then the Rust harness mirrors
the specs rather than executing the production engine.

## Why Quint over raw TLA+

| Feature | TLA+ | Quint |
|---------|------|-------|
| Syntax | Mathematical notation | TypeScript-like |
| REPL | None | Interactive exploration |
| Model checker | TLC (explicit-state) | Apalache (SMT / bounded) |
| Test runner | External scripts | Built-in `quint test` |
| Learning curve | Weeks | Days |

## Scope and non-goals

- Bounded verification only — invariants are checked over small, fixed instances, not proved for arbitrary sizes.
- Hand-written algorithm models live in `lean4/`; unchecked executable proof models live in `verus/`. Neither establishes direct Rust verification.
- No performance modeling or benchmarking here.

## License

MIT (Quint specifications and tests). The `conformance/` Rust crate is BSL 1.1, matching the Nucleus engine. Model mirrors are scaffolding; the separately pinned WAL trace adapter requires actual execution.

## Assurance scope

`multi_raft.qnt` models durable per-term votes, log freshness, message-gated
partitions, crashes, historical leaders and full-prefix log matching. Full-state
log delivery abstracts AppendEntries retries; neither liveness nor shipping Rust
refinement is checked. Fault and conformance scenarios use that concrete quorum model; transaction
crash fixtures separately model persistent votes. Point-key conformance now records
reads-from and WW/WR/RW dependencies over shared keys and checks graph cycles,
with a conflicting serial positive case and a write-skew negative witness.
A separate finite interval history model checks a conflicting serial case and
rejects concurrent empty-range inserts and an omitted visible row. Arbitrary
SQL predicates and production range-index refinement remain excluded.

Session terminal histories, original owners, CSRF use counts, original snapshot
data, previous configuration/version values and broadcast recipient/delivery
histories are independent state. Finite safety simulation proves no eventual
progress claim; fair scheduling would require temporal verification.

The queued Raft model has immutable vote-request/response and snapshot payloads,
separate deliveries, durable term/vote/log state, and volatile candidate vote sets
cleared on crash. Network messages intentionally survive crashes and may arrive
stale; current-term/role checks discard stale replies. Snapshot installation
rejects stale terms/commit positions and incompatible committed prefixes.
AppendEntries remains a complete-prefix abstraction. The checked safety relation
maps historical concrete elections to abstract leader uniqueness; it is not a
full transition refinement or liveness proof. New source scenarios are unexecuted
until the pinned Quint job runs.

`production-trace.json` pins the public Nucleus WAL API adapter to exact source
hashes. `scripts/production_trace.py` runs append/commit/abort/checkpoint, sync and
two reopen scans against an independent literal event oracle. It then copies the
production crate and makes abort emit a commit record; the same oracle must fail
an actual executed assertion. Both legs are mandatory CI execution gates, not
results inferred from these files. Model mirrors remain scaffolding.
