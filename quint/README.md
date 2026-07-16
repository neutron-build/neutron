# Neutron Quint

This directory is the Neutron protocol-verification suite within the broader Neutron ecosystem.

Formal specifications of the stateful, distributed protocols behind Nucleus and the Neutron frameworks, written in [Quint](https://quint-lang.org/) — a modern TLA+ alternative with TypeScript-like syntax, a REPL, executable tests, and bounded model checking via Apalache.

Where Lean 4 proves single-node algorithms and Verus verifies Rust code directly, Quint covers the concurrent, multi-node protocols: consensus, resharding, distributed transactions, replication, and the framework state machines that coordinate them.

## What it is

Each spec is an executable state machine plus a set of named safety invariants. Two levels of checking run against it:

- **`quint test`** executes hand-written `run` scenarios — concrete traces that drive the state machine through a sequence of actions and assert the invariants hold at the end.
- **`quint verify`** hands the spec to Apalache, which model-checks every invariant across all reachable states of a *bounded* instance (for example, 3 nodes across 2 Raft groups, or 4 shards over 100 keys). This is exhaustive within those bounds, not a proof for arbitrary cluster sizes.

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
  tests/          # 14 test modules — `run` scenarios per protocol,
                  #   incl. fault_injection_test (partitions + crashes)
  conformance/    # spec-as-oracle scaffolding (Quint + Rust crate)
  scripts/        # check.sh, simulate.sh, ci.sh
```

15 `.qnt` specification files, 14 `.qnt` test modules (~130 `run` scenarios), plus the conformance harness.

## Running

Install Quint (requires Node.js), then run against any spec or test file:

```bash
npm i -g @informalsystems/quint

# Type-check a spec
quint typecheck specs/nucleus/multi_raft.qnt

# Run the `run` test scenarios for a protocol
quint test tests/multi_raft_test.qnt

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
- Single-node algorithm correctness lives in `lean4/`; direct Rust-code verification lives in `verus/`.
- No performance modeling or benchmarking here.

## License

MIT (Quint specifications and tests). The `conformance/` Rust crate is BSL 1.1, matching the Nucleus engine it verifies.
