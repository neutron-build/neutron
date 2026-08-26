# 06 — Cluster

Verified 2026-08-25 against `nucleus start --help`, `src/main.rs:812-888`, and
`deploy/README.md`. This runbook exists to state what is and is not supported, and to
explain flags operators will encounter — not to operate a cluster.

## Supported: one node

Single-node Nucleus is the supported tier: MVCC, WAL durability, and crash recovery are
real and continuously tested. Every deployment path in `deploy/` runs exactly one
process, on purpose, and `replicas: 1` in the k3s manifest is a **hard constraint, not
a default**.

## Not supported: distributed

Distributed mode is incomplete (`DATABASE_COMPLETION.md` Milestone 9) and unsupported.
Concretely, in the current tree:

- **Raft hard state is never persisted** — a cluster that loses a node loses its
  consensus state across a restart.
- **Replication ships raw SQL strings**, so `now()`, `random()`, and `nextval()`
  diverge across replicas. Scaling past one replica produces nodes that silently
  disagree with each other.
- **Replica mode refuses to start** unless `NUCLEUS_EXPERIMENTAL_REPLICATION=1` is set
  (`src/main.rs:822`), and the refusal message states why: a replica does not apply
  the records it receives, is fully writable, and serves an empty database while
  reporting that it is replicating.
- **Cluster membership authenticates the host, not the node**: node ids are
  self-asserted, so any admitted peer can speak as any node
  (`docs/RESIDUAL_RISKS.md` entry 5).
- Rolling upgrade is blocked on the same milestone plus two installable versions.

For high availability today, replicate at the storage or infrastructure layer.

## Flags you will encounter (and what they currently do)

- `--join <addr>` — join an existing cluster at that address (outbound).
- `--cluster-listen` (or `NUCLEUS_CLUSTER_LISTEN=1`) — listen on the cluster port so
  other nodes can `--join` THIS node (the seed node). Off by default: a single-node
  server opens no cluster port at all. **Naming is provisional** — do not engrave it
  in orchestration tooling (`deploy/README.md`).
- `--cluster-port` (default 5433), `--replicate-from`, `--replication-port`
  (default 5434), `--region`.
- Token guards: a non-loopback cluster transport with no `NUCLEUS_CLUSTER_TOKEN`, or a
  non-loopback replication transport with no `NUCLEUS_REPLICATION_TOKEN`, refuses to
  start (`src/main.rs:867-888`). No token configured also means inbound replicas are
  refused outright (fail-closed). Loopback is exempt for local development.
- Node-to-node TLS: `NUCLEUS_INTERNAL_TLS=1` requires `_CERT`, `_KEY`, `_CA`; mutual
  TLS in both directions. `NUCLEUS_CLUSTER_TOKEN` remains enforced as a second factor.

## If you run a multi-node experiment anyway

Set `NUCLEUS_EXPERIMENTAL_REPLICATION=1` on replicas, both tokens on non-loopback
binds, and internal TLS — and treat the resulting cluster as a disposable development
artifact. It can lose data on restart and serve divergent answers. Do not put
production traffic on it.
