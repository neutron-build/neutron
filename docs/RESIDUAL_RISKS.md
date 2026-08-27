# Residual risks

What Nucleus and the Neutron SDKs do **not** yet do well, stated plainly and
kept separate from the release notes on purpose. Release notes describe what
changed; this file describes what has not been hardened, so that reading only
the good news is not possible.

Every entry names how it was established. Where a limit is enforced by a test,
that test is named — several of these are *characterization* tests, which pass
today by asserting the current bad behaviour and fail the moment it improves.
That is deliberate: an unfixed limitation with a test attached cannot quietly
become folklore.

Last verified: 2026-08-26, against HEAD after the soak-flake B-tree fix
(task-plan Batch 1; probes 43/43 across three consecutive full runs) — with
the standing caveat that CI counts are re-verified when the workflows run the
tree.

---

## 1. A transaction spanning SQL and a specialty model is not atomic — outside streams

**Status: implemented for streams (2026-08-21); designed, not implemented, for
the other twelve models.**

Nucleus has fourteen data models. SQL writes go through the page WAL with a
commit record. The other thirteen each own an append log.

For **streams, KV strings, documents, graph, timeseries, datalog, blob, and
vector** (vector since 2026-08-26: an HNSW-indexed VECTOR column's row writes
commit or roll back with the SQL row, crash-probed both directions),
the fix is implemented end to end: every enlisted write is tagged with the
coordinating transaction id, the commit record is CRC-covered on both WAL
backends and survives compaction, specialty checkpoints are ordered before
the SQL checkpoint with a retention pin, and recovery discards tagged
records whose transaction never committed — absence of a commit record means
discard, so there is no in-doubt state and no operator call. A crash anywhere
between the tagged append and the commit record leaves both writes or
neither, and rollback retracts what the transaction appended. Pinned by
`probe_crossmodel_commit_order` and `probe_crossmodel_atomicity`.

**Columnar and collections-KV** carry the full tagged plumbing but their
in-transaction writes are refused outright (no rollback before-image yet),
so no uncommitted record can be produced through SQL — atomic by refusal,
not by mechanism, until a write-set design lands.

The remaining models — FTS (design-never: the index snapshot beats the WAL
at startup) and geo (writer-less — geo persists as SQL columns and its WAL
receives no writes) — still append with no notion of the transaction that
produced a record. CDC is **decided, not pending**: events fire at statement time and
never enlist — fire-and-forget is the contract (2026-08-26; the NU-107
product call resolved to keeping it), so a CDC consumer sees events for
writes that a concurrent crash or rollback may then undo.

**If this matters to you:** keep cross-model writes idempotent, or confine a
transaction to SQL and the seven atomic specialty surfaces above.

## 2. Two index paths read the whole table and then narrow the answer

**Status: correct answers, wrong cost. Pinned by characterization tests.**

- **Vector similarity (HNSW).** A top-k similarity query still reads every row
  in the table; the index reorders a completed scan. Pinned by
  `test_similarity_search_still_reads_every_row`, whose message says to
  delete it and assert the real bound once the path is fixed.
- **JSONB containment (GIN).** Same shape: the index filters positions out of a
  full scan rather than replacing it. Pinned by
  `test_gin_containment_still_reads_every_row`.

Range scans, point lookups, ordered scans and FTS lookups do reach their
indexes, and each is asserted by scan count rather than by rows returned —
because a query that loses its index gets slower and never wrong, so no
row-level assertion can catch it.

**If this matters to you:** vector and JSONB-containment query time grows with
the table, not with the result.

## 3. JSONB, ARRAY and VECTOR values have no ordering

`Value::cmp` has no arm for these three, so any two JSONB documents — or two
arrays, or two vectors — compare as equal to each other regardless of contents.

Reachability was measured rather than assumed, and it is narrower than it
sounds: `SELECT DISTINCT` and `GROUP BY` over a JSONB column return the right
number of groups, because those paths do not use this comparison.
`ORDER BY` does use it, and returns every row while placing them in no
meaningful order. Pinned by
`test_distinct_does_not_collapse_composite_values`.

**If this matters to you:** do not `ORDER BY` a JSONB, ARRAY or VECTOR column
and expect a stable or meaningful sequence.

## 4. Startup rebuilds every B-tree index, and the cost is linear

Index structures are not persisted; they are rebuilt from the table at startup.
Measured on a development machine: **0.44s at 10,000 rows and 4.91s at
100,000** — roughly 49 microseconds per row, so a one-million-row table with
three indexes is about two and a half minutes before the database is ready.

The O(1) alternative — persist each index's root page id and reopen — is not
done because a root page **moves** when the root splits, so a naively persisted
id can name a page that is no longer a root, and reopening there returns wrong
answers rather than slow ones. Rebuilding is the option that cannot be subtly
wrong.

**If this matters to you:** budget startup time proportional to your largest
indexed table.

## 5. Cluster membership authenticates the host, not the node

Node-to-node links are mutually authenticated with TLS: a peer without a
CA-signed certificate cannot complete the handshake in either direction.

That establishes **admission** — this host belongs to the cluster. It does not
establish **identity**: the node id in an envelope and in a join request is
self-asserted, so any admitted peer can speak as any node id, and vote as it.

Binding a claimed node id to its certificate needs a per-node certificate
subject, and the configuration currently expresses a single cluster-wide server
name. That is a configuration design decision, not a code change.

**If this matters to you:** every host you admit to a Nucleus cluster is trusted
as every node in it.

## 6. `RETENTION_SET` accepts a policy that nothing enforces

**Status: decided 2026-08-26 — permanent warn-only through 1.0.** Enforcement
is not planned and the function will not be removed: pre-1.0 callers correctly
read the absence of an error as acceptance, and deleting it would break them.

The function parses, validates and registers a retention policy, and no
component ever acts on it. It now emits a warning saying so, and it is
documented here rather than removed, because removing it would break callers
who — correctly — read the absence of an error as acceptance.

**If this matters to you:** retention is not implemented. Delete old data
yourself.

## 7. The benchmark numbers in `docs/benchmarks/` are not trustworthy

They were measured against the in-memory MVCC engine rather than the storage
engine the server runs, and failed operations were timed as successes — an
engine returning errors quickly can look like a fast engine. They also
contradict `BENCH_VS_POSTGRES.md` by roughly 49x on INSERT.

They are kept for the methodology, not the figures. Do not quote a number from
that directory.

## 8. Whole categories of performance are unmeasured against competitors

There is no published comparison of:

- **full-text search** against Elasticsearch
- **OLAP** aggregation against ClickHouse (blocked locally on an operator-level
  Gatekeeper quarantine of the installed binary, not on the code)

Graph traversal **has** been measured against Neo4j 5.26
(`nucleus/docs/BENCH_VS_NEO4J.md`, 2026-08-20) and vector search against
Qdrant 1.19 with pgvector as an in-run control (`BENCH_VS_QDRANT.md`,
2026-08-20) — both on a single development machine, and the Qdrant run under
stated resource handicaps (4-vCPU VM against a 10-core host, REST/JSON rather
than gRPC). Treat those two as measured-once-with-caveats, not as defensible
numbers. Vector search is also measured against pgvector, and SQL against
PostgreSQL, SQLite, SurrealDB, CockroachDB, TiDB, MongoDB and Redis.

**If this matters to you:** treat every specialist-workload comparison other
than graph and vector as unproven until one exists; treat the graph and vector
ones as single-machine indications, and run the harness on your own hardware
before quoting either.

## 9. Scale beyond a development machine has never been run

The 1M-to-100M row, sustained-concurrency, p50/p95/p99 workload has harnesses
and has never been executed on hardware that could produce a defensible number.
A laptop cannot: there is no `tc`/netem, no cgroups v2, no `dm-flakey`, and the
soak probe's memory gate is a silent no-op on macOS.

Relatedly, the Python framework benchmark was run here three times and the
result was **negative and is published as such**: on this machine class the
worst single-repeat throughput deviation on a green run is 95.4%, and one repeat
recorded zero requests per second with zero errors. A 10x regression is a 90%
drop, so no threshold can both survive a green run and catch a real regression.
**Decided 2026-08-26: no Nucleus-engine performance gate is wired on this
hardware class** — the numbers above are why. The TypeScript SDK's own
benchmark gate stands; it runs on stable-enough hardware to mean something.

## 10. "Formally verified" means less than it sounds like

The Lean 4 development has **92 theorems, 3 axioms and zero `sorry`s**, and it
proves properties of **hand-written simplified models** of MVCC, the B-tree, the
WAL and Raft. Those models are not the shipping engine, and no extraction or
refinement connects them to it.

Three axioms that earlier versions carried turned out to be **false**, not
merely unproven, and were discharged by fixing the statements.

**If this matters to you:** the proofs are evidence that the designs are
coherent. They are not evidence that the binary implements them.

## 11. An SDK that compiles and passes its own tests may still be broken

Until 2026-08-20 the Zig SDK had 320 passing unit tests and had never spoken to
a live engine. Its first live scoring found its pgwire client desyncing on
**110 of 200 queries**: it read a response with a single read and left the tail
in the socket whenever the server split a message, so the next query decoded the
previous one's bytes.

All seven SDKs are now scored against a live engine on every change, and the
cross-SDK diff refuses to count a missing SDK as agreement. The lesson is kept
here because it applies to any client library, including ones not in this repo:
a green unit suite over a mocked transport proves the mock.

## 12. `mobile-preview/` is an experiment

It has no tests and no CI workflow. It is not a supported pillar, and nothing
public claims it is.

## 13. An open enlisted transaction pins the WAL, and nothing sweeps it by default

Making streams transactions crash-safe (row 1) has a cost: while any enlisted
transaction is open, WAL truncation is held so its commit record cannot be
pruned. The idle-in-transaction sweep that would bound this is off by default
(`idle_in_transaction_timeout_secs = 0`), so one forgotten `BEGIN` plus one
XADD grows the WAL without bound.

Related known gap: `STREAM_XACK` used to discard its WAL error (acknowledged
work could be lost on a crash); since 2026-08-23 it records the PEL owner
pre-ack and fails the statement when the WAL append fails, like XADD.

**If this matters to you:** set `idle_in_transaction_timeout_secs`, or close
enlisted transactions promptly.

## 14. There are three disjoint pub/sub fabrics, and they do not bridge

`NOTIFY` (and pgwire `LISTEN`) uses the async notification hub;
`PUBSUB_PUBLISH` and the embedded API use the synchronous pubsub registry; and
the RESP `PUBLISH`/`SUBSCRIBE` surface uses a third, separate subscriber
registry. A message published on one fabric never reaches subscribers on
another — a client subscribed via RESP cannot hear a `NOTIFY` the SQL side
sends, and vice versa.

**Decided 2026-08-26: documented, not unified.** Unifying them is a
post-1.0 redesign (one delivery fabric with three protocol front-ends), not a
patch; hiding the seams now would produce half-bridged semantics that are
worse than honestly separate ones. Related: the cluster router's outbox
(`drain_outbox`) has no production drainer — it exists for the distributed
programme and is inert in a single-node deployment.

**If this matters to you:** pick one pub/sub fabric per integration and stay
on it; do not assume `NOTIFY` reaches RESP subscribers.

## 15. Write conflicts under high contention kill and retry by design

Nucleus resolves write-write conflicts wait-die: a younger transaction that
would block on an older one's lock is killed and must be retried by the
client. Under adversarial contention this produces a retry storm — the
correctness-safe choice at the cost of throughput. **Decided 2026-08-26:
documented behavior**, with a throughput lane (wound-wait or queueing)
parked post-1.0.

**If this matters to you:** batch contended writes, or expect retriable
errors (and retry them) on hot keys.

## 16. `synchronous_commit = off` loses committed writes on a hard stop

That is the documented contract of `off`, not a bug: durability is deferred
to the next flush/checkpoint, so a `kill -9` or OS crash inside that window
loses recently-acknowledged writes. At the default `on`, 43 controlled trials
across TERM/INT/KILL, quiescent and under a live writer, lost zero
acknowledged rows; the one filed loss report reproduced only under `off`,
and its proposed root cause was disproved.

**If this matters to you:** keep `synchronous_commit` at its default `on`
(or set it per-session only where the loss window is acceptable).

---

## How to read the absence of an entry

This register covers the areas that have been audited. It is not a proof that
everything else is hardened. The durable statement is narrower and more useful:
every item above is either enforced by a test that fails when the behaviour
changes, or measured with the measurement recorded.
