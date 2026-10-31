# Comparing Neutron database clients

Neutron has independently runnable PostgreSQL comparison fixtures for TypeScript,
Python and Go. They compare actual public APIs and database outcomes, rather than
mock execution. They are development tools, not a promise of feature parity or
universal performance superiority.

| Language | Compared APIs | Fixture |
| --- | --- | --- |
| TypeScript | Neutron SQL, Drizzle, Prisma and raw `pg` | [TypeScript instructions](../typescript/benchmarks/orm-project/README.md) |
| Python | Neutron native SQL/Pydantic, SQLAlchemy ORM, SQLAlchemy Core and raw asyncpg | [Python instructions](../python/benchmarks/orm-project/README.md) |
| Go | Neutron native typed SQL, GORM and raw pgx | [Go instructions](../go/benchmarks/orm-project/README.md) |

The TypeScript fixture pins Prisma 7.10.0, Drizzle 0.45.3 and pg 8.23.1. It can
evaluate published Neutron SQL 0.1.0 or an explicitly identified candidate
tarball. The Python fixture pins SQLAlchemy 2.0.54 and greenlet 3.3.2 and records
the actual interpreter, asyncpg, Pydantic and candidate SDK source. The Go
fixture pins GORM 1.31.1, its PostgreSQL driver 1.6.0 and the resolved module
graph. Python and Go evaluate checked-out candidate SDK source. Their results
do not establish registry installation or a new released version.

## What correctness checks establish

The fixtures use composite tenant keys, related projects and documents, signed
64-bit versions, exact numeric values, nullable text and binary data. Independent
native PostgreSQL queries check the values and final state. Cases include
point reads and misses, ordered keyset pages, relationships and empty parents,
create/delete, concurrent conditional updates with one winner, intentional
rollback and foreign-key failures without effects. Python and Go include native
peer association probes separately from the matched explicit two-query relation.

Neutron's Python and Go paths use native typed SQL. SQLAlchemy ORM and GORM
provide additional model/session/association features. Equal results in these
cases do not make those feature sets equivalent. SQLAlchemy Core is separately
named and is not substituted for the ORM. A successful native peer relationship
probe is also distinct from Neutron's explicit parameter-bound SQL workflow.

The current Python and Go fixture profiles require PostgreSQL 17. These
comparisons do not certify Nucleus, other databases, schema migration ownership,
RLS, application authentication or full generated-model type coverage. The
administrator account operates only within a freshly created disposable fixture;
tenant predicates are correctness controls rather than a least-privilege test.

## How to read timing results

Timed operations are warm point reads, 20-row keyset pages and parent/children
reads, with a maximum pool size of four and concurrency one and four. Correctness
must pass before timing. For the balanced read measurements, SQL logging uses
separate untimed instances; validation and report serialization occur outside
each public API latency measurement. Throughput includes dispatch and validation.
The older TypeScript diagnostic smoke includes normalization and assertions in
its timer and retains SQL auditing; interpret that diagnostic separately.
Preserve raw errors, samples,
source hashes, package/module identities, server settings and trial variation.

Python uses a four-provider Williams ordering to balance positions and adjacent
provider pairs. Go uses all six permutations of three providers. The existing
TypeScript fixture uses cyclic balanced provider positions. These designs have
different limitations; results should retain their fixture identity rather than
being merged into one cross-language leaderboard.

Python currently repeats a small fixed hot working set, whereas Go selects a
deterministic range of keys. Neutron's Python API includes Pydantic validation;
SQLAlchemy ORM includes object materialization and Session lifecycle, and Core
includes Connection lifecycle and transaction reset. Go's GORM decimal field
uses a configured exact Scanner/Valuer rather than floating-point conversion.
Go explicitly matches simple protocol across providers; it does not benchmark
GORM/pgx's default prepared-statement caching. These are application API costs
under documented configurations.

Do not treat small differences or overlapping trial ranges as an overall winner.
The read-only timing profiles do not measure cold start, memory, writes, sustained
production capacity, migrations or developer adoption effort. Write and rollback
correctness checks are not write-performance benchmarks. Run comparisons without
competing builds or benchmarks, and keep enough raw samples for any statistical
claim being made.

## Running and recovering fixtures

Follow each fixture's instructions for the exact dependency and command setup.
Install comparison dependencies privately rather than changing application or
system environments. Keep credential-bearing URLs in private configuration or
the environment, and use fresh private output directories. Each runner has a
6 GiB free-space guard and refuses output reuse.

Fixtures create randomly named databases and drop only databases they created.
Python and Go record cleanup outcomes; check those after success or failure.
For TypeScript, verify that the named fixture database was removed. A forced
process kill or machine failure can leave a fixture behind; verify its identity before
removing it. Never point cleanup at an application database.

For a quick untimed check, Python and Go support `--correctness-only`. Run these
checks before allocating a quiet measurement window. The per-language READMEs
describe package provenance, SQL-audit limits and result files.

## Improving Neutron from a comparison

Fix reproduced correctness and usability gaps first. For example, optional typed
row lookup is available as `db.sql.query_one_or_none(...)` and
`tx.sql.query_one_or_none(...)`; both return `None` for a missing row and validate
present rows through the supplied Pydantic model. The transaction method uses
the same physical connection and participates in the surrounding commit or
rollback. A live regression verifies hit/miss, session identity, validation
failure and rollback behavior.

Treat performance hypotheses such as mapper allocation or protocol overhead as
profiling candidates. A faster raw-client path does not justify removing
validation or adding a universal ORM runtime. Establish the target workload and
required semantics, profile a demonstrated bottleneck, and rerun the affected
correctness and measurement cases after a change.

## Extended application experiments

The extended profiles add transactional write timing, fresh-process startup,
process memory observations, bounded mixed-load phases and an executable
application service. Follow the language README for its exact commands and
output contract; use a different output directory from the read comparison.

Writes exercise actual provider transaction APIs and exact values. Python and
TypeScript time a create/conditional-update/read/delete transaction. Go also
measures separately committed create/update/delete operations, readback and
stale updates, with native verification between steps. Compare providers within
the same language profile; these are different transaction contracts across
languages. A completed application operation is not a SQL statement or database
transaction count.

Fresh-process startup uses the existing PostgreSQL server and warm host caches.
Record the distinction between first-result readiness and the total child
lifecycle, which can include validation, memory probes, connection closure and
process exit. This is process-cold behavior, not a cold database, filesystem,
container or machine restart. Connection initialization policies can differ.

RSS includes the language runtime, imports, pools and harness. Isolated child
experiments execute 1,000 verified reads after the first result. Python imports
all providers; Go uses a shared binary; TypeScript dynamically imports its chosen
provider. These observations do not isolate library allocation or establish a
cross-language memory ranking. Parent load-process RSS can include retained
samples, allocator history and earlier providers. Treat that as diagnostic;
separate current RSS, sampled peak and any OS process-lifetime high-water mark.
Sampling and external memory probes add overhead and can miss short peaks.

The final bounded load profile repeats 30-second closed-loop phases with four
workers and balanced provider ordering. Retain raw operation timestamps, error
categories and five-second windows. Throughput includes consumer scheduling and
validation; independent oracle queries may also consume database resources.
This is a reproducible local workload, not a maximum-capacity test or hours-long
production soak. Read/write mixtures, transaction boundaries and working sets
are recorded per language, so do not merge their rates into one leaderboard.

The adoption scenario runs a tenant-scoped document or issue service through
create, edit, stale-version conflict, keyset list, parent/children, rollback and
delete. It checks tenant separation and persisted state using a native oracle.
Inspect the provider-specific models, schema declarations, SQL, transaction code
and setup steps. Passing this small service shows that integration works for
those use cases; it is not a human usability study, productivity score, full
framework certification or automatic-migration parity. The fixture owns schema
DDL; an application must still nominate its migration authority.

The fixture's integration requirements are concrete:

| Provider | Application code and setup exercised |
| --- | --- |
| Neutron TypeScript | Table declarations, public query builders and callback transactions; explicit tenant predicates |
| Drizzle | Table declarations, a node-postgres driver and query builders/callback transactions |
| Prisma | A Prisma schema, generated client, PostgreSQL adapter and callback transactions |
| Neutron Python | Pydantic result models, parameter-bound SQL and transaction context managers |
| SQLAlchemy ORM | Declarative models, async Sessions and ORM statement/mutation APIs |
| SQLAlchemy Core | Table metadata, statement builders and async Connections/transactions |
| Neutron Go | Struct mappings, parameter-bound SQL and explicit transactions |
| GORM | Model tags, configured exact decimal Scanner/Valuer, query builders and transactions |

Raw driver baselines also require their own SQL and transaction lifecycle code.
The examples run application services directly. They do not test a particular
web framework, HTTP throughput, production authentication, or migration from an
existing customer project.
