# Python database comparison

This reproducible **candidate-source** harness compares Neutron's public native
SQL/Pydantic client, SQLAlchemy 2 ORM, SQLAlchemy 2 Core, and raw asyncpg. They are
four distinct providers. Neutron's Python client is not relabeled as a full ORM,
and Core queries are not relabeled as ORM queries.

The database is PostgreSQL 17. Every provider uses asyncpg and a maximum of four
physical connections. This is an application API comparison: Neutron includes
Pydantic validation, ORM includes object materialization and Session lifecycle,
Core includes Connection lifecycle, and raw asyncpg returns native records.
SQLAlchemy's implicit transaction and connection reset costs are included.
Sessions are never shared between concurrent tasks. SQLAlchemy's native
`selectinload` composite-key relationship is tested separately from the common
explicit two-query bounded relation. ORM and Core use real SQLAlchemy public
statement builders, mapped models and mutation APIs, not textual SQL adapters.

## Run

Use a Python 3.11+ interpreter with the candidate framework's runtime
dependencies. Do not modify a project or system environment to run this harness.
Install the two pinned extra dependencies into a **new private target directory**
and retain pip's JSON installation report with the results. Check that at least
6 GiB is available both before and after installation. `--no-deps` is intentional:
the existing interpreter supplies `typing_extensions`, which is recorded in the
environment report. Do not install or run when the guard is unmet.

```sh
mkdir -m 700 /private/tmp/neutron-python-comparison-deps
/path/to/python -m pip install --no-cache-dir --no-deps --only-binary=:all: \
  --target /private/tmp/neutron-python-comparison-deps \
  --report /private/tmp/neutron-python-install.json \
  -r python/benchmarks/orm-project/requirements.txt
chmod 600 /private/tmp/neutron-python-install.json
PYTHONPATH="/private/tmp/neutron-python-comparison-deps:$PWD/python" \
  /path/to/python python/benchmarks/orm-project/run.py \
  --admin-file /private/path/resources.json \
  --output /private/path/python-comparison-first-run
```

The administrator file must be a private regular file (0600), containing a
`postgres_admin_url` key. `--admin-key` selects a different JSON key. Never put a
credential-bearing URL in a shell command. The file's database account must be
able to create/drop a temporary database and terminate its own database sessions.
The comparison uses that same account inside its fixture; it **does not certify
RLS, least privilege or application permission isolation**.

Use `--correctness-only` to run correctness and SQL audit without any timed
operations. The report records zero timed calls and measurement as not run.

The output parent must exist. Each run creates a new private output directory
and refuses reuse. It creates only a randomized `neutron_compare_py_<hex>`
database, writes no migrations to an existing application database, and drops
its owned database on normal success or a handled failure. `cleanup.json` records
the actual cleanup outcome. A forced process kill can leave the named fixture;
inspect its identity before removing it. Credentials are not included in normal
result files or terminal output. A failure returns a nonzero status rather than
automatically retrying writes.

## Correctness before timing

The schema matches the TypeScript comparison's tables: composite tenant keys,
BIGINT versions, NUMERIC(40,18), nullable text, arbitrary BYTEA, composite foreign
key and relation index. Two tenants use the same IDs. Boundary values include
int64 min/max and a value above JavaScript's exact-number limit, a 40-digit decimal,
NULL versus empty text, all 256 byte values and empty bytes.

A separate native asyncpg connection obtains server-cast decimal/integer text
and hex bytes. Every provider must preserve the actual native Python types and
match this oracle. The harness checks point hit/miss, tenant isolation in query
predicates, keyset reads, relationships and empty/missing relationships,
create/delete, rollback, PostgreSQL foreign-key SQLSTATE 23503 without effects,
and **two concurrent same-version CAS calls with exactly one winner**. Final
database snapshots must equal the initial snapshot. This is query predicate
correctness, not an RLS or authorization test.

SQL audit uses separate provider instances and checks application SELECT counts
(point/page: one; relation: two). Callbacks may expose driver reset SQL and omit
transaction protocol commands. They are **not a wire-message or round-trip
counter**, and no callback is attached to a timed provider.

## Measurement and limits

Default measurement runs four Williams-order trials balancing provider positions
and all twelve directed adjacent predecessor pairs, 100 measured calls per
phase, 20 warmups, concurrency 1 and 4, and point/20-row keyset/20-child relationship
reads over 4,000 documents. This is 9,600 timed operations. More samples can be
requested; trial counts must be positive multiples of four. Every result is
validated outside its individual operation timer; the full native snapshot must
remain unchanged. Raw nanosecond samples and per-trial p50/p95/p99 are retained.
The whole-phase throughput includes dispatch, assertions and evidence writing;
it is not pure database throughput. Four local trials do not establish statistical
significance or performance superiority.

The timed queries use a **fixed hot working set**: tenant `a`, document17,
keyset after20, and project3 with20children, repeated throughout each phase.
This intentionally measures repeated-query behavior, not a varied or random
application working set. Other language harnesses can use different access
patterns; those results must not be compared as language performance rankings.

Reports record source/SDK hashes and import origin, dependency/runtime versions,
server configuration, fixture identity and cleanup. This harness compares a
source candidate, not a published wheel. Dependencies were selected as explicit
reproducible pins, not advertised as the latest releases. Behavior follows the
[SQLAlchemy asyncio documentation](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html)
and [asyncpg dialect documentation](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#asyncpg).

It does not measure cold startup, sustained writes, memory, large-result
streaming, migrations, ORM change tracking parity, retries/cancellation, network
faults, Nucleus, other databases or compatibility with historical framework
artifacts. Do not rank Python against TypeScript/Go results. Do not infer that
Neutron replaces all SQLAlchemy features from these finite checks.

## Extended local experiment

`extended.py` reuses these genuine providers and owned PostgreSQL17 fixtures.
Set the same candidate-source `PYTHONPATH` and private pinned dependency path
as above, then run:

```sh
python extended.py --admin-file /private/resources.local.json --output /private/new-output
```

Outputs refuse reuse. The default runs four Williams-order trials,100 sequential
transactional CRUD samples and30 seconds of closed-loop c4 mixed load per
provider/trial (three point reads then one transaction per worker). Each write
creates a document, guarded-updates it, reads its exact native values and deletes
it within one transaction. Every transaction result and final fixture is checked
outside the API latency timer by an independent asyncpg oracle. This is a bounded
local soak, not hours-long production capacity, throughput at a fixed arrival
rate or a contention benchmark. Consumer throughput includes oracle checks,
dispatch and memory-sampler overhead.

Fresh subprocess trials record whole process launch/import/connect/first-read/
close wall time, parent-observed first-result handoff, connection initialization and first-read latency, with warm OS
and database caches. Each child imports the shared all-provider modules, so this
is not a clean minimal-provider startup or package-footprint comparison.
`ps` current RSS and `getrusage` OS high-water RSS are process measures, not heap
allocation or retained model sizes. Sustained parent RSS retains allocator/import
history across phases; raw arrays are saved and discarded per phase. Do not rank
provider memory from that shared process. One-second sampling misses short peaks;
OS high-water captures process history but cannot attribute it to one provider.
Each fresh child separately completes1000 validated point reads and reports current
RSS before/after and OS high-water; it has no prior-provider/sample-array history,
but still imports the shared all-provider modules. The total lifecycle includes
this1000-read memory probe and is not startup time. First-result handoff precedes it.

`DocumentService` is an executable tiny tenant-scoped application slice: create,
edit with optimistic conflict, keyset list, project detail and explicit abandoned
write. Its scenario proves same-ID second-tenant isolation and exact persisted
state. This is implementation acceptance, not a subjective usability study,
external-user adoption, schema-migration parity or a production app. Provider
setup uses four aligned pooled connections; Neutron needs Pydantic models and
explicit SQL/transaction calls, SQLAlchemy ORM needs mapped models/session
configuration, Core uses SQLAlchemy tables/builders/engine transactions, and
rawasyncpg uses explicit SQL/pool/transaction calls. See `providers.py`,
`extended_providers.py` and `DocumentService` for the actual required code.
One harness-owned DDL authority creates the fixture; no provider runs migrations.
`--correctness-only` runs the acceptance scenarios without performance phases.
