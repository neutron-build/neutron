# Go database comparison

This independently runnable comparison exercises Neutron's native typed SQL
client, GORM's public model/query/transaction APIs, and a named raw-pgx baseline.
It is not a claim that Neutron is a feature-equivalent Go ORM or universally
faster. It uses PostgreSQL 17, not Nucleus.

The nested Go module pins GORM and its PostgreSQL driver without changing the
production SDK module. Its local `replace` deliberately evaluates the checked-out
candidate SDK; it does not prove a published Go module can be installed.

## Run

Start with at least **6 GiB** free disk space. Build/download work must also obey
this guard; the executable checks free space before creating a fixture. Provide
a PostgreSQL administrator URL through `ORM_ADMIN_URL`, never a command argument
or committed file. The user must have permission to create and drop databases.

```sh
cd go/benchmarks/orm-project
go mod download
go build -o /absolute/private/path/go-comparison .
/absolute/private/path/go-comparison \
  --out /absolute/private/path/fresh-output \
  --samples 100 --warmups 100 --trials 6
```

The output must be an absolute, new directory with an existing parent. The
runner makes it mode 0700 and `report.json` mode 0600. It only drops its own
cryptographically random database. Existing output is refused before connection
or mutation. Ordinary errors and context timeouts attempt cleanup. A process
kill or machine failure can leave that uniquely named fixture behind; the report
records its name once created. It never writes the administrator URL or password.
Do not run comparative timing while another benchmark or build is using the
machine. Inspect `owned_database_removed` even when a run fails.
Use `--correctness-only` to run the live correctness, untimed SQL audit and
native association probes before scheduling an exclusive measurement window.

## Compared contract

All three providers use pgx with maximum four connections, one-hour connection
lifetime, five-minute idle timeout, and simple protocol. Neutron's current public
SQL methods force simple protocol; the other providers are explicitly configured
to match. This does **not** measure pgx/GORM's default implicit prepared caches.
GORM uses `database/sql`; Neutron and raw pgx use `pgxpool`, so the pool
implementations differ. The report includes their settings.

Each provider reads the same tables: two tenants with matching composite IDs,
202 projects and 4,000 documents, exact `NUMERIC(40,18)`, signed-int64 extrema and
values above JavaScript's safe-number range, nullable versus empty strings, NULL
versus empty bytes, and all 256 byte values. GORM's decimal field uses a configured
`database/sql.Scanner`/`driver.Valuer` mapping to exact decimal text; this is not an
out-of-the-box float mapping. Neutron's decimal uses a string-kind field. The
independent oracle reads native `amount::text`, `version::text` and hex bytes.
Before accepting that oracle, the runner verifies native fixture cardinalities
and explicit signed-int64/decimal/NULL/empty/arbitrary-byte anchor values.

Correctness covers point/miss, keyset boundaries, tenant-qualified composite
relationships, empty/missing parents, create/read/delete, explicit rollback,
foreign-key failure without effects, and two concurrently released CAS updates
with exactly one winner, the correct stored winning value, and zero-row stale
CAS. All correctness mutations use explicit transactions in every provider.
GORM's default automatic write transactions remain enabled. Writes are not timed.
Native preloading is separately probed with GORM's real composite association
mapping; its result is retained even if it fails.

Timed reads are a single document, a 20-row keyset page and a parent with its
20 children. Relationships intentionally use two sequential queries through
each provider's public API. GORM uses its builder rather than raw SQL. Native
GORM `Preload` is a separate capability probe, not silently substituted with the
manual strategy. An untimed audit verifies one, one and two SQL statements (one
for a missing parent); it is not a network packet counter.

## Results and limits

Six trials use every permutation of the three providers. Each provider therefore
occupies each position twice, with both directional orderings represented. Every
phase warms up verified calls and records at least 100 measured calls at
concurrency one and four. `--trials` must be a multiple of six; `--samples` can
increase the bounded default. Warmups and SQL audit use no recorded latency.

Per-call API latency includes query execution and row materialization, then stops
before oracle comparison. Report serialization occurs after timing. Phase
throughput also includes dispatch, post-call validation and goroutine scheduling.
Every measured result is checked against the preloaded native oracle. Native
database digests must be unchanged after correctness and after measurement.
Those ordered digests include both project and document rows. Stale CAS also
must leave the native winning row unchanged before it is deleted for cleanup.
Raw indexed latency samples, all error categories, nearest-rank p50/p95/p99,
throughput, fixture DDL hash, candidate-source hashes, module build information,
Go/runtime identity, PostgreSQL settings and revision are preserved in the report.

These are warm local reads on one PostgreSQL fixture. The runner does not establish
cold-start performance, memory use, sustained capacity, write throughput,
production reliability, schema-migration parity, multiple-database support or
cross-language performance rankings. A passing matched relation does not give
Neutron an association builder. Native GORM association probes are explicitly
separate from the overall matched-workload result.

GORM configuration follows its official documentation:
[PostgreSQL and pooling](https://gorm.io/docs/connecting_to_the_database.html),
[Scanner/Valuer custom types](https://gorm.io/docs/data_types.html), and
[native preloading](https://gorm.io/docs/preload.html).

## Extended evaluation

`--extended --duration-seconds 30` adds genuine committed create, optimistic CAS,
read, stale CAS and delete calls, with a native pgx oracle after each operation.
All providers retain their explicit transaction APIs; each write commits
independently, rather than silently replacing the scenario with one batch. API
latency excludes the oracle; consumer time and phase throughput include it.
Each phase first verifies five untimed warmup lifecycles per worker.
Six provider permutations balance order, with write concurrency one/four and
30-second closed-loop mixed page/read/write phases at concurrency four. Raw
per-call timestamps and 5-second windows are retained. This is a bounded soak,
not hours of production reliability, fixed-rate overload or capacity testing.

Fresh child processes open only the selected provider, execute a first exact
read and 1,000 verified page reads. Process spawn-to-first-result includes a
baseline `ps` probe; connect-to-first-query is separately recorded. OS executable
cache and PostgreSQL are warm. This does not measure machine/server cold boot,
build time, dependency installation or per-package import cost: one Go binary
contains all three providers. RSS is sampled every 100 ms, and can miss peaks.
Child baseline and observed peak describe the whole process, not library-only
allocation or PostgreSQL memory. Parent phase RSS includes the orchestrator and
retained raw samples; it is not a comparative provider memory footprint.

`adoption.go` is a tenant-scoped issue service exercised against each provider:
create/edit/list/project relationship, stale edit conflict, second-tenant
isolation, discarded-draft rollback and delete. This is executable application
integration evidence, not a user study or a subjective usability ranking.
Neutron/raw pgx require explicit SQL and row mappings; GORM uses the existing
models, exact Decimal Scanner/Valuer and builder/transaction configuration.
Schema ownership remains harness-native SQL; no AutoMigrate or equivalence to a
production migration workflow is claimed. `--extended --correctness-only`
executes correctness and the adoption scenario without comparative timing.

For a diagnostic smoke only, `--extended --diagnostic-one-trial --trials 1
--duration-seconds 10` executes one unbalanced ordering. It cannot support a
comparative ranking; retain the normal six-trial30-second run for evaluation.
