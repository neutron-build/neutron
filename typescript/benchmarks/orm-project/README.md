# PostgreSQL public ORM correctness fixture

This independent fixture runs published Neutron SQL 0.1.0, Drizzle 0.45.3, Prisma/client/adapter 7.10.0 and pg 8.23.1 against identical PostgreSQL tables, indexes and seeds. `raw-pg` is an explicitly named driver baseline and independent oracle. It does not require the showcase application, native Python/Go services or Studio.

Run on Node 22.18+ with PostgreSQL CREATE DATABASE permission. Supply a private runtime directory outside the repository and a fresh output directory; secrets come from the environment:

```sh
ORM_RUNTIME_DIR=/private/owned/orm-runtime \
ORM_OUTPUT_DIR=/private/owned/run-001 \
ORM_ADMIN_URL="$PRIVATE_POSTGRES_ADMIN_URL" \
node typescript/benchmarks/orm-project/run.mjs --install
```

Omit `--install` to reuse the exact installed packages. The launcher verifies manifest and actual package versions, copies these source files to that runtime, and generates Prisma's actual client. The npm package and lock live there, never in this repository. Preserve that lock, toolchain versions, source hash and raw output for reproduction. Installation requires an allocated dependency budget (allow up to 2 GiB) and at least the 6 GiB free-space guard; the launcher checks the guard before and after install. No workspace install or engine build is needed.

Every run creates one uniquely named `v10_orm_*` database and drops it in `finally`. Use only an owned PostgreSQL control, never a production endpoint. Pool budgets are four connections per adapter. No automatic retries. Application tables are composite tenant-keyed projects/documents with an FK and matching relationship index. Tenant predicates are tested; this is not an RLS or authentication certification.

Correctness checks point hit/miss, tenant predicates, ordered keyset pages, parent/children and empty relations, create/delete, concurrent version CAS, intentional rollback and FK-failure nonmutation. Values include signed int64 boundaries, integers above JavaScript precision, exact numeric(40,18), null/empty strings and empty/arbitrary bytes. An independent pg connection reads native text/hex values and final-state hashes; all providers must agree. Decimal formatting uses exact `toFixed(18)` for Prisma Decimal, never Number. Temporal, jobs, schema rollout, migration ownership, restore and the original ten application gates remain separate.

Relationship endpoint strategy is explicit: Neutron and Drizzle use two public ORM-builder queries. Prisma uses its stable `include` API. The native Neutron/Drizzle relational loaders are probed separately and failures recorded in `capabilities.jsonl`; a passing builder endpoint does not mean those loaders passed. In the tested releases Neutron's loader rejects the composite PK metadata, and Drizzle's nested JSON projection loses int64/numeric precision and does not decode the bytea custom type correctly. The fixture does not patch packages, pretend those limitations passed or substitute hand-written SQL for ORM operations.

Raw artifacts: manifest.json; correctness.jsonl; capabilities.jsonl; operations.jsonl; smoke.jsonl; sql-audit.jsonl. Core failures return nonzero. Capability probes have separate PASS/FAIL outcomes. Smoke records 20 warmup +100 sequential point operations/provider with p50/p95/p99 and every error, timed normalization/assertion against a precomputed oracle. Fixed provider order, one small trial and instrumentation overhead make these diagnostic samples unsuitable for rankings, throughput claims or p99 comparisons. ORM SQL logger/driver query hooks capture statement audits, including checked-out pg clients; they are not a verified network roundtrip counter. Neutron event fingerprints and Prisma/Drizzle query text are different observability surfaces, disclosed in raw records. No native-value secrets should be placed in fixture inputs.


## Candidate artifacts

Set `ORM_NEUTRON_TARBALL` to an absolute path to a separately built candidate
package, and `ORM_CANDIDATE_REVISION` to its full source commit. Use a fresh
private runtime with `--install`. The manifest records the archive SHA256 and
operator-supplied source revision and clearly labels it `candidate-tarball`;
the package's unchanged version does not turn it into a published release.
Preserve the build provenance alongside the archive. A changed archive or fixture
identity requires another runtime. Published-control runs omit both variables.
The expected package version is currently 0.1.0 in both profiles.

## Read-only measurement

After the correctness run passes, use a **different fresh output directory** and
append `--measure`. This runs four balanced-order trials for each provider,
operation (point, 20-row keyset page, 20-child relationship) and concurrency
(1 and 4). Each tenant has 100 projects and 2000 documents with identical indexes,
exact native values and warmed data. The relationship strategies remain the
same two-builder-query versus Prisma-include strategies described above.

Defaults are 100 warmups and 1000 measured calls per trial/operation. Override
`ORM_TRIALS` with a multiple of four (4–20), `ORM_SAMPLES` (100–10000) and
`ORM_WARMUPS` (20–1000), recording the resulting manifest. Small runs only verify
the harness. Each provider occupies every order position equally. SQL auditing
runs on separate preflight instances; measured instances have no query logger or
driver wrapper. API latency stops before normalization/assertion, and file output
occurs after each measured phase. Calls/second includes dispatch and validation,
so it describes this consumer workload rather than database capacity.

The measurement verifies every returned result and the unchanged database state.
It retains every latency/error, per-trial p50/p95/p99, throughput, machine/server,
package/lock/source identities and cross-trial min/median/max. These are **local,
warm, read-only comparisons**, not production rankings, write performance,
statistical confidence intervals or migration/Studio measurements. Run published
and candidate profiles separately. Inspect errors and trial variation before
interpreting differences; never merge failed operations into a successful result.
