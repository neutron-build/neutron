The TypeScript SQL client (`@neutron-build/sql`) and the Go ORM core accept an
explicitly selected, **uncertified** finite Nucleus profile named
`nucleus-relational-rc-v1-candidate`, mirroring the Python entry point described
in `PYTHON_ADMISSION.md`. Nothing here is qualified: no native run, engine build
or fixture execution has happened, packages stay disabled and the historical
assessment in `profile.json` is unchanged.

## TypeScript

`createDatabase({ driver | url, profile, tables })`. Without `profile` nothing
changes. `profile: "postgres-direct"` admits a reported PostgreSQL identity only
(Nucleus, CockroachDB, Yugabyte and others are refused). The Nucleus candidate
requires `current_setting('server_version')` to report `16.0 (Nucleus)` and
`version()` to report `PostgreSQL 16.0 (Nucleus 1.2.0 — The Definitive
Database)`, publishes a frozen `db.endpointIdentity` (capabilities `point-crud`,
`read-committed-transaction`, `savepoint`; `packageEnabled` false) and replaces
`db.driver` with a guarded adapter. Registered `tables` must be schema-qualified
physical tables whose every column is integer, bigint, boolean, text, jsonb or
timestamptz (no serial, varchar, arrays, enums, custom codecs or generated
columns); this is checked for the whole table, projected or not, before any
statement. The guard admits only the builders' generated single-row point
`select`/`insert`/`update`/`delete` over those tables, one equality predicate,
integer/bigint/boolean/string/null parameters and UTC timestamptz text. Raw SQL,
comments, extra statements, joins, `limit`/`order by`, relational and nested
queries, aggregates, cursors (refused by capability before BEGIN), prepared
statements, batches with the default REPEATABLE READ, and any BEGIN other than
the default or READ COMMITTED are refused before dispatch. The only
capability that can resolve is `jsonb-functions` (the timestamptz wire read),
through its registered live probe.

## Go

`orm.AdmitPostgresDirect(ctx, db)` checks a reported PostgreSQL identity.
`orm.AdmitNucleusFinite(ctx, db, tables...)` checks the exact candidate identity
through the supplied executor and returns a `*FiniteExecutor`, an `Executor`
that is a drop-in for every existing orm function. `orm.NewFiniteTable` accepts
models whose fields are int32, int64, bool, string, `time.Time` or `JSON`
(nullable pointers allowed) on a schema-qualified table; `int`, floats,
`Decimal`, `UUID`, `Bytea`, temporal helpers, enums and named user types are
refused. The guard admits only generated point `SELECT`/`INSERT`/`UPDATE`/
`DELETE` with one `"col" = $n` predicate and `int32`/`int64`/bool/string/nil/
`JSON`/UTC `time.Time` arguments. LIMIT and ORDER BY are outside the grammar, so
`SelectOne`, `Stream` and keyset paging are refused. Owned transactions go
through `FiniteExecutor.WithTransaction` (admitted pool only; default or READ
COMMITTED, read-write, not deferrable) and `FiniteScope.Savepoint`.

## Known gaps

- TypeScript identity needs two fixed read-only queries (`current_setting` and
  `version()`) before admission because the adapter interface does not expose
  the startup parameter report; Python reads it from the connection. A
  `jsonb-functions` probe can also run before a shape refusal on timestamptz
  tables. Neither executes caller SQL.
- TypeScript `db.driver.pin()` and Go raw `*Scope`/pool values obtained outside
  the finite wrappers bypass the guard. The guards are finite operation
  contracts over generated SQL, not security boundaries.
- Go cannot see database column types, only model field types; the native
  qualifier is the only evidence the columns are the assumed families.
- TypeScript default (no `profile`) still recognizes Nucleus lazily and runs
  without admission; Python's default rejects Nucleus. Changing the TypeScript
  default would break existing recorded flows and was not done.
- Existing Go `WithTransaction` and `Scope` internals are unchanged; guarding
  them requires the wrappers above.

## Native qualifiers (authored, not run)

`ts_admission_native.mjs` (Node) and `go-admission-native/main.go` (Go) provision
isolated schemas with raw `pg`/`pgx`, run point CRUD with SQL NULL versus JSON
null, UTC microseconds, int4/int8, a caught nested savepoint failure, outer
rollback and the refusal list against a PostgreSQL control and the Nucleus
candidate (both bundled TypeScript adapters), compare native checkpoints, drop
the schemas and fail the run when cleanup fails. Each requires a fresh installed
package root (TypeScript) or fresh-checkout module root proven by build-time
source paths (Go), the exact Nucleus binary file and recorded SHA-256, and
environment variable names for the two owned URLs. Reports record package/module
and executable hashes. The binary hash verifies only the file given to the
script; the coordinator must bind the running endpoint process to that binary.
Reports contain no URLs or credentials.

```sh
node conformance/polyglot/nucleus/ts_admission_native.mjs \
  --postgres-url-env PG_OWNED_URL --nucleus-url-env NUCLEUS_OWNED_URL \
  --package-root /consumer/node_modules/@neutron-build/sql \
  --binary-file /path/to/exact/nucleus --binary-sha256 RECORDED_SHA256 \
  --report /path/to/evidence/ts-nucleus-finite.json

# Go: copy main.go into a fresh consumer module, plain `go build`, then
go-admission-native -postgres-url-env PG_OWNED_URL -nucleus-url-env NUCLEUS_OWNED_URL \
  -module-root /fresh/checkout/go -binary-file /path/to/exact/nucleus \
  -binary-sha256 RECORDED_SHA256 -report /path/to/evidence/go-nucleus-finite.json
```

Run the TypeScript suite (`pnpm test` in `typescript/packages/neutron-sql`,
which includes `profile.test.ts`) and `go test ./orm -run 'Finite|Nucleus|Postgres'`
plus `gofmt`/`go vet` on compute-2 before using either qualifier.
