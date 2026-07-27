# JDBC compatibility harness

Runs `Main.java` through the official pgjdbc driver (42.7.7, downloaded into
`lib/` on first run) against a release Nucleus server.

```sh
sh run.sh              # build release nucleus + run
sh run.sh --no-build   # reuse existing target/release/nucleus
```

SKIPs (exit 0) when no JDK is available or the driver jar can't be fetched;
exits non-zero on any failed check.

## What it covers

- Simple statements and result sets
- Typed prepared statements (int/bigint/text/bool/float8/numeric/timestamp/
  date/bytea/null) through the extended protocol
- **Binary result transfer**: the same prepared select re-executed past
  `prepareThreshold` so pgjdbc names the server statement and switches to
  binary format for results (this is what caught the missing binary NUMERIC
  result encoding)
- Transactions: commit, rollback, and PostgreSQL error-state semantics
  (statement error aborts the transaction; later statements fail with 25P02
  until rollback)
- Batches, including a failing row surfacing as `BatchUpdateException` with
  the connection still usable
- `getGeneratedKeys` (RETURNING)
- `DatabaseMetaData`: getTables / getColumns / getPrimaryKeys (these exercise
  `information_schema._pg_expandarray` and `(composite).field` access), plus
  ResultSetMetaData through Describe
- **Query cancellation**: `setQueryTimeout` fires a wire `CancelRequest` on a
  fresh connection; the running statement must die with SQLSTATE 57014 and
  the connection must remain usable

## Findings log (2026-07-23, all fixed)

- Timestamp/date text params carry a UTC offset (`2026-07-23 ...123456-07`,
  `1999-12-31 -08`) — the strict ISO parsers rejected them
- Binary NUMERIC results were sent as text bytes under a binary column
- Aborted-transaction errors carried SQLSTATE 22000 instead of 25P02
- `CURRENT_CATALOG` unknown; pg_type lacked typnotnull/typbasetype/typtypmod;
  pg_attribute lacked attlen
- Qualified/unaliased projections kept their qualifier (`a.atttypid`) instead
  of the PostgreSQL default output name (`atttypid`); aggregates were named by
  their expression text (`COUNT(*)`) instead of the function name (`count`)
- `information_schema._pg_expandarray` and composite `(x).field` access were
  unsupported (getPrimaryKeys hard-fails without them)
- CancelRequest was unimplemented; once implemented, a long rayon filter was
  found to stall the tokio IO driver so the cancel connection was not even
  read until the query finished (fixed with `block_in_place` + cooperative
  cancel checks in the long executor loops)
