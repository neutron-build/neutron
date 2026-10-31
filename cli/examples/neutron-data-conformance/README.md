# Native PostgreSQL scalar read/write conformance

This standalone operator fixture creates a uniquely named disposable PostgreSQL
database. It does not use the reference application's schema or migration ledger.
The administrator must have CREATE DATABASE permission. The runner drops only the
database it created, including on a failed check.

Use an existing Python environment with asyncpg and the candidate Python SDK's
runtime dependencies, an existing TypeScript dependency directory containing pg
and its types, Node, TypeScript, and the cached Go dependencies. The runner does
not install dependencies. Run from an exact candidate checkout and supply a CLI
binary built from the reviewed candidate source:

```sh
export ADMIN_DATABASE_URL='postgresql://...'
export NEUTRON_CLI=/private/path/to/neutron
export CONFORMANCE_RUNTIME=/private/path/to/new-empty-run-directory
export CONFORMANCE_TS_DEPS=/path/to/existing/node_modules
export CONFORMANCE_TSC=/path/to/existing/tsc
python cli/examples/neutron-data-conformance/run.py
```

Keep the runtime directory private. It contains diagnostics, generated models,
compiled consumers, and provenance hashes. Connection URLs are passed through the
environment and are never printed by the runner; subprocess diagnostics are
retained privately. The runner requires at least 6 GiB free disk space and uses
bounded commands with offline Go dependency resolution.

The fixture covers the ten builtin scalar type kinds admitted by
`lossless-read-v1`: int2, int4, int8, numeric, bool, text, varchar, bpchar, uuid,
and bytea. It checks signed integer boundaries, numeric(40,18), Unicode and empty
text, NULL, native Python UUID/Decimal/bytes, and byte payloads against an
independent SQL oracle. Generated models are read representations; this is not
an insert/update model or an ORM comparison. Writes below use explicit native
parameter binding, not generated write authority. All three CLI languages must refuse
a batch containing an unsupported temporal column without overwriting an existing
sentinel or producing partial outputs.

Temporal reads are a separate check. Python datetime and Go time.Time preserve
UTC microseconds. The default TypeScript PostgreSQL Date representation preserves
milliseconds only; an explicit SQL UTC text projection can preserve microseconds
without claiming that the generated scalar profile supports temporal columns.
TypeScript generated-model scalar reads use an explicit SQL-only
`PgTransport(url, {valueProfile: 'lossless-read-v1'})`. The default transport's
unsafe-int8 refusal is checked separately. No casts are used for the generated
scalar reads. This option requires a candidate SDK that implements the explicit
value profile; compilation fails against an older SDK rather than falling back.

For a separately reviewed TypeScript SDK candidate, set `CONFORMANCE_TS_SOURCE`
to its `typescript/packages/neutron-nucleus/src` directory. The runner copies that
exact source into the private runtime and records every source hash. Go and
Python always use the checkout containing this example. Provenance also records
the CLI binary hash, generated source hashes before Go's required package header,
compiler versions, runtime source hashes, and database cleanup outcome.


## Native writes and cross-language updates

The same runner also generates a selected-read model for a separate scalar
`writes` table with a required positive bigint revision. Each of the three
native clients parameter-binds all ten scalar kinds, reads back signed minima
and the exact negative numeric boundary, creates a required-present NULL row,
and conditionally updates the first row to signed maxima, the full-width
positive decimal, UUID and empty text/bytes. A repeated stale revision changes
zero rows. Transactions update an existing row, delete the NULL row and insert
a temporary row; rollback must undo all three effects.

After every language has written its two rows, each language reads and performs
conditional revision updates on both other writers' rows. It then rejects the
same stale revisions. All three read the final six rows. The runner compares
every phase with independently constructed expected values and PostgreSQL
text/hex oracle queries, including exact revisions beyond JavaScript's safe
integer range, NULL versus empty, and rollback row absence. Provenance records
the nine phases, six cross-writer update pairs and final state.

This is a PostgreSQL scalar native-client contract, not equivalent rich ORMs,
generated write models, temporal writes, Nucleus support, public authentication
or released-artifact certification. Existing separate temporal read and type
refusal checks still run. Connection URLs remain private; Go builds stay bounded
and the 6 GiB free-space guard is checked before and after compilation/consumers.
