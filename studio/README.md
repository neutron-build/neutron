# Neutron Studio

Visual database management for Nucleus — browse all **14** data models in one
UI: SQL, Key-Value, Vector, Timeseries, Document, Graph, Full-Text Search, Geo,
Blob, Streams, Columnar, Datalog, CDC, and Pub/Sub, plus a schema designer and
code generator.

## Architecture

A Preact SPA (`src/`) served by a Go backend embedded in the Neutron CLI
(`cli/internal/studio/`). Launch it with `neutron studio`; the CLI opens the
UI in a browser.

- **Frontend**: Preact + signals, TanStack Table data grids, CodeMirror 6 SQL
  editor, Observable Plot charts, MapLibre GL for geo (loaded on demand). Each
  data model has a dedicated module under `src/modules/`, loaded as its own
  chunk.
- **Backend**: Go server inside the CLI speaking pgwire to Nucleus;
  connections and saved state are managed server-side.

## Model modules

`src/modules/` contains one browser per model — sql, kv, vector, timeseries,
document, graph, fts, geo, blob, streams, columnar, datalog, cdc, pubsub —
plus `schema` (schema designer and code generator).

## Atomic commits and retry outcomes (S02)

Row edits can be staged as a structured operation list and committed as
ONE transaction through `POST /api/table/v2/commit` (siblings:
`/preview`, `/outcome`, `/revert`). An operation is `insert`, `update` or
`delete` in the S01 row protocol — full-key identity, relation binding,
xmin version guard, tagged wire values — and batches are bounded: at most
100 operations and a 1 MiB body per request. Both limits can be lowered
at launch (`NEUTRON_STUDIO_MAX_COMMIT_OPERATIONS`,
`NEUTRON_STUDIO_MAX_MUTATION_BYTES`); they can never be raised past the
coded bounds.

Every commit carries a client-generated `operationId` (idempotency key).
The server records one outcome per ID, transactionally with the batch:

- Same ID + same payload → the recorded outcome is **replayed** (marked
  `"replayed": true`); the operations are not executed again. This is how
  a dropped response after a commit resolves: look the outcome up
  (`POST /api/table/v2/outcome`) or retry the identical request.
- Same ID + different payload → `409 state "operation_conflict"`. IDs are
  single-use; send a fresh ID for different content.
- Concurrent duplicate while the first is committing → `409 state
  "in_progress"` (retry the same payload to receive the outcome).
- Expired or evicted outcome records → `"unknown"`, never a guess and
  never safe-to-repeat: a retried commit on a spent-unknown ID is refused
  (`409 state "unknown"`) until the client verifies the table state and
  uses a new ID.

Outcome records are in-process with bounded retention (capacity 256, TTL
15 minutes, tombstones for evicted IDs). A Studio restart therefore makes
prior IDs unknown — the honest direction; no hidden tables are created in
your database to remember them.

`POST /api/table/v2/preview` runs the identical validation and execution
inside a transaction that is always rolled back and reports the per-op
diff (before/after values, would-be keys). `POST /api/table/v2/revert`
undoes a committed batch through the inverse recorded at commit time —
updates restored to their old values, inserted rows deleted, deleted rows
re-inserted — under its own fresh `revertOperationId` (reverts are
commits and deduplicate like any other). Reverts are refused honestly
(`409 state "irreversible"`) where the inverse cannot be exact: identity
or serial keys that cannot be re-supplied, generated columns, values that
cannot round-trip the wire, or FK cascade/set-null/set-default side
effects the inverse does not capture — including side effects that first
appear BETWEEN commit and revert (rows that started referencing the
committed value after the commit): the revert aborts with nothing
applied. A batch is reversible only when every operation is.

## The data editor (S03)

Table views edit by STAGING: a cell edit, a row delete or a typed insert
joins the local draft (the commit bar) instead of writing immediately;
`Preview` dry-runs the whole draft, `Commit` sends it as ONE atomic batch
under the S02 protocol, `Revert commit` undoes the last batch server-side.
A failed commit keeps the draft staged and focuses the first offending
row in its grid — the batch error names `operations[N]` and the Nth
staged edit is pinned.

Staged drafts are addressed by the full-key row identity captured at read
time, never by row position: paging, re-sorting and re-filtering move
rows without detaching (or re-attaching to the wrong row) — a draft whose
row left the current page simply stays staged. Staged values render
highlighted over the committed cell until commit or discard.

Every editor is typed from the server's catalog metadata: booleans edit
as true/false, JSON as validated JSON text, and bigint/numeric/temporal
values as their canonical text re-tagged on the wire (digits never cross
through a JavaScript number). Each column carries an explicit three-way
value state — a value (empty text is a real empty string), SQL `NULL`,
and, on insert, `DEFAULT` (omit the column). Composite foreign keys
navigate by the whole tuple (any component's link opens the referenced
row filtered on all of them).

Reads support multiple ANDed filters and ordered multi-column sorts
(`GET /api/table?filters=[...]&sorts=[...]`, at most 8 and 4; unknown
columns are refused with 400) and separate the row counts: `rowCount` is
the fetched page, `filterCount` applies the read's conditions,
`totalCount` applies none. Connection switching never carries a draft: a
view bound to another connection refuses to stage, the commit bar refuses
to commit another connection's edits, and the server independently
re-refuses any operation whose relation binding was not read through the
request's connection.

## Development

`npm run dev` serves the SPA on port 5173 and proxies `/api` to the Go
server on 4983. The server accepts state-changing requests only from its
own exact origin plus a per-launch session token, so start it with the dev
origin allowed explicitly:

```bash
NEUTRON_STUDIO_DEV_ORIGIN=http://localhost:5173 neutron studio
```

## Schema navigation and performance diagnosis (S05)

### Navigation, search and deep links

The sidebar tree lists **views alongside tables** (PostgreSQL connections;
views browse read-only through the same table surface) and has a search box
filtering by name across schemas, plus a refresh button that re-fetches the
live catalog (the designer triggers the same refresh after an apply or a
stale-plan refusal). Every browsable surface has a shareable URL:

```text
#/c/<connId>/t/<schema>/<table>          browse rows
#/c/<connId>/v/<schema>/<view>          (views also use t/)
#/c/<connId>/designer[/<schema>/<table>]
#/c/<connId>/inspect/<schema>/<table>   structure detail
#/c/<connId>/diagnostics[/<schema>/<table>]
#/c/<connId>/sql
```

Names are URL-encoded, so `MixedCase View` survives. Opening a browsable
tab updates the hash; loading or pasting a hash connects the named
connection and opens the tab.

### Object detail

`GET /api/schema/object` returns one relation's metadata from the same
schema document v2 the CLI works with (`neutron schema pull` writes it):
columns with the planner's DDL type spelling and tagged defaults,
PK/unique/check/FK constraints, indexes (method, key parts, INCLUDE,
predicates) and FK relationships in both directions as ordered column
tuples, or a view's definition. Objects inventoried as opaque (extension
owned, partitioned, RLS-bearing…) are reported as exactly that. A relation
dropped concurrently answers 404. Nucleus is refused: v2 catalog
conformance is not established there. The schema designer loads tables
through this endpoint, so it shows and edits the CLI's identities.

### Reviewable migration plans from the designer

The designer does not build DDL. It sends structured edits
(`create-table`, `drop-table`, `add-column`, `drop-column`,
`rename-column`, `alter-column-type`, `set/drop-not-null`,
`set/drop-default`, `add-index`, `drop-index`) to
`POST /api/schema/plan`, which applies them to a copy of the freshly
introspected document and plans the difference with the CLI's own diff and
live expression normalizer — the same inputs `neutron db push` uses. The
response carries the statements, the per-operation risk report of
`migrate generate` (destructive / data loss / reversibility), the planner's
warnings, the target document, and the equivalent command:

```sh
neutron db push --dry-run --schema target.schema.json \
  [--rename 'schema.table.old>schema.table.new'] [--allow-destructive]
```

prints the same statements, and `neutron migrate generate --mode snapshot
--schema target.schema.json` from a `schema baseline` records the same
operations (both are covered by an end-to-end test against the CLI binary).

Edits follow PostgreSQL's own semantics, made explicit: dropping a column
drops the table's indexes and constraints involving it as whole objects
(listed in the review); anything that would need `CASCADE` — a foreign key
from another table, a view, a generated column, a trigger — refuses the
edit, as does renaming a column that SQL text elsewhere (views, CHECK
expressions, expression indexes) refers to. Column types outside the
document contract (`serial`, `char`, `time`, …) are refused with the
accepted vocabulary; the SQL editor remains the surface for those.

`POST /api/schema/apply` executes a reviewed plan only:

- it takes the migration runner's advisory lock (a running migration or
  push makes it wait up to 10 s, then answers `409 locked`);
- a database with migration history answers `409 migration-managed`, as
  `neutron db push` does — download the target document and generate a
  migration file instead;
- it re-plans under the lock and requires the same `planId`: if the
  statements changed since review (a concurrent schema change), it answers
  `409 stale-plan` with the fresh plan for review. An unrelated concurrent
  change that leaves the statements identical does not invalidate the
  review;
- a plan with destructive or data-loss operations needs
  `allowDestructive: true` (the review's acknowledgement checkbox). The
  CLI planner drops and recreates views around table alterations, so such
  plans count as destructive too;
- all statements run in one transaction; afterwards the catalog is
  re-introspected and diffed against the target (`verification: "in-sync"`,
  or `"drift"` with the residual statements).

### Performance diagnosis

`GET /api/diagnostics/queries` returns this Studio process's duration log
for the connection (editor statements and table reads, newest first, with a
threshold filter and p50/p95/max). Durations are wall-clock as measured by
Studio, including network and result transfer; statement text is recorded
as submitted and bound parameters never are. When `pg_stat_statements` is
installed and readable, the server's top statements for the database are
included; otherwise the response says why. `GET /api/diagnostics/table-stats`
returns the table's counters from `pg_stat_user_tables` (sequential/index
scans, live/dead tuples, analyze/vacuum times), sizes, and per-index scan
counts with `pg_get_indexdef` definitions; counters are cumulative since the
last statistics reset, so a zero-scan index is an observation, not a
verdict. EXPLAIN results render as a tree with every node field PostgreSQL
emitted.

### timestamptz input discipline

A timestamptz value without an explicit UTC offset would be silently
interpreted in the server's SESSION timezone (an 8-hour shift for a
Vancouver user). Both write paths — the editor's tagged cells and the SQL
editor's bound `{t:"timestamptz"}` parameters — refuse such values with a
message naming the hazard and the fix: append an explicit offset
(`+02:00`, optionally after one space; `UTC`/`GMT` also count) or use the
canonical UTC form (`2026-09-24T12:34:56Z`). Named zones such as
`America/Vancouver` are refused as well, because the client cannot validate
them. Chosen
over silently normalizing to UTC: refusing is the only option that never
guesses what instant the user meant. `timestamp without time zone` is
unaffected (offset-less is its canonical form).

## Testing

Frontend: `npm test` (vitest) and `npm run build` in `studio/`. Backend:
`go test ./...` in `cli/` covers `internal/studio`. CI runs both via
`cli.yml`, which triggers on `studio/**` and `cli/**`.

## Status

Implemented and under active development — private workspace software in this
monorepo, not a published product.

---

*This file replaced a pre-implementation design document (2026-08-19). That
document said "all **9** data models", listed a Tauri desktop wrapper, AG
Grid, D3-force, Cytoscape.js, and a Rust backend, described a file layout that
does not match the tree, and ended with "Status: Planned — not yet
implemented" — while `studio/src/modules/` already ships a browser for each of
the 14 models and the Go backend runs in CI. Found by the S97 claims audit.*
