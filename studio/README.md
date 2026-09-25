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

## Large results, import/export and keyboard access (S06)

The result grids virtualize: only the rows in the scroll viewport plus a
fixed overscan exist in the DOM, whatever the result size, with spacer
rows keeping the scroll height exact. Bounded reads are the contract, not
a courtesy: one table page is at most 1,000 rows (larger limits are
refused with 400, never silently clamped — page with offset or export
instead), and the SQL editor retains at most 10,000 rows of a result,
marking it `truncated` with the limit named in the grid. Offset paging is
deterministic: the primary key is the unique tail of every table read, so
unchanged-data pages neither repeat nor skip rows.

The grid is a WAI-ARIA `grid` usable without a mouse: one roving tab stop
moves with the active cell, arrows/PageUp/PageDown/Home/End navigate,
Enter/F2 open the typed editor (Enter follows a read-only FK link, Delete
stages a row delete), and focus returns to the cell after commit/cancel.
Column headers are named sort buttons carrying `aria-sort`; a truncated
result announces itself through a live region.

**Export** is streamed server-side: `POST /api/table/v2/export` validates
the table, format (CSV/JSON/NDJSON) and the same filter/sort/match
grammar, and returns a single-use ticket; `GET .../download` redeems it
once (browser-marked cross-site requests refused) and streams rows through
a fixed buffer — memory does not scale with the table, an abandoned
download cancels the statement and returns the connection, and an error
mid-stream aborts the response so the browser reports a failed download
instead of keeping a truncated file that looks complete. Cell text is
PostgreSQL's own `::text` under pinned output settings (ISO dates, hex
bytea), so int8/numeric digits and microsecond timestamptz never cross a
double or a locale formatter. CSV follows COPY conventions (NULL is an
unquoted empty field, `""` is the empty string); JSON/NDJSON carry exact
number literals (NaN/Infinity as strings). The SQL editor's bounded result
exports in-memory with the same conventions. The older model-module
exports (KV/documents/FTS/pub-sub lists) are unchanged.

**Import** (CSV, JSON array or NDJSON) reads the file in streamed chunks,
shows a mapping table and live preview (NULL vs empty string vs DEFAULT
visible per cell), and sends batches of at most 100 rows / 900 KiB: each
batch is ONE transaction through the S02 commit machinery — all rows or
none — with the whole-import atomicity stated explicitly in the dialog.
Digits stay exact end-to-end (int8 validated against its 64-bit range,
numeric/bytea/temporal re-tagged on the wire; JSON numbers keep their
literal text, never `JSON.parse`d into doubles). A per-batch operation ID
plus a persisted journal make interruptions recoverable without duplicate
rows: after a drop, restart or failure the in-flight batch is resolved
through the server's recorded outcome first, and an honestly-unknown
outcome is a decision (check the table, then mark committed or retry),
never a guess. A failing batch names its source row; skip-row/skip-batch/
retry are recorded in the journal, and resuming re-reads the same file
(checked by name, size and mtime).

## Development

`npm run dev` serves the SPA on port 5173 and proxies `/api` to the Go
server on 4983. The server accepts state-changing requests only from its
own exact origin plus a per-launch session token, so start it with the dev
origin allowed explicitly:

```bash
NEUTRON_STUDIO_DEV_ORIGIN=http://localhost:5173 neutron studio
```

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
