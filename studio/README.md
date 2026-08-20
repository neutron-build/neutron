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
