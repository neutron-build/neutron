# Neutron Studio

Visual database management for Nucleus. Browse all 9 data models in one UI — SQL, Key-Value, Vector, Timeseries, Document, Graph, Full-Text Search, Geo, and Pub/Sub. Schema designer + code generator + database browser in one tool.

## Philosophy

Light core, modular data viewers. The core shell boots in under 3 seconds (~200KB JS). Every data model viewer is a separate chunk loaded on demand — you never pay for the vector explorer if you only use SQL.

## Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Frontend | Neutron TS + Preact | Dogfooding — 3KB vs React's 42KB |
| State | @preact/signals | Already in the monorepo, zero new deps |
| Desktop | Tauri 2.0 | 28MB RAM vs 250MB Electron, 10MB installer |
| Query editor | CodeMirror 6 + @tidbcloud/codemirror-extension-sql-autocomplete | 150KB vs Monaco's 2MB+; schema-aware completion |
| Charts | Observable Plot | 5KB vs Chart.js 60KB |
| Graph explorer | D3-force | Force-directed layout for relationship visualization |
| Schema ERD | Cytoscape.js | Hierarchical layout, compound nodes for schema grouping, built-in pan/zoom |
| Credentials | Tauri keyring plugin | OS native credential store (macOS Keychain, Windows Credential Manager) — Stronghold is deprecated |
| Geo | MapLibre GL | Loaded only when a geo layer is opened |
| Styling | CSS Modules + design tokens | No Tailwind, full CSS for complex components |
| Data grid | AG Grid (>100K rows) / TanStack + react-window (<10K) | Right tool for each scale — AG Grid handles 100K+ with DOM virtualization |
| Backend | Neutron Rust | Speaks pgwire to Nucleus, credentials never leave server |
| Live data | SSE (not WebSocket) | Unidirectional streams, HTTP/2 compatible |

## Core vs Modules

**Core** (always loaded, ~200KB budget):
- Connection manager
- Navigation shell
- Schema tree sidebar
- SQL grid browser
- Single-tab query editor
- Settings

**Loaded on demand:**
- Query Workspace — multi-tab, history, EXPLAIN
- Schema Designer — ERD canvas + non-SQL config forms
- Code Generator — TypeScript, Rust, Python, Go, Zig output
- Timeseries Viewer — Grafana-style time range picker + Observable Plot charts
- Vector Explorer — Apple Embedding Atlas (UMAP Wasm + WebGPU), handles millions of vectors, density clustering, similarity search UI
- Graph Explorer — D3 force-directed layout, record relationship browser
- Document Explorer — per-stage pipeline builder with live preview (MongoDB Compass model)
- Pub/Sub Monitor — live channel stream viewer
- FTS Explorer — ranked results, highlight, facets
- Geo Explorer — MapLibre GL map with layer controls

## UI Approach for 9 Data Models

One consistent shell, model-specific content area. Every data model gets exactly two views:
1. **Browser** — see and edit data
2. **Query** — write a query in the appropriate language for that model

The model is identified by a permanent colour badge in the header. The sidebar schema tree shows all 9 types with distinct icons. The mental model is identical across every model: click an object in the tree, browse it in the content area. Model-specific knowledge is contained in the content component — the shell, navigation, and keyboard shortcuts are universal.

## Inline Editing (TablePlus Model)

Inline cell editing with a pending commit bar at the bottom. Changes are staged locally. Cmd+S commits. Cmd+Z reverts. Nothing is written to the database until you explicitly commit. This is the best mental model for database writes ever designed — adopted verbatim from TablePlus.

## Schema Designer

- **SQL tables** — ERD canvas built on Cytoscape.js with hierarchical layout, draggable cards, and bezier FK relationship lines
- **Non-SQL models** — structured config forms (KV TTL, vector dimensions, timeseries retention, etc.)
- Schema state tracked as JSON, diffs computed between snapshots to generate migration scripts
- **Code generator** — live preview of idiomatic output as you make changes. No separate "run generate" step.

### Code Generator Output

```ts
// TypeScript — Drizzle-style, no codegen required at runtime
export const users = sql.table('users', {
  id: t.uuid().primaryKey(),
  name: t.text().notNull(),
})
export const sessions = kv.store('sessions', { ttl: 3600 })
export const embeddings = vector.collection('embeddings', { dimensions: 1536 })
```

```rust
// Rust — proc macro
nucleus::schema! {
    users: sql::Table { id: Uuid, name: String }
    sessions: kv::Store { ttl: 3600 }
    embeddings: vector::Collection { dimensions: 1536 }
}
```

## What We Took From Each Tool

| Tool | What we adopted |
|------|----------------|
| TablePlus | Inline cell editing + pending commit bar (Cmd+S/Cmd+Z) |
| MongoDB Compass | Per-stage pipeline builder with live preview, schema analysis |
| RedisInsight | Per-key-type custom visualisers (every data type gets a purpose-built view) |
| Qdrant Web UI | Embedding Atlas UMAP projection for vector data — handles millions of points, WebGPU-accelerated |
| Grafana | Time range picker (relative + absolute), panel composition model |
| Surrealist | Multi-model GUI shell, Tauri dual-deployment (web + desktop app) |

## What We Avoided

| Tool | What we avoided |
|------|----------------|
| DBeaver / Compass | Electron (10x memory overhead) — Tauri only |
| TablePlus / Beekeeper | SQL-only mindset bolted onto non-SQL data |
| Prisma Studio | Proprietary codegen with vendor lock-in |
| DBeaver | Menus 3 levels deep — everything within 2 levels or command palette |
| Most web tools | Auto-save on blur — pending commit model always |
| Chart.js / Recharts | 60-100KB chart libraries — Observable Plot at 5KB |
| Tauri Stronghold | Deprecated, will be removed in Tauri v3 — use system keyring plugin |

## Build Phases

**Phase 1** — Core shell + SQL browser (a better TablePlus)
**Phase 2** — All 9 data model browsers
**Phase 3** — Schema designer + code generator
**Phase 4** — Advanced visualizations (vector t-SNE, graph force layout, document pipeline builder, geo map) + Tauri desktop packaging

## File Structure

```
studio/
├── src/
│   ├── core/               # Always-loaded shell (~200KB budget)
│   │   ├── Shell.tsx       # App chrome, sidebar, navigation
│   │   ├── ConnectionManager.tsx
│   │   ├── SchemaTree.tsx
│   │   └── Settings.tsx
│   ├── modules/            # Loaded on demand (separate Vite chunks)
│   │   ├── sql/            # SQL grid + query editor
│   │   ├── kv/             # KV browser
│   │   ├── vector/         # Vector explorer (t-SNE)
│   │   ├── timeseries/     # Timeseries charts
│   │   ├── document/       # Document pipeline builder
│   │   ├── graph/          # Force-directed graph
│   │   ├── fts/            # Full-text search
│   │   ├── geo/            # MapLibre map
│   │   ├── pubsub/         # Pub/Sub monitor
│   │   ├── designer/       # Schema designer + ERD
│   │   └── codegen/        # Code generator
│   ├── components/         # Shared UI (grid, tabs, modals, badges)
│   └── main.tsx
├── src-tauri/              # Desktop wrapper
└── package.json
```

## Status

Planned — not yet implemented.
