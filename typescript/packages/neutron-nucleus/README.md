# @neutron-build/nucleus

TypeScript client for the Nucleus database.

14 data models over PostgreSQL wire protocol — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, PubSub, Columnar, Datalog, CDC.

## Installation

```bash
npm install @neutron-build/nucleus
```

## Usage

```typescript
import { createClient } from "@neutron-build/nucleus";
import { query } from "@neutron-build/nucleus/sql";
import { vectorSearch } from "@neutron-build/nucleus/vector";
```

## Documentation

[neutron.build](https://neutron.build)

## Documents and graph relationships

Measured against Nucleus 1.0.2 by `conformance/live/orm/x02-nucleus-leg.mjs`
(real engine, plus a plain-PostgreSQL control). Everything below is what
that leg observes; what it does not observe is not claimed.

### Capability gate

The document and graph relationship surfaces below are gated like
`@neutron-build/sql`'s capabilities: on first use each client runs
read-only, value-asserting probes, and an operation whose capability does
not resolve `supported` throws `NucleusCapabilityError` before sending its
own statements. On plain PostgreSQL every one of them throws
`NucleusFeatureError` with no statement beyond the version check.

```typescript
const report = await client.graph.capabilities();
// [{ capability: "graph-adjacency", status: "supported", evidence: "probed: ..." }, ...]
```

| capability | Nucleus 1.0.2 | used by |
|---|---|---|
| `document-collections` | supported (probed) | `document.collection()` |
| `graph-adjacency` | supported (probed) | `graph.traverse()`, bounded `graph.shortestPath()`, `sqlNodes().hydrate()` |
| `graph-property-match` | supported (probed) | `sqlNodes()` row lookups |
| `graph-tenant-isolation` | unsupported | not offered |
| `graph-query-parameters` | unsupported | `graph.query(cypher, params)` throws |
| `graph-multi-label` | unsupported | `graph.addNode([a, b])` throws |
| `specialty-session-isolation` | unsupported | not offered |
| `atomic-sql-specialty-writes` | unsupported | not offered |

### Schema-aware document collections

The engine stores any JSON; there is no server-side schema. A collection
adds client-side validation:

```typescript
import { createClient } from "@neutron-build/nucleus";
import { withDocument, DocumentValidationError } from "@neutron-build/nucleus/document";

const client = await createClient({ url }).use(withDocument).connect();
const profiles = client.document.collection("tenant_a_profiles", {
  schema: {
    fields: {
      userId: { type: "integer" },
      login: { type: "string" },
      tags: { type: "array", optional: true, items: { type: "string" } },
    },
    additionalProperties: false,
  },
  boundTo: { table: "users" }, // identity metadata only; creates nothing
});

const id = await profiles.insert({ userId: 42, login: "ada" });
await profiles.insert({ userId: "42", login: "ada" });
// DocumentValidationError: $.userId expected integer, got "42"
```

- Validation runs before any statement is sent and reports every issue
  (`path`, `expected`, `got`).
- `integer` fields reject values beyond `Number.MAX_SAFE_INTEGER`: the
  document store keeps numbers as doubles, so `2^53 + 1` would be stored as
  `2^53`.
- `undefined` counts as absent (JSON drops it); `object` fields must be
  plain JSON objects, not `Date`/`Map`/class instances.
- `update(id, patch)` shallow-merges, validates the merged document, and
  replaces it. The read and the replace are separate statements with no
  isolation between them, so a concurrent update in between is overwritten.
- Missing and deleted documents: `get` returns `null`, `update` and
  `delete` return `false`, `path` returns `null`. The same holds for an id
  that belongs to another collection. Ids must be positive safe integers.

Every statement a collection sends is scoped to it, and the engine keeps
collections apart, including the unnamed default collection: one collection
cannot read, update, delete or query another's documents. **This is
namespacing, not access control.** Any session can name any collection, and
in the default passwordless deployment every login runs as the superuser.
Two tenants are isolated only if your application picks the collection
name, never the tenant. Under a row-level-security principal the engine
refuses every document and graph function; the gate then reports
`document-collections` unsupported and the collection throws.

### Graph traversal tied to SQL ids

The engine offers one-hop adjacency (`GRAPH_NEIGHBORS`) and an unbounded
`GRAPH_SHORTEST_PATH`. That scalar silently ignores a max-depth argument,
so the client computes bounded paths itself. Traversal is bounded by the
API:

```typescript
import { withGraph } from "@neutron-build/nucleus/graph";

const reach = await client.graph.traverse(startNodeId, { maxDepth: 3, edgeTypes: ["FOLLOWS"] });
// reach.startPresent, reach.nodes: [{ id, depth, viaEdgeId, viaEdgeType }],
// reach.statements, reach.truncated
```

- `maxDepth` is required (1..32). `maxNodes` caps visited nodes (default
  1000, at most 10000); hitting it sets `truncated`.
- Cycles and self-loops are safe: each node is expanded at most once.
  `statements` counts the traversal's own statements: one `GRAPH_NODE` for
  the start, then one `GRAPH_NEIGHBORS` per expanded node.
- `edgeTypes` filters on the client (the engine primitive has no type
  argument); `direction` is `out`, `in` or `both`.
- A start node that does not exist returns `startPresent: false`, not an
  empty traversal. A deleted node stops being reachable, and its edges are
  deleted with it.

Nodes bound to SQL rows:

```typescript
const users = client.graph.sqlNodes({ table: "users" }); // no statements, no DDL
const nodeId = await users.addRowNode(42, "User");       // one node per row
await users.addRowEdge(42, 7, "FOLLOWS");                 // both rows need nodes
const hydrated = await users.hydrate(reach.nodes.map((n) => n.id));
// [{ nodeId, sqlRef: { table: "users", id: 7, idColumn: "id" }, row: {...} | null }]
```

- The row identity is stamped into the reserved node properties
  `sqlref_schema`, `sqlref_table` and `sqlref_row`. The stamp overrides
  user properties with those names.
- `addRowNode` throws `NucleusConflictError` if the row already has a node.
  The check and the insert are two statements with no uniqueness in the
  engine, so concurrent callers can still race; `findNodeByRow` then throws
  `NucleusConflictError` instead of picking one of the duplicates.
- `findNodeByRow` scans the graph's nodes with one `GRAPH_QUERY` (there is
  no property index). A binding claims only nodes stamped with its own
  table and schema.
- `hydrate` reads stamps with one `GRAPH_NODE` per node, then the rows with
  parameterized `SELECT * FROM "table" WHERE "id" IN (...)` in chunks of
  256. A row deleted after its node was created comes back as `row: null`
  with `sqlRef` intact, and the node stays; deciding what to do with it is
  yours. Nodes without this binding's stamp come back as
  `sqlRef: null, row: null`. Row ids are positive safe integers.
- `addRowEdge` to a row without a node throws `NucleusNotFoundError` naming
  the row; `addEdge` to a missing node names the missing endpoint.

### Transaction scope

This client runs every document and graph call as its own statement on the
connection pool. It does not put them inside an SQL transaction, and
nothing in it makes SQL and document/graph writes atomic.

What the engine does when you issue the statements yourself on one
connection, as measured:

- `ROLLBACK`, including after a failed SQL statement, removes that
  transaction's SQL rows, graph nodes and documents; `COMMIT` publishes all
  three.
- There is no isolation for documents and graphs. Another session reads an
  open transaction's graph nodes and documents before `COMMIT`, while its
  SQL rows stay invisible. This is why atomic SQL+graph writes are not
  advertised.
- After the engine process is killed, committed documents and graph nodes
  survive. Graph nodes and documents written by a transaction that was
  still open are gone. Power-loss durability was not measured.

### Limitations

- The graph is one global store. It has no tenant, namespace or permission
  boundary, so any client can traverse into any tenant's nodes. Use separate
  engines for graph tenant isolation.
- `graph.query()` takes Cypher text only. Inline values yourself, and never
  interpolate untrusted input.
- `NUCLEUS_FEATURES()` does not exist on Nucleus 1.0.2, so the per-model
  feature flags fall back to all-enabled once the engine is identified. The
  capability gate above covers the document and graph relationship
  surfaces; the older per-model methods are not gated by it.
- Studio's document module scopes to a collection name you type. Because
  `/api/query` does not bind parameters yet, it accepts only
  `[A-Za-z0-9_-]` in that name.

## License

MIT
