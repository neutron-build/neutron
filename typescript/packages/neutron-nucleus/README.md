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

## Migration metadata namespace

The SDK migration APIs infer one persistent metadata schema from `current_schema()`
at the start of each invocation and qualify all history and claim operations,
including transaction writes, adoption, diagnostics and force-unlock. Configure
the connection search path so that its first existing schema is the intended
migration schema. Temporary shadows, a history or claim found in a later search
path schema, and metadata views or unlogged tables are refused before metadata mutation; remove the
shadow or configure the intended schema first. Quoted schema identifiers are
supported. Existing history version columns must have actual builtin
`pg_catalog.int2`, `int4` or `int8` identity; domains and custom types are refused
before claim metadata is created. Canonical CLI text-ID histories still require
`neutron migrate`.

This freezes metadata resolution across pooled connections and migration SQL
using `SET LOCAL search_path`. Supplied up/down SQL retains its own name-resolution
semantics. Connections must target the same database and authorization principal;
this does not pin a session or protect metadata from privileged concurrent DDL.
The row claim, explicit recovery, checksums and adoption rules are unchanged.

Actual persistent catalog identity is required. Published Nucleus 1.2.0 lacks the
required `pg_catalog.to_regclass` capability, so these migration, status,
lock-info and force-unlock APIs refuse that experimental provider path with an
unsupported namespace profile before mutation. PostgreSQL wire compatibility
alone does not admit a migration provider. Other client SQL/model APIs are not
certified or refused by this migration-specific check.

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


### PostgreSQL scalar read profile

Generated `lossless-read-v1` TypeScript models require an explicit SQL-only
transport. The default transport keeps safe-number int8 behavior for existing
model count/ID APIs and rejects integers beyond JavaScript precision.

```ts
import { createClient, PgTransport } from '@neutron-build/nucleus';
import { withSQL } from '@neutron-build/nucleus/sql';
const transport = new PgTransport(process.env.DATABASE_URL!, {
  valueProfile: 'lossless-read-v1',
});
const db = await createClient({ url: process.env.DATABASE_URL!, transport })
  .use(withSQL).connect();
// db.sql.query<GeneratedRow>('SELECT ...') preserves int8/numeric as strings.
```

This PostgreSQL text-protocol profile matches the generator's ten builtin scalar
types: int2/int4 numbers, int8/numeric strings, boolean, text/varchar/bpchar,
UUID string and bytea Buffer (a Uint8Array). NULL remains null. Bytea requires
hexadecimal output. Non-null values of other types refuse; the generator rejects
unsupported column types before writing models. In particular, temporal values,
arrays, JSON and domains are outside the generated profile. A null field alone
does not prove its database type. This option does not make generic query types
runtime schema validation.

Parser policy is local to the pool; creating a Neutron transport does not change
node-postgres's process-wide parsers or another library's int8 reads. Transaction
reads use the same pool policy. Use separate default transports for model plugins
and SDK migration APIs; those compositions deliberately refuse the SQL read
profile. This profile does not certify Nucleus, HTTP/mobile or temporal precision.

## Mobile operation safety and deadlines

Mobile SQL calls have no automatic retry or caching by default: `SELECT`
can invoke mutating Nucleus model functions. A caller can assert a pure read
using `{ readOnly: true }`; only those calls can retry. Opt-in result caching
also requires `{ cache: true }` and transport `cacheEnabled: true`. Do not mark
increments, lock acquisition, stream group reads, publication, or other
mutations as pure reads. Mutations invalidate and fence cached reads, including
reads overlapping writes and transactions on the same transport. Other clients'
writes require explicit `invalidateCache()` or expiration.

Mobile dispatched writes and remote BEGIN are never automatically replayed. A lost
response, deadline, abort after dispatch, or server 5xx produces
`NucleusUnknownOutcomeError` (`UNKNOWN_OUTCOME`): the operation may have
completed, and callers must reconcile server state before retrying. There is
no server-backed HTTP idempotency-key protocol here. Offline queueing covers
only writes known to be undispatched and sends each queued operation once.
HTTP BEGIN also classifies a success response with missing/invalid identity as
`UNKNOWN_OUTCOME`; a typed HTTP rejection or `{ ok: false }` BEGIN envelope
remains a server rejection. The base HTTP and pgwire transports do not implement
Mobile's general mutation-ambiguity wrapper. Remote transaction terminal errors
require reconciliation; they do not authorize replay or establish rollback.

HTTP and mobile requests default to a 30-second deadline covering headers,
status/error bodies, and JSON body consumption. Set `timeout: 0` to disable
the deadline. HTTP caller cancellation yields `AbortError`; deadline expiry
yields `TimeoutError`. Aborting HTTP consumption does not prove a server-side
mutation was rolled back. The deadline applies per attempt, not to the full
Mobile retry sequence. Pure-read retry backoff is not abort-aware: cancellation
during that wait is checked before the next dispatch, after the wait ends.
There is no 30-second overall retry budget.

PgTransport reserves an independent cancellation connection before submitting
signal-bearing SQL and drains target and cancellation work before release.
Cancellation can fail or race a successful result; it does not guarantee bounded
server termination. PgTransactionTransport currently checks signals only before
dispatch and cannot cancel a running transaction statement. Embedded transport
rejects signal-bearing calls. Configured Nucleus client transports transfer
lifecycle ownership to the client, including cleanup on failed connect/plugin
initialization; callers needing a borrowed transport must retain that boundary
in their own adapter.

Blob store and metadata tags are separate engine writes. Tag failures or
cancellation can leave the object stored with partial metadata; no automatic
delete or restoration runs without a version ownership primitive. A failed
store response can have an unknown outcome. Reconcile the object explicitly;
a concurrent writer's acknowledged object must never be deleted as cleanup.

Migration rollback selects its frontier from persisted history, newest numeric
version first. Every requested entry must have a local migration, a matching
verified up checksum, and non-empty down SQL before any down script runs.
`steps` must be a finite non-negative integer; zero returns without transport
work. Adopted histories with unverified checksums require explicit
reconciliation before rollback.


TIME_BUCKET admission is write-free: call `timeseries.admitPureCapabilities()`
to run scalar negative controls without inserting points. Raw TS_RANGE conformance
needs points: explicitly consent with
`probeCapabilities({allowPersistentProbeWrites:true})` on an owned diagnostic
namespace/instance, then export
`await timeseries.admissionProfile({disposeDiagnosticNamespace: () => ownedNamespace.dispose()})`.
The owner callback must dispose the actual namespace/engine using a genuine lifecycle;
raw-range profiles refuse if it is missing, and failed disposal never publishes a
profile or repeats uncertain cleanup. Pure bucket profiles need no disposal callback. There is no point-deletion
primitive; merely closing a connection does not erase persistent diagnostic points.
A production client can `.use(withTimeSeriesProfile(profile))` without performing
writes there. Profiles are immutable, minted only by measured diagnostics, and bind
the exact endpoint/auth config digest, live VERSION() and NUCLEUS_FEATURES() digest.
Production rechecks identity with pure queries before each gated read. A different
endpoint/engine, changed version/features, copied/unverified object or stale profile
refuses; a diagnostic engine at another URL does not certify production. Profiles
are in-process only, default to five minutes and allow at most one day. Requalify
after deployment/restart; no general engine certification is implied. For configured
HTTP/PG transports identity is automatic; custom transports must explicitly provide
capabilityEndpoint and live identity queries, or their evidence cannot be handed off.
No arbitrary boolean admits unknown raw functions. Existing same-model diagnostics
remain usable, and failure to insert points cannot disable pure bucketing evidence.

Transports expose typed `capabilities`; missing custom metadata means unknown.
`createClient({requiredCapabilities})` admits requirements before feature queries.
The common SQL lifecycle module is a required dependency but imports no driver/ORM.
PG cancellation is a server attempt, never a universal termination guarantee;
transaction PG is pre-dispatch only, HTTP/mobile abort response consumption, and
embedded refuses signals. HTTP advertises BEGIN ambiguity separately from Mobile's
general mutation unknown-outcome wrapper; pgwire/embedded preserve native errors.
Transport close is terminal and later operations refuse; PG drains all allocated
pools before reporting every cleanup failure and never recreates a pool after close.
Mobile close rejects undispatched queued operations. Raw transaction ownership remains
scoped separately and does not promise rollback of external effects.

BEGIN validates the entire envelope and exact native header/body identity, including
legacy object responses without `ok`. Null/malformed success, non-byte/control/blank
or whitespace-normalized identity, lost response and 5xx produce UNKNOWN_OUTCOME
with sanitized BEGIN context and cause, after one send. Explicit protocol rejection
or recognized HTTP client rejection remains definitive. Reconcile ambiguity before
retrying; no transaction ID is invented. Mobile caching defaults false: enable it
and assert both `readOnly:true` and `cache:true` per call, including scalar SELECTs.
