// Shared types used across the frontend

import type { WireTag } from './wire'

export interface Connection {
  id: string
  name: string
  url: string          // masked on return from server: postgres://user:***@host/db
  isNucleus: boolean
  nucleusVersion?: string
  pgVersion?: string
  lastConnected?: string
}

export interface ConnectionInput {
  name: string
  url: string          // full url, sent once to server, stored server-side
}

export interface TestResult {
  ok: boolean
  isNucleus: boolean
  version: string
  error?: string
}

// --- Schema types ---

export interface SqlTable {
  schema: string
  name: string
  columns: SqlColumn[]
  rowCount?: number
}

/** One named relation (S05: a view in the navigation tree). */
export interface SqlRelationRef {
  schema: string
  name: string
}

export interface SqlColumn {
  name: string
  type: string
  nullable: boolean
  default?: string
  isPrimaryKey: boolean
}

export interface KvStore {
  name: string
  keyCount: number
}

export interface VectorIndex {
  name: string
  dimensions: number
  metric: string
  count: number
}

export interface TsMetric {
  name: string
  count: number
  minTs?: string
  maxTs?: string
}

export interface DocCollection {
  name: string
  count: number
}

export interface GraphStore {
  name: string
  nodeCount: number
  edgeCount: number
}

export interface FtsIndex {
  name: string
  docCount: number
}

export interface GeoLayer {
  name: string
  pointCount: number
}

export interface BlobStore {
  name: string
  blobCount: number
}

export interface PubSubChannel {
  name: string
}

export interface Stream {
  name: string
  length: number
}

export interface ColumnarTable {
  name: string
  rowCount: number
}

export interface DatalogStore {
  predicateCount: number
  ruleCount: number
}

export interface Schema {
  sql: SqlTable[]
  /** S05 navigation: user-schema views (PostgreSQL connections). */
  views: SqlRelationRef[]
  kv: KvStore[]
  vector: VectorIndex[]
  timeseries: TsMetric[]
  document: DocCollection[]
  graph: GraphStore[]
  fts: FtsIndex[]
  geo: GeoLayer[]
  blob: BlobStore[]
  pubsub: PubSubChannel[]
  streams: Stream[]
  columnar: ColumnarTable[]
  datalog: DatalogStore | null
  cdc: boolean
}

export interface NucleusFeatures {
  isNucleus: boolean
  version: string
  models: string[]
}

// --- Query types ---

export interface QueryResult {
  columns: string[]
  rows: unknown[][]
  rowCount: number
  duration: number   // ms
  error?: string
  /** Authoritative PK columns of the queried table (table reads only). */
  keyColumns?: string[]
  /** Per-row version strings (xmin) aligned with rows (table reads only). */
  versions?: string[]
  versioned?: boolean
  /** Relation binding ("<connection epoch>:<relation oid>") the server
   *  requires on every v2 mutation for these rows. */
  binding?: string
  /** Authoritative editing state of the table read (server catalog). */
  readOnly?: boolean
  readOnlyReason?: string
  /** SQL editor (S04): the request ID the statement ran under. */
  requestId?: string
  /** SQL editor (S04): the statement was cancelled at the user's request. */
  canceled?: boolean
  /** SQL editor (S04): PostgreSQL SQLSTATE of the error, when there is one. */
  sqlState?: string
  /** SQL editor (S04): after a cancel, whether the backend passed the
   *  post-cancel probe and went back to the pool (false: it was discarded). */
  connectionReused?: boolean
  /** Rows matching the read's conditions (filters + match), unpaginated. */
  filterCount?: number
  /** Rows in the table ignoring all conditions. */
  totalCount?: number
  /** SQL editor (S06): more rows existed than the server retains per
   *  result; `rows` holds the first `rowLimit` of them. */
  truncated?: boolean
  rowLimit?: number
}

/** One component of a full-tuple equality filter; value is a wire cell. */
export interface MatchCell {
  column: string
  value: unknown
}

// --- S01 typed row identities ---

/** One component of a row's full key tuple; value is a decoded or tagged cell. */
export interface KeyCell {
  column: string
  value: unknown
}

/** A row addressed by connection + relation binding + schema + table + full
 *  PK tuple + version. */
export interface RowIdentity {
  connectionId: string
  binding: string
  schema: string
  table: string
  key: KeyCell[]
  version: string
}

/** Authoritative editable-column metadata from the server's catalog. */
export interface TableMetaColumn {
  name: string
  type: string
  tag: WireTag | null
  nullable: boolean
  isKey: boolean
  generated: boolean
  identity: boolean
  hasDefault: boolean
  autoAssigned: boolean
  editable: boolean
  readOnlyReason?: string
  insertable?: boolean
}

export interface TableMeta {
  exists: boolean
  /** "<connection epoch>:<relation oid>"; mutations must present it. */
  binding?: string
  canDelete?: boolean
  keyColumns: string[]
  versioned: boolean
  readOnly: boolean
  readOnlyReason?: string
  columns: TableMetaColumn[]
}

/** Outcome envelope for v2 mutations: success data or an explicit state. */
export interface MutationOutcome {
  rowsAffected?: number
  /** New row version after an update/insert (refreshes the identity). */
  version?: string
  /** Inserted row's key cells, tagged. */
  key?: KeyCell[]
  error?: string
  /** Row changed since read (stale write refused). */
  conflict?: boolean
  /** Row no longer matches the key (stale, deleted, or not visible). */
  missing?: boolean
  currentVersion?: string
}

// --- S02 staged commits and retry outcomes ---

export type CommitOpKind = 'insert' | 'update' | 'delete'

/** One staged row operation. Field shapes mirror the server's strict
 *  per-kind contract: inserts carry only values, updates carry
 *  key/version/column and value-or-isNull, deletes carry key/version. */
export interface CommitOperation {
  op: CommitOpKind
  schema: string
  table: string
  /** Relation binding from the table read. */
  binding: string
  /** insert: column -> wire cell; omitted column = DEFAULT, null = SQL NULL. */
  values?: Record<string, unknown>
  /** update/delete: full key tuple as wire cells. */
  key?: KeyCell[]
  /** update/delete: row version at read time. */
  version?: string
  /** update: target column. */
  column?: string
  /** update: new wire value (mutually exclusive with isNull). */
  value?: unknown
  /** update: set the column to SQL NULL. */
  isNull?: boolean
}

/** Per-operation result of a committed batch. */
export interface CommitOpResult {
  index: number
  op: CommitOpKind
  rowsAffected: number
  /** Inserted row's key cells (refreshes the identity). */
  key?: KeyCell[]
  /** New row version after update/insert. */
  version?: string
}

/** Commit/revert outcome envelope. Errors surface via ApiError states. */
export interface CommitResponse {
  operationId: string
  rowsAffected: number
  operations: CommitOpResult[]
  /** Whether this commit can be undone through /table/v2/revert. */
  reversible: boolean
  reversibleReason?: string
  /** True when this body was replayed from the recorded outcome. */
  replayed?: boolean
  /** Revert responses name the reverted operation ID. */
  reverted?: string
}

/** One operation's dry-run diff from /table/v2/preview. */
export interface PreviewOpDiff {
  index: number
  op: CommitOpKind
  schema: string
  table: string
  key?: KeyCell[]
  /** update: the edited column. */
  column?: string
  /** update/delete: the pre-commit value(s) as wire cells. */
  before?: unknown
  /** update: the post-commit value; insert: the values map. */
  after?: unknown
}

export interface PreviewResponse {
  ok: boolean
  counts: Record<string, number>
  operations: PreviewOpDiff[]
  error?: string
  state?: string
}

/** Recorded-outcome lookup from /table/v2/outcome. "unknown" is the honest
 *  answer for never-seen, expired, evicted and post-restart IDs: the client
 *  must verify table state before any retry, never auto-recommit. */
export type OutcomeState = 'committed' | 'failed' | 'unknown' | 'in_progress'

export interface OutcomeResponse {
  operationId: string
  state: OutcomeState
  /** HTTP status the commit itself returned/would return. */
  status?: number
  /** The recorded outcome body (present for terminal states). */
  response?: CommitResponse
  reversible?: boolean
  reversibleReason?: string
  error?: string
}

export interface KeyedQueryResult extends QueryResult {
  keyColumns: string[]
  versions: string[]
}

// --- S03 data editor ---

/** One staged cell value: an explicit value (editable text), SQL NULL, or
 *  DEFAULT (insert only — an omitted column). The three never coerce into
 *  each other; the wire discipline is the S01 contract. */
export type CellEdit =
  | { kind: 'value'; text: string }
  | { kind: 'null' }
  | { kind: 'default' }

/** One ANDed filter component of a multi-filter read. */
export interface TableFilter {
  column: string
  op: string
  value?: string
}

/** One key of a multi-sort read; earlier entries take precedence. */
export interface TableSort {
  column: string
  dir: 'asc' | 'desc'
}

// --- Tab types ---

// --- Schema designer types ---

export interface ColumnDetail {
  name: string
  dataType: string
  isNullable: boolean
  default: string | null
  isPrimaryKey: boolean
  ordinal: number
}

export interface FKDetail {
  /** Constraint name (grouped shape). */
  name?: string
  /** Legacy per-column spelling, set for single-column FKs. */
  column?: string
  refSchema: string
  refTable: string
  refColumn?: string
  /** Complete tuples: every local column and every referenced column, in
   *  constraint order. Composite FK navigation targets the whole tuple. */
  columns?: string[]
  refColumns?: string[]
  composite?: boolean
}

export interface IndexDetail {
  name: string
  columns: string[]
  isUnique: boolean
}

export interface SavedQuery {
  id: string
  name: string
  sql: string
  createdAt: string
}

export interface QueryHistoryEntry {
  sql: string
  executedAt: string
  duration: number
  rowCount: number
  /** Bound parameter values ($1..$n) the statement ran with; null is SQL NULL. */
  params?: (string | null)[]
  /** Outcome; absent on entries written before S04 (treated as ok). */
  status?: 'ok' | 'error' | 'canceled'
}

// --- SQL editor: cancellation and EXPLAIN (S04) ---

export interface CancelQueryResponse {
  requestId: string
  /** sent: pg_cancel_backend delivered to the running backend;
   *  canceled-before-dispatch: the statement was still queued and never ran. */
  state: 'sent' | 'canceled-before-dispatch'
  method?: string
}

/** A successful EXPLAIN: PostgreSQL's FORMAT JSON document, unmodified. */
export interface ExplainPlan {
  ok: true
  requestId: string
  engine: string
  format: 'json'
  plan: unknown
  analyze: boolean
  /** True only for ANALYZE: the statement was executed. */
  executed: boolean
  writesAllowed: boolean
  readOnly: boolean
  /** Always false: Studio never commits from EXPLAIN. */
  committed: false
  duration: number
}

/** An EXPLAIN that produced no plan, with the reason. */
export interface ExplainRefusal {
  ok: false
  /** unsupported: no plan exists for this statement/engine;
   *  write-blocked: ANALYZE would write and writes were not allowed;
   *  canceled / sql-error: execution outcome. */
  state: 'unsupported' | 'write-blocked' | 'canceled' | 'sql-error'
  error: string
  requestId?: string
  sqlState?: string
  executed?: boolean
  engine?: string
  connectionReused?: boolean
}

export type ExplainOutcome = ExplainPlan | ExplainRefusal

export type TabKind =
  | 'sql-browser'
  | 'sql-editor'
  | 'schema-designer'
  | 'schema-inspector'
  | 'diagnostics'
  | 'kv'
  | 'vector'
  | 'timeseries'
  | 'document'
  | 'graph'
  | 'fts'
  | 'geo'
  | 'blob'
  | 'pubsub'
  | 'streams'
  | 'columnar'
  | 'datalog'
  | 'cdc'
  | 'connection-manager'

export interface Tab {
  id: string
  kind: TabKind
  label: string
  // context: which object is open
  objectSchema?: string
  objectName?: string
  /** Pre-applied SQL-browser filter. */
  filter?: { column: string; op: string; value: string }
  /** Pre-applied full-tuple equality filter (FK follow, incl. composite). */
  match?: MatchCell[]
  /** SQL editor (S05): initial statement text (e.g. from a slow-query entry). */
  initialSql?: string
}

// --- Pending changes ---

export interface PendingChange {
  id: string
  model: string
  label: string       // human-readable: "users.name: 'Alice' → 'Bob'"
  sql: string         // the SQL to execute on commit
  revert: () => void  // fn to undo the local state change
}

// --- S05: schema navigation and performance diagnosis ---

/** Object detail from GET /api/schema/object (introspection v2). */
export interface SchemaObjectDetail {
  kind: 'table' | 'view' | 'opaque'
  schema: string
  name: string
  source: 'introspection-v2'
  documentSHA256: string
  table?: {
    columns: Array<{
      name: string
      type: string
      notNull: boolean
      isPrimaryKey: boolean
      default?: { kind: string; sql?: string }
      generated?: { expression: string }
    }>
    constraints: Array<{
      name: string
      type: string
      columns?: string[]
      expression?: string
      references?: { table: string; columns: string[]; onDelete?: string; onUpdate?: string; match?: string }
      deferrable?: boolean
      initiallyDeferred?: boolean
    }>
    indexes: Array<{
      name: string
      unique: boolean
      method: string
      key: Array<{ column?: string; expression?: string; order?: string; nulls?: string; opclass?: string }>
      where?: string
      include?: string[]
    }>
    /** Outgoing foreign keys: the other table by schema/name; columns are
     *  the referencing side, refColumns the referenced side (ordered tuples). */
    references: SchemaFKEdge[]
    /** Incoming foreign keys from other tables, same shape. */
    referencedBy: SchemaFKEdge[]
  }
  view?: { definition: string; checkOption?: string; securityInvoker?: boolean }
  opaque?: { opaqueKind: string; reason: string; owner?: string }
}

/** One visual designer edit (POST /api/schema/plan | apply). */
export interface SchemaChange {
  op:
    | 'create-table' | 'drop-table'
    | 'add-column' | 'drop-column' | 'rename-column' | 'alter-column-type'
    | 'set-not-null' | 'drop-not-null' | 'set-default' | 'drop-default'
    | 'add-index' | 'drop-index'
  schema: string
  table: string
  column?: string
  from?: string
  to?: string
  type?: string
  notNull?: boolean
  default?: string
  index?: string
  unique?: boolean
  columns?: Array<{ name: string; type: string; notNull: boolean; default?: string; isPrimaryKey: boolean }>
}

/** One foreign-key relationship as seen from an inspected table. */
export interface SchemaFKEdge {
  constraint: string
  schema: string
  name: string
  columns: string[]
  refColumns: string[]
}

/** One classified plan statement (the M03 plan builder's operation). */
export interface PlanOperation {
  index: number
  sql: string
  down: string
  destructive: boolean
  dataLoss: boolean
  reversibility: 'reversible' | 'irreversible' | 'manual'
}

/** A reviewable migration plan: the CLI planner's own statements for the
 * designer's changes, with the M03 risk report and the target document. */
export interface SchemaPlanResponse {
  /** Binds an apply to exactly this plan's statements. */
  planId: string
  /** Canonical hash of the live schema document the plan starts from. */
  baseSha256: string
  targetSha256: string
  up: string[]
  down: string[]
  warnings: string[]
  operations: PlanOperation[]
  risk: {
    hasDestructive: boolean
    hasDataLoss: boolean
    statementCount: number
    irreversibleCount: number
    overallReversibility: 'reversible' | 'irreversible' | 'manual'
  }
  transactionMode: string
  /** `--rename` values reproducing the plan's renames in the CLI. */
  renameFlags: string[]
  /** Objects PostgreSQL removes together with a dropped column, made explicit. */
  designerNotes: string[]
  /** The same plan through the CLI, with `target` saved as target.schema.json. */
  cliEquivalent: string
  /** The target schema document v2 (canonical JSON). */
  target: unknown
  applied: boolean
  /** Apply only: "in-sync", "drift" (see residual) or "unverified: …". */
  verification?: string
  residual?: string[]
}

/** One recorded statement execution (GET /api/diagnostics/queries). */
export interface LoggedStatement {
  at: string
  connectionId: string
  surface: string
  requestId?: string
  sql: string
  durationMs: number
  rowCount?: number
  state: 'ok' | 'error' | 'canceled'
  error?: string
}

export interface DiagnosticsQueriesResponse {
  entries: LoggedStatement[]
  stats: { count: number; p50Ms: number; p95Ms: number; maxMs: number }
  scope: string
  pgStatStatements?:
    | { available: true; statements: Array<{ query: string; calls: number; totalMs: number; meanMs: number }>; note: string }
    | { available: false; reason: string }
}

/** Per-table usage statistics (GET /api/diagnostics/table-stats). */
export interface TableStatsResponse {
  schema: string
  table: string
  stats: {
    seqScan: number
    idxScan: number
    idxTupFetch: number
    nLiveTup: number
    nDeadTup: number
    nModSinceAnalyze: number
    lastAnalyze?: string | null
    lastAutoAnalyze?: string | null
    lastVacuum?: string | null
    lastAutoVacuum?: string | null
  }
  indexes: Array<{ name: string; definition: string; unique: boolean; idxScan: number; idxTupRead: number; sizeBytes: number }>
  notes: string[]
  relSizeBytes?: number
  totalSizeBytes?: number
  sizeUnavailableReason?: string
}

// --- S06: streamed export and batched import ---

export type ExportFormat = 'csv' | 'json' | 'ndjson'

/** A validated export awaiting its single streamed download. */
export interface ExportTicket {
  ticket: string
  /** Same-origin download URL; redeemable once, shortly. */
  url: string
  filename: string
  format: ExportFormat
  expiresIn: number
}

/** One import batch: insert-only rows, committed atomically as a unit. */
export interface ImportBatchRequest {
  connectionId: string
  /** Client-generated idempotency key for this batch attempt. */
  operationId: string
  schema: string
  table: string
  binding: string
  /** column -> wire cell; an omitted column is DEFAULT, null is SQL NULL. */
  rows: Array<Record<string, unknown>>
}

export interface ImportBatchResponse {
  operationId: string
  /** Rows inserted by this batch (all of them, or the batch failed). */
  applied: number
  replayed?: boolean
}

export interface ImportOutcomeResponse {
  operationId: string
  state: OutcomeState
  status?: number
  response?: Record<string, unknown>
  error?: string
}
