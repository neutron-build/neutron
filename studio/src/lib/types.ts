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
}

// --- Pending changes ---

export interface PendingChange {
  id: string
  model: string
  label: string       // human-readable: "users.name: 'Alice' → 'Bob'"
  sql: string         // the SQL to execute on commit
  revert: () => void  // fn to undo the local state change
}
