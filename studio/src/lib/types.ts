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
}

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
