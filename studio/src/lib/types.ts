// Shared types used across the frontend

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
}

// --- Pending changes ---

export interface PendingChange {
  id: string
  model: string
  label: string       // human-readable: "users.name: 'Alice' → 'Bob'"
  sql: string         // the SQL to execute on commit
  revert: () => void  // fn to undo the local state change
}
