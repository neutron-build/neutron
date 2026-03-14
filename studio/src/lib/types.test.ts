import { describe, it, expect } from 'vitest'
import type {
  Connection, ConnectionInput, TestResult, Schema, NucleusFeatures,
  QueryResult, Tab, TabKind, PendingChange, ColumnDetail, IndexDetail,
  SavedQuery, QueryHistoryEntry, SqlTable, SqlColumn, KvStore,
  VectorIndex, TsMetric, DocCollection, GraphStore, FtsIndex,
  GeoLayer, BlobStore, PubSubChannel, Stream, ColumnarTable, DatalogStore,
} from './types'

describe('types', () => {
  it('should allow creating a Connection', () => {
    const conn: Connection = {
      id: 'c1',
      name: 'My DB',
      url: 'postgres://user:***@localhost/db',
      isNucleus: true,
      nucleusVersion: '0.1.0',
      pgVersion: '16.1',
      lastConnected: '2025-01-01T00:00:00Z',
    }
    expect(conn.id).toBe('c1')
    expect(conn.isNucleus).toBe(true)
  })

  it('should allow creating a ConnectionInput', () => {
    const input: ConnectionInput = {
      name: 'Test',
      url: 'postgres://user:pass@host/db',
    }
    expect(input.name).toBe('Test')
    expect(input.url).toContain('postgres://')
  })

  it('should allow creating a TestResult', () => {
    const success: TestResult = { ok: true, isNucleus: true, version: '0.1.0' }
    expect(success.ok).toBe(true)

    const failure: TestResult = { ok: false, isNucleus: false, version: '', error: 'Connection refused' }
    expect(failure.ok).toBe(false)
    expect(failure.error).toBe('Connection refused')
  })

  it('should allow creating a Schema with all 14 models', () => {
    const schema: Schema = {
      sql: [{ schema: 'public', name: 'users', columns: [], rowCount: 42 }],
      kv: [{ name: 'cache', keyCount: 100 }],
      vector: [{ name: 'embeddings', dimensions: 384, metric: 'cosine', count: 1000 }],
      timeseries: [{ name: 'cpu_usage', count: 5000, minTs: '2025-01-01', maxTs: '2025-06-01' }],
      document: [{ name: 'docs', count: 50 }],
      graph: [{ name: 'social', nodeCount: 200, edgeCount: 500 }],
      fts: [{ name: 'articles', docCount: 1000 }],
      geo: [{ name: 'locations', pointCount: 300 }],
      blob: [{ name: 'files', blobCount: 25 }],
      pubsub: [{ name: 'events' }],
      streams: [{ name: 'logs', length: 10000 }],
      columnar: [{ name: 'analytics', rowCount: 1000000 }],
      datalog: { predicateCount: 10, ruleCount: 5 },
      cdc: true,
    }
    expect(schema.sql.length).toBe(1)
    expect(schema.kv[0].keyCount).toBe(100)
    expect(schema.vector[0].dimensions).toBe(384)
    expect(schema.datalog?.predicateCount).toBe(10)
    expect(schema.cdc).toBe(true)
  })

  it('should allow creating a QueryResult', () => {
    const result: QueryResult = {
      columns: ['id', 'name', 'email'],
      rows: [[1, 'Alice', 'alice@test.com'], [2, 'Bob', 'bob@test.com']],
      rowCount: 2,
      duration: 3.5,
    }
    expect(result.columns.length).toBe(3)
    expect(result.rows.length).toBe(2)
    expect(result.duration).toBe(3.5)
  })

  it('should allow creating a QueryResult with error', () => {
    const result: QueryResult = {
      columns: [],
      rows: [],
      rowCount: 0,
      duration: 0,
      error: 'syntax error at position 5',
    }
    expect(result.error).toBeTruthy()
  })

  it('should validate all TabKind values', () => {
    const kinds: TabKind[] = [
      'sql-browser', 'sql-editor', 'schema-designer',
      'kv', 'vector', 'timeseries', 'document', 'graph',
      'fts', 'geo', 'blob', 'pubsub', 'streams',
      'columnar', 'datalog', 'cdc', 'connection-manager',
    ]
    expect(kinds.length).toBe(17)
  })

  it('should allow creating a Tab with context', () => {
    const tab: Tab = {
      id: 'tab-1',
      kind: 'sql-browser',
      label: 'users',
      objectSchema: 'public',
      objectName: 'users',
    }
    expect(tab.kind).toBe('sql-browser')
    expect(tab.objectSchema).toBe('public')
  })

  it('should allow creating a PendingChange', () => {
    let reverted = false
    const change: PendingChange = {
      id: 'pc-1',
      model: 'sql',
      label: "users.name: 'Alice' -> 'Bob'",
      sql: "UPDATE users SET name = 'Bob' WHERE id = 1",
      revert: () => { reverted = true },
    }
    expect(change.model).toBe('sql')
    change.revert()
    expect(reverted).toBe(true)
  })

  it('should allow creating ColumnDetail and IndexDetail', () => {
    const col: ColumnDetail = {
      name: 'id',
      dataType: 'bigserial',
      isNullable: false,
      default: null,
      isPrimaryKey: true,
      ordinal: 1,
    }
    expect(col.isPrimaryKey).toBe(true)
    expect(col.isNullable).toBe(false)

    const idx: IndexDetail = {
      name: 'users_email_idx',
      columns: ['email'],
      isUnique: true,
    }
    expect(idx.isUnique).toBe(true)
    expect(idx.columns).toEqual(['email'])
  })

  it('should allow creating SavedQuery and QueryHistoryEntry', () => {
    const sq: SavedQuery = {
      id: 'sq-1',
      name: 'Active users',
      sql: 'SELECT * FROM users WHERE active = true',
      createdAt: '2025-01-01T00:00:00Z',
    }
    expect(sq.name).toBe('Active users')

    const qhe: QueryHistoryEntry = {
      sql: 'SELECT 1',
      executedAt: '2025-01-01T00:00:00Z',
      duration: 1.5,
      rowCount: 1,
    }
    expect(qhe.duration).toBe(1.5)
  })
})
