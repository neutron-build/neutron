import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { api } from './api'

describe('api', () => {
  const mockFetch = vi.fn()

  beforeEach(() => {
    vi.stubGlobal('fetch', mockFetch)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  function mockOk(data: unknown) {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve(data),
    })
  }

  function mockError(status: number, text: string) {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status,
      text: () => Promise.resolve(text),
    })
  }

  describe('connections', () => {
    it('list: should GET /api/connections', async () => {
      const conns = [{ id: 'c1', name: 'Test', url: 'pg://test', isNucleus: false }]
      mockOk(conns)

      const result = await api.connections.list()
      expect(result).toEqual(conns)
      expect(mockFetch).toHaveBeenCalledWith('/api/connections', {
        method: 'GET',
        headers: undefined,
        body: undefined,
      })
    })

    it('add: should POST /api/connections with body', async () => {
      const conn = { id: 'c1', name: 'New', url: 'pg://new', isNucleus: false }
      mockOk(conn)

      const result = await api.connections.add({ name: 'New', url: 'pg://new' })
      expect(result).toEqual(conn)
      expect(mockFetch).toHaveBeenCalledWith('/api/connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New', url: 'pg://new' }),
      })
    })

    it('remove: should DELETE /api/connections/:id', async () => {
      mockOk(undefined)

      await api.connections.remove('c1')
      expect(mockFetch).toHaveBeenCalledWith('/api/connections/c1', {
        method: 'DELETE',
        headers: undefined,
        body: undefined,
      })
    })

    it('test: should POST /api/connections/test', async () => {
      mockOk({ ok: true, isNucleus: true, version: '0.1.0' })

      const result = await api.connections.test('pg://test')
      expect(result.ok).toBe(true)
      expect(result.isNucleus).toBe(true)
      expect(mockFetch).toHaveBeenCalledWith('/api/connections/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: 'pg://test' }),
      })
    })

    it('connect: should POST /api/connections/:id/connect', async () => {
      mockOk({
        features: { isNucleus: true, version: '0.1.0', models: ['sql'] },
        schema: { sql: [], kv: [], vector: [], timeseries: [], document: [], graph: [], fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false },
      })

      const result = await api.connections.connect('c1')
      expect(result.features.isNucleus).toBe(true)
    })
  })

  describe('query', () => {
    it('should POST /api/query with SQL and connectionId', async () => {
      mockOk({ columns: ['id'], rows: [[1]], rowCount: 1, duration: 2 })

      const result = await api.query('SELECT 1', 'c1')
      expect(result.columns).toEqual(['id'])
      expect(mockFetch).toHaveBeenCalledWith('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1', connectionId: 'c1' }),
      })
    })

    it('should pass params when provided', async () => {
      mockOk({ columns: [], rows: [], rowCount: 0, duration: 1 })

      await api.query('SELECT $1', 'c1', [42])
      expect(mockFetch).toHaveBeenCalledWith('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT $1', connectionId: 'c1', params: [42] }),
      })
    })
  })

  describe('schema', () => {
    it('should GET /api/schema with connectionId', async () => {
      mockOk({ sql: [], kv: [] })

      await api.schema('c1')
      expect(mockFetch).toHaveBeenCalledWith(
        '/api/schema?connectionId=c1',
        { method: 'GET', headers: undefined, body: undefined }
      )
    })
  })

  describe('features', () => {
    it('should GET /api/features', async () => {
      mockOk({ isNucleus: true, version: '0.1.0', models: ['sql'] })

      const result = await api.features('c1')
      expect(result.isNucleus).toBe(true)
    })
  })

  describe('tableData', () => {
    it('should GET /api/table with pagination params', async () => {
      mockOk({ columns: ['id'], rows: [[1]], rowCount: 1, duration: 5 })

      await api.tableData('c1', 'public', 'users', 100, 50)
      expect(mockFetch).toHaveBeenCalledWith(
        '/api/table?connectionId=c1&schema=public&table=users&limit=100&offset=50',
        { method: 'GET', headers: undefined, body: undefined }
      )
    })

    it('should use default limit and offset', async () => {
      mockOk({ columns: [], rows: [], rowCount: 0, duration: 0 })

      await api.tableData('c1', 'public', 'users')
      expect(mockFetch).toHaveBeenCalledWith(
        '/api/table?connectionId=c1&schema=public&table=users&limit=200&offset=0',
        expect.anything()
      )
    })
  })

  describe('columns', () => {
    it('should GET /api/columns with encoded params', async () => {
      mockOk({ columns: [], indexes: [] })

      await api.columns('c1', 'my schema', 'my table')
      expect(mockFetch).toHaveBeenCalledWith(
        `/api/columns?connectionId=c1&schema=${encodeURIComponent('my schema')}&table=${encodeURIComponent('my table')}`,
        expect.anything()
      )
    })
  })

  describe('ddl', () => {
    it('should POST /api/ddl', async () => {
      mockOk({ ok: true, duration: 12 })

      const result = await api.ddl('c1', 'CREATE TABLE test (id int)')
      expect(result.ok).toBe(true)
    })
  })

  describe('codegen', () => {
    it('should GET /api/codegen', async () => {
      mockOk({ code: 'type User struct { ID int }' })

      const result = await api.codegen('c1', 'public', 'users', 'go')
      expect(result.code).toContain('User')
    })
  })

  describe('savedQueries', () => {
    it('list: should GET /api/saved-queries', async () => {
      mockOk([{ id: 'sq1', name: 'test', sql: 'SELECT 1', createdAt: '' }])

      const result = await api.savedQueries.list()
      expect(result.length).toBe(1)
    })

    it('save: should POST /api/saved-queries', async () => {
      mockOk({ id: 'sq1', name: 'test', sql: 'SELECT 1', createdAt: '' })

      const result = await api.savedQueries.save('test', 'SELECT 1')
      expect(result.name).toBe('test')
    })

    it('remove: should DELETE /api/saved-queries/:id', async () => {
      mockOk(undefined)

      await api.savedQueries.remove('sq1')
      expect(mockFetch).toHaveBeenCalledWith('/api/saved-queries/sq1', {
        method: 'DELETE',
        headers: undefined,
        body: undefined,
      })
    })
  })

  describe('error handling', () => {
    it('should throw on non-ok response with error text', async () => {
      mockError(400, 'Bad request')
      await expect(api.connections.list()).rejects.toThrow('Bad request')
    })

    it('should throw with HTTP status when no error text', async () => {
      mockError(500, '')
      await expect(api.connections.list()).rejects.toThrow('HTTP 500')
    })
  })
})
