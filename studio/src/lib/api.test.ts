import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { api, ApiError, _setSessionTokenForTests } from './api'

describe('api', () => {
  const mockFetch = vi.fn()

  beforeEach(() => {
    vi.stubGlobal('fetch', mockFetch)
    _setSessionTokenForTests('test-session-token')
  })

  afterEach(() => {
    vi.restoreAllMocks()
    _setSessionTokenForTests(null)
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

  function mockJSONError(status: number, body: unknown) {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status,
      text: () => Promise.resolve(JSON.stringify(body)),
    })
  }

  const sessionHeaders = {
    'Content-Type': 'application/json',
    'X-Studio-Session': 'test-session-token',
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

    it('add: should POST /api/connections with body and session token', async () => {
      const conn = { id: 'c1', name: 'New', url: 'pg://new', isNucleus: false }
      mockOk(conn)

      const result = await api.connections.add({ name: 'New', url: 'pg://new' })
      expect(result).toEqual(conn)
      expect(mockFetch).toHaveBeenCalledWith('/api/connections', {
        method: 'POST',
        headers: sessionHeaders,
        body: JSON.stringify({ name: 'New', url: 'pg://new' }),
      })
    })

    it('remove: should DELETE /api/connections/:id with session token', async () => {
      mockOk(undefined)

      await api.connections.remove('c1')
      expect(mockFetch).toHaveBeenCalledWith('/api/connections/c1', {
        method: 'DELETE',
        headers: sessionHeaders,
        body: undefined,
      })
    })

    it('test: should POST /api/connections/test with the session token', async () => {
      // The server gates every state-changing /api/ request centrally; a
      // connection test opens a network connection, so it is one.
      mockOk({ ok: true, isNucleus: true, version: '0.1.0' })

      const result = await api.connections.test('pg://test')
      expect(result.ok).toBe(true)
      expect(result.isNucleus).toBe(true)
      expect(mockFetch).toHaveBeenCalledWith('/api/connections/test', {
        method: 'POST',
        headers: sessionHeaders,
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
    it('should POST /api/query with SQL, connectionId and the session token', async () => {
      // The SQL editor can execute mutations; the server guards it like the
      // row endpoints, so the token must be presented.
      mockOk({ columns: ['id'], rows: [[1]], rowCount: 1, duration: 2 })

      const result = await api.query('SELECT 1', 'c1')
      expect(result.columns).toEqual(['id'])
      expect(mockFetch).toHaveBeenCalledWith('/api/query', {
        method: 'POST',
        headers: sessionHeaders,
        body: JSON.stringify({ sql: 'SELECT 1', connectionId: 'c1' }),
      })
    })

    it('decodes tagged lossless cells in query results, passes plain cells through', async () => {
      mockOk({
        columns: ['id', 'balance', 'payload', 'posted_at', 'name'],
        rows: [
          [{ t: 'int8', v: '9007199254740993' }, { t: 'numeric', v: '1.50' }, { t: 'bytea', v: '00ff10' }, { t: 'timestamptz', v: '2026-03-08T07:30:00.123456Z' }, 'plain'],
        ],
        rowCount: 1,
        duration: 2,
      })

      const result = await api.query('SELECT * FROM t', 'c1')
      const row = result.rows[0]
      expect(row[0]).toBe(9007199254740993n)
      expect(row[1]).toBe('1.50')
      expect(Array.from(row[2] as Uint8Array)).toEqual([0x00, 0xff, 0x10])
      expect(row[3]).toBe('2026-03-08T07:30:00.123456Z')
      expect(row[4]).toBe('plain')
    })

    it('should pass params when provided', async () => {
      mockOk({ columns: [], rows: [], rowCount: 0, duration: 1 })

      await api.query('SELECT $1', 'c1', [42])
      expect(mockFetch).toHaveBeenCalledWith('/api/query', {
        method: 'POST',
        headers: sessionHeaders,
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

    it('remove: should DELETE /api/saved-queries/:id with session token', async () => {
      mockOk(undefined)

      await api.savedQueries.remove('sq1')
      expect(mockFetch).toHaveBeenCalledWith('/api/saved-queries/sq1', {
        method: 'DELETE',
        headers: sessionHeaders,
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

  describe('session token bootstrap', () => {
    it('fetches the token lazily once and presents it on mutations', async () => {
      _setSessionTokenForTests(null)
      mockOk({ token: 'fresh-launch-token' })
      mockOk({ rowsAffected: 1 })

      await api.tableInsert({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', values: { a: 1 } })

      expect(mockFetch).toHaveBeenNthCalledWith(1, '/api/session')
      expect(mockFetch).toHaveBeenNthCalledWith(2, '/api/table/v2/insert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Studio-Session': 'fresh-launch-token' },
        body: JSON.stringify({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', values: { a: 1 } }),
      })

      // Second mutation reuses the cached token without refetching.
      mockOk({ rowsAffected: 1 })
      await api.tableInsert({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', values: { a: 2 } })
      expect(mockFetch).toHaveBeenCalledTimes(3)
    })

    it('a failed session fetch is not cached: the next mutation fetches again', async () => {
      _setSessionTokenForTests(null)
      mockFetch.mockResolvedValueOnce({ ok: false, status: 404, text: () => Promise.resolve('') })
      mockOk({ rowsAffected: 1 })

      await api.tableDeleteV2({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', key: [{ column: 'id', value: 1 }], version: '5' })
      expect(mockFetch).toHaveBeenNthCalledWith(2, '/api/table/v2/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', key: [{ column: 'id', value: 1 }], version: '5' }),
      })

      mockOk({ token: 'late-token' })
      mockOk({ rowsAffected: 1 })
      await api.tableDeleteV2({ connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1', key: [{ column: 'id', value: 2 }], version: '5' })
      expect(mockFetch).toHaveBeenNthCalledWith(3, '/api/session')
      expect(mockFetch.mock.calls[3][1].headers['X-Studio-Session']).toBe('late-token')
    })

    it('a stale session token (server restarted) refetches once and retries', async () => {
      _setSessionTokenForTests('old-launch-token')
      mockJSONError(403, { error: 'missing or invalid session token', auth: 'session' })
      mockOk({ token: 'new-launch-token' })
      mockOk({ rowsAffected: 1, version: '2' })

      const res = await api.tableUpdateV2({
        connectionId: 'c1', schema: 'public', table: 't', binding: 'e:1',
        key: [{ column: 'id', value: 1 }], version: '1', column: 'a', value: 'x',
      })
      expect(res.version).toBe('2')
      expect(mockFetch).toHaveBeenCalledTimes(3)
      expect(mockFetch.mock.calls[0][1].headers['X-Studio-Session']).toBe('old-launch-token')
      expect(mockFetch.mock.calls[1][0]).toBe('/api/session')
      expect(mockFetch.mock.calls[2][1].headers['X-Studio-Session']).toBe('new-launch-token')
      // The retried request is byte-identical apart from the token.
      expect(mockFetch.mock.calls[2][1].body).toBe(mockFetch.mock.calls[0][1].body)
    })

    it('an origin refusal is never retried', async () => {
      mockJSONError(403, { error: 'origin not allowed', auth: 'origin' })
      const err = await api.query('DELETE FROM t', 'c1').catch(e => e as ApiError)
      expect(err).toBeInstanceOf(ApiError)
      expect(err.auth).toBe('origin')
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('a persistently invalid token surfaces the auth error after one retry', async () => {
      mockJSONError(403, { error: 'missing or invalid session token', auth: 'session' })
      mockOk({ token: 'still-wrong' })
      mockJSONError(403, { error: 'missing or invalid session token', auth: 'session' })
      const err = await api.ddl('c1', 'DROP TABLE t').catch(e => e as ApiError)
      expect(err.status).toBe(403)
      expect(err.auth).toBe('session')
      expect(mockFetch).toHaveBeenCalledTimes(3)
    })
  })

  describe('v2 mutation protocol', () => {
    it('tableMeta GETs the authoritative metadata', async () => {
      mockOk({ exists: true, keyColumns: ['id'], versioned: true, readOnly: false, columns: [] })

      const meta = await api.tableMeta('c1', 'public', 'users')
      expect(meta.keyColumns).toEqual(['id'])
      expect(mockFetch).toHaveBeenCalledWith(
        '/api/table/v2/meta?connectionId=c1&schema=public&table=users',
        expect.anything()
      )
    })

    it('tableUpdateV2 posts the full-key identity and version', async () => {
      mockOk({ rowsAffected: 1, version: '99' })

      const res = await api.tableUpdateV2({
        connectionId: 'c1', schema: 'public', table: 'docs', binding: 'e1:16385',
        key: [{ column: 'tenant_id', value: 1 }, { column: 'id', value: 2 }],
        version: '98', column: 'payload', value: 'x', isNull: false,
      })
      expect(res.version).toBe('99')
      expect(mockFetch).toHaveBeenCalledWith('/api/table/v2/update', {
        method: 'POST',
        headers: sessionHeaders,
        body: JSON.stringify({
          connectionId: 'c1', schema: 'public', table: 'docs', binding: 'e1:16385',
          key: [{ column: 'tenant_id', value: 1 }, { column: 'id', value: 2 }],
          version: '98', column: 'payload', value: 'x', isNull: false,
        }),
      })
    })

    it('a 409 conflict body becomes an ApiError carrying the conflict state', async () => {
      // The server's actual 409 body shape: {"error", "state", "currentVersion"}.
      mockJSONError(409, {
        state: 'conflict',
        currentVersion: '1234',
        error: 'update refused: row changed since it was read (current row version 1234)',
      })

      const err = await api.tableUpdateV2({
        connectionId: 'c1', schema: 'public', table: 't', binding: 'e1:9',
        key: [{ column: 'id', value: 1 }], version: '1', column: 'a', value: 'x',
      }).catch(e => e as ApiError)

      expect(err).toBeInstanceOf(ApiError)
      expect(err.status).toBe(409)
      expect(err.conflict).toBe(true)
      expect(err.missing).toBe(false)
      expect(err.currentVersion).toBe('1234')
      expect(err.message).toContain('row changed since it was read')
    })

    it('a 409 missing body becomes an ApiError with the missing state', async () => {
      mockJSONError(409, { state: 'missing', error: 'matched no row' })

      const err = await api.tableDeleteV2({
        connectionId: 'c1', schema: 'public', table: 't', binding: 'e1:9',
        key: [{ column: 'id', value: 9 }], version: '1',
      }).catch(e => e as ApiError)

      expect(err.status).toBe(409)
      expect(err.missing).toBe(true)
      expect(err.conflict).toBe(false)
    })

    it('tableInsert posts values with the binding and decodes the new identity', async () => {
      mockOk({ rowsAffected: 1, version: '7', key: [{ column: 'id', value: 42 }], binding: 'e1:9' })

      const res = await api.tableInsert({
        connectionId: 'c1', schema: 'public', table: 't', binding: 'e1:9',
        values: { name: 'x' },
      })
      expect(res.version).toBe('7')
      expect(res.key).toEqual([{ column: 'id', value: 42 }])
      expect(mockFetch).toHaveBeenCalledWith('/api/table/v2/insert', {
        method: 'POST',
        headers: sessionHeaders,
        body: JSON.stringify({
          connectionId: 'c1', schema: 'public', table: 't', binding: 'e1:9',
          values: { name: 'x' },
        }),
      })
    })

    it('tableData results carry keyColumns and versions through decode', async () => {
      mockOk({
        columns: ['id', 'body'],
        rows: [[{ t: 'int8', v: '9007199254740993' }, 'x']],
        rowCount: 1,
        duration: 0,
        keyColumns: ['id'],
        versions: ['42'],
        versioned: true,
      })

      const result = await api.tableData('c1', 'public', 't')
      expect(result.rows[0][0]).toBe(9007199254740993n)
      const keyed = result as import('./types').KeyedQueryResult
      expect(keyed.keyColumns).toEqual(['id'])
      expect(keyed.versions).toEqual(['42'])
    })
  })

  describe('S01 identity binding and full-tuple navigation', () => {
    it('a 409 binding body marks the identity stale', async () => {
      mockJSONError(409, { state: 'binding', error: 'public.t is no longer the relation these rows were read from' })
      const err = await api.tableDeleteV2({
        connectionId: 'c1', schema: 'public', table: 't', binding: 'old:1',
        key: [{ column: 'id', value: 1 }], version: '1',
      }).catch(e => e as ApiError)
      expect(err.status).toBe(409)
      expect(err.state).toBe('binding')
      expect(err.binding).toBe(true)
      expect(err.conflict).toBe(false)
      expect(err.missing).toBe(false)
    })

    it('tableData sends a full-tuple match as one JSON parameter', async () => {
      mockOk({ columns: [], rows: [], rowCount: 0, duration: 0 })
      const match = [
        { column: 'tenant_id', value: 1 },
        { column: 'id', value: { t: 'int8', v: '9007199254740993' } },
      ]
      await api.tableData('c1', 'public', 'docs', 200, 0, undefined, undefined, match)
      const url = new URL('http://x' + mockFetch.mock.calls[0][0])
      expect(url.pathname).toBe('/api/table')
      expect(JSON.parse(url.searchParams.get('match')!)).toEqual(match)
      // A GET read carries no session header.
      expect(mockFetch.mock.calls[0][1].headers).toBeUndefined()
    })

    it('large keys survive the JSON round trip: tagged cells are sent verbatim', async () => {
      mockOk({ rowsAffected: 1, version: '9' })
      await api.tableUpdateV2({
        connectionId: 'c1', schema: 'public', table: 'big', binding: 'e:1',
        key: [{ column: 'id', value: { t: 'int8', v: '9223372036854775807' } }],
        version: '8', column: 'v', value: 'x',
      })
      const sent = JSON.parse(mockFetch.mock.calls[0][1].body)
      expect(sent.key[0].value).toEqual({ t: 'int8', v: '9223372036854775807' })
    })
  })
})
