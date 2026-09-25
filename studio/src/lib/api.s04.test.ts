import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { api, ApiError, _setSessionTokenForTests } from './api'

// Request shapes of the S04 SQL editor endpoints: every one is a mutating
// POST carrying the session token (the server gates them like the row
// endpoints), and EXPLAIN refusals resolve as data, not exceptions.

describe('api — SQL editor (S04)', () => {
  const mockFetch = vi.fn()
  const sessionHeaders = { 'Content-Type': 'application/json', 'X-Studio-Session': 'test-session-token' }

  beforeEach(() => {
    vi.stubGlobal('fetch', mockFetch)
    mockFetch.mockReset()
    _setSessionTokenForTests('test-session-token')
  })

  afterEach(() => {
    vi.restoreAllMocks()
    _setSessionTokenForTests(null)
  })

  function respond(status: number, body: unknown) {
    mockFetch.mockResolvedValueOnce({
      ok: status >= 200 && status < 300,
      status,
      json: () => Promise.resolve(body),
      text: () => Promise.resolve(JSON.stringify(body)),
    })
  }

  it('query sends params and the request id', async () => {
    respond(200, { columns: [], rows: [], rowCount: 0, duration: 1, requestId: 'req-00000001' })
    const res = await api.query('SELECT $1', 'c1', ['x', null], 'req-00000001')
    expect(res.requestId).toBe('req-00000001')
    expect(mockFetch).toHaveBeenCalledWith('/api/query', {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({ sql: 'SELECT $1', connectionId: 'c1', params: ['x', null], requestId: 'req-00000001' }),
    })
  })

  it('cancelQuery posts the connection and request id', async () => {
    respond(200, { requestId: 'req-00000001', state: 'sent', method: 'pg_cancel_backend' })
    const res = await api.cancelQuery('c1', 'req-00000001')
    expect(res.state).toBe('sent')
    expect(mockFetch).toHaveBeenCalledWith('/api/query/cancel', {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({ connectionId: 'c1', requestId: 'req-00000001' }),
    })
  })

  it('cancelQuery surfaces a finished statement as a 404 ApiError', async () => {
    respond(404, { error: 'no running statement', state: 'not-running', requestId: 'req-00000001' })
    await expect(api.cancelQuery('c1', 'req-00000001')).rejects.toMatchObject({ status: 404, state: 'not-running' })
  })

  it('explain returns the plan with ok: true', async () => {
    respond(200, { requestId: 'r', engine: 'postgresql', format: 'json', plan: [{ Plan: { 'Node Type': 'Result' } }], analyze: false, executed: false, writesAllowed: false, readOnly: true, committed: false, duration: 1 })
    const out = await api.explain({ connectionId: 'c1', sql: 'SELECT 1', analyze: false })
    expect(out).toMatchObject({ ok: true, executed: false, committed: false })
    expect(mockFetch).toHaveBeenCalledWith('/api/query/explain', {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({ connectionId: 'c1', sql: 'SELECT 1', analyze: false }),
    })
  })

  it('explain resolves 422 refusals as ok: false with the server state', async () => {
    respond(422, { error: 'EXPLAIN ANALYZE executes the statement, and this statement writes.', state: 'write-blocked', executed: false, sqlState: '25006' })
    const out = await api.explain({ connectionId: 'c1', sql: 'DELETE FROM t', analyze: true })
    expect(out).toEqual({ ok: false, error: 'EXPLAIN ANALYZE executes the statement, and this statement writes.', state: 'write-blocked', executed: false, sqlState: '25006' })
  })

  it('explain still throws request-shape errors', async () => {
    respond(400, { error: 'allowWrites applies only to EXPLAIN ANALYZE' })
    await expect(api.explain({ connectionId: 'c1', sql: 'SELECT 1', analyze: false, allowWrites: true })).rejects.toBeInstanceOf(ApiError)
  })
})
