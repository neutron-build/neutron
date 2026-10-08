import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { act } from 'preact/test-utils'
import { CDCModule } from './CDCModule'
import { activeConnection, toasts } from '../../lib/store'
import type { QueryResult } from '../../lib/types'
vi.mock('../../lib/api', async original => {
  const m = await original<typeof import('../../lib/api')>()
  return { ...m, api: { ...m.api, query: vi.fn(), cancelQuery: vi.fn() }, mutationHeaders: vi.fn() }
})
import { api, mutationHeaders } from '../../lib/api'
const query = vi.mocked(api.query)
const ok = (cell: unknown = 0): QueryResult => ({ columns: ['v'], rows: [[cell]], rowCount: 1, duration: 0 })
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const connect = (id: string) => act(() => { activeConnection.value = { id, name: id, url: 'pg://test', isNucleus: true } })
const finish = async (fn: () => void) => { await act(async () => { fn(); await Promise.resolve() }) }
beforeEach(() => {
  connect('c1'); toasts.value = []; query.mockReset()
  vi.mocked(api.cancelQuery).mockReset().mockResolvedValue({} as never)
  vi.mocked(mutationHeaders).mockReset().mockResolvedValue({})
})
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers() })

import { buildCdcQuery, parseCdcEvents, eventsToResult } from './CDCModule'
const events = (table: string) => JSON.stringify([{ seq: 1, table, change: 'INSERT', ts: 0 }])
const base = async (sql: string) => sql.includes('CDC_COUNT') ? ok(1) : ok(events('current-table'))
describe('CDC actual component polling/request ownership', () => {
  it('uses exported query/parser/result helpers and refuses corrupt event responses', () => {
    expect(buildCdcQuery(300, 100, "a'b")).toBe("SELECT CDC_TABLE_READ('a''b', 200, 100)")
    expect(eventsToResult(parseCdcEvents(events('A'))).rows[0][1]).toBe('A')
    expect(() => parseCdcEvents('broken')).toThrow(/unavailable/)
  })

  it('connection change cancels the original request and late count cannot dispatch another read', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation((sql, conn) => conn === 'c1' ? old.promise : base(sql))
    render(<CDCModule />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    const first = query.mock.calls[0]
    expect(first[1]).toBe('c1'); expect(first[3]).toEqual(expect.any(String))
    connect('c2')
    await screen.findAllByText('current-table')
    await waitFor(() => expect(api.cancelQuery).toHaveBeenCalledWith('c1', first[3]))
    await finish(() => old.resolve(ok(500)))
    expect(query.mock.calls.filter(([, conn]) => conn === 'c1')).toHaveLength(1)
    expect(screen.queryByText('500 events')).toBeNull()
  })

  it('filter changes fence an in-flight row result and its error/loading/scroll continuation', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('CDC_READ') ? old.promise : base(sql))
    render(<CDCModule initialTable="target" />)
    // Start an all-table read so it can complete behind the next table read.
    await screen.findAllByText('current-table')
    fireEvent.change(screen.getByLabelText('Table'), { target: { value: 'all' } })
    await waitFor(() => expect(query.mock.calls.some(([sql]) => sql.includes('CDC_READ'))).toBe(true))
    fireEvent.change(screen.getByLabelText('Table'), { target: { value: 'target' } })
    await waitFor(() => expect(query.mock.calls.filter(([sql]) => sql.includes('CDC_TABLE_READ')).length).toBeGreaterThan(1))
    await finish(() => old.resolve({ ...ok(), error: 'old row error' }))
    expect(screen.queryByRole('alert')).toBeNull()
    expect(screen.getByText('Refresh', { selector: 'button' })).toBeTruthy()
  })

  it('current read failure displays unavailable instead of an empty event log', async () => {
    query.mockImplementation(async sql => sql.includes('CDC_COUNT') ? ok(1) : { ...ok(), error: 'log denied' })
    render(<CDCModule />)
    expect((await screen.findByRole('alert')).textContent).toContain('log denied')
    expect(screen.queryByText('1 events')).toBeNull()
    expect(screen.queryByText('No rows')).toBeNull()
  })

  it('unmount cancels the acquired request and prevents late errors and second-stage dispatch', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(() => old.promise)
    const view = render(<CDCModule />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    const first = query.mock.calls[0]
    view.unmount()
    expect(api.cancelQuery).toHaveBeenCalledWith('c1', first[3])
    await finish(() => old.reject(new Error('late count failure')))
    expect(query).toHaveBeenCalledOnce(); expect(toasts.value).toHaveLength(0)
  })

  it('owns one non-overlapping polling timer and cancels it when auto refresh is off', async () => {
    query.mockImplementation(sql => base(sql))
    render(<CDCModule />)
    await screen.findAllByText('current-table')
    vi.useFakeTimers()
    fireEvent.change(screen.getByLabelText('Auto-refresh'), { target: { value: '1' } })
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    const before = query.mock.calls.length
    await act(async () => { await vi.advanceTimersByTimeAsync(1000) })
    expect(query.mock.calls.length).toBe(before + 2)
    fireEvent.change(screen.getByLabelText('Auto-refresh'), { target: { value: 'off' } })
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    const stopped = query.mock.calls.length
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    expect(query.mock.calls).toHaveLength(stopped)
  })
})
