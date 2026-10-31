import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { act } from 'preact/test-utils'
import { ColumnarModule } from './ColumnarModule'
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

import { buildInsertSql, aggregateSql } from './ColumnarModule'
describe('Columnar actual component request and draft ownership', () => {
  it('imports production SQL builders rather than copying them', () => {
    expect(buildInsertSql("a'b", 'n=1, label=x')).toBe("SELECT COLUMNAR_INSERT('a''b', 'n', 1, 'label', 'x')")
    expect(aggregateSql('A', 'SUM', "x'y")).toBe("SELECT COLUMNAR_SUM('A', 'x''y')")
  })

  it('late old-table metadata cannot overwrite the current table', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes("COLUMNAR_COUNT('A')") ? old.promise : Promise.resolve(ok(22)))
    render(<ColumnarModule name="A" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    fireEvent.input(screen.getByPlaceholderText('table name'), { target: { value: 'B' } })
    fireEvent.blur(screen.getByPlaceholderText('table name'))
    await screen.findByText('22 rows')
    await finish(() => old.resolve(ok(11)))
    expect(screen.queryByText('11 rows')).toBeNull(); expect(screen.getByDisplayValue('B')).toBeTruthy()
  })

  it('late c1 query cannot replace c2 rows or release its running state', async () => {
    const old = deferred<QueryResult>(), next = deferred<QueryResult>()
    query.mockImplementation((sql, conn) => sql === 'SELECT marker' ? conn === 'c1' ? old.promise : next.promise : Promise.resolve(ok(0)))
    render(<ColumnarModule name="A" />)
    fireEvent.input(document.querySelector('textarea')!, { target: { value: 'SELECT marker' } })
    fireEvent.click(screen.getByText('▶ Run', { selector: 'button' }))
    connect('c2')
    await waitFor(() => expect(screen.getByText('▶ Run', { selector: 'button' })).toBeTruthy())
    fireEvent.click(screen.getByText('▶ Run', { selector: 'button' }))
    await finish(() => old.resolve(ok('old-marker')))
    expect(screen.getByText('Running…', { selector: 'button' })).toBeTruthy()
    await finish(() => next.resolve(ok('new-marker')))
    await screen.findByText('new-marker'); expect(screen.queryByText('old-marker')).toBeNull()
  })

  it('query input ABA invalidates old success and old errors', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql === 'SELECT marker' ? old.promise : Promise.resolve(ok(0)))
    render(<ColumnarModule name="A" />)
    const editor = document.querySelector('textarea')!
    fireEvent.input(editor, { target: { value: 'SELECT marker' } }); fireEvent.click(screen.getByText('▶ Run', { selector: 'button' }))
    fireEvent.input(editor, { target: { value: 'SELECT other' } }); fireEvent.input(editor, { target: { value: 'SELECT marker' } })
    await finish(() => old.resolve({ ...ok(), error: 'old error' }))
    expect(screen.queryByRole('alert')).toBeNull(); expect(screen.queryByText('old error')).toBeNull()
  })

  it('insert coalesces repeats and keeps a later draft', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('COLUMNAR_INSERT') ? write.promise : Promise.resolve(ok(0)))
    render(<ColumnarModule name="A" />)
    const draft = screen.getByPlaceholderText('col1=val1, col2=val2')
    fireEvent.input(draft, { target: { value: 'n=1' } })
    const button = screen.getByText('Insert', { selector: 'button' })
    fireEvent.click(button); fireEvent.click(button)
    fireEvent.input(draft, { target: { value: 'n=2' } })
    await finish(() => write.resolve(ok()))
    expect(screen.getByDisplayValue('n=2')).toBeTruthy()
    expect(query.mock.calls.filter(([sql]) => sql.includes('COLUMNAR_INSERT'))).toHaveLength(1)
  })

  it('current metadata failures are unavailable, and unmounted failures remain silent', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(() => old.promise)
    const view = render(<ColumnarModule name="A" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    view.unmount(); await finish(() => old.reject(new Error('old error')))
    expect(toasts.value).toHaveLength(0)
    query.mockResolvedValue({ ...ok(), error: 'count denied' })
    render(<ColumnarModule name="B" />)
    expect((await screen.findByRole('alert')).textContent).toContain('count denied')
    expect(screen.queryByText('0 rows')).toBeNull()
  })
})
