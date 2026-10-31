import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { act } from 'preact/test-utils'
import { StreamsModule } from './StreamsModule'
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

import { parseStreamEntries, entriesToResult } from './StreamsModule'
const base = async (sql: string) => sql.includes('STREAM_XRANGE') ? ok('[]') : sql.includes('STREAM_XREADGROUP') ? ok('[{"id":"1-0","fields":{"v":"old"}}]') : ok(0)
const readGroup = async () => {
  fireEvent.input(screen.getByPlaceholderText('group'), { target: { value: 'G' } })
  fireEvent.input(screen.getByPlaceholderText('consumer'), { target: { value: 'C' } })
  fireEvent.click(screen.getByText('Read as group', { selector: 'button' }))
  await screen.findByText('1-0')
}
describe('Streams actual component ownership and production parsing', () => {
  it('uses exported production parser/result and rejects malformed nonempty responses', () => {
    expect(entriesToResult(parseStreamEntries('[{"id":"1-0","fields":{"v":"a"}}]')).rows).toEqual([['1-0', '{"v":"a"}']])
    expect(parseStreamEntries('')).toEqual([])
    expect(() => parseStreamEntries('broken')).toThrow(/unavailable/)
  })

  it('old connection metadata and rows cannot replace a newer view', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation((sql, conn) => conn === 'c1' ? old.promise : sql.includes('STREAM_XLEN') ? Promise.resolve(ok(22)) : Promise.resolve(ok('[{"id":"22-0","fields":{"v":"new"}}]')))
    render(<StreamsModule name="A" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    connect('c2')
    await screen.findByText('22 entries')
    await screen.findByText('22-0')
    await finish(() => old.resolve(ok('[{"id":"old","fields":{}}]')))
    expect(screen.queryByText('old')).toBeNull(); expect(screen.getByText('22 entries')).toBeTruthy()
  })

  it('range input change invalidates a pending failure before another read', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('STREAM_XRANGE') ? old.promise : base(sql))
    render(<StreamsModule name="A" />)
    await waitFor(() => expect(query.mock.calls.some(([sql]) => sql.includes('STREAM_XRANGE'))).toBe(true))
    fireEvent.input(document.querySelector('input[type=number][class*=rangeInput]')!, { target: { value: '50' } })
    await finish(() => old.reject(new Error('old range failure')))
    expect(screen.queryByRole('alert')).toBeNull()
    expect(toasts.value).toHaveLength(0)
  })

  it('current SQL failure stays unavailable instead of publishing empty entries', async () => {
    query.mockImplementation(async sql => sql.includes('STREAM_XRANGE') ? { ...ok(), error: 'entries denied' } : ok(0))
    render(<StreamsModule name="A" />)
    expect((await screen.findByRole('alert')).textContent).toContain('entries denied')
    expect(screen.queryByText('No rows')).toBeNull()
  })

  it('append coalesces repeat actions and retains a newer draft after success', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('STREAM_XADD') ? write.promise : base(sql))
    render(<StreamsModule name="A" />)
    fireEvent.input(screen.getByPlaceholderText('field'), { target: { value: 'v' } })
    fireEvent.input(screen.getByPlaceholderText('value'), { target: { value: 'old' } })
    const append = screen.getByText('Append', { selector: 'button' })
    fireEvent.click(append); fireEvent.click(append)
    fireEvent.input(screen.getByPlaceholderText('value'), { target: { value: 'new' } })
    await finish(() => write.resolve(ok()))
    expect(screen.getByDisplayValue('new')).toBeTruthy()
    expect(query.mock.calls.filter(([sql]) => sql.includes('STREAM_XADD'))).toHaveLength(1)
  })

  it('ACK retains the loaded A/G identity after stream input changes and coalesces repeats', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('STREAM_XACK') ? write.promise : base(sql))
    render(<StreamsModule name="A" />); await readGroup()
    fireEvent.input(screen.getByPlaceholderText('stream name'), { target: { value: 'B' } })
    const ack = screen.getByText('ACK', { selector: 'button' })
    fireEvent.click(ack); fireEvent.click(ack)
    expect(query.mock.calls.filter(([sql]) => sql.includes('STREAM_XACK'))).toHaveLength(1)
    expect(query.mock.calls.find(([sql]) => sql.includes('STREAM_XACK'))?.slice(0, 2)).toEqual(["SELECT STREAM_XACK('A', 'G', 1, 0)", 'c1'])
    await finish(() => write.resolve(ok()))
    await waitFor(() => expect(screen.queryByText('ACK', { selector: 'button' })).toBeNull())
  })

  it('unmounted group read cannot acquire a pending batch or emit errors', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('STREAM_XREADGROUP') ? old.promise : base(sql))
    const view = render(<StreamsModule name="A" />)
    fireEvent.input(screen.getByPlaceholderText('group'), { target: { value: 'G' } })
    fireEvent.input(screen.getByPlaceholderText('consumer'), { target: { value: 'C' } })
    fireEvent.click(screen.getByText('Read as group', { selector: 'button' }))
    view.unmount()
    await finish(() => old.resolve({ ...ok(), error: 'old failure' }))
    expect(toasts.value).toHaveLength(0)
  })
})
