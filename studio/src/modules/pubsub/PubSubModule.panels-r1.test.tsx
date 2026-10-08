import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { act } from 'preact/test-utils'
import { PubSubModule } from './PubSubModule'
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

import { parseChannels } from './PubSubModule'
const base = async (sql: string) => sql.includes('PUBSUB_CHANNELS') ? ok('new-channel') : ok(2)
describe('PubSub actual publish/read ownership and honest capabilities', () => {
  it('imports the production channel parser and labels live subscription unavailable', () => {
    query.mockImplementation(sql => base(sql))
    expect(parseChannels(' a, b,, ')).toEqual(['a', 'b'])
    render(<PubSubModule name="A" />)
    expect(screen.getByText(/SQL query UI cannot hold open/)).toBeTruthy()
  })

  it('late old-channel count cannot start a channels continuation or replace current info', async () => {
    const old = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes("PUBSUB_SUBSCRIBERS('A')") ? old.promise : base(sql))
    render(<PubSubModule name="A" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    fireEvent.input(screen.getByPlaceholderText('channel name'), { target: { value: 'B' } })
    fireEvent.blur(screen.getByPlaceholderText('channel name'))
    await screen.findByText('2 subscribers')
    const calls = query.mock.calls.length
    await finish(() => old.resolve(ok(99)))
    expect(query.mock.calls).toHaveLength(calls); expect(screen.queryByText('99 subscribers')).toBeNull()
  })

  it('publishes exact payload, coalesces keyboard repeat, and retains a newer draft', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('PUBSUB_PUBLISH') ? write.promise : base(sql))
    render(<PubSubModule name="A" />)
    const editor = screen.getByPlaceholderText('Message payload...')
    fireEvent.input(editor, { target: { value: '  old\n' } })
    fireEvent.keyDown(editor, { key: 'Enter', ctrlKey: true }); fireEvent.keyDown(editor, { key: 'Enter', ctrlKey: true })
    fireEvent.input(editor, { target: { value: 'new' } })
    await finish(() => write.resolve(ok(2)))
    expect(screen.getByDisplayValue('new')).toBeTruthy()
    const calls = query.mock.calls.filter(([sql]) => sql.includes('PUBSUB_PUBLISH'))
    expect(calls).toHaveLength(1); expect(calls[0][0]).toBe("SELECT PUBSUB_PUBLISH('A', '  old\n')")
  })

  it.each(['error', 'canceled', 'network'])('publish %s preserves its draft and never logs success', async state => {
    query.mockImplementation(async sql => {
      if (!sql.includes('PUBSUB_PUBLISH')) return base(sql)
      if (state === 'network') throw new Error('network unavailable')
      return state === 'error' ? { ...ok(), error: 'publish denied' } : { ...ok(), canceled: true }
    })
    render(<PubSubModule name="A" />)
    fireEvent.input(screen.getByPlaceholderText('Message payload...'), { target: { value: 'draft' } })
    fireEvent.click(screen.getByText('Publish', { selector: 'button' }))
    await screen.findByRole('alert')
    expect(screen.getByDisplayValue('draft')).toBeTruthy()
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })

  it('cleared log is not repopulated by an older publish, while the server outcome remains visible', async () => {
    const write = deferred<QueryResult>()
    let sends = 0
    query.mockImplementation(sql => sql.includes('PUBSUB_PUBLISH') && ++sends === 2 ? write.promise : base(sql))
    render(<PubSubModule name="A" />)
    const editor = screen.getByPlaceholderText('Message payload...')
    fireEvent.input(editor, { target: { value: 'first' } }); fireEvent.click(screen.getByText('Publish', { selector: 'button' }))
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'success')).toBe(true))
    fireEvent.input(editor, { target: { value: 'second' } }); fireEvent.click(screen.getByText('Publish', { selector: 'button' }))
    fireEvent.click(screen.getByText('Clear log', { selector: 'button' }))
    await finish(() => write.resolve(ok(2)))
    expect(screen.queryByText('second', { selector: 'span' })).toBeNull()
    expect(toasts.value.filter(t => t.kind === 'success')).toHaveLength(2)
  })

  it('late publish after connection ABA or unmount does not log, clear, or toast', async () => {
    const write = deferred<QueryResult>()
    query.mockImplementation(sql => sql.includes('PUBSUB_PUBLISH') ? write.promise : base(sql))
    const view = render(<PubSubModule name="A" />)
    fireEvent.input(screen.getByPlaceholderText('Message payload...'), { target: { value: 'draft' } })
    fireEvent.click(screen.getByText('Publish', { selector: 'button' }))
    connect('c2'); connect('c1'); view.unmount()
    await finish(() => write.resolve(ok(2)))
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
    expect(query.mock.calls.filter(([sql]) => sql.includes('PUBSUB_PUBLISH'))).toHaveLength(1)
  })
})
