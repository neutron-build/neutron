import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor, act } from '@testing-library/preact'
import { activeConnection, schema, stagedEdits, clearStaged, toasts, tableDataRevision, commitStaged } from '../../lib/store'
import type { Schema, TableMeta, TablePageResult, QueryResult } from '../../lib/types'
import { ApiError } from '../../lib/api'
import { SQLBrowser } from './SQLBrowser'

vi.mock('../../lib/api', async importOriginal => {
  const original = await importOriginal<typeof import('../../lib/api')>()
  return { ...original, api: { tablePage: vi.fn(), tableData: vi.fn(), tableMeta: vi.fn(),
    tableFKs: vi.fn().mockResolvedValue({ fks: [] }), commitOperations: vi.fn() } }
})
import { api } from '../../lib/api'
const tablePage = vi.mocked(api.tablePage)
const tableData = vi.mocked(api.tableData)
const tableMeta = vi.mocked(api.tableMeta)

function nativeMeta(): TableMeta {
  return { exists: true, keyColumns: ['id'], binding: 'epoch:12345', versioned: true, readOnly: false, canDelete: true, columns: [
    { name: 'id', type: 'bigint', tag: 'int8', nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, insertable: true },
    { name: 'body', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
  ] }
}
function page(n: number, hasNext = true, body = `page ${n} body`): TablePageResult {
  // API decoding/shape admission is qualified independently in api.test.ts;
  // these decoded route-boundary fixtures isolate rendered navigation state.
  return { columns: ['id', 'body'], rows: [[BigInt(n), body]], versions: [`${100 + n}`], keyColumns: ['id'],
    binding: 'epoch:12345', versioned: true, readOnly: false, rowCount: 1, duration: 0,
    hasNext, nextCursor: hasNext ? `cursor-${n + 1}` : '', consistency: 'live-keyset/request-repeatable-read' }
}
function cell(): HTMLElement { return document.querySelector('tr[data-row-index="0"] td[data-col-index="1"]')! }
function stage(text: string) {
  fireEvent.dblClick(cell())
  const editor = document.querySelector('input[aria-label$=" value"]')!
  fireEvent.input(editor, { target: { value: text } })
  fireEvent.keyDown(editor, { key: 'Enter' })
}

beforeEach(() => {
  vi.clearAllMocks(); clearStaged(); toasts.value = []; tableDataRevision.value = {}
  activeConnection.value = { id: 'c1', name: 'native', url: 'postgres://masked', isNucleus: false }
  schema.value = { sql: [{ schema: 'public', name: 'notes', columns: [
    { name: 'id', type: 'bigint', nullable: false, isPrimaryKey: true },
    { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
  ] }], kv: [], vector: [], timeseries: [], document: [], graph: [], fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false } as Schema
  tableMeta.mockImplementation(async () => nativeMeta())
  tablePage.mockImplementation(async (_c, _s, _t, _limit, cursor = '') => page(cursor ? Number(cursor.slice(7)) : 1))
  tableData.mockResolvedValue({ ...page(1, false, 'offset body') } as QueryResult)
})
afterEach(cleanup)

describe('SQLBrowser native bigint keyset integration', () => {
  it('waits for authoritative metadata, uses exact hasNext and restores opaque previous cursors', async () => {
    let finishMeta!: (meta: TableMeta) => void
    tableMeta.mockImplementationOnce(() => new Promise(resolve => { finishMeta = resolve }))
    render(<SQLBrowser schema="public" table="notes" />)
    expect(tablePage).not.toHaveBeenCalled(); expect(tableData).not.toHaveBeenCalled()
    await act(async () => finishMeta(nativeMeta()))
    await screen.findByText('page 1 body')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, '')
    expect(screen.getByRole('navigation', { name: 'Table pages' })).toBeTruthy()
    const next = screen.getByRole('button', { name: 'Next →' })
    const prev = screen.getByRole('button', { name: '← Prev' })
    next.focus(); fireEvent.click(next)
    await screen.findByText('page 2 body')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, 'cursor-2')
    expect(document.activeElement).toBe(next)
    fireEvent.click(prev); await screen.findByText('page 1 body')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, '')
    expect((prev as HTMLButtonElement).disabled).toBe(true)
    tablePage.mockResolvedValueOnce(page(2, false))
    fireEvent.click(next); await screen.findByText('page 2 body')
    expect((next as HTMLButtonElement).disabled).toBe(true)
    expect(tableData).not.toHaveBeenCalled()
    expect(screen.getByRole('status').textContent).toContain('each page is a new database snapshot')
  })

  it('preserves exact key drafts off-page, on refresh and after authoritative commit invalidation', async () => {
    render(<SQLBrowser schema="public" table="notes" />)
    await screen.findByText('page 1 body'); stage('draft survives')
    const draft = stagedEdits.value[0]
    expect(draft.operation.key).toEqual([{ column: 'id', value: { t: 'int8', v: '1' } }])
    fireEvent.click(screen.getByRole('button', { name: 'Next →' })); await screen.findByText('page 2 body')
    expect(stagedEdits.value[0]).toBe(draft)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' })); await screen.findByText('draft survives')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, '')
    expect(stagedEdits.value[0]).toBe(draft)
    vi.mocked(api.commitOperations).mockResolvedValue({ operationId: 'owned-op', rowsAffected: 1 } as never)
    tablePage.mockResolvedValueOnce({ ...page(1, false, 'committed DB value'), versions: ['999'] })
    await act(async () => { await commitStaged('c1') })
    await screen.findByText('committed DB value')
    expect(stagedEdits.value).toHaveLength(0)
    stage('second draft'); expect(stagedEdits.value[0].operation.version).toBe('999')
    expect(tableData).not.toHaveBeenCalled()
  })

  it('never falls back on stale/resource/auth/network errors; explicit refresh preserves drafts', async () => {
    render(<SQLBrowser schema="public" table="notes" />)
    await screen.findByText('page 1 body'); stage('kept draft')
    const draft = stagedEdits.value[0]
    for (const refusal of [new ApiError(409, 'stale cursor'), new ApiError(413, 'page too large'), new ApiError(403, 'wrong origin'), new ApiError(502, 'backend failed'), new Error('network interrupted')]) {
      tablePage.mockRejectedValueOnce(refusal)
      fireEvent.click(screen.getByRole('button', { name: 'Next →' }))
      await waitFor(() => expect(toasts.value.some(t => t.message.includes(refusal.message))).toBe(true))
      expect(tableData).not.toHaveBeenCalled(); expect(stagedEdits.value[0]).toBe(draft)
      expect(screen.getByText(/Page 1 ·/)).toBeTruthy()
      expect((screen.getByRole('button', { name: 'Next →' }) as HTMLButtonElement).disabled).toBe(true)
      fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' })); await screen.findByText('kept draft')
    }
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' })); await screen.findByText('kept draft')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, '')
  })

  it('publishes only the latest metadata and row request across commit resets', async () => {
    let finishOld!: (result: TablePageResult) => void
    tablePage.mockImplementationOnce(() => new Promise(resolve => { finishOld = resolve }))
    render(<SQLBrowser schema="public" table="notes" />)
    await waitFor(() => expect(tablePage).toHaveBeenCalledTimes(1))
    tablePage.mockResolvedValueOnce(page(1, false, 'fresh authoritative value'))
    await act(async () => { tableDataRevision.value = { c1: 1 } })
    await screen.findByText('fresh authoritative value')
    await act(async () => { finishOld(page(99, true, 'late stale value')); await Promise.resolve() })
    expect(screen.queryByText('late stale value')).toBeNull()
    expect(screen.getByText(/Page 1 ·/)).toBeTruthy()
    expect((screen.getByRole('button', { name: 'Next →' }) as HTMLButtonElement).disabled).toBe(true)
  })

  it('preserves an older-binding draft without overlaying a replacement row', async () => {
    render(<SQLBrowser schema="public" table="notes" />); await screen.findByText('page 1 body'); stage('old relation draft')
    const oldDraft = stagedEdits.value[0]
    tableMeta.mockResolvedValue({ ...nativeMeta(), binding: 'new-epoch:54321' })
    tablePage.mockResolvedValue({ ...page(1, false, 'replacement row'), binding: 'new-epoch:54321' })
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' }))
    await screen.findByText('replacement row')
    expect(screen.queryByText('old relation draft')).toBeNull()
    expect(stagedEdits.value[0]).toBe(oldDraft)
    expect(screen.getByRole('status').textContent).toContain('earlier table connection remain staged')
  })

  it('ignores a stale metadata failure after a newer successful revision read', async () => {
    let failOld!: (error: Error) => void
    tableMeta.mockImplementationOnce(() => new Promise((_resolve, reject) => { failOld = reject }))
    render(<SQLBrowser schema="public" table="notes" />)
    await waitFor(() => expect(tableMeta).toHaveBeenCalledTimes(1))
    await act(async () => { tableDataRevision.value = { c1: 1 } })
    await screen.findByText('page 1 body')
    await act(async () => { failOld(new Error('stale metadata failure')); await Promise.resolve() })
    expect(screen.getByText('page 1 body')).toBeTruthy()
    expect(toasts.value.some(t => t.message.includes('stale metadata failure'))).toBe(false)
  })

  it('retains the last successful page/history on a failed refresh until fresh rows publish', async () => {
    render(<SQLBrowser schema="public" table="notes" />); await screen.findByText('page 1 body')
    fireEvent.click(screen.getByRole('button', { name: 'Next →' })); await screen.findByText('page 2 body')
    tablePage.mockRejectedValueOnce(new ApiError(502, 'refresh interrupted'))
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' }))
    await waitFor(() => expect(toasts.value.some(t => t.message.includes('refresh interrupted'))).toBe(true))
    expect(screen.getByText(/Page 2 ·/)).toBeTruthy()
    expect((screen.getByRole('button', { name: '← Prev' }) as HTMLButtonElement).disabled).toBe(true)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' })); await screen.findByText('page 1 body')
    expect(screen.getByText(/Page 1 ·/)).toBeTruthy()
  })

  it('resets paging for changed reference-match input', async () => {
    const view = render(<SQLBrowser schema="public" table="notes" />); await screen.findByText('page 1 body')
    fireEvent.click(screen.getByRole('button', { name: 'Next →' })); await screen.findByText('page 2 body')
    view.rerender(<SQLBrowser schema="public" table="notes" initialMatch={[{ column: 'id', value: { t: 'int8', v: '1' } }]} />)
    await screen.findByText('offset body')
    expect(tableData).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, 0, undefined, undefined, undefined, [{ column: 'id', value: { t: 'int8', v: '1' } }])
    expect(screen.getByRole('status').textContent).toContain('reference matches')
    expect(screen.getByText('offset body')).toBeTruthy()
  })

  it('bounds previous navigation to 64 cursor starts and resets history with page size', async () => {
    render(<SQLBrowser schema="public" table="notes" />); await screen.findByText('page 1 body')
    for (let n = 2; n <= 66; n++) { fireEvent.click(screen.getByRole('button', { name: 'Next →' })); await screen.findByText(`page ${n} body`) }
    expect(screen.getByRole('status').textContent).toContain('last 64 pages')
    for (let n = 65; n >= 2; n--) { fireEvent.click(screen.getByRole('button', { name: '← Prev' })); await screen.findByText(`page ${n} body`) }
    expect((screen.getByRole('button', { name: '← Prev' }) as HTMLButtonElement).disabled).toBe(true)
    expect(screen.getByText(/Page 2 ·/)).toBeTruthy()
    fireEvent.change(screen.getByLabelText('Rows per page'), { target: { value: '100' } })
    await screen.findByText('page 1 body')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 100, '')
    expect(screen.getByRole('status').textContent).not.toContain('last 64 pages')
  }, 20000)
})

describe('explicit offset capability fallback', () => {
  it('shows server unsupported-profile reason and retries admission on refresh', async () => {
    tablePage.mockRejectedValueOnce(new ApiError(400, 'JSON is not keyset-certified', { state: 'unsupported-profile' }))
    render(<SQLBrowser schema="public" table="notes" />)
    await screen.findByText('offset body')
    expect(screen.getByRole('status').textContent).toContain('JSON is not keyset-certified')
    expect(tableData).toHaveBeenCalledTimes(1)
    expect(tablePage).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' })); await screen.findByText('page 1 body')
    expect(tablePage).toHaveBeenCalledTimes(2)
  })

  it.each(['Nucleus', 'non-bigint', 'matched-reference', 'initial-filter'])('names %s before choosing offset without a probe', async kind => {
    if (kind === 'Nucleus') activeConnection.value = { ...activeConnection.value!, isNucleus: true }
    if (kind === 'non-bigint') tableMeta.mockResolvedValue({ ...nativeMeta(), columns: nativeMeta().columns.map(c => c.isKey ? { ...c, type: 'int4', tag: null } : c) })
    render(<SQLBrowser schema="public" table="notes" initialMatch={kind === 'matched-reference' ? [{ column: 'id', value: { t: 'int8', v: '1' } }] : undefined} initialFilter={kind === 'initial-filter' ? { column: 'body', op: 'eq', value: 'offset body' } : undefined} />)
    await screen.findByText('offset body')
    expect(tablePage).not.toHaveBeenCalled()
    expect(screen.getByRole('status').textContent).toContain('offset paging')
  })

  it('switches to explicit offset for sorting and back to keyset when cleared, keeping drafts', async () => {
    render(<SQLBrowser schema="public" table="notes" />); await screen.findByText('page 1 body'); stage('sort-safe draft')
    const draft = stagedEdits.value[0]
    const bodyHeader = () => Array.from(document.querySelectorAll('th')).find(th => th.textContent?.includes('body'))!
    fireEvent.click(bodyHeader())
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('custom sorting'))
    expect(tableData.mock.calls[0][7]).toEqual([{ column: 'body', dir: 'asc' }])
    expect(stagedEdits.value[0]).toBe(draft)
    fireEvent.click(bodyHeader()); await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    fireEvent.click(bodyHeader()); await waitFor(() => expect(tablePage).toHaveBeenCalledTimes(2))
    await screen.findByText('sort-safe draft')
    expect(tablePage).toHaveBeenLastCalledWith('c1', 'public', 'notes', 200, '')
    expect(stagedEdits.value[0]).toBe(draft)
  })

  it('treats ordinary request400 and changed metadata as errors, without fallback', async () => {
    tablePage.mockRejectedValueOnce(new ApiError(400, 'malformed request'))
    render(<SQLBrowser schema="public" table="notes" />)
    await waitFor(() => expect(toasts.value.some(t => t.message.includes('malformed request'))).toBe(true))
    expect(tableData).not.toHaveBeenCalled()
    tablePage.mockResolvedValueOnce({ ...page(1), binding: 'replacement:999' })
    fireEvent.click(screen.getByRole('button', { name: 'Refresh rows' }))
    await waitFor(() => expect(toasts.value.some(t => t.message.includes('metadata changed'))).toBe(true))
    expect(tableData).not.toHaveBeenCalled()
  })
})
