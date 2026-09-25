import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { EditorView } from '@codemirror/view'
import { CompletionContext, type CompletionResult, type CompletionSource } from '@codemirror/autocomplete'
import { activeConnection, schema, toasts, features } from '../../lib/store'
import { ApiError, _setSessionTokenForTests } from '../../lib/api'
import type { QueryResult, Schema, SqlTable, ExplainOutcome } from '../../lib/types'
import { SQLEditor } from './SQLEditor'

// Rendered SQLEditor tests for S04 (V15 editor leg): the REAL component with
// the REAL CodeMirror editor and lang-sql completion. Only the fetch
// boundary (lib/api) is mocked; the server side is covered by the Go E2E
// leg (sqlexec_e2e_test.go) against real Postgres.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      query: vi.fn(),
      cancelQuery: vi.fn(),
      explain: vi.fn(),
      savedQueries: { list: vi.fn().mockResolvedValue([]), save: vi.fn(), remove: vi.fn() },
    },
  }
})

import { api } from '../../lib/api'

const query = vi.mocked(api.query)
const cancelQuery = vi.mocked(api.cancelQuery)
const explain = vi.mocked(api.explain)

function fullSchema(tables: SqlTable[]): Schema {
  return {
    sql: tables,
    kv: [], vector: [], timeseries: [], document: [], graph: [],
    fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [],
    datalog: null, cdc: false,
  }
}

const catalog: SqlTable[] = [
  { schema: 'public', name: 'users', columns: [{ name: 'id', type: 'int4', nullable: false, isPrimaryKey: true }] },
  { schema: 'billing', name: 'invoices', columns: [{ name: 'total', type: 'numeric', nullable: false, isPrimaryKey: false }] },
]

function deferred<T>() {
  let resolve!: (v: T) => void
  const promise = new Promise<T>(r => { resolve = r })
  return { promise, resolve }
}

async function editorView(): Promise<EditorView> {
  let view: EditorView | null = null
  await waitFor(() => {
    const dom = document.querySelector('.cm-editor') as HTMLElement | null
    view = dom ? EditorView.findFromDOM(dom) : null
    expect(view).toBeTruthy()
  })
  return view!
}

async function setDoc(text: string) {
  const view = await editorView()
  view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text }, selection: { anchor: text.length } })
  return view
}

async function completionsFromEditor(view: EditorView): Promise<string[]> {
  const pos = view.state.doc.length
  const sources = view.state.languageDataAt<CompletionSource>('autocomplete', pos)
  const labels: string[] = []
  for (const source of sources) {
    const res = (await source(new CompletionContext(view.state, pos, true))) as CompletionResult | null
    if (res) labels.push(...res.options.map(o => o.label))
  }
  return labels
}

function ok(r: Partial<QueryResult>): QueryResult {
  return { columns: ['n'], rows: [[1]], rowCount: 1, duration: 3, ...r }
}

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
  _setSessionTokenForTests('test-session-token')
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'local', url: 'postgres://x', isNucleus: false }
  features.value = { ...features.value, isNucleus: false }
  schema.value = fullSchema(catalog)
})

afterEach(() => {
  cleanup()
  _setSessionTokenForTests(null)
})

describe('SQLEditor — cancellation', () => {
  it('cancels the running statement by its request id and reports the server outcome', async () => {
    render(<SQLEditor tabId="t1" />)
    await setDoc('SELECT pg_sleep(60)')
    const pending = deferred<QueryResult>()
    query.mockReturnValueOnce(pending.promise)
    cancelQuery.mockResolvedValueOnce({ requestId: 'x', state: 'sent', method: 'pg_cancel_backend' })

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' }) as HTMLButtonElement
    expect(cancelBtn.disabled).toBe(true)

    fireEvent.click(screen.getByRole('button', { name: '▶ Run' }))
    await waitFor(() => expect(query).toHaveBeenCalledTimes(1))
    const [sqlSent, connId, params, requestId] = query.mock.calls[0]
    expect(sqlSent).toBe('SELECT pg_sleep(60)')
    expect(connId).toBe('c1')
    expect(params).toBeUndefined()
    expect(requestId).toMatch(/^[A-Za-z0-9_-]{8,128}$/)

    await waitFor(() => expect(cancelBtn.disabled).toBe(false))
    expect(screen.getByRole('status').textContent).toBe('Query running')
    cancelBtn.focus()
    fireEvent.click(cancelBtn)
    await waitFor(() => expect(cancelQuery).toHaveBeenCalledWith('c1', requestId))
    expect(screen.getByRole('status').textContent).toBe('Cancel requested…')
    // A second click does not send a second cancel.
    fireEvent.click(cancelBtn)
    expect(cancelQuery).toHaveBeenCalledTimes(1)

    pending.resolve(ok({ columns: [], rows: [], rowCount: 0, error: 'canceling statement due to user request', canceled: true, sqlState: '57014', connectionReused: true, requestId }))
    const alert = await screen.findByText(/Query canceled on the server/)
    expect(alert.closest('[role="alert"]')).toBeTruthy()
    expect(alert.textContent).toContain('connection was verified and reused')
    // Keyboard focus moves back to Run instead of falling to the body.
    await waitFor(() => expect(document.activeElement).toBe(screen.getByRole('button', { name: '▶ Run' })))
    expect(cancelBtn.disabled).toBe(true)

    const history = JSON.parse(localStorage.getItem('neutron:query-history:c1')!)
    expect(history[0]).toMatchObject({ sql: 'SELECT pg_sleep(60)', status: 'canceled' })
  })

  it('treats a 404 cancel (statement already finished) as a no-op', async () => {
    render(<SQLEditor tabId="t2" />)
    await setDoc('SELECT 1')
    const pending = deferred<QueryResult>()
    query.mockReturnValueOnce(pending.promise)
    cancelQuery.mockRejectedValueOnce(new ApiError(404, 'no running statement', { state: 'not-running' }))
    fireEvent.click(screen.getByRole('button', { name: '▶ Run' }))
    await waitFor(() => expect(query).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(cancelQuery).toHaveBeenCalled())
    pending.resolve(ok({}))
    await waitFor(() => expect(screen.getByRole('button', { name: '▶ Run' })).toHaveProperty('disabled', false))
    expect(toasts.value.filter(t => t.kind === 'error')).toHaveLength(0)
  })
})

describe('SQLEditor — parameters', () => {
  it('renders one input per positional parameter and binds text / NULL', async () => {
    render(<SQLEditor tabId="t3" />)
    await setDoc("SELECT * FROM users WHERE id = $1 AND note = $2 AND '$3' <> ''")
    const first = await screen.findByLabelText('$1') as HTMLInputElement
    expect(screen.getByLabelText('$2')).toBeTruthy()
    expect(screen.queryByLabelText('$3')).toBeNull()

    fireEvent.input(first, { target: { value: '9007199254740993' } })
    fireEvent.click(screen.getByLabelText('$2 is NULL'))
    query.mockResolvedValueOnce(ok({}))
    fireEvent.click(screen.getByRole('button', { name: '▶ Run' }))
    await waitFor(() => expect(query).toHaveBeenCalled())
    expect(query.mock.calls[0][2]).toEqual(['9007199254740993', null])

    // History keeps the parameters and restores them.
    await setDoc('SELECT 1')
    await waitFor(() => expect(screen.queryByLabelText('$1')).toBeNull())
    fireEvent.click(screen.getByRole('button', { name: 'History' }))
    fireEvent.click(await screen.findByTitle("SELECT * FROM users WHERE id = $1 AND note = $2 AND '$3' <> ''"))
    const restored = await screen.findByLabelText('$1') as HTMLInputElement
    expect(restored.value).toBe('9007199254740993')
    expect((screen.getByLabelText('$2 is NULL') as HTMLInputElement).checked).toBe(true)
  })
})

describe('SQLEditor — EXPLAIN', () => {
  const planResult: ExplainOutcome = {
    ok: true, requestId: 'r', engine: 'postgresql', format: 'json',
    plan: [{ Plan: { 'Node Type': 'ModifyTable', Operation: 'Delete', 'Total Cost': 1, 'Plan Rows': 0 } }],
    analyze: false, executed: false, writesAllowed: false, readOnly: true, committed: false, duration: 1,
  }

  it('explains without executing by default', async () => {
    render(<SQLEditor tabId="t4" />)
    await setDoc('DELETE FROM users')
    explain.mockResolvedValueOnce(planResult)
    fireEvent.click(screen.getByRole('button', { name: 'Explain' }))
    await waitFor(() => expect(explain).toHaveBeenCalled())
    expect(explain.mock.calls[0][0]).toMatchObject({ connectionId: 'c1', sql: 'DELETE FROM users', analyze: false, allowWrites: false })
    expect(await screen.findByText('Estimated plan. The statement was not executed.')).toBeTruthy()
    expect(query).not.toHaveBeenCalled()
  })

  it('makes ANALYZE execution explicit and requires a second opt-in for writes', async () => {
    render(<SQLEditor tabId="t5" />)
    await setDoc('DELETE FROM users')
    expect(screen.queryByLabelText('Allow writes (rolled back)')).toBeNull()

    fireEvent.click(screen.getByLabelText('Analyze'))
    expect(screen.getByRole('note').textContent).toContain('This statement writes. EXPLAIN ANALYZE executes statements')
    expect(screen.getByRole('button', { name: 'Explain Analyze' })).toBeTruthy()

    explain.mockResolvedValueOnce({ ok: false, state: 'write-blocked', error: 'EXPLAIN ANALYZE executes the statement, and this statement writes.', executed: false })
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }))
    await waitFor(() => expect(explain).toHaveBeenCalledTimes(1))
    expect(explain.mock.calls[0][0]).toMatchObject({ analyze: true, allowWrites: false })
    expect((await screen.findByRole('alert')).textContent).toContain('Nothing was executed.')

    fireEvent.click(screen.getByLabelText('Allow writes (rolled back)'))
    expect(screen.getByRole('note').textContent).toContain('including its writes, inside a transaction that Studio then rolls back')
    explain.mockResolvedValueOnce({ ...planResult, analyze: true, executed: true, writesAllowed: true, readOnly: false })
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }))
    await waitFor(() => expect(explain).toHaveBeenCalledTimes(2))
    expect(explain.mock.calls[1][0]).toMatchObject({ analyze: true, allowWrites: true })
    expect(await screen.findByText(/with writes allowed, then rolled back/)).toBeTruthy()

    // Turning Analyze off also withdraws the write opt-in.
    fireEvent.click(screen.getByLabelText('Analyze'))
    fireEvent.click(screen.getByLabelText('Analyze'))
    expect((screen.getByLabelText('Allow writes (rolled back)') as HTMLInputElement).checked).toBe(false)
  })

  it('does not offer EXPLAIN on Nucleus connections', async () => {
    activeConnection.value = { id: 'c1', name: 'nucleus', url: 'postgres://x', isNucleus: true }
    render(<SQLEditor tabId="t6" />)
    await editorView()
    expect((screen.getByRole('button', { name: 'Explain' }) as HTMLButtonElement).disabled).toBe(true)
    expect((screen.getByLabelText('Analyze') as HTMLInputElement).disabled).toBe(true)
  })
})

describe('SQLEditor — catalog-aware completion', () => {
  it('uses the selected schema for unqualified completion in the live editor', async () => {
    render(<SQLEditor tabId="t7" />)
    const select = screen.getByLabelText('Completion schema') as HTMLSelectElement
    expect(Array.from(select.options).map(o => o.value)).toEqual(['billing', 'public'])
    expect(select.value).toBe('public')

    let view = await setDoc('SELECT * FROM ')
    let labels = await completionsFromEditor(view)
    expect(labels).toContain('users')
    expect(labels).not.toContain('invoices')

    fireEvent.change(select, { target: { value: 'billing' } })
    await waitFor(async () => {
      view = await editorView()
      labels = await completionsFromEditor(view)
      expect(labels).toContain('invoices')
    })
    expect(labels).not.toContain('users')
  })

  it('follows catalog refreshes', async () => {
    render(<SQLEditor tabId="t8" />)
    const view = await setDoc('SELECT * FROM ')
    schema.value = fullSchema([...catalog, { schema: 'public', name: 'fresh_table', columns: [] }])
    await waitFor(async () => expect(await completionsFromEditor(view)).toContain('fresh_table'))
  })
})

describe('SQLEditor — saved queries keep working', () => {
  it('lists and loads saved queries into the editor', async () => {
    vi.mocked(api.savedQueries.list).mockResolvedValueOnce([{ id: 's1', name: 'by id', sql: 'SELECT * FROM users WHERE id = $1', createdAt: '' }])
    render(<SQLEditor tabId="t9" />)
    const view = await editorView()
    fireEvent.click(screen.getByRole('button', { name: 'Saved' }))
    fireEvent.click(await screen.findByText('by id'))
    await waitFor(() => expect(view.state.doc.toString()).toBe('SELECT * FROM users WHERE id = $1'))
    expect(await screen.findByLabelText('$1')).toBeTruthy()
  })
})
