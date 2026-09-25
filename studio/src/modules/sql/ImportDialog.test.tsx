import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { ImportDialog } from './ImportDialog'
import type { TableMeta } from '../../lib/types'

// Rendered ImportDialog tests (S06): the a11y dialog contract (role, label,
// focus management, live regions), streamed preview + mapping, and the
// wiring between decisions and the transport/journal — only the transport
// boundary (designed for injection) and the API fetch boundary are stubbed.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      tableMeta: vi.fn(),
      previewOperations: vi.fn(),
    },
  }
})

import { api, ApiError } from '../../lib/api'

const tableMeta = vi.mocked(api.tableMeta)
const previewOperations = vi.mocked(api.previewOperations)

function meta(): TableMeta {
  return {
    exists: true,
    keyColumns: ['id'],
    versioned: true,
    readOnly: false,
    canDelete: true,
    binding: 'e1:16390',
    columns: [
      { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only', insertable: true },
      { name: 'qty', type: 'bigint', tag: 'int8', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      { name: 'note', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
    ],
  }
}

function dialogProps(over: Record<string, unknown> = {}) {
  return {
    connectionId: 'c1', schema: 'public', table: 'imp', meta: meta(),
    onClose: vi.fn(), onImported: vi.fn(), ...over,
  }
}

function makeFile(text: string, name = 'imp.csv', lastModified = 1000): File {
  const f = new File([text], name, { type: 'text/csv' })
  Object.defineProperty(f, 'lastModified', { value: lastModified })
  return f
}

function storageStub(): { storage: Storage; store: Map<string, string> } {
  const store = new Map<string, string>()
  const storage = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
    get length() { return store.size },
    key: () => null,
    clear: () => store.clear(),
  } as unknown as Storage
  return { storage, store }
}

function chooseFile(f: File) {
  const input = document.querySelector('input[type="file"]') as HTMLInputElement
  Object.defineProperty(input, 'files', { value: [f], configurable: true })
  fireEvent.change(input)
}

beforeEach(() => {
  tableMeta.mockReset()
  // a meta refresh that cannot reach the server falls back to the meta the
  // dialog was opened with (the designed path); only the binding is needed
  tableMeta.mockRejectedValue(new Error('offline'))
  previewOperations.mockReset()
})
afterEach(cleanup)

describe('ImportDialog accessibility contract', () => {
  it('opens as a labelled modal dialog and focuses the first control', async () => {
    render(<ImportDialog {...dialogProps()} />)
    const dlg = screen.getByRole('dialog')
    expect(dlg.getAttribute('aria-modal')).toBe('true')
    expect(dlg.getAttribute('aria-labelledby')).toBeTruthy()
    expect(document.getElementById(dlg.getAttribute('aria-labelledby')!)!.textContent).toContain('Import into public.imp')
    await waitFor(() => expect(document.activeElement).toBe(document.querySelector('input[type="file"]')))
  })

  it('Escape closes when idle; Tab cycles inside the dialog (focus trap)', () => {
    const onClose = vi.fn()
    render(<ImportDialog {...dialogProps({ onClose })} />)
    const dlg = screen.getByRole('dialog')
    fireEvent.keyDown(dlg, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
    // the close button is the last focusable control in the header order
    const closeBtn = screen.getByRole('button', { name: 'Close import dialog' })
    fireEvent.focus(closeBtn)
    fireEvent.keyDown(dlg, { key: 'Tab' })
    expect(document.activeElement).toBe(document.querySelector('input[type="file"]'))
  })
})

describe('preview and mapping', () => {
  it('streams a CSV file into a preview with auto-mapped columns and NULL vs empty visible', async () => {
    render(<ImportDialog {...dialogProps()} />)
    chooseFile(makeFile('id,qty,note\n1,9007199254740993,\n2,,set\n3,"",""\n'))
    await waitFor(() => expect(screen.getByText(/Column mapping/)).toBeTruthy())
    // auto-map matched by name: three columns, all mapped to source fields
    const selects = document.querySelectorAll('select[aria-label^="Source for column"]')
    expect(selects).toHaveLength(3)
    expect((selects[0] as HTMLSelectElement).value).toBe('f:c0')
    const preview = screen.getByLabelText('Preview of the first rows as they will be inserted')
    expect(preview.textContent).toContain('9007199254740993') // digits exact in preview
    expect(preview.textContent).toContain('NULL') // unquoted empty under COPY convention
    expect(preview.textContent).toContain('(empty string)') // quoted ""
  })

  it('an unparseable file surfaces a role=alert error, not a silent preview', async () => {
    render(<ImportDialog {...dialogProps()} />)
    chooseFile(makeFile('id,qty\n1,"never closed\n'))
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('unterminated')
  })

  it('a NOT NULL unmapped column blocks Import with a visible reason', async () => {
    render(<ImportDialog {...dialogProps()} />)
    chooseFile(makeFile('qty,note\n1,x\n')) // no id source field
    await waitFor(() => expect(screen.getByText(/Column mapping/)).toBeTruthy())
    const problems = screen.getByLabelText('Mapping problems')
    expect(problems.textContent).toContain('id is NOT NULL without a default')
    expect((screen.getByRole('button', { name: 'Import' }) as HTMLButtonElement).disabled).toBe(true)
  })

  it('the dry-run asks the server and reports the outcome or the offending row', async () => {
    previewOperations.mockResolvedValue({ ok: true, counts: { insert: 2 } } as never)
    render(<ImportDialog {...dialogProps()} />)
    chooseFile(makeFile('id,qty,note\n1,5,a\n2,6,b\n'))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Dry-run first batch' })).toBeTruthy())
    fireEvent.click(screen.getByRole('button', { name: 'Dry-run first batch' }))
    await waitFor(() => expect(previewOperations).toHaveBeenCalledTimes(1))
    const ops = previewOperations.mock.calls[0][0].operations
    expect(ops).toHaveLength(2)
    expect(ops[0]).toMatchObject({ op: 'insert', schema: 'public', table: 'imp', binding: 'e1:16390' })
    expect(ops[0].values).toEqual({ id: '1', qty: { t: 'int8', v: '5' }, note: 'a' })
    await waitFor(() => expect(screen.getByText(/would insert/)).toBeTruthy())
  })
})

describe('running, journaling and recovery decisions', () => {
  function transport() {
    const sent: Array<{ operationId: string; rows: unknown[] }> = []
    return {
      sent,
      sendBatch: vi.fn(async (req: { operationId: string; rows: unknown[] }) => {
        sent.push({ operationId: req.operationId, rows: req.rows })
        return { operationId: req.operationId, applied: req.rows.length }
      }),
      outcome: vi.fn(async (connId: string, opId: string) => ({ operationId: opId, state: 'unknown' as const })),
    }
  }

  it('imports batch-wise through the transport, persists the journal, and completes honestly', async () => {
    const { storage, store } = storageStub()
    const tr = transport()
    const props = dialogProps({ storage, transport: tr })
    render(<ImportDialog {...props} />)
    chooseFile(makeFile('id,qty,note\n1,1,a\n2,2,b\n3,3,c\n'))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Import' })).toBeTruthy())
    fireEvent.click(screen.getByRole('button', { name: 'Import' }))

    await waitFor(() => expect(screen.getByText(/Import complete/)).toBeTruthy())
    expect(tr.sendBatch).toHaveBeenCalledTimes(1)
    expect(tr.sent[0].rows[0]).toEqual({ id: '1', qty: { t: 'int8', v: '1' }, note: 'a' })
    // the journal is gone on completion (nothing left to resume)
    expect(store.size).toBe(0)
    expect(props.onImported).toHaveBeenCalled()
  })

  it('a failed batch demands a decision (role=alert); skipping the failed row completes', async () => {
    const { storage } = storageStub()
    const tr = transport()
    tr.sendBatch.mockImplementation(async (req: { operationId: string; rows: Array<{ note?: string }> }) => {
      tr.sent.push({ operationId: req.operationId, rows: req.rows })
      if (req.rows.some(r => r.note === 'bad')) {
        throw new ApiError(409, 'operations[1]: duplicate key value violates unique constraint', {
          state: 'constraint', body: { failedRow: 1, applied: 0 },
        })
      }
      return { operationId: req.operationId, applied: req.rows.length }
    })
    const props = dialogProps({ storage, transport: tr })
    render(<ImportDialog {...props} />)
    chooseFile(makeFile('id,qty,note\n1,1,ok\n2,2,bad\n3,3,ok\n'))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Import' })).toBeTruthy())
    fireEvent.click(screen.getByRole('button', { name: 'Import' }))

    const decision = await screen.findByRole('alert')
    expect(decision.textContent).toContain('Batch 1')
    expect(decision.textContent).toContain('row 2')
    fireEvent.click(screen.getByRole('button', { name: /Skip row 2 and continue/ }))
    await waitFor(() => expect(screen.getByText(/Import complete/)).toBeTruthy())
    // the dialog batches at its configured size: one batch of three rows
    // (fails), then the skip-row retry sends the remaining two
    expect(tr.sent.map(s => s.rows)).toEqual([
      [{ id: '1', qty: { t: 'int8', v: '1' }, note: 'ok' }, { id: '2', qty: { t: 'int8', v: '2' }, note: 'bad' }, { id: '3', qty: { t: 'int8', v: '3' }, note: 'ok' }],
      [{ id: '1', qty: { t: 'int8', v: '1' }, note: 'ok' }, { id: '3', qty: { t: 'int8', v: '3' }, note: 'ok' }],
    ])
  })

  it('an unfinished journal offers resume for the SAME file and refuses a different one', async () => {
    const { storage } = storageStub()
    const j = {
      version: 1 as const,
      importId: 'z9',
      plan: {
        connectionId: 'c1', schema: 'public', table: 'imp', binding: 'e1:16390',
        format: 'csv' as const, csv: { delimiter: ',', header: true },
        values: { emptyUnquoted: 'null' as const, nullMarker: null },
        mapping: { id: { kind: 'field' as const, field: 'c0' }, qty: { kind: 'default' as const }, note: { kind: 'default' as const } },
        batchSize: 2,
      },
      file: { name: 'imp.csv', size: 24, lastModified: 1000 },
      nextRow: 2, rowsCommitted: 2, batchesCommitted: 1, skipped: [], nextBatch: 2,
      status: 'paused' as const,
      startedAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z',
    }
    storage.setItem('neutron-studio:import:c1:public.imp', JSON.stringify(j))
    render(<ImportDialog {...dialogProps({ storage })} />)
    const note = await screen.findByText(/is unfinished/)
    expect(note.textContent).toContain('imp.csv')
    expect(note.textContent).toContain('2 rows committed in 1 batch')
    // a different file is refused with an explanation
    chooseFile(makeFile('id,qty\n9,9\n', 'other.csv', 2000))
    expect((await screen.findByRole('alert')).textContent).toContain('not the file the unfinished import started from')
    // the same file resumes: preview rebuilt from the journal's own mapping
    chooseFile(makeFile('id,qty,note\n1,1,a\n2,2,b\n', 'imp.csv', 1000))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Resume import' })).toBeTruthy())
  })
})
