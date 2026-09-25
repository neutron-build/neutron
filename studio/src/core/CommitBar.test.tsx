import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import {
  stagedEdits, stageEdit, clearStaged, commitPhase, commitError, lastCommit, lastPreview,
  activeConnection, toasts, failedEditFocus,
} from '../lib/store'
import { _setSessionTokenForTests, ApiError } from '../lib/api'
import type { CommitOperation, CommitResponse, PreviewResponse } from '../lib/types'
import { CommitBar } from './CommitBar'

// Rendered CommitBar tests for the S02 atomic commit flow: staged
// operations, preview, atomic commit, retained drafts on failure and
// server-side revert. Only the fetch boundary (lib/api) is mocked; the
// backend contract is pinned by the Go E2E leg.

vi.mock('../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      commitOperations: vi.fn(),
      previewOperations: vi.fn(),
      operationOutcome: vi.fn(),
      revertOperation: vi.fn(),
    },
  }
})

import { api } from '../lib/api'

const commitOperations = vi.mocked(api.commitOperations)
const previewOperations = vi.mocked(api.previewOperations)
const revertOperation = vi.mocked(api.revertOperation)

const updateOp: CommitOperation = {
  op: 'update', schema: 'public', table: 'docs', binding: 'e:1',
  key: [{ column: 'id', value: 1 }], version: '9',
  column: 'note', value: 'staged',
}

const okResponse: CommitResponse = {
  operationId: 'op-1', rowsAffected: 1,
  operations: [{ index: 0, op: 'update', rowsAffected: 1, version: '10' }],
  reversible: true,
}

beforeEach(() => {
  cleanup()
  stagedEdits.value = []
  commitPhase.value = 'idle'
  commitError.value = null
  lastCommit.value = null
  lastPreview.value = null
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'one', url: 'postgres://a', isNucleus: false }
  _setSessionTokenForTests('test-session-token')
  commitOperations.mockReset()
  previewOperations.mockReset()
  revertOperation.mockReset()
})

afterEach(() => {
  activeConnection.value = null
  _setSessionTokenForTests(null)
})

describe('CommitBar — staged atomic commits (S02)', () => {
  it('renders nothing without staged edits or a committed batch', () => {
    const { container } = render(<CommitBar />)
    expect(container.querySelector(`.${'bar'}`)).toBeNull()
    expect(screen.queryByTitle(/Commit all staged edits/)).toBeNull()
  })

  it('lists staged operations and commits them as one batch', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    commitOperations.mockResolvedValueOnce(okResponse)
    render(<CommitBar />)

    expect(screen.getByText('docs.note = staged')).toBeTruthy()
    const commitBtn = screen.getByTitle(/Commit all staged edits as one atomic batch/)
    fireEvent.click(commitBtn)

    await waitFor(() => expect(commitOperations).toHaveBeenCalledTimes(1))
    const sent = commitOperations.mock.calls[0][0]
    expect(sent.connectionId).toBe('c1')
    expect(sent.operations).toEqual([updateOp])
    expect(sent.operationId).toMatch(/^[0-9a-f-]{36}$/)
    await waitFor(() => expect(stagedEdits.value).toEqual([]))
    expect(screen.getByTitle(/Undo committed batch/)).toBeTruthy()
  })

  it('a refused commit keeps the staged edits visible (reconcilable draft)', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    commitOperations.mockRejectedValueOnce(new ApiError(409, 'row changed since it was read', { state: 'conflict' }))
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Commit all staged edits as one atomic batch/))
    await waitFor(() => expect(commitPhase.value).toBe('failed'))
    expect(stagedEdits.value.length).toBe(1)
    expect(screen.getByText('docs.note = staged')).toBeTruthy()
    expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('row changed'))).toBe(true)
  })

  it('preview shows the dry-run counts and applies nothing', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    const report: PreviewResponse = {
      ok: true, counts: { insert: 0, update: 1, delete: 0 },
      operations: [{ index: 0, op: 'update', schema: 'public', table: 'docs', column: 'note', before: 'old', after: 'staged' }],
    }
    previewOperations.mockResolvedValueOnce(report)
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Dry-run this batch/))
    await waitFor(() => expect(previewOperations).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.getByText(/preview: 0\+ 1~ 0−/)).toBeTruthy())
    expect(stagedEdits.value.length).toBe(1)
  })

  it('per-edit discard removes exactly one staged operation', () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'one' })
    stageEdit({ connectionId: 'c1', operation: { ...updateOp, value: 'two' }, label: 'two' })
    render(<CommitBar />)

    fireEvent.click(screen.getAllByTitle('Discard this staged edit')[0])
    expect(stagedEdits.value.length).toBe(1)
    expect(stagedEdits.value[0].label).toBe('two')
  })

  it('revert undoes the last committed batch through the server', async () => {
    lastCommit.value = { operationId: 'op-1', response: okResponse, at: Date.now() }
    revertOperation.mockResolvedValueOnce({ ...okResponse, reverted: 'op-1' })
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Undo committed batch/))
    await waitFor(() => expect(revertOperation).toHaveBeenCalledTimes(1))
    expect(revertOperation.mock.calls[0][0].operationId).toBe('op-1')
    await waitFor(() => expect(lastCommit.value).toBeNull())
  })

  it('an irreversible commit shows its honest refusal instead of a revert button', () => {
    lastCommit.value = {
      operationId: 'op-9', at: Date.now(),
      response: { ...okResponse, operationId: 'op-9', reversible: false, reversibleReason: 'identity column "id" cannot be re-inserted on revert' },
    }
    render(<CommitBar />)
    const btn = screen.getByTitle(/identity column/) as HTMLButtonElement
    expect(btn.disabled).toBe(true)
  })

  it('staged edits bound to another connection never commit through the active one', async () => {
    stageEdit({ connectionId: 'c2', operation: { ...updateOp, binding: 'e:2' }, label: 'foreign conn' })
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Commit all staged edits as one atomic batch/))
    await waitFor(() => expect(toasts.value.some(t => t.message.includes('another connection'))).toBe(true))
    expect(commitOperations).not.toHaveBeenCalled()
  })

  it('a binding refusal at commit keeps the draft and surfaces the state', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    commitOperations.mockRejectedValueOnce(new ApiError(409,
      'public.docs is no longer the relation these rows were read from; reload before editing', { state: 'binding' }))
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Commit all staged edits as one atomic batch/))
    await waitFor(() => expect(commitPhase.value).toBe('failed'))
    expect(stagedEdits.value.length).toBe(1)
    expect(screen.getByRole('alert').textContent).toContain('no longer the relation')
  })

  it('a failed commit pins the first offending staged edit (error focus)', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'first' })
    stageEdit({ connectionId: 'c1', operation: { ...updateOp, value: 'second' }, label: 'second' })
    commitOperations.mockRejectedValueOnce(new ApiError(409,
      'operations[1]: update refused: row changed since it was read', { state: 'conflict' }))
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Commit all staged edits as one atomic batch/))
    await waitFor(() => expect(commitPhase.value).toBe('failed'))
    expect(failedEditFocus.value).not.toBeNull()
    expect(failedEditFocus.value!.editId).toBe(stagedEdits.value[1].id)
    // the bar announces the error and is keyboard reachable
    const alert = screen.getByRole('alert')
    expect(alert.textContent).toContain('operations[1]')
    expect(alert.getAttribute('tabIndex')).toBe('-1')
    // the draft stays staged for reconciliation
    expect(stagedEdits.value.length).toBe(2)
  })

  it('preview detail lists the per-operation dry-run diff', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    const report: PreviewResponse = {
      ok: true, counts: { insert: 1, update: 1, delete: 1 },
      operations: [
        { index: 0, op: 'update', schema: 'public', table: 'docs', column: 'note', before: 'old', after: 'staged' },
        { index: 1, op: 'insert', schema: 'public', table: 'docs', after: { id: 9 } },
        { index: 2, op: 'delete', schema: 'public', table: 'docs', key: [{ column: 'id', value: 3 }] },
      ],
    }
    previewOperations.mockResolvedValueOnce(report)
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Dry-run this batch/))
    await waitFor(() => expect(screen.getByText(/preview: 1\+ 1~ 1−/)).toBeTruthy())
    fireEvent.click(screen.getByText(/preview: 1\+ 1~ 1−/))
    const ops = screen.getAllByText(/^(\+ insert|~ update|− delete)/)
    expect(ops.length).toBe(3)
    expect(ops[0].textContent).toContain('"old"')
    expect(ops[1].textContent).toContain('insert public.docs')
    expect(ops[2].textContent).toContain('id=3')
  })
})
