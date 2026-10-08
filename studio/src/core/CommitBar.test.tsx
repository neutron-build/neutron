import { commitReceiptFor, commitRefusalFor } from '../lib/commitFixture'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import {
  stagedEdits, stageEdit, clearStaged, commitPhase, commitError, lastCommit, lastPreview,
  activeConnection, toasts, failedEditFocus, limitsReport, pendingCommits, _resetCommitBatchesForTests,
} from '../lib/store'
import limitsFixture from '../lib/limits.fixture.json'
import type { LimitsReport } from '../lib/types'
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
  _resetCommitBatchesForTests()
  vi.mocked(api.operationOutcome).mockReset()
  stagedEdits.value = []
  commitPhase.value = 'idle'
  commitError.value = null
  lastCommit.value = null
  lastPreview.value = null
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'one', url: 'postgres://a', isNucleus: false }
  // X06: the bar claims atomicity only when the SQL limits establish it
  // (PostgreSQL here; the Nucleus wording is pinned below).
  limitsReport.value = limitsFixture.postgres as LimitsReport
  _setSessionTokenForTests('test-session-token')
  commitOperations.mockReset()
  previewOperations.mockReset()
  revertOperation.mockReset()
})

afterEach(() => {
  activeConnection.value = null
  limitsReport.value = null
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
    commitOperations.mockImplementationOnce(async input => commitReceiptFor(input.operationId, input.operations, true))
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
    commitOperations.mockImplementationOnce(async input => { throw commitRefusalFor(new ApiError(409, 'row changed since it was read', { state: 'conflict' }), input.operationId) })
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
    lastCommit.value = { connectionId: 'c1', operationId: 'op-1', response: okResponse, at: Date.now() }
    revertOperation.mockResolvedValueOnce({ ...okResponse, reverted: 'op-1' })
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Undo committed batch/))
    await waitFor(() => expect(revertOperation).toHaveBeenCalledTimes(1))
    expect(revertOperation.mock.calls[0][0].operationId).toBe('op-1')
    await waitFor(() => expect(lastCommit.value).toBeNull())
  })

  it('an irreversible commit shows its honest refusal instead of a revert button', () => {
    lastCommit.value = { connectionId: 'c1',
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
    commitOperations.mockImplementationOnce(async input => { throw commitRefusalFor(new ApiError(409,
      'public.docs is no longer the relation these rows were read from; reload before editing', { state: 'binding' }), input.operationId) })
    render(<CommitBar />)

    fireEvent.click(screen.getByTitle(/Commit all staged edits as one atomic batch/))
    await waitFor(() => expect(commitPhase.value).toBe('failed'))
    expect(stagedEdits.value.length).toBe(1)
    expect(screen.getByRole('alert').textContent).toContain('no longer the relation')
  })

  it('a failed commit pins the first offending staged edit (error focus)', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'first' })
    stageEdit({ connectionId: 'c1', operation: { ...updateOp, value: 'second' }, label: 'second' })
    commitOperations.mockImplementationOnce(async input => { throw commitRefusalFor(new ApiError(409,
      'operations[1]: update refused: row changed since it was read', { state: 'conflict' }), input.operationId) })
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

describe('CommitBar — engine limits wording (X06)', () => {
  it('never says atomic on an engine whose SQL transactions are partial', async () => {
    activeConnection.value = { id: 'c1', name: 'nuc', url: 'postgres://n', isNucleus: true }
    limitsReport.value = limitsFixture.nucleus as LimitsReport
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    commitOperations.mockImplementationOnce(async input => commitReceiptFor(input.operationId, input.operations, true))
    render(<CommitBar />)
    expect(screen.queryByTitle(/atomic batch/)).toBeNull()
    const btn = screen.getByTitle(/Commit all staged edits in one transaction/)
    expect(btn.getAttribute('title')).toContain('DDL is NOT transactional')
    fireEvent.click(btn)
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'success')).toBe(true))
    const msg = toasts.value.find(t => t.kind === 'success')!.message
    expect(msg).not.toMatch(/atomically/)
    expect(msg).toMatch(/not verified atomic/)
  })

  it('claims nothing while limits are not loaded', () => {
    limitsReport.value = null
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note = staged' })
    render(<CommitBar />)
    expect(screen.queryByTitle(/atomic batch/)).toBeNull()
    expect(screen.getByTitle(/limits are not loaded/)).toBeTruthy()
  })
})


describe('CommitBar shortcut ownership boundaries', () => {
  it('cannot commit or discard hidden staged operations from dialog controls', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'pending workspace edit' })
    commitOperations.mockImplementationOnce(async input => commitReceiptFor(input.operationId, input.operations, true))
    render(<><CommitBar /><div role="dialog" aria-label="Import or command dialog"><input aria-label="Dialog input" /></div><button>Workspace action</button></>)
    const input = screen.getByRole('textbox', { name: 'Dialog input' })
    for (const modifier of [{ ctrlKey: true }, { metaKey: true }]) {
      fireEvent.keyDown(input, { key: 's', ...modifier })
      fireEvent.keyDown(input, { key: 'z', ...modifier })
    }
    expect(commitOperations).not.toHaveBeenCalled()
    expect(stagedEdits.value).toHaveLength(1)
    expect(commitPhase.value).toBe('idle')
    // The same intentional save shortcut still works in the workspace.
    fireEvent.keyDown(screen.getByRole('button', { name: 'Workspace action' }), { key: 's', ctrlKey: true })
    await waitFor(() => expect(commitOperations).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(stagedEdits.value).toHaveLength(0))
  })

  it('preserves native text undo while retaining workspace staged undo', () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'pending workspace edit' })
    render(<><CommitBar /><textarea aria-label="SQL text" /><button>Workspace action</button></>)
    const input = screen.getByRole('textbox', { name: 'SQL text' })
    const nativeUndo = new KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true, cancelable: true })
    input.dispatchEvent(nativeUndo)
    expect(nativeUndo.defaultPrevented).toBe(false)
    expect(stagedEdits.value).toHaveLength(1)
    fireEvent.keyDown(screen.getByRole('button', { name: 'Workspace action' }), { key: 'z', ctrlKey: true })
    expect(stagedEdits.value).toHaveLength(0)
  })
})

it('repeated and simultaneous save shortcuts send one frozen operation ID', async () => {
  stageEdit({connectionId:'c1',operation:{op:'insert',schema:'public',table:'docs',binding:'e:1',values:{note:'draft'}},label:'insert'})
  let finish!:(response:CommitResponse)=>void
  commitOperations.mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve}))
  render(<><CommitBar/><button>Workspace shortcut</button></>)
  const workspace=screen.getByText('Workspace shortcut')
  await fireEvent.keyDown(workspace,{key:'s',ctrlKey:true})
  await fireEvent.keyDown(workspace,{key:'s',ctrlKey:true,repeat:true})
  await fireEvent.keyDown(workspace,{key:'s',ctrlKey:true})
  await waitFor(()=>expect(commitOperations).toHaveBeenCalledTimes(1))
  stageEdit({connectionId:'c1',operation:updateOp,label:'later draft'})
  finish(commitReceiptFor(commitOperations.mock.calls[0][0].operationId, commitOperations.mock.calls[0][0].operations, true))
  await waitFor(()=>expect(commitPhase.value).toBe('committed'))
  expect(stagedEdits.value.map(e=>e.label)).toEqual(['later draft'])
})

// R4: exercise the actual count-independent bar and keyboard recovery.
it.each(['button', 'keyboard'])('recovers discarded uncertain batch through %s on its original connection', async mode => {
  let lose!: (error: Error) => void
  commitOperations.mockImplementation(() => new Promise((_resolve, reject) => { lose = reject }))
  vi.mocked(api.operationOutcome).mockImplementationOnce(async (_c, id) => ({operationId:id, state:'unknown'}))
  stageEdit({ connectionId: 'c1', operation: updateOp, label: 'original' })
  render(<CommitBar />)
  fireEvent.click(screen.getByTitle(/Commit all staged edits/))
  await waitFor(() => expect(commitOperations).toHaveBeenCalledTimes(1))
  const operationId = commitOperations.mock.calls[0][0].operationId
  fireEvent.click(screen.getByTitle('Discard every staged edit'))
  expect(stagedEdits.value).toHaveLength(0)
  lose(new Error('lost response'))
  await waitFor(() => expect(pendingCommits.value[0]?.state).toBe('uncertain'))
  activeConnection.value = { id: 'c2', name: 'two', url: 'postgres://b', isNucleus: false }
  vi.mocked(api.operationOutcome).mockResolvedValueOnce({ operationId, state: 'committed', response: commitReceiptFor(operationId, commitOperations.mock.calls[0][0].operations, true) } as never)
  const recovery = await screen.findByRole('button', { name: `Check outcome ${operationId} on c1` })
  if (mode === 'button') fireEvent.click(recovery)
  else fireEvent.keyDown(document.body, { key: 's', ctrlKey: true })
  await waitFor(() => expect(pendingCommits.value).toHaveLength(0))
  expect(api.operationOutcome).toHaveBeenLastCalledWith('c1', operationId)
  expect(commitOperations).toHaveBeenCalledTimes(1)
  expect(lastCommit.value?.operationId).toBe(operationId)
})

it.each([{}, null, 'wrong-committed-id', 'wrong-failed-id'])('zero-draft UI retains outcome recovery after invalid receipt %j', async malformed => {
  stageEdit({ connectionId: 'c1', operation: updateOp, label: 'original' })
  commitOperations.mockResolvedValueOnce(malformed as never)
  vi.mocked(api.operationOutcome).mockImplementation(async (_c, id) => malformed === 'wrong-committed-id'
    ? { operationId: 'other', state: 'committed', response: commitReceiptFor('other', [updateOp]) }
    : malformed === 'wrong-failed-id'
      ? { operationId: 'other', state: 'failed', status: 409, response: { operationId: 'other', error: 'no' } } as never
      : { operationId: id, state: 'unknown' })
  render(<CommitBar />)
  fireEvent.click(screen.getByText(/Commit 1 change/))
  await waitFor(() => expect(commitOperations).toHaveBeenCalledTimes(1))
  const operationId = commitOperations.mock.calls[0][0].operationId
  await screen.findByRole('button', { name: `Check outcome ${operationId} on c1` })
  clearStaged('c1')
  const recovery = await screen.findByRole('button', { name: `Check outcome ${operationId} on c1` })
  vi.mocked(api.operationOutcome).mockImplementationOnce(async (_c, id) => ({ operationId: id, state: 'committed', response: commitReceiptFor(id, [updateOp]) }))
  fireEvent.click(recovery)
  await waitFor(() => expect(lastCommit.value?.operationId).toBe(operationId))
  expect(commitOperations).toHaveBeenCalledTimes(1)
  expect(api.operationOutcome).toHaveBeenLastCalledWith('c1', operationId)
})
