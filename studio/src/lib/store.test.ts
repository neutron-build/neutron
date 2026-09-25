import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  connections, activeConnection, connectionLoading, connectionError,
  features, isNucleus, schema, schemaLoading,
  tabs, activeTabId, activeTab, openTab, closeTab,
  pendingChanges, pendingCount, addPending, removePending, revertLast, clearPending,
  theme, toggleTheme,
  paletteOpen, paletteQuery, openPalette, closePalette,
  toasts, toast, bindingActive,
  stagedEdits, stagedCount, stageEdit, removeStagedEdit, discardLastStaged, clearStaged,
  stagedForTable, keyStringOf, firstOffendingOpIndex, failedEditFocus,
  commitStaged, previewStaged, revertLastCommit,
  commitPhase, commitError, lastCommit, lastPreview,
} from './store'
import type { Tab, PendingChange, CommitResponse, PreviewResponse } from './types'
import { ApiError } from './api'

// The S02 store tests mock only the fetch boundary (lib/api), per the
// repo's testing convention; backend semantics live in the Go E2E leg.
vi.mock('./api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('./api')>()
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

import { api } from './api'

const commitOperations = vi.mocked(api.commitOperations)
const previewOperations = vi.mocked(api.previewOperations)
const operationOutcome = vi.mocked(api.operationOutcome)
const revertOperation = vi.mocked(api.revertOperation)

describe('store — connection state', () => {
  beforeEach(() => {
    connections.value = []
    activeConnection.value = null
    connectionLoading.value = false
    connectionError.value = null
  })

  it('should start with empty connections', () => {
    expect(connections.value).toEqual([])
    expect(activeConnection.value).toBeNull()
  })

  it('should set and read active connection', () => {
    const conn = { id: 'c1', name: 'Test', url: 'pg://test', isNucleus: false }
    activeConnection.value = conn
    expect(activeConnection.value).toEqual(conn)
  })

  it('should track loading and error state', () => {
    connectionLoading.value = true
    expect(connectionLoading.value).toBe(true)

    connectionError.value = 'Failed to connect'
    expect(connectionError.value).toBe('Failed to connect')
  })
})

describe('store — features', () => {
  beforeEach(() => {
    features.value = { isNucleus: false, version: '', models: [] }
  })

  it('should compute isNucleus from features signal', () => {
    expect(isNucleus.value).toBe(false)
    features.value = { isNucleus: true, version: '0.1.0', models: ['sql', 'kv'] }
    expect(isNucleus.value).toBe(true)
  })
})

describe('store — schema', () => {
  beforeEach(() => {
    schema.value = null
    schemaLoading.value = false
  })

  it('should start with null schema', () => {
    expect(schema.value).toBeNull()
  })
})

describe('store — tabs', () => {
  beforeEach(() => {
    tabs.value = []
    activeTabId.value = null
  })

  it('should start with no tabs', () => {
    expect(tabs.value).toEqual([])
    expect(activeTab.value).toBeNull()
  })

  it('should open a new tab', () => {
    const tab: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    openTab(tab)
    expect(tabs.value.length).toBe(1)
    expect(activeTabId.value).toBe('t1')
    expect(activeTab.value).toEqual(tab)
  })

  it('should not duplicate tabs with same kind, objectSchema, objectName', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(1)
    // Should activate the existing tab
    expect(activeTabId.value).toBe('t1')
  })

  it('should allow tabs with different objectName', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'sql-browser', label: 'posts', objectSchema: 'public', objectName: 'posts' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(2)
    expect(activeTabId.value).toBe('t2')
  })

  it('should close a tab', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(2)
    expect(activeTabId.value).toBe('t2')

    closeTab('t2')
    expect(tabs.value.length).toBe(1)
    // Should activate the previous tab
    expect(activeTabId.value).toBe('t1')
  })

  it('should handle closing the last tab', () => {
    const tab: Tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    openTab(tab)
    closeTab('t1')
    expect(tabs.value.length).toBe(0)
    expect(activeTabId.value).toBeNull()
  })

  it('should not change activeTabId when closing a non-active tab', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    const tab2: Tab = { id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' }
    const tab3: Tab = { id: 't3', kind: 'vector', label: 'embeddings', objectName: 'embeddings' }
    openTab(tab1)
    openTab(tab2)
    openTab(tab3)

    // t3 is active, close t1
    closeTab('t1')
    expect(activeTabId.value).toBe('t3')
    expect(tabs.value.length).toBe(2)
  })
})

describe('store — pending changes', () => {
  beforeEach(() => {
    pendingChanges.value = []
  })

  it('should start with zero pending changes', () => {
    expect(pendingCount.value).toBe(0)
  })

  it('should add pending changes', () => {
    const change: PendingChange = {
      id: 'p1',
      model: 'sql',
      label: 'test change',
      sql: "UPDATE x SET y = 1",
      revert: () => {},
    }
    addPending(change)
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p1')
  })

  it('should remove a pending change by id', () => {
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => {} })
    expect(pendingCount.value).toBe(2)

    removePending('p1')
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p2')
  })

  it('should revert the last pending change', () => {
    let revertCalled = false
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => { revertCalled = true } })

    revertLast()
    expect(revertCalled).toBe(true)
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p1')
  })

  it('should do nothing when reverting with no changes', () => {
    revertLast()
    expect(pendingCount.value).toBe(0)
  })

  it('should clear all pending changes', () => {
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => {} })
    clearPending()
    expect(pendingCount.value).toBe(0)
  })
})

describe('store — theme', () => {
  beforeEach(() => {
    theme.value = 'dark'
  })

  it('should toggle theme from dark to light', () => {
    expect(theme.value).toBe('dark')
    toggleTheme()
    expect(theme.value).toBe('light')
  })

  it('should toggle theme from light to dark', () => {
    theme.value = 'light'
    toggleTheme()
    expect(theme.value).toBe('dark')
  })
})

describe('store — command palette', () => {
  beforeEach(() => {
    paletteOpen.value = false
    paletteQuery.value = ''
  })

  it('should open palette and clear query', () => {
    paletteQuery.value = 'old search'
    openPalette()
    expect(paletteOpen.value).toBe(true)
    expect(paletteQuery.value).toBe('')
  })

  it('should close palette', () => {
    openPalette()
    closePalette()
    expect(paletteOpen.value).toBe(false)
  })
})

describe('store — toast notifications', () => {
  beforeEach(() => {
    toasts.value = []
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should add a toast', () => {
    toast('success', 'Changes saved')
    expect(toasts.value.length).toBe(1)
    expect(toasts.value[0].kind).toBe('success')
    expect(toasts.value[0].message).toBe('Changes saved')
  })

  it('should add multiple toasts', () => {
    toast('info', 'Info message')
    toast('error', 'Error message')
    expect(toasts.value.length).toBe(2)
  })

  it('should auto-remove toast after 4 seconds', () => {
    toast('success', 'Will disappear')
    expect(toasts.value.length).toBe(1)

    vi.advanceTimersByTime(4000)
    expect(toasts.value.length).toBe(0)
  })

  it('should not remove other toasts when one expires', () => {
    toast('info', 'First')
    vi.advanceTimersByTime(2000)
    toast('info', 'Second')
    expect(toasts.value.length).toBe(2)

    // First toast disappears at t=4000
    vi.advanceTimersByTime(2000)
    expect(toasts.value.length).toBe(1)
    expect(toasts.value[0].message).toBe('Second')
  })
})

describe('store — editing binding (S01 lost-window semantics)', () => {
  beforeEach(() => {
    activeConnection.value = { id: 'c1', name: 'one', url: 'postgres://a', isNucleus: false }
  })
  afterEach(() => {
    activeConnection.value = null
  })

  it('a binding is active while its connection is the active connection', () => {
    const binding = { connectionId: 'c1', schema: 'public', table: 'docs' }
    expect(bindingActive(binding)).toBe(true)
  })

  it('switching connections deactivates bindings captured under the old connection', () => {
    const binding = { connectionId: 'c1', schema: 'public', table: 'docs' }
    activeConnection.value = { id: 'c2', name: 'two', url: 'postgres://b', isNucleus: false }
    expect(bindingActive(binding)).toBe(false)
  })

  it('no active connection deactivates every binding', () => {
    activeConnection.value = null
    expect(bindingActive({ connectionId: 'c1', schema: 'public', table: 't' })).toBe(false)
  })
})

describe('store — staged edits and commit outcomes (S02)', () => {
  const updateOp = {
    op: 'update' as const, schema: 'public', table: 'docs', binding: 'e:1',
    key: [{ column: 'id', value: 1 }], version: '9',
    column: 'note', value: 'staged',
  }

  beforeEach(() => {
    stagedEdits.value = []
    commitPhase.value = 'idle'
    commitError.value = null
    lastCommit.value = null
    lastPreview.value = null
    activeConnection.value = { id: 'c1', name: 'one', url: 'postgres://a', isNucleus: false }
    commitOperations.mockReset()
    previewOperations.mockReset()
    operationOutcome.mockReset()
    revertOperation.mockReset()
  })
  afterEach(() => {
    activeConnection.value = null
  })

  it('stages edits with a stable list contract', () => {
    const a = stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    const b = stageEdit({ connectionId: 'c2', operation: { ...updateOp, binding: 'e:2' }, label: 'other-conn' })
    expect(stagedCount.value).toBe(2)
    expect(a.id).not.toBe(b.id)

    removeStagedEdit(a.id)
    expect(stagedEdits.value.map(e => e.id)).toEqual([b.id])

    discardLastStaged()
    expect(stagedCount.value).toBe(0)
    expect(discardLastStaged()).toBeUndefined()

    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'x' })
    stageEdit({ connectionId: 'c2', operation: updateOp, label: 'y' })
    clearStaged('c1')
    expect(stagedEdits.value.every(e => e.connectionId === 'c2')).toBe(true)
    clearStaged()
    expect(stagedCount.value).toBe(0)
  })

  it('a successful commit sends one atomic batch under one operation ID and clears only that connection\'s draft', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    stageEdit({ connectionId: 'c2', operation: updateOp, label: 'other' })

    const response: CommitResponse = {
      operationId: 'server-sees-this', rowsAffected: 1,
      operations: [{ index: 0, op: 'update', rowsAffected: 1, version: '10' }],
      reversible: true,
    }
    commitOperations.mockResolvedValue(response)

    const res = await commitStaged('c1')
    expect(res).toEqual(response)
    expect(commitOperations).toHaveBeenCalledTimes(1)
    const sent = commitOperations.mock.calls[0][0]
    expect(sent.connectionId).toBe('c1')
    expect(sent.operationId).toMatch(/^[0-9a-f-]{36}$/)
    expect(sent.operations).toEqual([updateOp])
    expect(commitPhase.value).toBe('committed')
    expect(stagedEdits.value.map(e => e.connectionId)).toEqual(['c2'])
    expect(lastCommit.value?.operationId).toBe(sent.operationId)
  })

  it('a refused commit retains the staged draft (reconcilable)', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    commitOperations.mockRejectedValue(new ApiError(409, 'row changed since it was read', { state: 'conflict' }))

    await expect(commitStaged('c1')).rejects.toBeInstanceOf(ApiError)
    expect(commitPhase.value).toBe('failed')
    expect(commitError.value).toContain('row changed since it was read')
    expect(stagedCount.value).toBe(1)
    expect(lastCommit.value).toBeNull()
  })

  it('a dropped response after commit resolves the recorded outcome with the SAME operation ID — no second send', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })

    // The fetch fails mid-flight (network drop) after the server committed.
    commitOperations.mockRejectedValueOnce(new TypeError('network dropped'))
    const recorded: CommitResponse = {
      operationId: 'resolved', rowsAffected: 1,
      operations: [{ index: 0, op: 'update', rowsAffected: 1, version: '10' }],
      reversible: true,
    }
    operationOutcome.mockResolvedValueOnce({ operationId: 'resolved', state: 'committed', status: 200, response: recorded })

    const res = await commitStaged('c1')
    expect(res).toEqual(recorded)
    expect(commitOperations).toHaveBeenCalledTimes(1) // never re-sent
    const sentId = commitOperations.mock.calls[0][0].operationId
    expect(operationOutcome).toHaveBeenCalledWith('c1', sentId)
    expect(commitPhase.value).toBe('committed')
    expect(stagedCount.value).toBe(0)
    expect(lastCommit.value?.response).toEqual(recorded)
  })

  it('an unknown outcome (expired/evicted/restart) never auto-recommits and keeps the draft', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    commitOperations.mockRejectedValueOnce(new TypeError('network dropped'))
    operationOutcome.mockResolvedValueOnce({ operationId: 'x', state: 'unknown' })

    await expect(commitStaged('c1')).rejects.toThrow('outcome unknown')
    expect(commitOperations).toHaveBeenCalledTimes(1)
    expect(commitPhase.value).toBe('failed')
    expect(stagedCount.value).toBe(1)
  })

  it('a failed recorded outcome surfaces the failure without a second send', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    commitOperations.mockRejectedValueOnce(new TypeError('network dropped'))
    operationOutcome.mockResolvedValueOnce({ operationId: 'x', state: 'failed', status: 409 })

    await expect(commitStaged('c1')).rejects.toThrow('network dropped')
    expect(commitOperations).toHaveBeenCalledTimes(1)
    expect(commitPhase.value).toBe('failed')
    expect(stagedCount.value).toBe(1)
  })

  it('a server-side unknown state (ambiguous commit) resolves the outcome instead of retrying', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    commitOperations.mockRejectedValueOnce(new ApiError(502, 'the outcome cannot be determined', { state: 'unknown' }))
    operationOutcome.mockResolvedValueOnce({ operationId: 'x', state: 'unknown' })

    await expect(commitStaged('c1')).rejects.toThrow('outcome unknown')
    expect(operationOutcome).toHaveBeenCalledTimes(1)
    expect(stagedCount.value).toBe(1)
  })

  it('an empty staged batch refuses to commit', async () => {
    await expect(commitStaged('c1')).rejects.toThrow('no staged edits')
    expect(commitOperations).not.toHaveBeenCalled()
  })

  it('preview runs the staged batch dry and stores the report', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'docs.note' })
    const report: PreviewResponse = {
      ok: true, counts: { insert: 0, update: 1, delete: 0 },
      operations: [{ index: 0, op: 'update', schema: 'public', table: 'docs', column: 'note', before: 'old', after: 'staged' }],
    }
    previewOperations.mockResolvedValueOnce(report)

    const res = await previewStaged('c1')
    expect(res).toEqual(report)
    expect(lastPreview.value).toEqual(report)
    expect(previewOperations.mock.calls[0][0].operations).toEqual([updateOp])
    expect(stagedCount.value).toBe(1) // preview stages nothing
  })

  it('revert sends the last committed operation ID under a fresh idempotency key', async () => {
    lastCommit.value = {
      operationId: 'original-op', at: Date.now(),
      response: { operationId: 'original-op', rowsAffected: 1, operations: [], reversible: true },
    }
    const reverted: CommitResponse = {
      operationId: 'new-key', rowsAffected: 1, operations: [], reversible: false, reverted: 'original-op',
    }
    revertOperation.mockResolvedValueOnce(reverted)

    const res = await revertLastCommit('c1')
    expect(res.reverted).toBe('original-op')
    expect(revertOperation).toHaveBeenCalledWith({
      connectionId: 'c1',
      operationId: 'original-op',
      revertOperationId: expect.stringMatching(/^[0-9a-f-]{36}$/),
    })
    expect(lastCommit.value).toBeNull()
    expect(commitPhase.value).toBe('idle')
  })

  it('revert without a committed batch is an honest refusal', async () => {
    await expect(revertLastCommit('c1')).rejects.toThrow('nothing to revert')
    expect(revertOperation).not.toHaveBeenCalled()
  })

  it('each commit attempt uses a fresh operation ID (spent IDs are never reused)', async () => {
    stageEdit({ connectionId: 'c1', operation: updateOp, label: 'a' })
    commitOperations.mockRejectedValueOnce(new ApiError(409, 'conflict', { state: 'conflict' }))
    await expect(commitStaged('c1')).rejects.toBeInstanceOf(ApiError)

    const ok: CommitResponse = { operationId: 'second', rowsAffected: 1, operations: [], reversible: true }
    commitOperations.mockResolvedValueOnce(ok)
    await commitStaged('c1')

    const firstId = commitOperations.mock.calls[0][0].operationId
    const secondId = commitOperations.mock.calls[1][0].operationId
    expect(firstId).not.toBe(secondId)
  })
})

describe('store — S03 table-scoped staging and error focus', () => {
  const upd = (id: number) => ({
    op: 'update' as const, schema: 'public', table: 'docs', binding: 'e:1',
    key: [{ column: 'id', value: id }], version: '9',
    column: 'note', value: `staged-${id}`,
  })

  beforeEach(() => {
    stagedEdits.value = []
    commitPhase.value = 'idle'
    commitError.value = null
    failedEditFocus.value = null
    activeConnection.value = { id: 'c1', name: 'one', url: 'postgres://a', isNucleus: false }
  })
  afterEach(() => { activeConnection.value = null })

  it('stagedForTable scopes by connection, schema and table', () => {
    stageEdit({ connectionId: 'c1', operation: upd(1), label: 'a' })
    stageEdit({ connectionId: 'c1', operation: { ...upd(2), table: 'other' }, label: 'b' })
    stageEdit({ connectionId: 'c2', operation: upd(3), label: 'c' })
    stageEdit({ connectionId: 'c1', operation: { op: 'insert', schema: 'public', table: 'docs', binding: 'e:1', values: { id: 9 } }, label: 'd' })

    const docs = stagedForTable('c1', 'public', 'docs')
    expect(docs.map(e => e.label)).toEqual(['a', 'd'])
    expect(stagedForTable('c1', 'public', 'other').map(e => e.label)).toEqual(['b'])
    expect(stagedForTable('c2', 'public', 'docs').map(e => e.label)).toEqual(['c'])
  })

  it('keyStringOf is canonical and distinct for tagged vs plain values', () => {
    expect(keyStringOf([{ column: 'id', value: 1 }])).toBe(keyStringOf([{ column: 'id', value: 1 }]))
    expect(keyStringOf([{ column: 'id', value: 1 }])).not.toBe(keyStringOf([{ column: 'id', value: 2 }]))
    expect(keyStringOf([{ column: 'a', value: 1 }, { column: 'b', value: 2 }]))
      .not.toBe(keyStringOf([{ column: 'b', value: 2 }, { column: 'a', value: 1 }]))
    expect(keyStringOf([{ column: 'id', value: { t: 'int8', v: '9007199254740993' } }]))
      .toBe('[["id",{"t":"int8","v":"9007199254740993"}]]')
  })

  it('firstOffendingOpIndex parses the server batch error position', () => {
    expect(firstOffendingOpIndex('operations[3]: update refused')).toBe(3)
    expect(firstOffendingOpIndex('operations[12]: column "x" ...')).toBe(12)
    expect(firstOffendingOpIndex('commit limited to 100 operations')).toBe(0)
    expect(firstOffendingOpIndex('')).toBe(0)
  })

  it('a refused commit pins the first offending staged edit for grid focus', async () => {
    stageEdit({ connectionId: 'c1', operation: upd(1), label: 'ok-row' })
    stageEdit({ connectionId: 'c1', operation: upd(2), label: 'bad-row' })
    commitOperations.mockRejectedValueOnce(new ApiError(409,
      'operations[1]: update refused: row changed since it was read (current row version 99)', { state: 'conflict' }))

    await expect(commitStaged('c1')).rejects.toBeInstanceOf(ApiError)
    expect(failedEditFocus.value).not.toBeNull()
    expect(failedEditFocus.value!.editId).toBe(stagedEdits.value[1].id)
    expect(failedEditFocus.value!.reason).toContain('operations[1]')
    // the draft stays staged
    expect(stagedCount.value).toBe(2)
  })

  it('a successful commit clears any stale focus pin', async () => {
    failedEditFocus.value = { editId: 'stale', reason: 'x' }
    stageEdit({ connectionId: 'c1', operation: upd(1), label: 'a' })
    commitOperations.mockResolvedValueOnce({
      operationId: 'op', rowsAffected: 1, operations: [], reversible: true,
    })
    await commitStaged('c1')
    expect(failedEditFocus.value).toBeNull()
  })
})
