import { signal, computed } from '@preact/signals'
import type { Connection, Schema, NucleusFeatures, Tab, PendingChange, CommitOperation, CommitResponse, PreviewResponse, OutcomeResponse, KeyCell } from './types'
import { api, ApiError } from './api'
import { serializeDeepLink } from './router'

// --- Connection state ---

export const connections = signal<Connection[]>([])
export const activeConnection = signal<Connection | null>(null)
export const connectionLoading = signal(false)
export const connectionError = signal<string | null>(null)

/** Connect a saved connection and refresh every connection-scoped signal
 * (features, schema, active connection). Shared by the connection manager
 * and the S05 deep-link router so both establish the same state. */
export async function connectConnection(id: string): Promise<void> {
  connectionLoading.value = true
  connectionError.value = null
  try {
    const { features: f, schema: sc } = await api.connections.connect(id)
    features.value = f
    schema.value = sc
    const conn = connections.value.find(c => c.id === id) ?? null
    if (conn) activeConnection.value = { ...conn, isNucleus: f.isNucleus }
  } catch (err: unknown) {
    connectionError.value = err instanceof Error ? err.message : String(err)
    throw err
  } finally {
    connectionLoading.value = false
  }
}

// --- Nucleus feature detection ---

export const features = signal<NucleusFeatures>({
  isNucleus: false,
  version: '',
  models: [],
})

export const isNucleus = computed(() => features.value.isNucleus)

// --- Schema ---

export const schema = signal<Schema | null>(null)
export const schemaLoading = signal(false)

// --- Tabs ---

export const tabs = signal<Tab[]>([])
export const activeTabId = signal<string | null>(null)

export const activeTab = computed(() =>
  tabs.value.find(t => t.id === activeTabId.value) ?? null
)

export function openTab(tab: Tab) {
  // Filtered views (FK follow) must not collapse into the unfiltered tab.
  if (!tab.filter && !tab.match) {
    const existing = tabs.value.find(t =>
      t.kind === tab.kind &&
      t.objectSchema === tab.objectSchema &&
      t.objectName === tab.objectName &&
      !t.initialSql
    )
    if (existing) {
      activeTabId.value = existing.id
      updateLocationHash(existing)
      return
    }
  }
  if (!tab.id) {
    tab.id = `tab-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  }
  tabs.value = [...tabs.value, tab]
  activeTabId.value = tab.id
  updateLocationHash(tab)
}

/** Keep the URL pointing at the active browsable tab (S05 deep links). */
function updateLocationHash(tab: Tab) {
  if (typeof window === 'undefined' || typeof history === 'undefined') return
  const conn = activeConnection.value
  if (!conn) return
  const link = serializeDeepLink(tab, conn.id)
  if (link === null) return
  if (window.location.hash !== link) {
    history.replaceState(null, '', link)
  }
}

export function closeTab(id: string) {
  const idx = tabs.value.findIndex(t => t.id === id)
  tabs.value = tabs.value.filter(t => t.id !== id)
  if (activeTabId.value === id) {
    const next = tabs.value[Math.max(0, idx - 1)]
    activeTabId.value = next?.id ?? null
  }
}

// --- Pending changes (commit bar) ---

export const pendingChanges = signal<PendingChange[]>([])

export const pendingCount = computed(() => pendingChanges.value.length)

export function addPending(change: PendingChange) {
  pendingChanges.value = [...pendingChanges.value, change]
}

export function removePending(id: string) {
  pendingChanges.value = pendingChanges.value.filter(c => c.id !== id)
}

export function revertLast() {
  const last = pendingChanges.value[pendingChanges.value.length - 1]
  if (!last) return
  last.revert()
  pendingChanges.value = pendingChanges.value.slice(0, -1)
}

export function clearPending() {
  pendingChanges.value = []
}

// --- Editing binding (S01 lost-window semantics) ---
//
// Every editing surface captures the connection it loaded its rows under.
// Connection switching is explicit and never redirects edits: while a
// different connection is active, a view bound to the old connection
// refuses new commits and asks for a reload instead of sending its stale
// identity to the newly active connection. The server independently
// re-validates the full identity (connection + schema + table + key +
// version) at mutation time, so a lost browser tab cannot smuggle an edit
// across connections even if this client-side guard were bypassed.

export interface EditingBinding {
  connectionId: string
  schema: string
  table: string
}

/** True while the captured binding still matches the active connection. */
export function bindingActive(binding: EditingBinding): boolean {
  return activeConnection.value?.id === binding.connectionId
}

// --- Staged edits and atomic commit outcomes (S02) ---
//
// Edits are staged locally as structured operations (never client-built
// SQL) and committed as ONE atomic batch under a client-generated
// operation ID. The retry contract mirrors the server's:
//
//   - a dropped response (network failure after the server committed) is
//     resolved by looking up the recorded outcome with the SAME operation
//     ID before any retry — a committed outcome completes the flow without
//     re-sending, an unknown outcome surfaces for manual verification and
//     NEVER auto-recommits;
//   - a refused commit (conflict, constraint, validation) retains every
//     staged edit locally — the draft stays reconcilable;
//   - a later attempt always uses a fresh operation ID (the failed one is
//     spent: same ID + different payload is an operation_conflict).

export interface StagedEdit {
  id: string
  connectionId: string
  operation: CommitOperation
  label: string
}

export const stagedEdits = signal<StagedEdit[]>([])

export const stagedCount = computed(() => stagedEdits.value.length)

let stageSeq = 0

export function stageEdit(edit: Omit<StagedEdit, 'id'>): StagedEdit {
  const staged: StagedEdit = { ...edit, id: `stage-${Date.now()}-${stageSeq++}` }
  stagedEdits.value = [...stagedEdits.value, staged]
  return staged
}

export function removeStagedEdit(id: string) {
  stagedEdits.value = stagedEdits.value.filter(e => e.id !== id)
}

export function discardLastStaged() {
  const last = stagedEdits.value[stagedEdits.value.length - 1]
  if (!last) return
  removeStagedEdit(last.id)
}

export function clearStaged(connectionId?: string) {
  stagedEdits.value = connectionId === undefined
    ? []
    : stagedEdits.value.filter(e => e.connectionId !== connectionId)
}

/** Staged edits scoped to one table on one connection (S03 grid overlay). */
export function stagedForTable(connectionId: string, schema: string, table: string): StagedEdit[] {
  return stagedEdits.value.filter(e =>
    e.connectionId === connectionId &&
    e.operation.schema === schema &&
    e.operation.table === table)
}

/** Canonical string form of a full key tuple: stable across renders, used
 *  to address a row's staged state and error-focus targets. Values are
 *  wire cells (tagged cells serialize deterministically). */
export function keyStringOf(key: KeyCell[]): string {
  return JSON.stringify(key.map(k => [k.column, k.value]))
}

// --- Error focus (S03) ---
//
// A failed commit pins the first offending staged edit; the data grid
// focuses that row so the user lands on the cause, not a generic error.

export interface FailedEditFocus {
  editId: string
  /** Decoded error message, surfaced by the bar as well. */
  reason: string
}

export const failedEditFocus = signal<FailedEditFocus | null>(null)

/** Index of the first operation named by a server batch error
 *  ("operations[N]: ..."), or 0 — the batch is refused as a unit. */
export function firstOffendingOpIndex(message: string): number {
  const m = /operations\[(\d+)\]/.exec(message)
  return m ? Number(m[1]) : 0
}

export type CommitPhase = 'idle' | 'committing' | 'committed' | 'failed'

export const commitPhase = signal<CommitPhase>('idle')
export const commitError = signal<string | null>(null)
export const lastCommit = signal<{ operationId: string; response: CommitResponse; at: number } | null>(null)
export const lastPreview = signal<PreviewResponse | null>(null)

function newOperationId(): string {
  return typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `op-${Date.now()}-${stageSeq++}`
}

/** Stage-then-commit flow for one connection's staged edits. */
export async function commitStaged(connectionId: string): Promise<CommitResponse> {
  const edits = stagedEdits.value.filter(e => e.connectionId === connectionId)
  if (edits.length === 0) throw new Error('no staged edits for this connection')

  const operationId = newOperationId()
  const payload = {
    connectionId,
    operationId,
    operations: edits.map(e => e.operation),
  }
  commitPhase.value = 'committing'
  commitError.value = null
  failedEditFocus.value = null
  try {
    const res = await api.commitOperations(payload)
    commitPhase.value = 'committed'
    lastCommit.value = { operationId, response: res, at: Date.now() }
    clearStaged(connectionId)
    return res
  } catch (err: unknown) {
    return await resolveFailedCommit(connectionId, operationId, err, edits)
  }
}

/** A commit attempt failed: either the server refused it (nothing applied —
 *  the draft stays staged), or the response was lost mid-flight and the
 *  recorded outcome decides. Unknown outcomes never auto-retry. The first
 *  offending operation pins the error focus so the grid can land the user
 *  on the cause. */
async function resolveFailedCommit(connectionId: string, operationId: string, err: unknown, edits: StagedEdit[]): Promise<CommitResponse> {
  const dropped = err instanceof TypeError || (err instanceof ApiError && err.state === 'unknown')
  if (dropped) {
    let outcome: OutcomeResponse | null = null
    try {
      outcome = await api.operationOutcome(connectionId, operationId)
    } catch {
      outcome = null // the lookup itself failed
    }
    if (outcome && outcome.state === 'committed' && outcome.response) {
      commitPhase.value = 'committed'
      lastCommit.value = { operationId, response: outcome.response, at: Date.now() }
      clearStaged(connectionId)
      return outcome.response
    }
    if (!outcome || outcome.state === 'unknown') {
      commitPhase.value = 'failed'
      commitError.value = 'commit outcome unknown — verify the table state before retrying with a new operation ID'
      throw new Error(commitError.value)
    }
    // failed / in_progress: fall through with the original error.
  }
  commitPhase.value = 'failed'
  commitError.value = err instanceof Error ? err.message : String(err)
  const offender = edits[firstOffendingOpIndex(commitError.value)]
  if (offender) {
    failedEditFocus.value = { editId: offender.id, reason: commitError.value }
  }
  throw err
}

/** Dry-run the staged batch; nothing is applied and nothing is staged anew. */
export async function previewStaged(connectionId: string): Promise<PreviewResponse> {
  const edits = stagedEdits.value.filter(e => e.connectionId === connectionId)
  if (edits.length === 0) throw new Error('no staged edits for this connection')
  const res = await api.previewOperations({
    connectionId,
    operations: edits.map(e => e.operation),
  })
  lastPreview.value = res
  return res
}

/** Undo the last committed batch through the server's recorded inverse. */
export async function revertLastCommit(connectionId: string): Promise<CommitResponse> {
  const last = lastCommit.value
  if (!last) throw new Error('nothing to revert')
  const res = await api.revertOperation({
    connectionId,
    operationId: last.operationId,
    revertOperationId: newOperationId(),
  })
  lastCommit.value = null
  commitPhase.value = 'idle'
  return res
}

// --- Theme ---

const storedTheme = typeof localStorage !== 'undefined'
  ? (localStorage.getItem('studio-theme') as 'dark' | 'light' | null)
  : null

export const theme = signal<'dark' | 'light'>(storedTheme ?? 'dark')

theme.subscribe(t => {
  document.documentElement.setAttribute('data-theme', t)
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem('studio-theme', t)
  }
})

export function toggleTheme() {
  theme.value = theme.value === 'dark' ? 'light' : 'dark'
}

// --- Schema refresh (S05) ---
//
// The schema signal previously updated only on connect. refreshSchema
// re-fetches the live catalog and updates the signal, so the tree,
// completion sources and every schema-derived view converge after DDL
// (designer applies, SQL editor DDL, the tree's refresh button for changes
// made elsewhere).

export const schemaRefreshing = signal(false)
export const schemaRefreshError = signal<string | null>(null)

export async function refreshSchema(connectionId?: string): Promise<Schema | null> {
  const id = connectionId ?? activeConnection.value?.id
  if (!id) return null
  schemaRefreshing.value = true
  schemaRefreshError.value = null
  try {
    const sc = await api.schema(id)
    schema.value = sc
    return sc
  } catch (err: unknown) {
    schemaRefreshError.value = err instanceof Error ? err.message : String(err)
    return null
  } finally {
    schemaRefreshing.value = false
  }
}

// --- Command palette ---

export const paletteOpen = signal(false)
export const paletteQuery = signal('')

export function openPalette() {
  paletteQuery.value = ''
  paletteOpen.value = true
}

export function closePalette() {
  paletteOpen.value = false
}

// --- Toast notifications ---

export interface Toast {
  id: string
  kind: 'success' | 'error' | 'info'
  message: string
}

export const toasts = signal<Toast[]>([])

export function toast(kind: Toast['kind'], message: string) {
  const id = crypto.randomUUID()
  toasts.value = [...toasts.value, { id, kind, message }]
  setTimeout(() => {
    toasts.value = toasts.value.filter(t => t.id !== id)
  }, 4000)
}
