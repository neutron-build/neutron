import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api, queryMutationOrThrow as runMutation } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './KVModule.module.css'

interface KVEntry {
  connectionId: string
  key: string
  value: string
  ttl: number | null // seconds remaining, null = no expiry
}

interface KVModuleProps {
  name: string
}

// KV_TTL returns remaining seconds, or -1 (no TTL) / -2 (missing key).
export function ttlFromEngine(v: unknown): number | null {
  if (v == null || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) && n >= 0 ? n : null
}

export function KVModule({ name }: KVModuleProps) {
  const entries = useSignal<KVEntry[]>([])
  const loading = useSignal(false)
  const selected = useSignal<KVEntry | null>(null)
  const editValue = useSignal('')
  const editTTL = useSignal('')
  const newKey = useSignal('')
  const newValue = useSignal('')
  const newTTL = useSignal('')
  const filterText = useSignal('')
  const saving = useSignal(false)
  const adding = useRef(false)
  const inlineVersion = useRef(0)
  const newVersion = useRef(0)

  // Inline editing state
  const inlineEditKey = useSignal<string | null>(null)
  const inlineEditValue = useSignal('')
  const inlineEditDirty = useSignal(false)

  // Delete confirmation state
  const confirmDeleteKey = useSignal<string | null>(null)
  const confirmTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // New key inline form
  const showNewKeyForm = useSignal(false)

  // RLS seals the specialty stores for non-superuser sessions
  const rlsDenied = useSignal<string | null>(null)

  const conn = activeConnection.value!
  const viewGeneration = useRef(0)
  const readGeneration = useRef(0)
  const selectionRevision = useRef(0)
  const deleting = useRef(new Set<string>())
  const connectionOwner = useRef(conn.id)
  function clearConfirmation() {
    if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    confirmTimerRef.current = null
    confirmDeleteKey.value = null
  }
  // Invalidate synchronously, before effects or any old callback can run.
  useEffect(() => activeConnection.subscribe(next => {
    if (next?.id === connectionOwner.current) return
    connectionOwner.current = next?.id ?? ''
    viewGeneration.current++; readGeneration.current++; selectionRevision.current++
    inlineVersion.current++; newVersion.current++
    clearConfirmation()
    entries.value = []; selected.value = null
    inlineEditKey.value = null; editValue.value = ''; editTTL.value = ''
    rlsDenied.value = null; loading.value = false
  }), [])

  async function load() {
    if (activeConnection.value?.id !== conn.id) return
    const view = viewGeneration.current
    const own = ++readGeneration.current
    const owns = () => activeConnection.value?.id === conn.id && view === viewGeneration.current && own === readGeneration.current
    loading.value = true
    try {
      // The KV store is a single global keyspace — enumerate keys with
      // KV_KEYS(pattern), which returns a JSON array of key strings.
      const keyRes = await api.query(`SELECT KV_KEYS('*')`, conn.id)
      if (!owns()) return
      if (keyRes.error || keyRes.canceled) throw new Error(keyRes.error || 'Read canceled')
      const cell = keyRes.rows[0]?.[0]
      const keys: string[] = Array.isArray(cell)
        ? cell.map(String)
        : typeof cell === 'string'
          ? (JSON.parse(cell) as string[])
          : []
      if (keys.length === 0) {
        entries.value = []
        return
      }
      // Fetch each key's value and remaining TTL in a single multi-column
      // select: KV_GET(k), KV_TTL(k) pairs.
      const cols = keys.flatMap(k => [`KV_GET(${sqlStr(k)})`, `KV_TTL(${sqlStr(k)})`]).join(', ')
      const valRes = await api.query(`SELECT ${cols}`, conn.id)
      if (!owns()) return
      if (valRes.error || valRes.canceled) throw new Error(valRes.error || 'Read canceled')
      const valRow = (valRes.rows[0] ?? []) as unknown[]
      entries.value = keys.map((k, i) => ({
        connectionId: conn.id,
        key: k,
        value: valRow[i * 2] != null ? String(valRow[i * 2]) : '',
        ttl: ttlFromEngine(valRow[i * 2 + 1]),
      }))
    } catch (err: unknown) {
      if (!owns()) return
      const msg = err instanceof Error ? err.message : String(err)
      rlsDenied.value = isRlsDenied(msg) ? msg : null
      toast('error', msg)
    } finally {
      if (owns()) loading.value = false
    }
  }

  useEffect(() => { void load(); return () => { viewGeneration.current++; readGeneration.current++; clearConfirmation() } }, [name, conn.id])

  // Clean up confirm timer on unmount
  useEffect(() => {
    return () => {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    }
  }, [])

  function selectEntry(e: KVEntry) {
    if (e.connectionId !== activeConnection.value?.id) return
    selectionRevision.current++
    selected.value = { ...e }
    editValue.value = e.value
    editTTL.value = e.ttl != null ? String(e.ttl) : ''
  }

  // Start inline editing a value cell
  function startInlineEdit(e: KVEntry, ev: Event) {
    ev.stopPropagation()
    if (e.connectionId !== activeConnection.value?.id) return
    inlineVersion.current++
    inlineEditKey.value = e.key
    inlineEditValue.value = e.value
    inlineEditDirty.value = false
  }

  function cancelInlineEdit() {
    inlineVersion.current++
    inlineEditKey.value = null
    inlineEditValue.value = ''
    inlineEditDirty.value = false
  }

  async function saveInlineEdit() {
    if (saving.value) return
    const version = inlineVersion.current
    const submitted = inlineEditValue.value
    const key = inlineEditKey.value
    if (!key || activeConnection.value?.id !== conn.id) return
    saving.value = true
    try {
      // Find the entry to preserve TTL
      const entry = entries.value.find(e => e.key === key)
      const ttlArg = entry?.ttl != null ? `, ${entry.ttl}` : ''
      await queryMutationOrThrow(
        `SELECT KV_SET(${sqlStr(key)}, ${sqlStr(submitted)}${ttlArg})`,
        conn.id
      )
      toast('success', `Saved ${key}`)
      if (inlineVersion.current === version && activeConnection.value?.id === conn.id) cancelInlineEdit()
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  async function saveEdit() {
    if (saving.value) return
    const e = selected.value
    if (!e || e.connectionId !== activeConnection.value?.id) return
    saving.value = true
    try {
      const ttlArg = editTTL.value ? `, ${parseInt(editTTL.value)}` : ''
      await queryMutationOrThrow(
        `SELECT KV_SET(${sqlStr(e.key)}, ${sqlStr(editValue.value)}${ttlArg})`,
        e.connectionId
      )
      toast('success', `Saved ${e.key}`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  const entryIdentity = (e: KVEntry) => JSON.stringify([e.connectionId, e.key])
  function requestDelete(entry: KVEntry, ev: Event) {
    ev.stopPropagation()
    if (entry.connectionId !== activeConnection.value?.id) { clearConfirmation(); return }
    const identity = entryIdentity(entry)
    if (deleting.current.has(identity)) return
    if (confirmDeleteKey.value === identity) {
      clearConfirmation()
      void doDelete(entry)
    } else {
      clearConfirmation()
      confirmDeleteKey.value = identity
      const timer = setTimeout(() => {
        if (confirmTimerRef.current === timer && confirmDeleteKey.value === identity) clearConfirmation()
      }, 3000)
      confirmTimerRef.current = timer
    }
  }

  async function doDelete(entry: KVEntry) {
    const identity = entryIdentity(entry)
    if (entry.connectionId !== activeConnection.value?.id || deleting.current.has(identity)) return
    const selectedAtDispatch = selected.value
    const revision = selectionRevision.current
    deleting.current.add(identity)
    try {
      await queryMutationOrThrow(`SELECT KV_DEL(${sqlStr(entry.key)})`, entry.connectionId)
      if (revision === selectionRevision.current && selected.value === selectedAtDispatch &&
          selected.value && entryIdentity(selected.value) === identity) selected.value = null
      toast('info', `Deleted ${entry.key} on ${entry.connectionId}`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally { deleting.current.delete(identity) }
  }

  async function addEntry() {
    if (adding.current || activeConnection.value?.id !== conn.id) return
    const version = newVersion.current
    if (!newKey.value.trim() || !newValue.value.trim()) return
    adding.current = true
    try {
      const ttlArg = newTTL.value ? `, ${parseInt(newTTL.value)}` : ''
      await queryMutationOrThrow(
        `SELECT KV_SET(${sqlStr(newKey.value)}, ${sqlStr(newValue.value)}${ttlArg})`,
        conn.id
      )
      if (newVersion.current === version && activeConnection.value?.id === conn.id) {
        newKey.value = ''; newValue.value = ''; newTTL.value = ''; showNewKeyForm.value = false
      }
      toast('success', 'Key added')
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally { adding.current = false }
  }

  const visible = filterText.value
    ? entries.value.filter(e => e.key.includes(filterText.value))
    : entries.value

  return (
    <div class={s.layout}>
      {/* Left panel — key list */}
      <div class={s.listPanel}>
        <div class={s.listToolbar}>
          <input
            class={s.filterInput}
            placeholder="Filter keys..."
            value={filterText.value}
            onInput={e => { filterText.value = (e.target as HTMLInputElement).value }}
          />
          <button class={s.refreshBtn} onClick={load} disabled={loading.value} title="Refresh">&#8634;</button>
          <button
            class={s.exportBtn}
            onClick={() => {
              const data = entries.value.map(e => ({ key: e.key, value: e.value, ttl: e.ttl as unknown }))
              exportCSV(data, `kv-${name}.csv`)
            }}
            disabled={entries.value.length === 0}
            title="Export CSV"
          >CSV</button>
          <button
            class={s.exportBtn}
            onClick={() => exportJSON(entries.value, `kv-${name}.json`)}
            disabled={entries.value.length === 0}
            title="Export JSON"
          >JSON</button>
          <button
            class={s.newKeyBtn}
            onClick={() => { newVersion.current++; showNewKeyForm.value = !showNewKeyForm.value }}
            title="New Key"
          >+</button>
        </div>

        {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}

        {/* New Key inline form */}
        {showNewKeyForm.value && (
          <div class={s.addForm}>
            <div class={s.addTitle}>New key</div>
            <input
              class={s.addInput}
              placeholder="Key"
              value={newKey.value}
              onInput={e => { newVersion.current++; newKey.value = (e.target as HTMLInputElement).value }}
            />
            <input
              class={s.addInput}
              placeholder="Value"
              value={newValue.value}
              onInput={e => { newVersion.current++; newValue.value = (e.target as HTMLInputElement).value }}
            />
            <div class={s.addRow}>
              <input
                class={`${s.addInput} ${s.ttlInput}`}
                placeholder="TTL (s)"
                type="number"
                value={newTTL.value}
                onInput={e => { newVersion.current++; newTTL.value = (e.target as HTMLInputElement).value }}
              />
              <button class={s.addBtn} onClick={addEntry}>Set</button>
              <button class={s.cancelBtn} onClick={() => { newVersion.current++; showNewKeyForm.value = false; newKey.value = ''; newValue.value = ''; newTTL.value = '' }}>Cancel</button>
            </div>
          </div>
        )}

        <div class={s.keyList}>
          {loading.value && <div class={s.loadingMsg}>Loading...</div>}
          {!loading.value && visible.length === 0 && (
            <div class={s.emptyMsg}>No keys{filterText.value ? ' matching filter' : ''}</div>
          )}
          {visible.map(e => {
            const isEditing = inlineEditKey.value === e.key
            const isConfirmingDelete = confirmDeleteKey.value === entryIdentity(e)
            return (
              <div
                key={entryIdentity(e)}
                class={`${s.keyRow} ${selected.value && entryIdentity(selected.value) === entryIdentity(e) ? s.keyRowActive : ''} ${isEditing ? s.keyRowEditing : ''}`}
                onClick={() => selectEntry(e)}
              >
                <span class={s.keyName}>{e.key}</span>
                {!isEditing && (
                  <span
                    class={s.inlineValue}
                    onClick={(ev) => startInlineEdit(e, ev)}
                    title="Click to edit"
                  >
                    {e.value.length > 30 ? e.value.slice(0, 30) + '...' : e.value}
                  </span>
                )}
                {isEditing && (
                  <span class={s.inlineEditGroup} onClick={(ev) => ev.stopPropagation()}>
                    <textarea
                      class={s.inlineTextarea}
                      value={inlineEditValue.value}
                      onInput={ev => {
                        inlineVersion.current++
                        inlineEditValue.value = (ev.target as HTMLTextAreaElement).value
                        inlineEditDirty.value = true
                      }}
                      onKeyDown={ev => {
                        if (ev.key === 'Escape') cancelInlineEdit()
                        if (ev.key === 'Enter' && (ev.ctrlKey || ev.metaKey)) saveInlineEdit()
                      }}
                    />
                    <span class={s.inlineEditActions}>
                      <button class={s.inlineSaveBtn} onClick={saveInlineEdit} disabled={saving.value || !inlineEditDirty.value}>Save</button>
                      <button class={s.inlineCancelBtn} onClick={cancelInlineEdit}>Cancel</button>
                    </span>
                  </span>
                )}
                {e.ttl != null && <span class={s.ttlBadge}>{formatTTL(e.ttl)}</span>}
                <button
                  class={`${s.deleteBtn} ${isConfirmingDelete ? s.deleteBtnConfirm : ''}`}
                  onClick={ev => requestDelete(e, ev)}
                  title={isConfirmingDelete ? 'Click again to confirm' : 'Delete key'}
                >{isConfirmingDelete ? 'Confirm?' : '\u00d7'}</button>
              </div>
            )
          })}
        </div>
      </div>

      {/* Right panel — value editor */}
      <div class={s.valuePanel}>
        {!selected.value ? (
          <div class={s.noSelection}>Select a key to view its value</div>
        ) : (
          <>
            <div class={s.valueHeader}>
              <span class={s.selectedKey}>{selected.value.key}</span>
              {selected.value.ttl != null && (
                <span class={s.ttlInfo}>Expires in {formatTTL(selected.value.ttl)}</span>
              )}
            </div>
            <textarea
              class={s.valueEditor}
              value={editValue.value}
              onInput={e => { selectionRevision.current++; editValue.value = (e.target as HTMLTextAreaElement).value }}
            />
            <div class={s.valueFooter}>
              <div class={s.ttlRow}>
                <label class={s.ttlLabel}>TTL (seconds, blank = no expiry)</label>
                <input
                  class={s.ttlField}
                  type="number"
                  placeholder="no expiry"
                  value={editTTL.value}
                  onInput={e => { selectionRevision.current++; editTTL.value = (e.target as HTMLInputElement).value }}
                />
              </div>
              <button class={s.saveBtn} onClick={saveEdit} disabled={saving.value}>
                {saving.value ? 'Saving...' : 'Save'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function sqlStr(s: string) {
  return `'${s.replace(/'/g, "''")}'`
}

export function formatTTL(seconds: number) {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  return `${Math.floor(seconds / 3600)}h`
}

const queryMutationOrThrow = (sql: string, connectionId: string, params?: unknown[]) => runMutation(sql, connectionId, params, api.query)
