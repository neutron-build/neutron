import { useSignal } from '@preact/signals'
import { useEffect, useRef, useCallback } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import s from './KVModule.module.css'

interface KVEntry {
  key: string
  value: string
  ttl: number | null // seconds remaining, null = no expiry
}

interface KVModuleProps {
  name: string
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

  // Inline editing state
  const inlineEditKey = useSignal<string | null>(null)
  const inlineEditValue = useSignal('')
  const inlineEditDirty = useSignal(false)

  // Delete confirmation state
  const confirmDeleteKey = useSignal<string | null>(null)
  const confirmTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // New key inline form
  const showNewKeyForm = useSignal(false)

  const conn = activeConnection.value!

  async function load() {
    loading.value = true
    try {
      const r = await api.query(
        `SELECT key, value, ttl FROM kv_scan(${sqlStr(name)}, '*', 500)`,
        conn.id
      )
      if (r.error) throw new Error(r.error)
      entries.value = r.rows.map(row => ({
        key: String(row[0]),
        value: String(row[1]),
        ttl: row[2] != null ? Number(row[2]) : null,
      }))
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loading.value = false
    }
  }

  useEffect(() => { load() }, [name])

  // Clean up confirm timer on unmount
  useEffect(() => {
    return () => {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    }
  }, [])

  function selectEntry(e: KVEntry) {
    selected.value = e
    editValue.value = e.value
    editTTL.value = e.ttl != null ? String(e.ttl) : ''
  }

  // Start inline editing a value cell
  function startInlineEdit(e: KVEntry, ev: Event) {
    ev.stopPropagation()
    inlineEditKey.value = e.key
    inlineEditValue.value = e.value
    inlineEditDirty.value = false
  }

  function cancelInlineEdit() {
    inlineEditKey.value = null
    inlineEditValue.value = ''
    inlineEditDirty.value = false
  }

  async function saveInlineEdit() {
    const key = inlineEditKey.value
    if (!key) return
    saving.value = true
    try {
      // Find the entry to preserve TTL
      const entry = entries.value.find(e => e.key === key)
      const ttlArg = entry?.ttl != null ? `, ${entry.ttl}` : ''
      await api.query(
        `SELECT kv_set(${sqlStr(name)}, ${sqlStr(key)}, ${sqlStr(inlineEditValue.value)}${ttlArg})`,
        conn.id
      )
      toast('success', `Saved ${key}`)
      cancelInlineEdit()
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  async function saveEdit() {
    const e = selected.value
    if (!e) return
    saving.value = true
    try {
      const ttlArg = editTTL.value ? `, ${parseInt(editTTL.value)}` : ''
      await api.query(
        `SELECT kv_set(${sqlStr(name)}, ${sqlStr(e.key)}, ${sqlStr(editValue.value)}${ttlArg})`,
        conn.id
      )
      toast('success', `Saved ${e.key}`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  // Delete with confirmation
  const requestDelete = useCallback((key: string, ev: Event) => {
    ev.stopPropagation()
    if (confirmDeleteKey.value === key) {
      // Already confirming — execute delete
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmDeleteKey.value = null
      doDelete(key)
    } else {
      // First click — show confirm
      confirmDeleteKey.value = key
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmTimerRef.current = setTimeout(() => {
        confirmDeleteKey.value = null
      }, 3000)
    }
  }, [])

  async function doDelete(key: string) {
    try {
      await api.query(`SELECT kv_delete(${sqlStr(name)}, ${sqlStr(key)})`, conn.id)
      if (selected.value?.key === key) selected.value = null
      toast('info', `Deleted ${key}`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  async function addEntry() {
    if (!newKey.value.trim() || !newValue.value.trim()) return
    try {
      const ttlArg = newTTL.value ? `, ${parseInt(newTTL.value)}` : ''
      await api.query(
        `SELECT kv_set(${sqlStr(name)}, ${sqlStr(newKey.value)}, ${sqlStr(newValue.value)}${ttlArg})`,
        conn.id
      )
      newKey.value = ''
      newValue.value = ''
      newTTL.value = ''
      showNewKeyForm.value = false
      toast('success', 'Key added')
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
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
            onClick={() => { showNewKeyForm.value = !showNewKeyForm.value }}
            title="New Key"
          >+</button>
        </div>

        {/* New Key inline form */}
        {showNewKeyForm.value && (
          <div class={s.addForm}>
            <div class={s.addTitle}>New key</div>
            <input
              class={s.addInput}
              placeholder="Key"
              value={newKey.value}
              onInput={e => { newKey.value = (e.target as HTMLInputElement).value }}
            />
            <input
              class={s.addInput}
              placeholder="Value"
              value={newValue.value}
              onInput={e => { newValue.value = (e.target as HTMLInputElement).value }}
            />
            <div class={s.addRow}>
              <input
                class={`${s.addInput} ${s.ttlInput}`}
                placeholder="TTL (s)"
                type="number"
                value={newTTL.value}
                onInput={e => { newTTL.value = (e.target as HTMLInputElement).value }}
              />
              <button class={s.addBtn} onClick={addEntry}>Set</button>
              <button class={s.cancelBtn} onClick={() => { showNewKeyForm.value = false; newKey.value = ''; newValue.value = ''; newTTL.value = '' }}>Cancel</button>
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
            const isConfirmingDelete = confirmDeleteKey.value === e.key
            return (
              <div
                key={e.key}
                class={`${s.keyRow} ${selected.value?.key === e.key ? s.keyRowActive : ''} ${isEditing ? s.keyRowEditing : ''}`}
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
                  onClick={ev => requestDelete(e.key, ev)}
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
              onInput={e => { editValue.value = (e.target as HTMLTextAreaElement).value }}
            />
            <div class={s.valueFooter}>
              <div class={s.ttlRow}>
                <label class={s.ttlLabel}>TTL (seconds, blank = no expiry)</label>
                <input
                  class={s.ttlField}
                  type="number"
                  placeholder="no expiry"
                  value={editTTL.value}
                  onInput={e => { editTTL.value = (e.target as HTMLInputElement).value }}
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

function formatTTL(seconds: number) {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  return `${Math.floor(seconds / 3600)}h`
}
