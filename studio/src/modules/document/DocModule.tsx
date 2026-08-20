import { useSignal } from '@preact/signals'
import { useEffect, useRef, useCallback } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './DocModule.module.css'

interface DocEntry {
  id: string
  data: unknown
}

interface DocModuleProps {
  name: string
}

// Editable JSON tree renderer with inline edit on click
function JsonNode({
  value,
  depth = 0,
  path,
  onEdit,
}: {
  value: unknown
  depth?: number
  path: string
  onEdit: (path: string, newValue: unknown) => void
}) {
  const collapsed = useSignal(depth > 2)
  const editing = useSignal(false)
  const editText = useSignal('')

  function startEdit(ev: Event) {
    ev.stopPropagation()
    editing.value = true
    editText.value = typeof value === 'string' ? value : JSON.stringify(value)
  }

  function cancelEdit() {
    editing.value = false
    editText.value = ''
  }

  function commitEdit() {
    const raw = editText.value
    let parsed: unknown
    // Try parsing as JSON first (for numbers, booleans, null, objects, arrays)
    try {
      parsed = JSON.parse(raw)
    } catch {
      // If it fails, treat as string
      parsed = raw
    }
    onEdit(path, parsed)
    editing.value = false
  }

  // Leaf nodes (null, boolean, number, string) are directly editable
  if (value === null || typeof value === 'boolean' || typeof value === 'number' || typeof value === 'string') {
    if (editing.value) {
      return (
        <span class={s.inlineEditWrap} onClick={(ev) => ev.stopPropagation()}>
          <input
            class={s.inlineEditInput}
            value={editText.value}
            onInput={ev => { editText.value = (ev.target as HTMLInputElement).value }}
            onKeyDown={ev => {
              if (ev.key === 'Enter') commitEdit()
              if (ev.key === 'Escape') cancelEdit()
            }}
          />
          <button class={s.inlineEditSave} onClick={commitEdit}>&#10003;</button>
          <button class={s.inlineEditCancel} onClick={cancelEdit}>&#10005;</button>
        </span>
      )
    }

    if (value === null) return <span class={`${s.jNull} ${s.jEditable}`} onClick={startEdit}>null</span>
    if (typeof value === 'boolean') return <span class={`${s.jBool} ${s.jEditable}`} onClick={startEdit}>{String(value)}</span>
    if (typeof value === 'number') return <span class={`${s.jNum} ${s.jEditable}`} onClick={startEdit}>{value}</span>
    return <span class={`${s.jStr} ${s.jEditable}`} onClick={startEdit}>"{value}"</span>
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span class={s.jBracket}>[]</span>
    return (
      <span>
        <button class={s.collapseBtn} onClick={() => { collapsed.value = !collapsed.value }}>
          {collapsed.value ? '\u25b6' : '\u25bc'}
        </button>
        <span class={s.jBracket}>[</span>
        {collapsed.value ? (
          <span class={s.jEllipsis} onClick={() => { collapsed.value = false }}>
            {value.length} items
          </span>
        ) : (
          <div class={s.jBlock} style={{ paddingLeft: `${(depth + 1) * 14}px` }}>
            {value.map((v, i) => (
              <div key={i} class={s.jLine}>
                <span class={s.jIndex}>{i}</span>
                <JsonNode value={v} depth={depth + 1} path={`${path}[${i}]`} onEdit={onEdit} />
                {i < value.length - 1 && <span class={s.jComma}>,</span>}
              </div>
            ))}
          </div>
        )}
        <span class={s.jBracket}>]</span>
      </span>
    )
  }

  if (typeof value === 'object') {
    const keys = Object.keys(value as object)
    if (keys.length === 0) return <span class={s.jBracket}>{'{}'}</span>
    return (
      <span>
        <button class={s.collapseBtn} onClick={() => { collapsed.value = !collapsed.value }}>
          {collapsed.value ? '\u25b6' : '\u25bc'}
        </button>
        <span class={s.jBracket}>{'{'}</span>
        {collapsed.value ? (
          <span class={s.jEllipsis} onClick={() => { collapsed.value = false }}>
            {keys.length} keys
          </span>
        ) : (
          <div class={s.jBlock} style={{ paddingLeft: `${(depth + 1) * 14}px` }}>
            {keys.map((k, i) => (
              <div key={k} class={s.jLine}>
                <span class={s.jKey}>"{k}"</span>
                <span class={s.jColon}>: </span>
                <JsonNode value={(value as Record<string, unknown>)[k]} depth={depth + 1} path={`${path}.${k}`} onEdit={onEdit} />
                {i < keys.length - 1 && <span class={s.jComma}>,</span>}
              </div>
            ))}
          </div>
        )}
        <span class={s.jBracket}>{'}'}</span>
      </span>
    )
  }

  return <span>{String(value)}</span>
}

export function DocModule({ name }: DocModuleProps) {
  const docs = useSignal<DocEntry[]>([])
  const loading = useSignal(false)
  const selected = useSignal<DocEntry | null>(null)
  const editRaw = useSignal('')
  const editMode = useSignal(false) // false = tree view, true = raw JSON editor
  const saving = useSignal(false)
  const page = useSignal(0)
  const total = useSignal(0)
  const limit = 50

  // Track if the document has been modified (for tree-view inline edits)
  const treeModified = useSignal(false)
  const treeData = useSignal<unknown>(null)

  // New document form
  const showNewDoc = useSignal(false)
  const newDocRaw = useSignal('{\n  \n}')

  // Delete confirmation
  const confirmDeleteId = useSignal<string | null>(null)
  const confirmTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Track raw editor dirty state
  const rawOriginal = useSignal('')

  // RLS seals the specialty stores for non-superuser sessions
  const rlsDenied = useSignal<string | null>(null)

  const conn = activeConnection.value!

  async function load() {
    loading.value = true
    try {
      // Single global document store — no collection argument. DOC_QUERY with
      // an empty filter returns a comma-separated list of all matching ids.
      const idRes = await api.query(`SELECT DOC_QUERY('{}')`, conn.id)
      if (idRes.error) throw new Error(idRes.error)
      const cell = idRes.rows[0]?.[0]
      const allIds = (cell == null || cell === '')
        ? []
        : String(cell).split(',').filter(Boolean)
      allIds.sort((a, b) => Number(a) - Number(b))
      total.value = allIds.length

      const pageIds = allIds.slice(page.value * limit, page.value * limit + limit)
      if (pageIds.length === 0) {
        docs.value = []
        return
      }
      // Fetch each document body with DOC_GET(id) in one multi-column select.
      const cols = pageIds.map(id => `DOC_GET(${id})`).join(', ')
      const dataRes = await api.query(`SELECT ${cols}`, conn.id)
      if (dataRes.error) throw new Error(dataRes.error)
      const row = (dataRes.rows[0] ?? []) as unknown[]
      docs.value = pageIds.map((id, i) => ({
        id,
        data: row[i] != null ? (typeof row[i] === 'string' ? JSON.parse(row[i] as string) : row[i]) : null,
      }))
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      rlsDenied.value = isRlsDenied(msg) ? msg : null
      toast('error', msg)
    } finally {
      loading.value = false
    }
  }

  useEffect(() => { load() }, [name, page.value])

  // Clean up confirm timer on unmount
  useEffect(() => {
    return () => {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    }
  }, [])

  function selectDoc(d: DocEntry) {
    selected.value = d
    const raw = JSON.stringify(d.data, null, 2)
    editRaw.value = raw
    rawOriginal.value = raw
    editMode.value = false
    treeData.value = structuredClone(d.data)
    treeModified.value = false
  }

  // Handle inline edits in tree view
  function handleTreeEdit(path: string, newValue: unknown) {
    if (!treeData.value || typeof treeData.value !== 'object') return
    const cloned = structuredClone(treeData.value) as Record<string, unknown>
    setNestedValue(cloned, path, newValue)
    treeData.value = cloned
    treeModified.value = true
    // Sync with raw editor
    editRaw.value = JSON.stringify(cloned, null, 2)
  }

  async function saveDoc() {
    const d = selected.value
    if (!d) return
    let parsed: unknown
    try {
      parsed = JSON.parse(editRaw.value)
    } catch {
      toast('error', 'Invalid JSON')
      return
    }
    saving.value = true
    try {
      const jsonStr = JSON.stringify(parsed).replace(/'/g, "''")
      await api.query(
        `SELECT DOC_UPDATE(${d.id}, '${jsonStr}')`,
        conn.id
      )
      toast('success', `Document ${d.id} saved`)
      treeModified.value = false
      rawOriginal.value = editRaw.value
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  async function saveTreeDoc() {
    const d = selected.value
    if (!d || !treeData.value) return
    saving.value = true
    try {
      const jsonStr = JSON.stringify(treeData.value).replace(/'/g, "''")
      await api.query(
        `SELECT DOC_UPDATE(${d.id}, '${jsonStr}')`,
        conn.id
      )
      toast('success', `Document ${d.id} saved`)
      treeModified.value = false
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  // New document
  async function insertDoc() {
    let parsed: unknown
    try {
      parsed = JSON.parse(newDocRaw.value)
    } catch {
      toast('error', 'Invalid JSON')
      return
    }
    saving.value = true
    try {
      const jsonStr = JSON.stringify(parsed).replace(/'/g, "''")
      await api.query(
        `SELECT DOC_INSERT('${jsonStr}')`,
        conn.id
      )
      showNewDoc.value = false
      newDocRaw.value = '{\n  \n}'
      toast('success', 'Document created')
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  // Delete with confirmation
  const requestDelete = useCallback((id: string, ev: Event) => {
    ev.stopPropagation()
    if (confirmDeleteId.value === id) {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmDeleteId.value = null
      doDelete(id)
    } else {
      confirmDeleteId.value = id
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmTimerRef.current = setTimeout(() => {
        confirmDeleteId.value = null
      }, 3000)
    }
  }, [])

  async function doDelete(id: string) {
    try {
      await api.query(`SELECT DOC_DELETE(${id})`, conn.id)
      if (selected.value?.id === id) selected.value = null
      toast('info', `Document ${id} deleted`)
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  const rawDirty = editRaw.value !== rawOriginal.value

  return (
    <div class={s.layout}>
      {/* Left: doc list */}
      <div class={s.listPanel}>
        {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}
        <div class={s.listHeader}>
          <span class={s.listTitle}>{name}</span>
          <span class={s.docCount}>{total.value} docs</span>
          <button class={s.newDocBtn} onClick={() => { showNewDoc.value = !showNewDoc.value }} title="New Document">+</button>
          <button class={s.refreshBtn} onClick={load} disabled={loading.value}>&#8634;</button>
          <button
            class={s.exportBtn}
            onClick={() => {
              const data = docs.value.map(d => ({ id: d.id, data: JSON.stringify(d.data) }))
              exportCSV(data, `docs-${name}.csv`)
            }}
            disabled={docs.value.length === 0}
            title="Export CSV"
          >CSV</button>
          <button
            class={s.exportBtn}
            onClick={() => exportJSON(docs.value, `docs-${name}.json`)}
            disabled={docs.value.length === 0}
            title="Export JSON"
          >JSON</button>
        </div>

        {/* New document form */}
        {showNewDoc.value && (
          <div class={s.newDocForm}>
            <div class={s.newDocTitle}>New Document</div>
            <textarea
              class={s.newDocEditor}
              value={newDocRaw.value}
              onInput={e => { newDocRaw.value = (e.target as HTMLTextAreaElement).value }}
              spellcheck={false}
              rows={8}
            />
            <div class={s.newDocActions}>
              <button class={s.saveBtn} onClick={insertDoc} disabled={saving.value}>
                {saving.value ? 'Creating...' : 'Create'}
              </button>
              <button class={s.cancelDocBtn} onClick={() => { showNewDoc.value = false }}>Cancel</button>
            </div>
          </div>
        )}

        <div class={s.docList}>
          {loading.value && <div class={s.msg}>Loading...</div>}
          {!loading.value && docs.value.length === 0 && (
            <div class={s.msg}>No documents</div>
          )}
          {docs.value.map(d => {
            const isConfirming = confirmDeleteId.value === d.id
            return (
              <div
                key={d.id}
                class={`${s.docRow} ${selected.value?.id === d.id ? s.docRowActive : ''}`}
                onClick={() => selectDoc(d)}
              >
                <span class={s.docId}>{d.id}</span>
                <span class={s.docPreview}>{previewDoc(d.data)}</span>
                <button
                  class={`${s.deleteBtn} ${isConfirming ? s.deleteBtnConfirm : ''}`}
                  onClick={ev => requestDelete(d.id, ev)}
                  title={isConfirming ? 'Click again to confirm' : 'Delete'}
                >{isConfirming ? 'Confirm?' : '\u00d7'}</button>
              </div>
            )
          })}
        </div>

        <div class={s.pagination}>
          <button class={s.pageBtn} onClick={() => { page.value-- }} disabled={page.value === 0}>&larr;</button>
          <span class={s.pageNum}>Page {page.value + 1}</span>
          <button class={s.pageBtn} onClick={() => { page.value++ }} disabled={(page.value + 1) * limit >= total.value}>&rarr;</button>
        </div>
      </div>

      {/* Right: document viewer/editor */}
      <div class={s.docPanel}>
        {!selected.value ? (
          <div class={s.noSelection}>Select a document to view it</div>
        ) : (
          <>
            <div class={s.docHeader}>
              <span class={s.docHeaderId}>{selected.value.id}</span>
              <div class={s.viewToggle}>
                <button
                  class={`${s.toggleBtn} ${!editMode.value ? s.toggleActive : ''}`}
                  onClick={() => {
                    editMode.value = false
                    // Sync tree data from raw if raw was edited
                    if (rawDirty) {
                      try {
                        treeData.value = JSON.parse(editRaw.value)
                        treeModified.value = true
                      } catch { /* ignore parse errors when switching */ }
                    }
                  }}
                >Tree</button>
                <button
                  class={`${s.toggleBtn} ${editMode.value ? s.toggleActive : ''}`}
                  onClick={() => {
                    editMode.value = true
                    // Sync raw from tree data if tree was modified
                    if (treeModified.value && treeData.value) {
                      editRaw.value = JSON.stringify(treeData.value, null, 2)
                    }
                  }}
                >Raw</button>
              </div>
            </div>

            {!editMode.value ? (
              <>
                <div class={s.treeView}>
                  <JsonNode value={treeData.value} depth={0} path="$" onEdit={handleTreeEdit} />
                </div>
                {treeModified.value && (
                  <div class={s.editFooter}>
                    <span class={s.modifiedBadge}>Modified</span>
                    <button class={s.discardBtn} onClick={() => {
                      if (selected.value) {
                        treeData.value = structuredClone(selected.value.data)
                        treeModified.value = false
                        editRaw.value = JSON.stringify(selected.value.data, null, 2)
                      }
                    }}>Discard</button>
                    <button class={s.saveBtn} onClick={saveTreeDoc} disabled={saving.value}>
                      {saving.value ? 'Saving...' : 'Save Document'}
                    </button>
                  </div>
                )}
              </>
            ) : (
              <>
                <textarea
                  class={s.rawEditor}
                  value={editRaw.value}
                  onInput={e => { editRaw.value = (e.target as HTMLTextAreaElement).value }}
                  spellcheck={false}
                />
                <div class={s.editFooter}>
                  {rawDirty && <span class={s.modifiedBadge}>Modified</span>}
                  {rawDirty && (
                    <button class={s.discardBtn} onClick={() => {
                      if (selected.value) {
                        editRaw.value = JSON.stringify(selected.value.data, null, 2)
                        rawOriginal.value = editRaw.value
                      }
                    }}>Discard</button>
                  )}
                  <button class={s.saveBtn} onClick={saveDoc} disabled={saving.value || !rawDirty}>
                    {saving.value ? 'Saving...' : 'Save Document'}
                  </button>
                </div>
              </>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function previewDoc(data: unknown): string {
  if (!data || typeof data !== 'object') return String(data)
  const keys = Object.keys(data as object)
  return keys.slice(0, 3).join(', ') + (keys.length > 3 ? '...' : '')
}

/** Set a value at a JSON path like "$.foo.bar[2].baz" */
function setNestedValue(obj: unknown, path: string, value: unknown): void {
  // Parse path: "$" is root, then ".key" or "[index]"
  const parts = parsePath(path)
  if (parts.length === 0) return

  let current: unknown = obj
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]
    if (typeof part === 'number' && Array.isArray(current)) {
      current = current[part]
    } else if (typeof part === 'string' && current && typeof current === 'object') {
      current = (current as Record<string, unknown>)[part]
    } else {
      return
    }
  }

  const last = parts[parts.length - 1]
  if (typeof last === 'number' && Array.isArray(current)) {
    current[last] = value
  } else if (typeof last === 'string' && current && typeof current === 'object') {
    (current as Record<string, unknown>)[last] = value
  }
}

function parsePath(path: string): (string | number)[] {
  const parts: (string | number)[] = []
  // Remove leading "$"
  let p = path.startsWith('$') ? path.slice(1) : path
  // Match .key or [index]
  const regex = /\.([^.[]+)|\[(\d+)\]/g
  let match: RegExpExecArray | null
  while ((match = regex.exec(p)) !== null) {
    if (match[1] !== undefined) {
      parts.push(match[1])
    } else if (match[2] !== undefined) {
      parts.push(parseInt(match[2], 10))
    }
  }
  return parts
}
