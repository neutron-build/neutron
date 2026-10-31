import { useSignal } from '@preact/signals'
import { useEffect, useRef, useCallback } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { queryMutationOrThrow as runMutation, api } from '../../lib/api'
import { parseDocument, serializeDocument, replaceDocument, JsonNumber, type DocumentValue, type DocumentPath } from '../../lib/documentJson'
import { exportCSV, exportDocumentJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './DocModule.module.css'

interface DocEntry {
  id: string
  data: DocumentValue
  raw: string
  connectionId: string
  collection: string
}

interface DocModuleProps {
  name: string
}

function docIdentity(d: DocEntry): string {
  return JSON.stringify([d.connectionId, d.collection, d.id])
}

// Editable JSON tree renderer with inline edit on click
function JsonNode({
  value,
  depth = 0,
  path,
  onEdit,
}: {
  value: DocumentValue
  depth?: number
  path: DocumentPath
  onEdit: (path: DocumentPath, newValue: DocumentValue) => void
}) {
  const collapsed = useSignal(depth > 2)
  const editing = useSignal(false)
  const editText = useSignal('')
  const editError = useSignal<string | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const leafRef = useRef<HTMLSpanElement>(null)
  const wasEditing = useRef(false)
  useEffect(() => {
    if (editing.value) inputRef.current?.focus()
    else if (wasEditing.current) leafRef.current?.focus()
    wasEditing.current = editing.value
  }, [editing.value])

  function startEdit(ev: Event) {
    ev.stopPropagation()
    editing.value = true
    editText.value = serializeDocument(value)
    editError.value = null
  }

  function cancelEdit() {
    editing.value = false
    editText.value = ''
    editError.value = null
    leafRef.current?.focus()
  }

  function commitEdit() {
    const raw = editText.value
    let parsed: DocumentValue
    try {
      parsed = parseDocument(raw)
    } catch (err) {
      editError.value = `Invalid JSON: ${err instanceof Error ? err.message : String(err)}`
      inputRef.current?.focus()
      return
    }
    onEdit(path, parsed)
    editing.value = false
    editError.value = null
    leafRef.current?.focus()
  }

  const leafControls = {
    role: 'button' as const, tabIndex: 0, ref: leafRef,
    'aria-label': `Edit JSON value at ${JSON.stringify(path)}`,
    onKeyDown: (ev: KeyboardEvent) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); startEdit(ev) }
    },
  }

  // Leaf nodes (null, boolean, number, string) are directly editable
  if (value === null || typeof value === 'boolean' || value instanceof JsonNumber || typeof value === 'string') {
    if (editing.value) {
      return (
        <span class={s.inlineEditWrap} onClick={(ev) => ev.stopPropagation()}>
          <input
            ref={inputRef}
            aria-label={`JSON value at ${JSON.stringify(path)}`}
            aria-invalid={!!editError.value}
            class={s.inlineEditInput}
            value={editText.value}
            onInput={ev => { editText.value = (ev.target as HTMLInputElement).value }}
            onKeyDown={ev => {
              if (ev.key === 'Enter') commitEdit()
              if (ev.key === 'Escape') cancelEdit()
            }}
          />
          <button aria-label="Save JSON value" class={s.inlineEditSave} onClick={commitEdit}>&#10003;</button>
          <button aria-label="Cancel JSON edit" class={s.inlineEditCancel} onClick={cancelEdit}>&#10005;</button>
          {editError.value && <span role="alert">{editError.value}</span>}
        </span>
      )
    }

    if (value === null) return <span class={`${s.jNull} ${s.jEditable}`} onClick={startEdit} {...leafControls}>null</span>
    if (typeof value === 'boolean') return <span class={`${s.jBool} ${s.jEditable}`} onClick={startEdit} {...leafControls}>{String(value)}</span>
    if (value instanceof JsonNumber) return <span class={`${s.jNum} ${s.jEditable}`} onClick={startEdit} {...leafControls}>{(value as JsonNumber).raw}</span>
    return <span class={`${s.jStr} ${s.jEditable}`} onClick={startEdit} {...leafControls}>"{value}"</span>
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span class={s.jBracket}>[]</span>
    return (
      <span>
        <button aria-label={`Toggle JSON children at ${JSON.stringify(path)}`} aria-expanded={!collapsed.value} class={s.collapseBtn} onClick={() => { collapsed.value = !collapsed.value }}>
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
                <JsonNode value={v} depth={depth + 1} path={[...path, i]} onEdit={onEdit} />
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
        <button aria-label={`Toggle JSON children at ${JSON.stringify(path)}`} aria-expanded={!collapsed.value} class={s.collapseBtn} onClick={() => { collapsed.value = !collapsed.value }}>
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
                <JsonNode value={(value as { [key: string]: DocumentValue })[k]} depth={depth + 1} path={[...path, k]} onEdit={onEdit} />
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
  const selectionGeneration = useRef(0)
  const editRaw = useSignal('')
  const editMode = useSignal(false) // false = tree view, true = raw JSON editor
  const saving = useSignal(false)
  const page = useSignal(0)
  const total = useSignal(0)
  const limit = 50

  // X02: collection scoping. The engine scopes every DOC_* statement to one
  // collection (a document in another collection reads as absent); an empty
  // value is the default collection. Collections are namespaces, not
  // permissions: any session may name any collection. Names are constrained
  // to this namespace vocabulary; every loaded entry retains its origin.
  const collection = useSignal('')
  const COLLECTION_RE = /^[a-zA-Z0-9_-]{0,64}$/

  // Track if the document has been modified (for tree-view inline edits)
  const treeModified = useSignal(false)
  const treeData = useSignal<DocumentValue>(null)

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

  /** Scoped-collection SQL helpers (empty = the default collection). */
  function collLit(coll = collection.value): string {
    return coll === '' ? '' : `'${coll}', `
  }
  const loadGeneration = useRef(0)
  const currentLoad = (generation: number, coll: string, connectionId: string) => generation === loadGeneration.current && coll === collection.value && connectionId === activeConnection.value?.id

  async function load() {
    const generation = ++loadGeneration.current
    const coll = collection.value
    const connectionId = conn.id
    const lit = collLit(coll)
    const owns = () => currentLoad(generation, coll, connectionId)
    loading.value = true
    try {
      // DOC_QUERY with an empty filter returns a comma-separated list of all
      // matching ids, scoped to the selected collection.
      const idRes = await api.query(`SELECT DOC_QUERY(${lit}'{}')`, connectionId)
      if (!owns()) return
      if (idRes.error) throw new Error(idRes.error)
      const cell = idRes.rows[0]?.[0]
      // Ids come from the engine; only plain digit tokens are kept, so
      // nothing but an integer is ever interpolated into the fetch below.
      const allIds = (cell == null || cell === '')
        ? []
        : String(cell).split(',').map(t => t.trim()).filter(t => /^[0-9]+$/.test(t))
      allIds.sort((a, b) => Number(a) - Number(b))
      total.value = allIds.length

      const pageIds = allIds.slice(page.value * limit, page.value * limit + limit)
      if (pageIds.length === 0) {
        docs.value = []
        return
      }
      // Fetch each document body with DOC_GET(id) in one multi-column select.
      const cols = pageIds.map(id => `DOC_GET(${lit}${id})`).join(', ')
      const dataRes = await api.query(`SELECT ${cols}`, connectionId)
      if (!owns()) return
      if (dataRes.error) throw new Error(dataRes.error)
      const row = (dataRes.rows[0] ?? []) as unknown[]
      docs.value = pageIds.map((id, i) => {
        const raw = typeof row[i] === 'string' ? row[i] as string : JSON.stringify(row[i] ?? null)
        return { id, raw, data: parseDocument(raw), connectionId, collection: coll }
      })
    } catch (err: unknown) {
      if (!owns()) return
      const msg = err instanceof Error ? err.message : String(err)
      rlsDenied.value = isRlsDenied(msg) ? msg : null
      toast('error', msg)
    } finally {
      if (owns()) loading.value = false
    }
  }

  useEffect(() => { load() }, [name, page.value, collection.value, conn.id])

  useEffect(() => {
    confirmDeleteId.value = null
    if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    confirmTimerRef.current = null
  }, [name, collection.value, conn.id])

  // Clean up confirm timer on unmount
  useEffect(() => {
    return () => {
      selectionGeneration.current++
      loadGeneration.current++
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
    }
  }, [])

  function selectDoc(d: DocEntry) {
    selectionGeneration.current++
    selected.value = { ...d }
    const raw = d.raw
    editRaw.value = raw
    rawOriginal.value = raw
    editMode.value = false
    treeData.value = parseDocument(d.raw)
    treeModified.value = false
  }

  // Handle inline edits in tree view
  function handleTreeEdit(path: DocumentPath, newValue: DocumentValue) {
    try {
      treeData.value = replaceDocument(treeData.value, path, newValue)
      treeModified.value = true
      editRaw.value = serializeDocument(treeData.value)
    } catch (err) { toast('error', err instanceof Error ? err.message : String(err)) }
  }

  async function saveDoc() {
    if (saving.value) return
    const d = selected.value
    if (!d) return
    try {
      parseDocument(editRaw.value)
    } catch {
      toast('error', 'Invalid JSON')
      return
    }
    const submitted = editRaw.value
    saving.value = true
    try {
      const jsonStr = submitted.replace(/'/g, "''")
      await queryMutationOrThrow(
        `SELECT DOC_UPDATE(${collLit(d.collection)}${d.id}, '${jsonStr}')`,
        d.connectionId
      )
      toast('success', `Document ${d.id} saved`)
      if (selected.value === d) {
        selected.value = { ...d, raw: submitted, data: parseDocument(submitted) }
        rawOriginal.value = submitted
        if (editRaw.value === submitted) treeModified.value = false
      }
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  async function saveTreeDoc() {
    if (saving.value) return
    const d = selected.value
    if (!d) return
    const submitted = serializeDocument(treeData.value)
    saving.value = true
    try {
      const jsonStr = submitted.replace(/'/g, "''")
      await queryMutationOrThrow(
        `SELECT DOC_UPDATE(${collLit(d.collection)}${d.id}, '${jsonStr}')`,
        d.connectionId
      )
      toast('success', `Document ${d.id} saved`)
      if (selected.value === d) {
        selected.value = { ...d, raw: submitted, data: parseDocument(submitted) }
        rawOriginal.value = submitted
        if (editRaw.value === submitted) treeModified.value = false
      }
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  // New document
  async function insertDoc() {
    if (saving.value) return
    try {
      parseDocument(newDocRaw.value)
    } catch {
      toast('error', 'Invalid JSON')
      return
    }
    const submitted = newDocRaw.value
    saving.value = true
    try {
      const jsonStr = submitted.replace(/'/g, "''")
      await queryMutationOrThrow(
        `SELECT DOC_INSERT(${collLit()}'${jsonStr}')`,
        conn.id
      )
      if (newDocRaw.value === submitted) {
        showNewDoc.value = false
        newDocRaw.value = '{\n  \n}'
      }
      toast('success', 'Document created')
      await load()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      saving.value = false
    }
  }

  // Delete with confirmation
  const requestDelete = useCallback((d: DocEntry, ev: Event) => {
    const id = docIdentity(d)
    ev.stopPropagation()
    if (confirmDeleteId.value === id) {
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmDeleteId.value = null
      doDelete(d)
    } else {
      confirmDeleteId.value = id
      if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current)
      confirmTimerRef.current = setTimeout(() => {
        confirmDeleteId.value = null
      }, 3000)
    }
  }, [conn.id])

  async function doDelete(d: DocEntry) {
    const id = d.id
    const selection = selected.value
    const generation = selectionGeneration.current
    try {
      await queryMutationOrThrow(`SELECT DOC_DELETE(${collLit(d.collection)}${id})`, d.connectionId)
      if (selectionGeneration.current === generation && selected.value === selection &&
          selection && docIdentity(selection) === docIdentity(d)) selected.value = null
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
          <input
            class={s.collInput}
            value={collection.value}
            placeholder="collection"
            title="Document collection (empty = default). Statements are scoped to it: a document in another collection reads as absent here."
            onInput={e => {
              const el = e.target as HTMLInputElement
              if (COLLECTION_RE.test(el.value)) {
                if (collection.value !== el.value) { confirmDeleteId.value = null; if (confirmTimerRef.current) clearTimeout(confirmTimerRef.current); confirmTimerRef.current = null; loadGeneration.current++; docs.value = []; total.value = 0; loading.value = false }
                collection.value = el.value
              }
              else el.value = collection.value // rejected characters never stick
            }}
            onKeyDown={e => { if (e.key === 'Enter') { page.value = 0; load() } }}
            onBlur={() => { page.value = 0; load() }}
            spellcheck={false}
          />
          <button class={s.newDocBtn} onClick={() => { showNewDoc.value = !showNewDoc.value }} title="New Document">+</button>
          <button class={s.refreshBtn} onClick={load} disabled={loading.value}>&#8634;</button>
          <button
            class={s.exportBtn}
            onClick={() => {
              const data = docs.value.map(d => ({ id: d.id, data: d.raw }))
              exportCSV(data, `docs-${name}.csv`)
            }}
            disabled={docs.value.length === 0}
            title="Export CSV"
          >CSV</button>
          <button
            class={s.exportBtn}
            onClick={() => exportDocumentJSON(docs.value, `docs-${name}.json`)}
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
            const isConfirming = confirmDeleteId.value === docIdentity(d)
            return (
              <div
                key={docIdentity(d)}
                role="button" tabIndex={0} aria-label={`Open document ${d.id} in ${d.collection || "default"} on ${d.connectionId}`}
                onKeyDown={ev => { if (ev.target === ev.currentTarget && (ev.key === "Enter" || ev.key === " ")) { ev.preventDefault(); selectDoc(d) } }}
                class={`${s.docRow} ${selected.value && docIdentity(selected.value) === docIdentity(d) ? s.docRowActive : ''}`}
                onClick={() => selectDoc(d)}
              >
                <span class={s.docId}>{d.id}</span>
                <span class={s.docPreview}>{previewDoc(d.data)}</span>
                <button
                  class={`${s.deleteBtn} ${isConfirming ? s.deleteBtnConfirm : ''}`}
                  onClick={ev => requestDelete(d, ev)}
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
              <span class={s.docHeaderId}>{selected.value.id} · {selected.value.collection || '(default)'} @{selected.value.connectionId}</span>
              <div class={s.viewToggle}>
                <button
                  class={`${s.toggleBtn} ${!editMode.value ? s.toggleActive : ''}`}
                  onClick={() => {
                    // Sync tree data from raw if raw was edited
                    if (rawDirty) {
                      try {
                        treeData.value = parseDocument(editRaw.value)
                        treeModified.value = true
                      } catch { toast('error', 'Invalid JSON'); return }
                    }
                    editMode.value = false
                  }}
                >Tree</button>
                <button
                  class={`${s.toggleBtn} ${editMode.value ? s.toggleActive : ''}`}
                  onClick={() => {
                    editMode.value = true
                    // Sync raw from tree data if tree was modified
                    if (treeModified.value && treeData.value) {
                      editRaw.value = serializeDocument(treeData.value)
                    }
                  }}
                >Raw</button>
              </div>
            </div>

            {!editMode.value ? (
              <>
                <div class={s.treeView}>
                  <JsonNode key={`${docIdentity(selected.value)}:${selectionGeneration.current}`} value={treeData.value} depth={0} path={[]} onEdit={handleTreeEdit} />
                </div>
                {treeModified.value && (
                  <div class={s.editFooter}>
                    <span class={s.modifiedBadge}>Modified</span>
                    <button class={s.discardBtn} onClick={() => {
                      if (selected.value) {
                        treeData.value = parseDocument(selected.value.raw)
                        treeModified.value = false
                        editRaw.value = selected.value.raw
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
                        editRaw.value = selected.value.raw
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
  if (data instanceof JsonNumber) return data.raw
  if (!data || typeof data !== 'object') return String(data)
  const keys = Object.keys(data as object)
  return keys.slice(0, 3).join(', ') + (keys.length > 3 ? '...' : '')
}


const queryMutationOrThrow = (sql: string, connectionId: string, params?: unknown[]) => runMutation(sql, connectionId, params, api.query)
