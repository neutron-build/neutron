import { useSignal, useComputed } from '@preact/signals'
import { useEffect, useRef, useState } from 'preact/hooks'
import {
  activeConnection, schema, openTab, toast, bindingActive,
  stagedEdits, stagedForTable, stageEdit, keyStringOf, failedEditFocus,
  type EditingBinding,
} from '../../lib/store'
import { api } from '../../lib/api'
import { encodeCell, encodeEdit, formatCell, WireEncodeError } from '../../lib/wire'
import { DataGrid, type FKTarget, type StagedRowState } from '../../components/DataGrid'
import { TypedEditor } from '../../components/TypedEditor'
import type {
  QueryResult, SqlColumn, FKDetail, TableMeta, KeyCell, MatchCell,
  CellEdit, TableFilter, TableSort, TableMetaColumn, CommitOperation,
} from '../../lib/types'
import { TableSearchPanel } from './TableSearchPanel'
import { ImportDialog } from './ImportDialog'
import type { ExportFormat } from '../../lib/types'
import s from './SQLBrowser.module.css'

interface SQLBrowserProps {
  schema: string
  table: string
  /** Pre-applied filter. */
  initialFilter?: { column: string; op: string; value: string }
  /** Pre-applied full-tuple equality filter (FK follow, incl. composite FKs). */
  initialMatch?: MatchCell[]
}

/** Page sizes (S06): bounded by the server's 1000-row page limit; the grid
 *  virtualizes whatever it holds. */
const PAGE_SIZES = [100, 200, 500, 1000]

/** Start a native browser download of a same-origin URL (streamed to disk). */
function startDownload(url: string) {
  const a = document.createElement('a')
  a.href = url
  a.rel = 'noopener'
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  a.remove()
}

const FILTER_OPS: Array<{ value: string; label: string }> = [
  { value: 'eq', label: '=' },
  { value: 'ne', label: '<>' },
  { value: 'lt', label: '<' },
  { value: 'lte', label: '<=' },
  { value: 'gt', label: '>' },
  { value: 'gte', label: '>=' },
  { value: 'like', label: 'LIKE' },
  { value: 'ilike', label: 'ILIKE' },
  { value: 'is-null', label: 'IS NULL' },
  { value: 'not-null', label: 'IS NOT NULL' },
]

/** Display text of a wire value inside a staged label/overlay. */
function wireText(v: unknown): string {
  if (v !== null && typeof v === 'object' && 't' in (v as Record<string, unknown>) && 'v' in (v as Record<string, unknown>)) {
    const cell = v as { t: string; v: string }
    return cell.t === 'bytea' ? '\\x' + cell.v : cell.v
  }
  return formatCell(v)
}

export function SQLBrowser({ schema: schemaName, table, initialFilter, initialMatch }: SQLBrowserProps) {
  const result = useSignal<QueryResult | null>(null)
  const loading = useSignal(false)
  const error = useSignal<string | null>(null)
  const limit = useSignal(200)
  const offset = useSignal(0)
  const filters = useSignal<TableFilter[]>(
    initialFilter ? [{ column: initialFilter.column, op: initialFilter.op, value: initialFilter.value }] : [])
  const appliedFilters = useSignal<TableFilter[]>(filters.value)
  const sorts = useSignal<TableSort[]>([])
  const fks = useSignal<Record<string, FKDetail>>({})
  const meta = useSignal<TableMeta | null>(null)
  const showInsert = useSignal(false)
  const insertEdits = useSignal<Record<string, CellEdit>>({})
  const exportFormat = useSignal<ExportFormat>('csv')
  const exporting = useSignal(false)
  const showImport = useSignal(false)
  const importBtnRef = useRef<HTMLButtonElement | null>(null)
  const [focusCell, setFocusCell] = useState<{ rowIndex: number; column?: string } | null>(null)

  // The editing binding: this view's rows were loaded under exactly this
  // connection, captured ONCE at mount (the view survives connection
  // switches without remounting). Connection switching is explicit —
  // staging and commits are refused while a different connection is active
  // (see store.ts); the server re-validates identity independently.
  const connRef = useRef<string | null>(null)
  if (connRef.current === null) {
    connRef.current = activeConnection.value?.id ?? ''
  }
  const conn = { id: connRef.current }
  const binding: EditingBinding = { connectionId: conn.id, schema: schemaName, table }
  const lostWindow = useComputed(() => !bindingActive(binding))

  const tableInfo = useComputed(() =>
    (schema.value?.sql ?? []).find(t => t.schema === schemaName && t.name === table) ?? null
  )

  async function loadMeta() {
    try {
      meta.value = await api.tableMeta(conn.id, schemaName, table)
    } catch (err: unknown) {
      // No authoritative metadata -> no editing (fail-safe direction).
      meta.value = {
        exists: false, keyColumns: [], versioned: false, readOnly: true,
        readOnlyReason: `editing state unavailable (${err instanceof Error ? err.message : String(err)})`,
        columns: [],
      }
    }
  }

  async function load() {
    loading.value = true
    error.value = null
    try {
      const active = appliedFilters.value.filter(f => f.column !== '')
      result.value = await api.tableData(
        conn.id, schemaName, table, limit.value, offset.value,
        active.length > 0 ? active : undefined,
        undefined,
        sorts.value.length > 0 ? sorts.value : undefined,
        initialMatch,
      )
    } catch (err: unknown) {
      error.value = err instanceof Error ? err.message : String(err)
      toast('error', `Failed to load ${table}: ${error.value}`)
    } finally {
      loading.value = false
    }
  }

  useEffect(() => {
    loadMeta()
    api.tableFKs(conn.id, schemaName, table)
      .then(r => {
        const map: Record<string, FKDetail> = {}
        for (const fk of r.fks ?? []) {
          // Every column of a constraint links to the WHOLE tuple: following
          // a composite FK filters the target by all components at once.
          // A column in several constraints links through the first.
          const local = fk.columns && fk.columns.length > 0 ? fk.columns : (fk.column ? [fk.column] : [])
          for (const c of local) {
            if (!map[c]) map[c] = fk
          }
        }
        fks.value = map
      })
      .catch(() => { /* FK links are optional polish */ })
  }, [schemaName, table])

  useEffect(() => { load() }, [schemaName, table])

  // Read-only state is AUTHORITATIVE (server catalog): no PK, no row
  // versions, or unavailable metadata all mean read-only, with the reason
  // the server gives. Composite keys are editable when versioned — the
  // full tuple addresses the row.
  const readOnlyReason = useComputed<string | null>(() => {
    if (meta.value?.readOnly) return meta.value.readOnlyReason ?? 'read-only'
    if (result.value?.readOnly) return result.value.readOnlyReason ?? 'read-only'
    return null
  })
  const metaColumns = useComputed(() => meta.value?.columns ?? [])
  const editable = useComputed(() => (meta.value !== null && !meta.value.readOnly && result.value?.readOnly !== true) || undefined)
  const canDelete = useComputed(() => meta.value?.canDelete === true)

  /** Build the versioned full-key identity for one row of the current read. */
  function identityFor(rowIndex: number): { key: KeyCell[]; version: string; binding: string } | null {
    const res = result.value
    const m = meta.value
    if (!res || !m || m.keyColumns.length === 0) return null
    if (!res.binding) return null
    const versions = res.versions
    if (!versions || versions[rowIndex] === undefined || versions[rowIndex] === null) return null
    const row = res.rows[rowIndex] as unknown[] | undefined
    if (!row) return null
    const tagFor = (col: string) => m.columns.find(c => c.name === col)?.tag ?? null
    const key: KeyCell[] = m.keyColumns.map(kc => {
      const idx = res.columns.indexOf(kc)
      return { column: kc, value: encodeCell(row[idx], tagFor(kc)) }
    })
    return { key, version: versions[rowIndex], binding: res.binding }
  }

  /** Row index by encoded key string for the current read. */
  function rowIndexByKey(keyString: string): number | undefined {
    const res = result.value
    const m = meta.value
    if (!res || !m) return undefined
    for (let i = 0; i < res.rows.length; i++) {
      const identity = identityFor(i)
      if (identity && keyStringOf(identity.key) === keyString) return i
    }
    return undefined
  }

  const stagedHere = useComputed(() => stagedForTable(conn.id, schemaName, table))

  /** The staged overlay: per-row staged updates/deletes, addressed by the
   *  full-key identity (NOT the row index), so navigation (paging, sorting,
   *  filtering) moves rows without ever detaching a draft from its row. */
  const stagedRows = useComputed<Map<number, StagedRowState>>(() => {
    const map = new Map<number, StagedRowState>()
    const res = result.value
    const m = meta.value
    if (!res || !m) return map
    for (const e of stagedHere.value) {
      const op = e.operation
      if (op.op === 'insert') continue
      const ks = keyStringOf(op.key ?? [])
      const idx = rowIndexByKey(ks)
      if (idx === undefined) continue // the row is not on this page; the draft survives globally
      const st = map.get(idx) ?? { updates: {} }
      if (op.op === 'update' && op.column) {
        st.updates[op.column] = {
          editId: e.id,
          edit: op.isNull ? { kind: 'null' } : { kind: 'value', text: wireText(op.value) },
        }
      } else if (op.op === 'delete') {
        st.deleteId = e.id
      }
      map.set(idx, st)
    }
    return map
  })

  // Failed-commit focus: land on the first offending staged row/cell; the
  // highlight lingers briefly so the user sees the cause, then clears.
  useEffect(() => {
    const focus = failedEditFocus.value
    if (!focus) return
    const edit = stagedEdits.value.find(e => e.id === focus.editId)
    if (!edit) return
    const op = edit.operation
    if (op.op === 'insert') return // no row to focus; the bar carries the error
    const idx = rowIndexByKey(keyStringOf(op.key ?? []))
    if (idx === undefined) {
      toast('info', 'the offending row is not on this page — clear or adjust filters to see it')
      return
    }
    setFocusCell({ rowIndex: idx, column: op.op === 'update' ? op.column : undefined })
    const t = setTimeout(() => setFocusCell(null), 2500)
    return () => clearTimeout(t)
  }, [failedEditFocus.value])

  function guardBinding(): boolean {
    if (!bindingActive(binding)) {
      toast('error', `This view is bound to connection ${binding.connectionId}; connection switching never carries edits — switch back or reload the table on the active connection`)
      return false
    }
    return true
  }

  function stageUpdate(rowIndex: number, column: string, edit: CellEdit) {
    if (!guardBinding()) return
    const identity = identityFor(rowIndex)
    if (!identity) {
      toast('error', 'Row identity unavailable (no versioned key) — reload the table before editing')
      return
    }
    const colMeta: TableMetaColumn | undefined = meta.value?.columns.find(c => c.name === column)
    if (!colMeta) return
    try {
      const enc = encodeEdit(edit, colMeta)
      if (enc.kind === 'omit') {
        toast('error', 'DEFAULT is an insert-only control; updates set a value or SQL NULL')
        return
      }
      const op: CommitOperation = enc.kind === 'null'
        ? { op: 'update', schema: schemaName, table, binding: identity.binding, key: identity.key, version: identity.version, column, isNull: true }
        : { op: 'update', schema: schemaName, table, binding: identity.binding, key: identity.key, version: identity.version, column, value: enc.value }
      const shown = enc.kind === 'null' ? 'NULL' : `'${wireText(enc.value)}'`
      stageEdit({ connectionId: conn.id, operation: op, label: `${table}.${column} → ${shown}` })
    } catch (err: unknown) {
      if (err instanceof WireEncodeError) {
        toast('error', err.message)
        return
      }
      throw err
    }
  }

  function stageDelete(rowIndex: number) {
    if (!guardBinding()) return
    const identity = identityFor(rowIndex)
    if (!identity) {
      toast('error', 'Row identity unavailable (no versioned key) — reload the table before deleting')
      return
    }
    const label = identity.key.map(k => `${k.column}=${wireText(k.value)}`).join(', ')
    stageEdit({
      connectionId: conn.id,
      operation: { op: 'delete', schema: schemaName, table, binding: identity.binding, key: identity.key, version: identity.version },
      label: `delete ${table} (${label})`,
    })
  }

  // --- insert form ---

  const insertable = useComputed<Array<{ column: TableMetaColumn; required: boolean }>>(() => {
    const m = meta.value
    if (!m || m.readOnly) return []
    const out: Array<{ column: TableMetaColumn; required: boolean }> = []
    for (const c of m.columns) {
      if (c.insertable !== true) continue
      out.push({ column: c, required: !c.nullable && !c.hasDefault && !c.autoAssigned })
    }
    return out
  })

  function openInsert() {
    if (!guardBinding()) return
    if (insertable.value.length === 0) return
    const init: Record<string, CellEdit> = {}
    for (const c of insertable.value) {
      // Required columns start as an empty value; everything else starts
      // as DEFAULT (omitted column) — the honest three-way initial state.
      init[c.column.name] = c.required ? { kind: 'value', text: '' } : { kind: 'default' }
    }
    insertEdits.value = init
    showInsert.value = true
  }

  function insertError(c: { column: TableMetaColumn; required: boolean }): string | null {
    const edit = insertEdits.value[c.column.name]
    if (!edit) return null
    if (edit.kind === 'null' && !c.column.nullable) return 'NOT NULL column cannot be NULL'
    if (edit.kind === 'default' && c.required) return 'required column — provide a value'
    if (edit.kind === 'value' && (c.column.type === 'boolean' || c.column.type === 'bool') && edit.text !== 'true' && edit.text !== 'false' && edit.text !== '') {
      return 'pick true or false'
    }
    return null
  }

  function stageInsert() {
    if (!guardBinding()) return
    const bindingStr = result.value?.binding ?? meta.value?.binding
    if (!bindingStr) {
      toast('error', 'Relation binding unavailable — reload the table before inserting')
      return
    }
    const values: Record<string, unknown> = {}
    try {
      for (const c of insertable.value) {
        const edit = insertEdits.value[c.column.name] ?? { kind: 'default' as const }
        const enc = encodeEdit(edit, c.column)
        if (enc.kind === 'omit') continue
        values[c.column.name] = enc.kind === 'null' ? null : enc.value
      }
    } catch (err: unknown) {
      if (err instanceof WireEncodeError) {
        toast('error', err.message)
        return
      }
      throw err
    }
    for (const c of insertable.value) {
      if (!(c.column.name in values) && c.required) {
        toast('error', `column ${c.column.name} is NOT NULL without a default — provide a value`)
        return
      }
    }
    stageEdit({
      connectionId: conn.id,
      operation: { op: 'insert', schema: schemaName, table, binding: bindingStr, values },
      label: `insert ${table} (${Object.keys(values).length} value${Object.keys(values).length === 1 ? '' : 's'})`,
    })
    showInsert.value = false
    insertEdits.value = {}
  }

  function setPageSize(n: number) {
    limit.value = n
    offset.value = 0
    load()
  }

  /** Streamed export (S06): the server validates and issues a single-use
   *  ticket; the browser downloads the rows straight to disk. The export
   *  covers every row matching the applied filters, in the current sort. */
  async function exportTable() {
    exporting.value = true
    try {
      const active = appliedFilters.value.filter(f => f.column !== '')
      const ticket = await api.tableExport({
        connectionId: conn.id, schema: schemaName, table,
        format: exportFormat.value,
        filters: active.length > 0 ? active : undefined,
        sorts: sorts.value.length > 0 ? sorts.value : undefined,
        match: initialMatch && initialMatch.length > 0 ? initialMatch : undefined,
      })
      startDownload(ticket.url)
      toast('info', `Exporting ${schemaName}.${table} as ${ticket.filename}`)
    } catch (err: unknown) {
      toast('error', `Export failed: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      exporting.value = false
    }
  }

  function closeImport() {
    showImport.value = false
    importBtnRef.current?.focus()
  }

  const canImport = useComputed(() =>
    editable.value === true && (meta.value?.columns ?? []).some(c => c.insertable === true))

  function handlePrev() {
    if (offset.value === 0) return
    offset.value = Math.max(0, offset.value - limit.value)
    load()
  }

  function handleNext() {
    if (!result.value) return
    if (result.value.rows.length < limit.value) return
    offset.value = offset.value + limit.value
    load()
  }

  function applyFilters() {
    const active = filters.value.filter(f => f.column !== '')
    if (active.length !== filters.value.length && filters.value.length > 0) {
      // an incomplete row exists — refuse to silently drop it
      toast('error', 'Pick a column for every filter row (or remove the empty row)')
      return
    }
    if (active.some(f => f.op !== 'is-null' && f.op !== 'not-null' && f.value === undefined)) {
      toast('error', 'Filter value required (use IS NULL / IS NOT NULL for null tests)')
      return
    }
    appliedFilters.value = active
    offset.value = 0
    load()
  }

  function clearFilters() {
    filters.value = []
    appliedFilters.value = []
    offset.value = 0
    load()
  }

  function setFilter(i: number, patch: Partial<TableFilter>) {
    filters.value = filters.value.map((f, idx) => idx === i ? { ...f, ...patch } : f)
  }

  function addFilterRow() {
    filters.value = [...filters.value, { column: '', op: 'eq', value: '' }]
  }

  function removeFilterRow(i: number) {
    filters.value = filters.value.filter((_, idx) => idx !== i)
  }

  function handleSort(column: string, additive: boolean) {
    const current = sorts.value
    if (!additive) {
      // plain click: this column becomes the single primary key (asc → desc → off)
      if (current.length === 1 && current[0].column === column) {
        sorts.value = current[0].dir === 'asc' ? [{ column, dir: 'desc' }] : []
      } else {
        sorts.value = [{ column, dir: 'asc' }]
      }
    } else {
      // shift-click: toggle the column inside the multi-sort list
      const idx = current.findIndex(k => k.column === column)
      if (idx < 0) {
        const next = [...current, { column, dir: 'asc' as const }]
        sorts.value = next.length > 4 ? next.slice(next.length - 4) : next
      } else if (current[idx].dir === 'asc') {
        sorts.value = current.map((k, i) => i === idx ? { ...k, dir: 'desc' as const } : k)
      } else {
        sorts.value = current.filter((_, i) => i !== idx)
      }
    }
    offset.value = 0
    load()
  }

  function followFK(fk: FKTarget, rowIndex: number) {
    const res = result.value
    const row = res?.rows[rowIndex] as unknown[] | undefined
    if (!res || !row) return
    const local = fk.columns && fk.columns.length > 0 ? fk.columns : []
    const refs = fk.refColumns && fk.refColumns.length > 0 ? fk.refColumns : (fk.refColumn ? [fk.refColumn] : [])
    if (local.length === 0 || local.length !== refs.length) return
    const tagFor = (col: string) => meta.value?.columns.find(c => c.name === col)?.tag ?? null
    // The whole tuple, component i of the local key -> component i of the
    // referenced key, as exact wire cells.
    const match: MatchCell[] = local.map((c, i) => ({
      column: refs[i],
      value: encodeCell(row[res.columns.indexOf(c)], tagFor(c)),
    }))
    const label = local.map((c, i) => `${refs[i]}=${formatCell(row[res.columns.indexOf(c)])}`).join(', ')
    openTab({
      id: '',
      kind: 'sql-browser',
      label: `${fk.refTable} (${label})`,
      objectSchema: fk.refSchema,
      objectName: fk.refTable,
      match,
    })
  }

  const info = tableInfo.value
  const cols: SqlColumn[] = info?.columns ?? []
  const res = result.value
  const shown = res?.rows.length ?? 0
  const stagedCount = stagedHere.value.length

  return (
    <div class={s.browser}>
      <div class={s.toolbar}>
        <div class={s.tableId}>
          <span class={s.schemaName}>{schemaName}</span>
          <span class={s.sep}>.</span>
          <span class={s.tableName}>{table}</span>
          {info && <span class={s.rowCount}>{info.rowCount?.toLocaleString() ?? '?'} rows</span>}
        </div>
        <div class={s.toolbarActions}>
          {editable.value && (
            <button class={s.btnAction} onClick={openInsert} title="Stage a new row (typed editors; DEFAULT omits the column)">
              + Insert
            </button>
          )}
          <span class={s.exportGroup}>
            <select
              class={s.filterSelect}
              aria-label="Export format"
              value={exportFormat.value}
              onChange={e => { exportFormat.value = (e.target as HTMLSelectElement).value as ExportFormat }}
            >
              <option value="csv">CSV</option>
              <option value="json">JSON</option>
              <option value="ndjson">NDJSON</option>
            </select>
            <button
              class={s.btnAction}
              onClick={exportTable}
              disabled={exporting.value}
              title="Download every row matching the applied filters, in the current sort (streamed; not limited to this page)"
            >
              Export
            </button>
          </span>
          {canImport.value && (
            <button
              ref={importBtnRef}
              class={s.btnAction}
              onClick={() => { if (guardBinding()) showImport.value = true }}
              title="Import rows from a CSV or JSON file (batched; each batch is one transaction)"
            >
              Import…
            </button>
          )}
          <button class={s.btnRefresh} onClick={load} disabled={loading.value} title="Refresh" aria-label="Refresh rows">
            ↺
          </button>
        </div>
      </div>

      {lostWindow.value && (
        <div class={s.lostWindowNote} role="note">
          This view is bound to connection {conn.id} — the active connection changed. Staged edits stay
          staged and can only commit after switching back; they are never sent through another connection.
        </div>
      )}

      {info && (
        <div class={s.columnBar}>
          {cols.map((col: SqlColumn) => (
            <span key={col.name} class={s.colPill} title={`${col.type}${col.nullable ? '' : ' NOT NULL'}${col.isPrimaryKey ? ' PK' : ''}`}>
              {col.isPrimaryKey && <span class={s.pkMark}>PK</span>}
              <span class={s.colName}>{col.name}</span>
              <span class={s.colType}>{col.type}</span>
            </span>
          ))}
        </div>
      )}

      <div class={s.filterBar}>
        {filters.value.map((f, i) => (
          <span class={s.filterRow} key={i}>
            <select class={s.filterSelect} aria-label={`Filter column ${i + 1}`} value={f.column}
              onChange={e => setFilter(i, { column: (e.target as HTMLSelectElement).value })}>
              <option value="">column…</option>
              {cols.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
            </select>
            <select class={s.filterSelect} aria-label={`Filter operator ${i + 1}`} value={f.op}
              onChange={e => setFilter(i, { op: (e.target as HTMLSelectElement).value })}>
              {FILTER_OPS.map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
            </select>
            {f.op !== 'is-null' && f.op !== 'not-null' && (
              <input
                class={s.filterInput}
                aria-label={`Filter value ${i + 1}`}
                value={f.value ?? ''}
                placeholder="value"
                onInput={e => setFilter(i, { value: (e.target as HTMLInputElement).value })}
                onKeyDown={e => { if (e.key === 'Enter') applyFilters() }}
              />
            )}
            <button class={s.filterRemove} aria-label={`Remove filter ${i + 1}`} title="Remove this filter" onClick={() => removeFilterRow(i)}>×</button>
          </span>
        ))}
        <button class={s.filterBtn} onClick={addFilterRow} title="Add an ANDed filter">+ Filter</button>
        <button class={s.filterBtn} onClick={applyFilters}>Apply</button>
        {(appliedFilters.value.length > 0 || filters.value.length > 0) && (
          <button class={s.filterBtn} onClick={clearFilters}>Clear</button>
        )}
      </div>

      {showInsert.value && (
        <div class={s.insertForm} role="form" aria-label={`Insert row into ${table}`}>
          <div class={s.insertHead}>stage an insert — value, NULL and DEFAULT are explicit per column; DEFAULT omits the column</div>
          <div class={s.insertGrid}>
            {insertable.value.map(c => (
              <label class={s.insertField} key={c.column.name}>
                <span class={s.insertColName}>
                  {c.required && <span class={s.requiredMark} title="NOT NULL without a default">*</span>}
                  {c.column.name}
                  <span class={s.colType}>{c.column.type}</span>
                </span>
                <TypedEditor
                  column={c.column}
                  mode="insert"
                  edit={insertEdits.value[c.column.name] ?? { kind: 'default' }}
                  error={insertError(c)}
                  onChange={next => { insertEdits.value = { ...insertEdits.value, [c.column.name]: next } }}
                  onCommit={stageInsert}
                  onCancel={() => { showInsert.value = false; insertEdits.value = {} }}
                />
              </label>
            ))}
          </div>
          <div class={s.insertActions}>
            <button class={s.filterBtn} onClick={stageInsert}>Stage insert</button>
            <button class={s.filterBtn} onClick={() => { showInsert.value = false; insertEdits.value = {} }}>Cancel</button>
          </div>
        </div>
      )}

      {meta.value && meta.value.exists && (
        <TableSearchPanel schema={schemaName} table={table} meta={meta.value} />
      )}
      <div class={s.grid}>
        {loading.value && <div class={s.loading}>Loading…</div>}
        {!loading.value && error.value && <div class={s.error} role="alert">{error.value}</div>}
        {!loading.value && readOnlyReason.value && (
          <div class={s.readOnlyNote} role="note">{readOnlyReason.value}</div>
        )}
        {!loading.value && res && (
          <DataGrid
            result={res}
            columns={editable.value ? metaColumns.value : undefined}
            onStageUpdate={editable.value ? stageUpdate : undefined}
            onStageDelete={canDelete.value ? stageDelete : undefined}
            canDelete={canDelete.value}
            stagedRows={stagedRows.value}
            fkColumns={fks.value}
            onFollowFK={followFK}
            sorts={sorts.value}
            onSort={handleSort}
            focusCell={focusCell}
            label={`${schemaName}.${table} rows`}
          />
        )}
      </div>

      <div class={s.pagination}>
        <button class={s.pageBtn} onClick={handlePrev} disabled={offset.value === 0}>
          ← Prev
        </button>
        <span class={s.pageInfo}>
          {offset.value + 1}–{offset.value + shown}
          {res?.filterCount !== undefined && <> of {res.filterCount.toLocaleString()} filtered</>}
          {res?.totalCount !== undefined && <> · {res.totalCount.toLocaleString()} total</>}
          {stagedCount > 0 && <> · <span class={s.stagedCount}>{stagedCount} staged</span></>}
        </span>
        <button
          class={s.pageBtn}
          onClick={handleNext}
          disabled={!res || res.rows.length < limit.value}
        >
          Next →
        </button>
        <label class={s.pageSize}>
          rows per page
          <select
            class={s.filterSelect}
            value={String(limit.value)}
            onChange={e => setPageSize(Number((e.target as HTMLSelectElement).value))}
          >
            {PAGE_SIZES.map(n => <option key={n} value={String(n)}>{n}</option>)}
          </select>
        </label>
      </div>

      {showImport.value && meta.value && (
        <ImportDialog
          connectionId={conn.id}
          schema={schemaName}
          table={table}
          meta={meta.value}
          onClose={closeImport}
          onImported={load}
        />
      )}
    </div>
  )
}
