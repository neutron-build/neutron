import { useSignal, useComputed } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, schema, openTab, toast, bindingActive, type EditingBinding } from '../../lib/store'
import { api, ApiError } from '../../lib/api'
import { encodeCell } from '../../lib/wire'
import { DataGrid, type FKTarget } from '../../components/DataGrid'
import type { QueryResult, SqlColumn, FKDetail, TableMeta, KeyCell } from '../../lib/types'
import s from './SQLBrowser.module.css'

interface SQLBrowserProps {
  schema: string
  table: string
  /** Pre-applied filter (FK follow opens a table filtered by the referenced column). */
  initialFilter?: { column: string; op: string; value: string }
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

export function SQLBrowser({ schema: schemaName, table, initialFilter }: SQLBrowserProps) {
  const result = useSignal<QueryResult | null>(null)
  const loading = useSignal(false)
  const error = useSignal<string | null>(null)
  const limit = useSignal(200)
  const offset = useSignal(0)
  const filterColumn = useSignal(initialFilter?.column ?? '')
  const filterOp = useSignal(initialFilter?.op ?? 'eq')
  const filterValue = useSignal(initialFilter?.value ?? '')
  const filterActive = useSignal(initialFilter !== undefined)
  const sortColumn = useSignal<string | null>(null)
  const sortDir = useSignal<'asc' | 'desc'>('asc')
  const fks = useSignal<Record<string, FKDetail>>({})
  const meta = useSignal<TableMeta | null>(null)

  // The editing binding: this view's rows were loaded under exactly this
  // connection, captured ONCE at mount (the view survives connection
  // switches without remounting). Connection switching is explicit —
  // commits are refused while a different connection is active (see
  // store.ts); the server re-validates identity independently.
  const connRef = useRef<string | null>(null)
  if (connRef.current === null) {
    connRef.current = activeConnection.value?.id ?? ''
  }
  const conn = { id: connRef.current }
  const binding: EditingBinding = { connectionId: conn.id, schema: schemaName, table }

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
      result.value = await api.tableData(
        conn.id, schemaName, table, limit.value, offset.value,
        filterActive.value && filterColumn.value
          ? { column: filterColumn.value, op: filterOp.value, value: filterValue.value }
          : undefined,
        sortColumn.value ? { column: sortColumn.value, dir: sortDir.value } : undefined,
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
          // Per-column links come from single-column FKs; composite FKs
          // navigate with the whole tuple (their full metadata is carried
          // on the FKDetail for the composite navigation UI).
          if (fk.columns && fk.columns.length > 1) continue
          if (fk.column) map[fk.column] = fk
        }
        fks.value = map
      })
      .catch(() => { /* FK links are optional polish */ })
  }, [schemaName, table])

  useEffect(() => { load() }, [schemaName, table, limit.value, offset.value, filterActive.value, sortColumn.value, sortDir.value])

  // Read-only state is AUTHORITATIVE (server catalog): no PK, no row
  // versions, or unavailable metadata all mean read-only, with the reason
  // the server gives. Composite keys are editable when versioned — the
  // full tuple addresses the row.
  const readOnlyReason = useComputed<string | null>(() => meta.value?.readOnly ? (meta.value?.readOnlyReason ?? 'read-only') : null)
  const metaColumns = useComputed(() => meta.value?.columns ?? [])
  const editable = useComputed(() => (meta.value !== null && !meta.value.readOnly) || undefined)

  /** Build the versioned full-key identity for one row of the current read. */
  function identityFor(rowIndex: number): { key: KeyCell[]; version: string } | null {
    const res = result.value
    const m = meta.value
    if (!res || !m || m.keyColumns.length === 0) return null
    const versions = res.versions
    if (!versions || versions[rowIndex] === undefined || versions[rowIndex] === null) return null
    const row = res.rows[rowIndex] as unknown[] | undefined
    if (!row) return null
    const tagFor = (col: string) => m.columns.find(c => c.name === col)?.tag ?? null
    const key: KeyCell[] = m.keyColumns.map(kc => {
      const idx = res.columns.indexOf(kc)
      return { column: kc, value: encodeCell(row[idx], tagFor(kc)) }
    })
    return { key, version: versions[rowIndex] }
  }

  function handlePrev() {
    if (offset.value === 0) return
    offset.value = Math.max(0, offset.value - limit.value)
  }

  function handleNext() {
    if (!result.value) return
    if (result.value.rows.length < limit.value) return
    offset.value = offset.value + limit.value
  }

  function applyFilter() {
    if (!filterColumn.value) {
      toast('error', 'Pick a column to filter on')
      return
    }
    offset.value = 0
    filterActive.value = true
    load()
  }

  function clearFilter() {
    filterColumn.value = ''
    filterOp.value = 'eq'
    filterValue.value = ''
    if (filterActive.value) {
      filterActive.value = false
    } else {
      load()
    }
    offset.value = 0
  }

  function handleSort(column: string) {
    if (sortColumn.value === column) {
      if (sortDir.value === 'asc') {
        sortDir.value = 'desc'
      } else {
        sortColumn.value = null
        sortDir.value = 'asc'
      }
    } else {
      sortColumn.value = column
      sortDir.value = 'asc'
    }
    offset.value = 0
  }

  async function commitEdit(rowIndex: number, column: string, value: string | null) {
    if (!bindingActive(binding)) {
      toast('error', `This view is bound to connection ${binding.connectionId}; connection switching never carries edits — reload the table on the active connection`)
      return
    }
    const identity = identityFor(rowIndex)
    if (!identity) {
      toast('error', 'Row identity unavailable (no versioned key) — reload the table before editing')
      return
    }
    try {
      const res = await api.tableUpdateV2({
        connectionId: conn.id, schema: schemaName, table,
        key: identity.key, version: identity.version,
        column, value: value ?? undefined, isNull: value === null,
      })
      if (res.error) {
        toast('error', `Update failed: ${res.error}`)
        return
      }
      if (res.rowsAffected !== 1) {
        toast('error', `Update failed: expected to change exactly 1 row, changed ${res.rowsAffected ?? 0}`)
        return
      }
      toast('success', `Updated ${table}.${column}`)
      await load()
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        if (err.conflict) {
          toast('error', `Edit conflict: ${err.message} — reload applied`)
          await load()
          return
        }
        if (err.missing) {
          toast('error', `Row is gone: ${err.message} — reload applied`)
          await load()
          return
        }
        toast('error', `Update failed: ${err.message}`)
        return
      }
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  function followFK(fk: FKTarget, value: unknown) {
    openTab({
      id: '',
      kind: 'sql-browser',
      label: `${fk.refTable} (${fk.refColumn}=${String(value)})`,
      objectSchema: fk.refSchema,
      objectName: fk.refTable,
      filter: { column: fk.refColumn!, op: 'eq', value: String(value) },
    })
  }

  const info = tableInfo.value
  const cols: SqlColumn[] = info?.columns ?? []

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
          <button class={s.btnRefresh} onClick={load} disabled={loading.value} title="Refresh">
            ↺
          </button>
        </div>
      </div>

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
        <select class={s.filterSelect} value={filterColumn.value} onChange={e => { filterColumn.value = (e.target as HTMLSelectElement).value }}>
          <option value="">column…</option>
          {cols.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
        </select>
        <select class={s.filterSelect} value={filterOp.value} onChange={e => { filterOp.value = (e.target as HTMLSelectElement).value }}>
          {FILTER_OPS.map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
        </select>
        {filterOp.value !== 'is-null' && filterOp.value !== 'not-null' && (
          <input
            class={s.filterInput}
            value={filterValue.value}
            placeholder="value"
            onInput={e => { filterValue.value = (e.target as HTMLInputElement).value }}
            onKeyDown={e => { if (e.key === 'Enter') applyFilter() }}
          />
        )}
        <button class={s.filterBtn} onClick={applyFilter}>Filter</button>
        {(filterActive.value || filterColumn.value) && (
          <button class={s.filterBtn} onClick={clearFilter}>Clear</button>
        )}
      </div>

      <div class={s.grid}>
        {loading.value && <div class={s.loading}>Loading…</div>}
        {!loading.value && error.value && <div class={s.error}>{error.value}</div>}
        {!loading.value && readOnlyReason.value && (
          <div class={s.readOnlyNote} role="note">{readOnlyReason.value}</div>
        )}
        {!loading.value && result.value && (
          <DataGrid
            result={result.value}
            columns={editable.value ? metaColumns.value : undefined}
            onCommitEdit={commitEdit}
            fkColumns={fks.value}
            onFollowFK={followFK}
            sortColumn={sortColumn.value}
            sortDir={sortDir.value}
            onSort={handleSort}
          />
        )}
      </div>

      <div class={s.pagination}>
        <button class={s.pageBtn} onClick={handlePrev} disabled={offset.value === 0}>
          ← Prev
        </button>
        <span class={s.pageInfo}>
          {offset.value + 1}–{offset.value + (result.value?.rows.length ?? 0)}
        </span>
        <button
          class={s.pageBtn}
          onClick={handleNext}
          disabled={!result.value || result.value.rows.length < limit.value}
        >
          Next →
        </button>
      </div>
    </div>
  )
}
