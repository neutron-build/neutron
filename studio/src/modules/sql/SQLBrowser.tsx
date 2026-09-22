import { useSignal, useComputed } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, openTab, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid, type FKTarget } from '../../components/DataGrid'
import type { QueryResult, SqlColumn, FKDetail } from '../../lib/types'
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

  const conn = activeConnection.value!

  const tableInfo = useComputed(() =>
    (schema.value?.sql ?? []).find(t => t.schema === schemaName && t.name === table) ?? null
  )

  const pkColumns = useComputed(() =>
    (tableInfo.value?.columns ?? []).filter(c => c.isPrimaryKey)
  )

  // Editing is offered only for tables with exactly one PK column. Composite
  // and no-key tables are read-only (the backend enforces this independently
  // of what the browser claims).
  const pkColumn = useComputed<string | undefined>(() =>
    pkColumns.value.length === 1 ? pkColumns.value[0].name : undefined
  )

  const readOnlyReason = useComputed<string | null>(() => {
    if (!tableInfo.value) return null
    if (pkColumns.value.length === 0) {
      return `${schemaName}.${table} has no primary key — rows are read-only`
    }
    if (pkColumns.value.length > 1) {
      return `${schemaName}.${table} has a composite primary key (${pkColumns.value.map(c => c.name).join(', ')}) — row edits need the full key and are read-only in this interim version`
    }
    return null
  })

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
    api.tableFKs(conn.id, schemaName, table)
      .then(r => {
        const map: Record<string, FKDetail> = {}
        for (const fk of r.fks ?? []) map[fk.column] = fk
        fks.value = map
      })
      .catch(() => { /* FK links are optional polish */ })
  }, [schemaName, table])

  useEffect(() => { load() }, [schemaName, table, limit.value, offset.value, filterActive.value, sortColumn.value, sortDir.value])

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
    const pk = pkColumn.value
    if (!pk) {
      toast('error', `Editing ${schemaName}.${table} requires a single-column primary key`)
      return
    }
    const pkValue = (result.value?.rows[rowIndex] as unknown[] | undefined)?.[result.value!.columns.indexOf(pk)]
    try {
      const res = await api.tableUpdate({
        connectionId: conn.id, schema: schemaName, table,
        pkColumn: pk, pkValue, column, value: value ?? undefined, isNull: value === null,
      })
      if (res.error) {
        toast('error', `Update failed: ${res.error}`)
        return
      }
      if (res.rowsAffected !== 1) {
        toast('error', `Update failed: expected to change exactly 1 row, changed ${res.rowsAffected}`)
        return
      }
      toast('success', `Updated ${table}.${column}`)
      await load()
    } catch (err: unknown) {
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
      filter: { column: fk.refColumn, op: 'eq', value: String(value) },
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
            pkColumn={pkColumn.value}
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
