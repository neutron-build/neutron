import { useSignal, useComputed } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult, SqlColumn } from '../../lib/types'
import s from './SQLBrowser.module.css'

interface SQLBrowserProps {
  schema: string
  table: string
}

export function SQLBrowser({ schema: schemaName, table }: SQLBrowserProps) {
  const result = useSignal<QueryResult | null>(null)
  const loading = useSignal(false)
  const error = useSignal<string | null>(null)
  const limit = useSignal(200)
  const offset = useSignal(0)

  const conn = activeConnection.value!

  const tableInfo = useComputed(() =>
    (schema.value?.sql ?? []).find(t => t.schema === schemaName && t.name === table) ?? null
  )

  async function load() {
    loading.value = true
    error.value = null
    try {
      result.value = await api.tableData(conn.id, schemaName, table, limit.value, offset.value)
    } catch (err: unknown) {
      error.value = err instanceof Error ? err.message : String(err)
      toast('error', `Failed to load ${table}: ${error.value}`)
    } finally {
      loading.value = false
    }
  }

  useEffect(() => { load() }, [schemaName, table, limit.value, offset.value])

  function handlePrev() {
    if (offset.value === 0) return
    offset.value = Math.max(0, offset.value - limit.value)
  }

  function handleNext() {
    if (!result.value) return
    if (result.value.rows.length < limit.value) return
    offset.value = offset.value + limit.value
  }

  const info = tableInfo.value

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
          {info.columns.map((col: SqlColumn) => (
            <span key={col.name} class={s.colPill} title={`${col.type}${col.nullable ? '' : ' NOT NULL'}${col.isPrimaryKey ? ' PK' : ''}`}>
              {col.isPrimaryKey && <span class={s.pkMark}>PK</span>}
              <span class={s.colName}>{col.name}</span>
              <span class={s.colType}>{col.type}</span>
            </span>
          ))}
        </div>
      )}

      <div class={s.grid}>
        {loading.value && <div class={s.loading}>Loading…</div>}
        {!loading.value && error.value && <div class={s.error}>{error.value}</div>}
        {!loading.value && result.value && (
          <DataGrid result={result.value} />
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
