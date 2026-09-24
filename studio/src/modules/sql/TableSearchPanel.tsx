import { useSignal, useComputed } from '@preact/signals'
import type { TableMeta, TableMetaColumn } from '../../lib/types'
import { api } from '../../lib/api'
import { activeConnection } from '../../lib/store'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './SQLBrowser.module.css'

/** X01: the table search journey — vector similarity (pgvector operators)
 * and full-text search (websearch_to_tsquery + ts_rank ordering), run
 * server-side as parameter-bound read-only queries. Vectors are not
 * row-editable values; searching them is the supported interaction. */

type SearchKind = 'vector' | 'fts'

interface TableSearchPanelProps {
  schema: string
  table: string
  meta: TableMeta
}

function vectorColumns(meta: TableMeta): TableMetaColumn[] {
  return meta.columns.filter(c => c.type === 'vector')
}

function ftsColumns(meta: TableMeta): TableMetaColumn[] {
  return meta.columns.filter(c => c.type === 'text' || c.type === 'varchar' || c.type === 'tsvector')
}

export function TableSearchPanel({ schema, table, meta }: TableSearchPanelProps) {
  const vecCols = vectorColumns(meta)
  const ftsCols = ftsColumns(meta)
  const kind = useSignal<SearchKind>(vecCols.length > 0 ? 'vector' : 'fts')
  const column = useSignal<string>((kind.value === 'vector' ? vecCols[0] : ftsCols[0])?.name ?? '')
  const operator = useSignal<'l2' | 'cosine' | 'inner-product' | 'l1'>('cosine')
  const query = useSignal('')
  const limit = useSignal(20)
  const running = useSignal(false)
  const result = useSignal<QueryResult | null>(null)

  const columnsForKind = useComputed(() => (kind.value === 'vector' ? vecCols : ftsCols))
  const description = useComputed(() => {
    if (kind.value === 'vector') {
      const op = operator.value
      const semantics =
        op === 'cosine' ? 'cosine distance (0 = identical direction), nearest first'
        : op === 'l2' ? 'L2 (Euclidean) distance, nearest first'
        : op === 'inner-product' ? 'negative inner product (most positive similarity first)'
        : 'L1 (Manhattan) distance, nearest first'
      return `ORDER BY "${column.value}" ${op === 'cosine' ? '<=>' : op === 'l2' ? '<->' : op === 'inner-product' ? '<#>' : '<+>'} $1::vector — ${semantics}. Requires the pgvector extension; scoring is pgvector's own.`
    }
    return `to_tsvector(...) @@ websearch_to_tsquery(...) with ts_rank(...) DESC ordering — core PostgreSQL full-text search.`
  })

  async function run() {
    const conn = activeConnection.value
    if (!conn || query.value.trim() === '' || column.value === '') return
    running.value = true
    result.value = null
    try {
      result.value = await api.tableSearch({
        connectionId: conn.id,
        schema,
        table,
        kind: kind.value,
        column: column.value,
        query: query.value.trim(),
        ...(kind.value === 'vector' ? { operator: operator.value } : {}),
        limit: limit.value,
      })
    } catch (err: unknown) {
      result.value = {
        columns: [], rows: [], rowCount: 0, duration: 0,
        error: err instanceof Error ? err.message : String(err),
      }
    } finally {
      running.value = false
    }
  }

  if (vecCols.length === 0 && ftsCols.length === 0) return null

  return (
    <div class={s.searchPanel}>
      <div class={s.searchRow}>
        <select
          class={s.select}
          value={kind.value}
          onChange={e => {
            const next = (e.currentTarget as HTMLSelectElement).value as SearchKind
            kind.value = next
            const cols = next === 'vector' ? vecCols : ftsCols
            column.value = cols[0]?.name ?? ''
            result.value = null
          }}
          title="Search kind: vector similarity (pgvector) or full-text (core PostgreSQL)"
        >
          {vecCols.length > 0 && <option value="vector">vector similarity</option>}
          {ftsCols.length > 0 && <option value="fts">full-text</option>}
        </select>
        <select
          class={s.select}
          value={column.value}
          onChange={e => { column.value = (e.currentTarget as HTMLSelectElement).value }}
        >
          {columnsForKind.value.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
        </select>
        {kind.value === 'vector' && (
          <select
            class={s.select}
            value={operator.value}
            onChange={e => { operator.value = (e.currentTarget as HTMLSelectElement).value as typeof operator.value }}
            title="pgvector distance operator (also selects the index opclass that accelerates it)"
          >
            <option value="cosine">&lt;=&gt; cosine</option>
            <option value="l2">&lt;-&gt; L2</option>
            <option value="inner-product">&lt;#&gt; inner product</option>
            <option value="l1">&lt;+&gt; L1</option>
          </select>
        )}
        <input
          class={s.searchInput}
          type="text"
          placeholder={kind.value === 'vector' ? 'query vector, e.g. [0.1, 0.2, 0.3]' : 'search terms, e.g. "exact phrase" OR vector'}
          value={query.value}
          onInput={e => { query.value = (e.currentTarget as HTMLInputElement).value }}
          onKeyDown={e => { if (e.key === 'Enter') void run() }}
        />
        <select
          class={s.select}
          value={String(limit.value)}
          onChange={e => { limit.value = Number((e.currentTarget as HTMLSelectElement).value) }}
        >
          {[10, 20, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
        </select>
        <button class={s.btnAction} disabled={running.value || query.value.trim() === ''} onClick={() => void run()}>
          {running.value ? 'searching…' : 'Search'}
        </button>
      </div>
      <div class={s.searchHint}>{description.value}</div>
      {result.value?.error && <div class={s.searchError}>{result.value.error}</div>}
      {result.value && !result.value.error && (
        <div class={s.searchResults}>
          <div class={s.searchMeta}>{result.value.rows.length} result{result.value.rows.length === 1 ? '' : 's'}</div>
          <DataGrid result={result.value} />
        </div>
      )}
    </div>
  )
}
