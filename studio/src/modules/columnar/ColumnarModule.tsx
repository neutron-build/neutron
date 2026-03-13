import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './ColumnarModule.module.css'

interface ColumnStat {
  name: string
  type: string
  nullPct: number
  minVal: string
  maxVal: string
  distinctCount: number
}

interface ColumnarModuleProps {
  name: string
}

const QUICK_QUERIES = [
  (t: string) => `SELECT COUNT(*) FROM columnar_scan('${t}')`,
  (t: string) => `SELECT * FROM columnar_scan('${t}') LIMIT 100`,
  (t: string) => `SELECT * FROM columnar_aggregate('${t}', 'count,sum,avg')`,
]

export function ColumnarModule({ name }: ColumnarModuleProps) {
  const rowCount = useSignal<number | null>(null)
  const colStats = useSignal<ColumnStat[]>([])
  const query = useSignal(`SELECT * FROM columnar_scan('${name}') LIMIT 100`)
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadMeta() {
      try {
        const r = await api.query(`SELECT row_count FROM columnar_info('${name}')`, conn.id)
        if (!r.error && r.rows.length > 0) rowCount.value = Number(r.rows[0][0])

        const sr = await api.query(
          `SELECT col_name, col_type, null_pct, min_val, max_val, distinct_count
           FROM columnar_stats('${name}')`,
          conn.id
        )
        if (!sr.error) {
          colStats.value = sr.rows.map(r => ({
            name: String(r[0]),
            type: String(r[1]),
            nullPct: Number(r[2]),
            minVal: String(r[3] ?? ''),
            maxVal: String(r[4] ?? ''),
            distinctCount: Number(r[5]),
          }))
        }
      } catch { /* non-critical */ }
    }
    loadMeta()
  }, [name])

  async function runQuery() {
    running.value = true
    result.value = null
    try {
      const r = await api.query(query.value, conn.id)
      result.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.tableName}>{name}</span>
        {rowCount.value != null && (
          <span class={s.pill}>{rowCount.value.toLocaleString()} rows</span>
        )}
      </div>

      {colStats.value.length > 0 && (
        <div class={s.statsPanel}>
          <div class={s.statsTitle}>Column Statistics</div>
          <div class={s.statsTable}>
            <div class={s.statsHeader}>
              <span class={s.sc}>Column</span>
              <span class={s.sc}>Type</span>
              <span class={s.sc}>Nulls</span>
              <span class={s.sc}>Min</span>
              <span class={s.sc}>Max</span>
              <span class={s.sc}>Distinct</span>
            </div>
            {colStats.value.map(c => (
              <div key={c.name} class={s.statsRow}>
                <span class={s.sc}><b class={s.mono}>{c.name}</b></span>
                <span class={s.sc}><span class={s.typeBadge}>{c.type}</span></span>
                <span class={s.sc}>{c.nullPct.toFixed(1)}%</span>
                <span class={s.sc}><span class={s.mono}>{c.minVal || '—'}</span></span>
                <span class={s.sc}><span class={s.mono}>{c.maxVal || '—'}</span></span>
                <span class={s.sc}>{c.distinctCount.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div class={s.queryPanel}>
        <div class={s.queryRow}>
          <div class={s.quickBtns}>
            {QUICK_QUERIES.map((fn, i) => (
              <button key={i} class={s.quickBtn} onClick={() => { query.value = fn(name) }}>
                {i === 0 ? 'COUNT' : i === 1 ? 'SCAN 100' : 'AGGREGATE'}
              </button>
            ))}
          </div>
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Running…' : '▶ Run'}
          </button>
        </div>
        <textarea
          class={s.queryInput}
          value={query.value}
          onInput={e => { query.value = (e.target as HTMLTextAreaElement).value }}
          rows={3}
          spellcheck={false}
          onKeyDown={e => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); runQuery() }
          }}
        />
      </div>

      <div class={s.grid}>
        {result.value
          ? <DataGrid result={result.value} />
          : <div class={s.hint}>Select a quick query or write your own and click Run</div>
        }
      </div>
    </div>
  )
}
