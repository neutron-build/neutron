import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './ColumnarModule.module.css'

interface ColumnarModuleProps {
  name: string
}

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

// Columnar tables ARE named user tables, so row data comes from a plain scan.
export const QUICK_QUERIES = [
  (t: string) => `SELECT COLUMNAR_COUNT(${sqlStr(t)})`,
  (t: string) => `SELECT * FROM ${t} LIMIT 100`,
]

type Agg = 'SUM' | 'AVG' | 'MIN' | 'MAX'

export function aggregateSql(table: string, agg: Agg, col: string): string {
  return `SELECT COLUMNAR_${agg}(${sqlStr(table)}, ${sqlStr(col)})`
}

const NUMERIC_RE = /^-?\d+(\.\d+)?$/

// COLUMNAR_INSERT(table, col1, val1, col2, val2, ...) — variadic pairs.
// Numeric-looking values are emitted unquoted so numeric aggregates work.
export function buildInsertSql(table: string, pairsInput: string): string {
  const parts: string[] = [sqlStr(table)]
  for (const pair of pairsInput.split(',')) {
    const eq = pair.indexOf('=')
    if (eq === -1) continue
    const key = pair.slice(0, eq).trim()
    const val = pair.slice(eq + 1).trim()
    if (!key) continue
    parts.push(sqlStr(key))
    parts.push(NUMERIC_RE.test(val) ? val : sqlStr(val))
  }
  return `SELECT COLUMNAR_INSERT(${parts.join(', ')})`
}

export function ColumnarModule({ name }: ColumnarModuleProps) {
  const rowCount = useSignal<number | null>(null)
  const query = useSignal(`SELECT * FROM ${name} LIMIT 100`)
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)

  // Aggregate helper
  const aggCol = useSignal('')

  // Insert helper (COLUMNAR_INSERT)
  const insertPairs = useSignal('')
  const inserting = useSignal(false)

  const conn = activeConnection.value!

  useEffect(() => {
    loadMeta()
  }, [name])

  async function loadMeta() {
    try {
      const r = await api.query(`SELECT COLUMNAR_COUNT(${sqlStr(name)})`, conn.id)
      if (!r.error && r.rows.length > 0) rowCount.value = Number(r.rows[0][0])
    } catch { /* non-critical */ }
  }

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

  function runAggregate(agg: Agg) {
    const col = aggCol.value.trim()
    if (!col) {
      toast('error', 'Column name is required')
      return
    }
    query.value = aggregateSql(name, agg, col)
    runQuery()
  }

  async function insertRow() {
    const sql = buildInsertSql(name, insertPairs.value)
    if (!sql.includes(',')) {
      toast('error', 'Enter at least one col=val pair')
      return
    }
    inserting.value = true
    try {
      await api.query(sql, conn.id)
      toast('success', `Inserted into ${name}`)
      insertPairs.value = ''
      await loadMeta()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      inserting.value = false
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

      {/* Aggregates (COLUMNAR_SUM/AVG/MIN/MAX) */}
      <div class={s.statsPanel}>
        <div class={s.statsTitle}>Aggregate</div>
        <div class={s.queryRow}>
          <input
            class={s.queryInput}
            style={{ height: 'auto' }}
            placeholder="column"
            value={aggCol.value}
            onInput={e => { aggCol.value = (e.target as HTMLInputElement).value }}
          />
          <div class={s.quickBtns}>
            {(['SUM', 'AVG', 'MIN', 'MAX'] as Agg[]).map(agg => (
              <button key={agg} class={s.quickBtn} onClick={() => runAggregate(agg)}>
                {agg}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Insert (COLUMNAR_INSERT) */}
      <div class={s.statsPanel}>
        <div class={s.statsTitle}>Insert row</div>
        <div class={s.queryRow}>
          <input
            class={s.queryInput}
            style={{ height: 'auto' }}
            placeholder="col1=val1, col2=val2"
            value={insertPairs.value}
            onInput={e => { insertPairs.value = (e.target as HTMLInputElement).value }}
          />
          <button class={s.runBtn} onClick={insertRow} disabled={inserting.value}>
            {inserting.value ? 'Inserting…' : 'Insert'}
          </button>
        </div>
      </div>

      <div class={s.queryPanel}>
        <div class={s.queryRow}>
          <div class={s.quickBtns}>
            {QUICK_QUERIES.map((fn, i) => (
              <button key={i} class={s.quickBtn} onClick={() => { query.value = fn(name) }}>
                {i === 0 ? 'COUNT' : 'SCAN 100'}
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
