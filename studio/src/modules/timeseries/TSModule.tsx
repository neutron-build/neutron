import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './TSModule.module.css'

interface TSModuleProps {
  name: string
}

// Tiny SVG sparkline for a series of values
function Sparkline({ values }: { values: number[] }) {
  if (values.length < 2) return null
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const w = 200
  const h = 40
  const pts = values.map((v, i) => {
    const x = (i / (values.length - 1)) * w
    const y = h - ((v - min) / range) * (h - 4) - 2
    return `${x},${y}`
  })
  return (
    <svg width={w} height={h} class={s.sparkline}>
      <polyline
        points={pts.join(' ')}
        fill="none"
        stroke="var(--model-ts)"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
    </svg>
  )
}

export function TSModule({ name }: TSModuleProps) {
  const from = useSignal('')
  const to = useSignal('')
  const aggFn = useSignal<'avg' | 'sum' | 'min' | 'max' | 'count'>('avg')
  const bucket = useSignal('1h')
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const sparkValues = useSignal<number[]>([])
  const stats = useSignal<{ count: number; min: number; max: number; avg: number } | null>(null)

  const conn = activeConnection.value!

  // Load quick stats on mount
  useEffect(() => {
    async function loadStats() {
      try {
        const r = await api.query(
          `SELECT COUNT(*), MIN(value), MAX(value), AVG(value)
           FROM ts_range('${name}', '-inf', '+inf')`,
          conn.id
        )
        if (!r.error && r.rows.length > 0) {
          const [count, min, max, avg] = r.rows[0] as number[]
          stats.value = { count, min, max, avg }
        }
      } catch { /* non-critical */ }
    }
    loadStats()
  }, [name])

  async function runQuery() {
    running.value = true
    result.value = null
    sparkValues.value = []
    try {
      const fromClause = from.value ? `'${from.value}'` : "'-inf'"
      const toClause = to.value ? `'${to.value}'` : "'+inf'"
      const r = await api.query(
        `SELECT time_bucket('${bucket.value}', ts) AS bucket,
                ${aggFn.value}(value) AS value
         FROM ts_range('${name}', ${fromClause}, ${toClause})
         GROUP BY 1 ORDER BY 1`,
        conn.id
      )
      result.value = r
      if (!r.error && r.rows.length > 0) {
        sparkValues.value = r.rows.map(row => Number(row[1])).filter(v => !isNaN(v))
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.metricName}>{name}</span>
        {stats.value && (
          <div class={s.statPills}>
            <span class={s.statPill}>{stats.value.count.toLocaleString()} pts</span>
            <span class={s.statPill}>min {fmt(stats.value.min)}</span>
            <span class={s.statPill}>max {fmt(stats.value.max)}</span>
            <span class={s.statPill}>avg {fmt(stats.value.avg)}</span>
          </div>
        )}
      </div>

      <div class={s.queryPanel}>
        <div class={s.queryRow}>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>From</label>
            <input class={s.fieldInput} type="datetime-local" value={from.value}
              onInput={e => { from.value = (e.target as HTMLInputElement).value }} />
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>To</label>
            <input class={s.fieldInput} type="datetime-local" value={to.value}
              onInput={e => { to.value = (e.target as HTMLInputElement).value }} />
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>Bucket</label>
            <select class={s.fieldSelect} value={bucket.value}
              onChange={e => { bucket.value = (e.target as HTMLSelectElement).value }}>
              <option value="1m">1 min</option>
              <option value="5m">5 min</option>
              <option value="1h">1 hr</option>
              <option value="1d">1 day</option>
              <option value="7d">7 days</option>
            </select>
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>Agg</label>
            <select class={s.fieldSelect} value={aggFn.value}
              onChange={e => { aggFn.value = (e.target as HTMLSelectElement).value as any }}>
              <option value="avg">avg</option>
              <option value="sum">sum</option>
              <option value="min">min</option>
              <option value="max">max</option>
              <option value="count">count</option>
            </select>
          </div>
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Loading…' : '▶ Query'}
          </button>
        </div>
      </div>

      {sparkValues.value.length > 0 && (
        <div class={s.chartArea}>
          <Sparkline values={sparkValues.value} />
          <span class={s.chartLabel}>{result.value?.rowCount} buckets</span>
        </div>
      )}

      <div class={s.grid}>
        {result.value && <DataGrid result={result.value} />}
        {!result.value && !running.value && (
          <div class={s.hint}>Set a time range and click Query to explore the metric</div>
        )}
      </div>
    </div>
  )
}

function fmt(n: number) {
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(2) + 'K'
  return Number(n.toFixed(4)).toString()
}
