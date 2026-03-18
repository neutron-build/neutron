import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './TSModule.module.css'

interface TSModuleProps {
  name: string
}

type ViewMode = 'chart' | 'grid'

// --- Observable Plot lazy loading (same pattern as CodeMirror in SQLEditor) ---
let plotLoaded = false
let Plot: typeof import('@observablehq/plot')

async function loadPlot() {
  if (plotLoaded) return
  Plot = await import('@observablehq/plot')
  plotLoaded = true
}

// --- Tiny SVG sparkline for the header stats area ---
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

// --- Observable Plot chart component ---
interface TimeChartProps {
  buckets: string[]
  values: number[]
  aggFn: string
}

function TimeChart({ buckets, values, aggFn }: TimeChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartReady = useSignal(false)

  useEffect(() => {
    let cancelled = false

    async function render() {
      await loadPlot()
      if (cancelled || !containerRef.current) return

      // Build data array with parsed dates
      const data = buckets.map((b, i) => ({
        time: new Date(b),
        value: values[i],
      })).filter(d => !isNaN(d.time.getTime()) && !isNaN(d.value))

      if (data.length === 0) return

      // Measure container
      const rect = containerRef.current.getBoundingClientRect()
      const width = Math.max(rect.width - 16, 300)
      const height = Math.max(rect.height - 16, 200)

      const chart = Plot.plot({
        width,
        height,
        marginLeft: 60,
        marginRight: 20,
        marginTop: 20,
        marginBottom: 40,
        style: {
          background: 'transparent',
          color: 'var(--text-secondary)',
          fontSize: '11px',
          fontFamily: 'var(--font-mono)',
        },
        x: {
          type: 'utc',
          label: 'Time',
          tickFormat: autoTickFormat(data[0].time, data[data.length - 1].time),
        },
        y: {
          label: `${aggFn}(value)`,
          grid: true,
        },
        marks: [
          // Area fill
          Plot.areaY(data, {
            x: 'time',
            y: 'value',
            fill: 'var(--model-ts)',
            fillOpacity: 0.15,
            curve: 'monotone-x',
          }),
          // Line
          Plot.lineY(data, {
            x: 'time',
            y: 'value',
            stroke: 'var(--model-ts)',
            strokeWidth: 2,
            curve: 'monotone-x',
          }),
          // Dots on each bucket
          Plot.dot(data, {
            x: 'time',
            y: 'value',
            fill: 'var(--model-ts)',
            r: data.length > 100 ? 1.5 : 3,
            tip: true,
          }),
          // Rule at y=0 if values go negative
          ...(Math.min(...values) < 0
            ? [Plot.ruleY([0], { stroke: 'var(--text-tertiary)', strokeDasharray: '4,3' })]
            : []),
        ],
      })

      // Clear previous
      containerRef.current.innerHTML = ''
      containerRef.current.appendChild(chart)
      chartReady.value = true
    }

    render()
    return () => { cancelled = true }
  }, [buckets, values, aggFn])

  return (
    <div ref={containerRef} class={s.chartContainer}>
      {!chartReady.value && <div class={s.chartLoading}>Rendering chart...</div>}
    </div>
  )
}

// Pick a sensible tick format based on the time range
function autoTickFormat(start: Date, end: Date): string {
  const diffMs = end.getTime() - start.getTime()
  const diffH = diffMs / (1000 * 60 * 60)
  if (diffH < 2) return '%H:%M:%S'
  if (diffH < 48) return '%H:%M'
  if (diffH < 24 * 60) return '%b %d'
  return '%Y-%m-%d'
}

export function TSModule({ name }: TSModuleProps) {
  const from = useSignal('')
  const to = useSignal('')
  const aggFn = useSignal<'avg' | 'sum' | 'min' | 'max' | 'count'>('avg')
  const bucket = useSignal('1h')
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const sparkValues = useSignal<number[]>([])
  const bucketLabels = useSignal<string[]>([])
  const stats = useSignal<{ count: number; min: number; max: number; avg: number } | null>(null)
  const viewMode = useSignal<ViewMode>('chart')

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
    bucketLabels.value = []
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
        bucketLabels.value = r.rows.map(row => String(row[0]))
        sparkValues.value = r.rows.map(row => Number(row[1])).filter(v => !isNaN(v))
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  const hasData = sparkValues.value.length > 0

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
              <option value="15m">15 min</option>
              <option value="1h">1 hr</option>
              <option value="6h">6 hr</option>
              <option value="1d">1 day</option>
              <option value="7d">7 days</option>
              <option value="30d">30 days</option>
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
            {running.value ? 'Loading...' : 'Query'}
          </button>
        </div>
      </div>

      {/* Mini sparkline preview + view toggle */}
      {hasData && (
        <div class={s.chartToolbar}>
          <div class={s.sparkArea}>
            <Sparkline values={sparkValues.value} />
            <span class={s.chartLabel}>
              {result.value?.rowCount} buckets &middot;
              range [{fmt(Math.min(...sparkValues.value))}, {fmt(Math.max(...sparkValues.value))}]
            </span>
          </div>
          <div class={s.viewToggle}>
            <button
              class={`${s.toggleBtn} ${viewMode.value === 'chart' ? s.toggleActive : ''}`}
              onClick={() => { viewMode.value = 'chart' }}
            >
              Chart
            </button>
            <button
              class={`${s.toggleBtn} ${viewMode.value === 'grid' ? s.toggleActive : ''}`}
              onClick={() => { viewMode.value = 'grid' }}
            >
              Table
            </button>
          </div>
        </div>
      )}

      {/* Main content area */}
      <div class={s.mainArea}>
        {hasData && viewMode.value === 'chart' ? (
          <TimeChart
            buckets={bucketLabels.value}
            values={sparkValues.value}
            aggFn={aggFn.value}
          />
        ) : hasData && viewMode.value === 'grid' ? (
          <div class={s.grid}>
            <DataGrid result={result.value!} />
          </div>
        ) : result.value && result.value.error ? (
          <div class={s.error}>{result.value.error}</div>
        ) : (
          <div class={s.hint}>
            {running.value
              ? 'Loading...'
              : 'Set a time range and click Query to explore the metric'}
          </div>
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
