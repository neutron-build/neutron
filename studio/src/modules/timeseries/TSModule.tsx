import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
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

// Bucket size options, in milliseconds (the engine's TS functions are numeric
// epoch-ms — there are no string intervals like '1h').
const BUCKET_OPTS: { label: string; ms: number }[] = [
  { label: '1 min', ms: 60_000 },
  { label: '5 min', ms: 5 * 60_000 },
  { label: '15 min', ms: 15 * 60_000 },
  { label: '1 hr', ms: 60 * 60_000 },
  { label: '6 hr', ms: 6 * 60 * 60_000 },
  { label: '1 day', ms: 24 * 60 * 60_000 },
  { label: '7 days', ms: 7 * 24 * 60 * 60_000 },
  { label: '30 days', ms: 30 * 24 * 60 * 60_000 },
]

const MAX_BUCKETS = 500

// Ingestion input: one point per line — "epoch_ms, value" or "ISO-8601, value"
// (ISO forms are converted to epoch ms client-side; the engine's surface is
// numeric). Values must be finite numbers. Returns one BATCHED insert select
// (the same multi-column pattern the bucket query uses) or an error message
// naming the bad line — nothing is sent when any line fails to parse.
export type IngestResult = { ok: true; sql: string; count: number } | { ok: false; error: string }

export function buildInsertSql(series: string, pointsInput: string): IngestResult {
  const safeSeries = sqlStr(series)
  const cols: string[] = []
  let count = 0
  for (const rawLine of pointsInput.split('\n')) {
    const line = rawLine.trim()
    if (!line) continue
    const eq = line.indexOf(',')
    if (eq === -1) return { ok: false, error: `Line ${JSON.stringify(line)}: expected "timestamp, value"` }
    const tRaw = line.slice(0, eq).trim()
    const vRaw = line.slice(eq + 1).trim()
    let ms: number
    if (/^-?\d+$/.test(tRaw)) {
      ms = Number(tRaw)
    } else {
      const parsed = Date.parse(tRaw)
      if (Number.isNaN(parsed)) return { ok: false, error: `Line ${JSON.stringify(line)}: timestamp is neither epoch-ms nor a parseable ISO-8601 date` }
      ms = parsed
    }
    const v = Number(vRaw)
    if (!Number.isFinite(v)) return { ok: false, error: `Line ${JSON.stringify(line)}: value is not a finite number` }
    cols.push(`TS_INSERT(${safeSeries}, ${ms}, ${v})`)
    count += 1
  }
  if (count === 0) return { ok: false, error: 'Enter at least one "timestamp, value" line' }
  if (count > MAX_BUCKETS) return { ok: false, error: `${count} points exceeds the ${MAX_BUCKETS}-point batch limit` }
  return { ok: true, sql: `SELECT ${cols.join(', ')}`, count }
}

// TS_RETENTION(max_age_ms) sets ONE policy for the ENTIRE engine — every
// series, existing history included, drained at the next checkpoint tick
// from wall-clock now. Destructive, retroactive, irreversible, no read-back.
export function buildRetentionSql(days: number): string {
  return `SELECT TS_RETENTION(${Math.round(days * 24 * 60 * 60_000)})`
}

export function TSModule({ name }: TSModuleProps) {
  const now = Date.now()
  // Series names are user-supplied (the engine has no series listing), so the
  // tab label is only the starting value.
  const seriesName = useSignal(name)
  // Range is expressed as epoch-milliseconds (numeric), matching the engine.
  const startMs = useSignal(String(now - 60 * 60_000))
  const endMs = useSignal(String(now))
  // The chart aggregates per bucket with TS_RANGE_AVG / TS_RANGE_COUNT; raw
  // points are available separately via TS_RANGE(series, start, end).
  const aggFn = useSignal<'avg' | 'count'>('avg')
  const bucketMs = useSignal(60 * 60_000)
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const sparkValues = useSignal<number[]>([])
  const bucketLabels = useSignal<string[]>([])
  const stats = useSignal<{ count: number; last: number | null } | null>(null)
  const viewMode = useSignal<ViewMode>('chart')
  const rlsDenied = useSignal<string | null>(null)

  // Ingestion (the ingestion -> query -> visualize journey starts here).
  const pointsInput = useSignal('')
  const inserting = useSignal(false)
  // Retention: global + destructive on this engine — the control stays
  // disabled until the acknowledgement checkbox is set, and the warning
  // is always visible.
  const retentionDays = useSignal('30')
  const retentionAck = useSignal(false)
  const settingRetention = useSignal(false)

  const conn = activeConnection.value!

  // Load quick stats: total point count and the last value for this series.
  function loadStats() {
    const series = seriesName.value.trim()
    if (!series) return
    api.query(
      `SELECT TS_COUNT(${sqlStr(series)}), TS_LAST(${sqlStr(series)})`,
      conn.id
    ).then(r => {
      if (r.error) {
        if (isRlsDenied(r.error)) rlsDenied.value = r.error
        return
      }
      if (r.rows.length > 0) {
        const [count, last] = r.rows[0] as unknown[]
        stats.value = {
          count: Number(count),
          last: last != null ? Number(last) : null,
        }
      }
    }).catch(() => { /* non-critical */ })
  }

  useEffect(() => {
    loadStats()
  }, [])

  async function ingestPoints() {
    const series = seriesName.value.trim()
    if (!series) {
      toast('error', 'Set a series name first')
      return
    }
    const parsed = buildInsertSql(series, pointsInput.value)
    if (!parsed.ok) {
      toast('error', parsed.error)
      return
    }
    inserting.value = true
    try {
      const r = await api.query(parsed.sql, conn.id)
      if (r.error) throw new Error(r.error)
      toast('success', `Inserted ${parsed.count} point${parsed.count === 1 ? '' : 's'} into ${series}`)
      pointsInput.value = ''
      loadStats()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      inserting.value = false
    }
  }

  async function applyRetention() {
    const series = seriesName.value.trim()
    if (!series) {
      toast('error', 'Set a series name first')
      return
    }
    const days = Number(retentionDays.value)
    if (!Number.isFinite(days) || days <= 0) {
      toast('error', 'Retention days must be a positive number')
      return
    }
    if (!retentionAck.value) return
    settingRetention.value = true
    try {
      const r = await api.query(buildRetentionSql(days), conn.id)
      if (r.error) throw new Error(r.error)
      toast('success', `Retention set to ${days} day(s) — GLOBAL policy, applies to EVERY series at the next checkpoint`)
      loadStats()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      settingRetention.value = false
    }
  }

  async function runQuery() {
    const series = seriesName.value.trim()
    if (!series) {
      toast('error', 'Set a series name')
      return
    }
    const start = Number(startMs.value)
    const end = Number(endMs.value)
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      toast('error', 'Enter a valid epoch-ms range (end > start)')
      return
    }
    const size = bucketMs.value
    const bucketCount = Math.ceil((end - start) / size)
    if (bucketCount > MAX_BUCKETS) {
      toast('error', `Range yields ${bucketCount} buckets (max ${MAX_BUCKETS}); widen the bucket size`)
      return
    }

    running.value = true
    result.value = null
    sparkValues.value = []
    bucketLabels.value = []
    try {
      // No server-side bucketing over a series, so each bucket window is
      // built client-side and aggregated with the real range function,
      // batched into one multi-column select.
      const fn = aggFn.value === 'count' ? 'TS_RANGE_COUNT' : 'TS_RANGE_AVG'
      const windows: number[] = []
      for (let b = start; b < end; b += size) windows.push(b)
      const cols = windows
        .map(b => `${fn}(${sqlStr(series)}, ${b}, ${Math.min(b + size, end)})`)
        .join(', ')
      const r = await api.query(`SELECT ${cols}`, conn.id)
      if (r.error) throw new Error(r.error)
      const row = (r.rows[0] ?? []) as unknown[]

      // Present the batched scalar row as a bucket/value grid for the DataGrid.
      const gridRows: unknown[][] = windows.map((b, i) => [
        new Date(b).toISOString(),
        row[i] != null ? Number(row[i]) : null,
      ])
      result.value = {
        columns: ['bucket', aggFn.value === 'count' ? 'count' : 'avg'],
        rows: gridRows,
        rowCount: gridRows.length,
        duration: r.duration,
      }
      bucketLabels.value = windows.map(b => new Date(b).toISOString())
      sparkValues.value = windows.map((_, i) => Number(row[i])).filter(v => !isNaN(v))
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
        <input
          class={s.fieldInput}
          style={{ width: 160 }}
          value={seriesName.value}
          placeholder="series name"
          title="Series name (user-supplied — the engine has no series listing)"
          onInput={e => { seriesName.value = (e.target as HTMLInputElement).value }}
          onKeyDown={e => { if (e.key === 'Enter') loadStats() }}
          onBlur={loadStats}
        />
        {stats.value && (
          <div class={s.statPills}>
            <span class={s.statPill}>{stats.value.count.toLocaleString()} pts</span>
            {stats.value.last != null && (
              <span class={s.statPill}>last {fmt(stats.value.last)}</span>
            )}
          </div>
        )}
      </div>

      {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}

      <div class={s.queryPanel}>
        <div class={s.queryRow}>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>Start (epoch ms)</label>
            <input class={s.fieldInput} type="number" value={startMs.value}
              onInput={e => { startMs.value = (e.target as HTMLInputElement).value }} />
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>End (epoch ms)</label>
            <input class={s.fieldInput} type="number" value={endMs.value}
              onInput={e => { endMs.value = (e.target as HTMLInputElement).value }} />
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>Bucket</label>
            <select class={s.fieldSelect} value={String(bucketMs.value)}
              onChange={e => { bucketMs.value = Number((e.target as HTMLSelectElement).value) }}>
              {BUCKET_OPTS.map(o => (
                <option key={o.ms} value={String(o.ms)}>{o.label}</option>
              ))}
            </select>
          </div>
          <div class={s.fieldGroup}>
            <label class={s.fieldLabel}>Agg</label>
            <select class={s.fieldSelect} value={aggFn.value}
              onChange={e => { aggFn.value = (e.target as HTMLSelectElement).value as 'avg' | 'count' }}>
              <option value="avg">avg</option>
              <option value="count">count</option>
            </select>
          </div>
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Loading...' : 'Query'}
          </button>
        </div>
      </div>

      {/* Ingestion: write points (batched TS_INSERT select), then query and
          visualize them — the full time-series journey in one module. */}
      <div class={s.queryPanel}>
        <div class={s.statsTitle}>Ingest points</div>
        <div class={s.queryRow}>
          <textarea
            class={s.queryInput}
            style={{ height: 'auto' }}
            placeholder={'one point per line:\n1730000000000, 1.5\n2026-09-24T05:00:00Z, 2.0'}
            value={pointsInput.value}
            onInput={e => { pointsInput.value = (e.target as HTMLTextAreaElement).value }}
            rows={3}
            spellcheck={false}
          />
          <button class={s.runBtn} onClick={ingestPoints} disabled={inserting.value}>
            {inserting.value ? 'Inserting…' : 'Insert'}
          </button>
        </div>
      </div>

      {/* Retention: honest about what the engine actually does. TS_RETENTION
          is a single GLOBAL policy — per-series retention does not exist and
          stays disabled here with that reason. */}
      <div class={s.queryPanel}>
        <div class={s.statsTitle}>Retention (global)</div>
        <div class={s.queryRow}>
          <input
            class={s.fieldInput}
            type="number"
            style={{ width: 90 }}
            value={retentionDays.value}
            onInput={e => { retentionDays.value = (e.target as HTMLInputElement).value }}
          />
          <span class={s.fieldLabel}>days</span>
          <label class={s.fieldLabel} style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <input
              type="checkbox"
              checked={retentionAck.value}
              onChange={e => { retentionAck.value = (e.target as HTMLInputElement).checked }}
            />
            I understand this is global
          </label>
          <button class={s.runBtn} onClick={applyRetention} disabled={!retentionAck.value || settingRetention.value}>
            {settingRetention.value ? 'Setting…' : 'Set retention'}
          </button>
        </div>
        <div class={s.chartLabel} style={{ display: 'block', marginTop: 4 }}>
          Destructive, retroactive and irreversible: one policy for EVERY series —
          at the next checkpoint tick the engine deletes all existing points older
          than this age (wall-clock based), and there is no undo or dry run.
          Per-series retention is not supported by the engine and stays disabled.
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

function sqlStr(s: string) {
  return `'${s.replace(/'/g, "''")}'`
}

function fmt(n: number) {
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(2) + 'K'
  return Number(n.toFixed(4)).toString()
}
