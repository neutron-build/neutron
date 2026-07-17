import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './CDCModule.module.css'

type Op = 'all' | 'INSERT' | 'UPDATE' | 'DELETE'
type RefreshInterval = 'off' | '1' | '2' | '5' | '10'

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

interface CdcEvent {
  seq: number
  table: string
  change: string
  ts: number
}

// CDC is a single GLOBAL log. CDC_READ / CDC_TABLE_READ return a JSON array of
// { seq, table, change, ts } (ts = epoch ms). There is no lsn/old_data/new_data.
export function parseCdcEvents(cell: unknown): CdcEvent[] {
  if (cell == null) return []
  const text = String(cell).trim()
  if (text === '') return []
  try {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? (parsed as CdcEvent[]) : []
  } catch {
    return []
  }
}

// The log is read forward from a sequence cursor, so to show the most recent
// `limit` events we start after (count - limit).
export function buildCdcQuery(count: number, limit: number, filterTable: string): string {
  const after = Math.max(0, count - limit)
  return filterTable !== 'all'
    ? `SELECT CDC_TABLE_READ(${sqlStr(filterTable)}, ${after}, ${limit})`
    : `SELECT CDC_READ(${after}, ${limit})`
}

export function eventsToResult(events: CdcEvent[]): QueryResult {
  return {
    columns: ['seq', 'table', 'change', 'ts'],
    rows: events.map(e => [e.seq, e.table, e.change, new Date(e.ts).toISOString()]),
    rowCount: events.length,
    duration: 0,
  }
}

export function CDCModule() {
  const totalCount = useSignal<number | null>(null)
  const tables = useSignal<string[]>([])
  const filterTable = useSignal('all')
  const filterOp = useSignal<Op>('all')
  const limit = useSignal(200)
  const result = useSignal<QueryResult | null>(null)
  const loading = useSignal(false)
  const refreshInterval = useSignal<RefreshInterval>('off')
  const gridRef = useRef<HTMLDivElement>(null)

  const conn = activeConnection.value!

  useEffect(() => {
    loadChanges()
  }, [])

  // Auto-refresh with configurable interval
  useEffect(() => {
    if (refreshInterval.value === 'off') return
    const ms = parseInt(refreshInterval.value) * 1000
    const id = setInterval(loadChanges, ms)
    return () => clearInterval(id)
  }, [refreshInterval.value, filterTable.value, filterOp.value, limit.value])

  async function loadChanges() {
    // Preserve scroll position
    const scrollTop = gridRef.current?.scrollTop ?? 0
    loading.value = true
    try {
      const countR = await api.query(`SELECT CDC_COUNT()`, conn.id)
      const count = !countR.error && countR.rows.length > 0 ? Number(countR.rows[0][0]) : 0
      totalCount.value = count

      const r = await api.query(buildCdcQuery(count, limit.value, filterTable.value), conn.id)
      if (r.error) {
        result.value = r
      } else {
        const cell = r.rows.length > 0 ? r.rows[0][0] : null
        let events = parseCdcEvents(cell)
        // Newest first
        events = events.slice().reverse()
        // Populate the table filter from what we have seen
        const seen = new Set(tables.value)
        for (const e of events) seen.add(e.table)
        tables.value = Array.from(seen).sort()
        // Operation filter is applied client-side (the JSON carries `change`).
        if (filterOp.value !== 'all') {
          events = events.filter(e => e.change === filterOp.value)
        }
        result.value = eventsToResult(events)
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loading.value = false
      // Restore scroll position after data loads
      requestAnimationFrame(() => {
        if (gridRef.current) {
          gridRef.current.scrollTop = scrollTop
        }
      })
    }
  }

  const isLive = refreshInterval.value !== 'off'

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.title}>Change Data Capture</span>
        {totalCount.value != null && (
          <span class={s.walPos} title="Total change events">{totalCount.value.toLocaleString()} events</span>
        )}
        <div class={s.refreshControl}>
          <label class={s.refreshLabel}>Auto-refresh</label>
          <select
            class={s.refreshSelect}
            value={refreshInterval.value}
            onChange={e => { refreshInterval.value = (e.target as HTMLSelectElement).value as RefreshInterval }}
          >
            <option value="off">Off</option>
            <option value="1">1s</option>
            <option value="2">2s</option>
            <option value="5">5s</option>
            <option value="10">10s</option>
          </select>
        </div>
        <span class={isLive ? s.liveDot : s.pausedDot} title={isLive ? 'Live' : 'Paused'} />
        {isLive && <span class={s.liveLabel}>LIVE</span>}
      </div>

      <div class={s.filterBar}>
        <div class={s.filterGroup}>
          <label class={s.filterLabel}>Table</label>
          <select class={s.filterSelect} value={filterTable.value}
            onChange={e => { filterTable.value = (e.target as HTMLSelectElement).value; loadChanges() }}>
            <option value="all">All tables</option>
            {tables.value.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div class={s.filterGroup}>
          <label class={s.filterLabel}>Operation</label>
          <select class={s.filterSelect} value={filterOp.value}
            onChange={e => { filterOp.value = (e.target as HTMLSelectElement).value as Op; loadChanges() }}>
            <option value="all">All</option>
            <option value="INSERT">INSERT</option>
            <option value="UPDATE">UPDATE</option>
            <option value="DELETE">DELETE</option>
          </select>
        </div>
        <div class={s.filterGroup}>
          <label class={s.filterLabel}>Limit</label>
          <select class={s.filterSelect} value={limit.value}
            onChange={e => { limit.value = parseInt((e.target as HTMLSelectElement).value); loadChanges() }}>
            <option value={100}>100</option>
            <option value={200}>200</option>
            <option value={500}>500</option>
          </select>
        </div>
        <button class={s.refreshBtn} onClick={loadChanges} disabled={loading.value}>
          {loading.value ? '...' : 'Refresh'}
        </button>
      </div>

      <div class={s.grid} ref={gridRef}>
        {result.value
          ? <DataGrid result={result.value} />
          : <div class={s.hint}>Loading CDC changes...</div>
        }
      </div>
    </div>
  )
}
