import { useSignal } from '@preact/signals'
import { useEffect, useId, useRef } from 'preact/hooks'
import { activeConnection } from '../../lib/store'
import { useRequestOwner } from '../../lib/requestOwner'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
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
    if (!Array.isArray(parsed) || parsed.some(e => !e || !Number.isSafeInteger(e.seq) || typeof e.table !== 'string' || typeof e.change !== 'string' || !Number.isFinite(e.ts))) throw new Error('Invalid CDC events')
    return parsed as CdcEvent[]
  } catch {
    throw new Error('CDC events unavailable: invalid response')
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

export function CDCModule({ initialTable }: { initialTable?: string } = {}) {
  const controlsId = useId()
  const totalCount = useSignal<number | null>(null)
  const tables = useSignal<string[]>(initialTable && initialTable !== 'all' ? [initialTable] : [])
  const filterTable = useSignal(initialTable || 'all')
  const filterOp = useSignal<Op>('all')
  const limit = useSignal(200)
  const result = useSignal<QueryResult | null>(null)
  const loading = useSignal(false)
  const refreshInterval = useSignal<RefreshInterval>('off')
  const rlsDenied = useSignal<string | null>(null)
  const gridRef = useRef<HTMLDivElement>(null)

  const conn = activeConnection.value
  const requests = useRequestOwner(JSON.stringify([conn?.id, initialTable, filterTable.value, filterOp.value, limit.value, refreshInterval.value]))
  const unavailable = useSignal<string | null>(null)
  const inFlight = useRef<{ connectionId: string; requestId: string } | null>(null)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const frame = useRef<number | null>(null)

  function cancelInFlight() {
    const request = inFlight.current
    inFlight.current = null
    if (request) void api.cancelQuery(request.connectionId, request.requestId).catch(() => {})
  }
  function invalidate() {
    requests.invalidate()
    if (timer.current) clearTimeout(timer.current)
    timer.current = null
    if (frame.current != null) cancelAnimationFrame(frame.current)
    frame.current = null
    cancelInFlight()
    totalCount.value = null; result.value = null; loading.value = false
    unavailable.value = null; rlsDenied.value = null
  }
  useEffect(() => {
    let current = activeConnection.value
    const stop = activeConnection.subscribe(next => {
      if (next !== current) { current = next; invalidate(); tables.value = filterTable.value === 'all' ? [] : [filterTable.value] }
    })
    return () => { invalidate(); stop() }
  }, [])

  useEffect(() => {
    filterTable.value = initialTable || 'all'
  }, [initialTable])

  // One effect owns one recursive timer. Awaiting a poll avoids overlapping polls.
  useEffect(() => {
    let disposed = false
    const ownsTimer = requests.begin('timer')
    async function tick() {
      if (disposed || !ownsTimer()) return
      if (!loading.value) await loadChanges()
      if (disposed || !ownsTimer() || refreshInterval.value === 'off') return
      timer.current = setTimeout(tick, Number(refreshInterval.value) * 1000)
    }
    void tick()
    return () => { disposed = true; invalidate() }
  }, [conn?.id, initialTable, refreshInterval.value, filterTable.value, filterOp.value, limit.value])

  async function loadChanges() {
    const connectionId = conn?.id
    const owns = requests.begin('read')
    if (!connectionId || !owns()) return
    cancelInFlight()
    const table = filterTable.value, op = filterOp.value, countLimit = limit.value
    const scrollTop = gridRef.current?.scrollTop ?? 0
    loading.value = true; unavailable.value = null; rlsDenied.value = null
    async function queryOwned(sql: string) {
      if (!owns()) return null
      const request = { connectionId: connectionId!, requestId: crypto.randomUUID() }
      inFlight.current = request
      try {
        return await api.query(sql, request.connectionId, undefined, request.requestId)
      } finally { if (inFlight.current === request) inFlight.current = null }
    }
    try {
      const countR = await queryOwned(`SELECT CDC_COUNT()`)
      if (!owns() || !countR) return
      if (countR.error || countR.canceled) throw new Error(countR.error || 'CDC count canceled')
      if (countR.rows[0]?.[0] == null || !Number.isSafeInteger(Number(countR.rows[0][0])) || Number(countR.rows[0][0]) < 0) throw new Error('CDC count unavailable')
      const count = Number(countR.rows[0][0])
      const r = await queryOwned(buildCdcQuery(count, countLimit, table))
      if (!owns() || !r) return
      if (r.error || r.canceled) throw new Error(r.error || 'CDC read canceled')
      if (!r.rows.length) throw new Error('CDC events unavailable')
      let events = parseCdcEvents(r.rows[0][0]).slice().reverse()
      const seen = new Set(tables.value)
      if (table !== 'all') seen.add(table)
      for (const event of events) seen.add(event.table)
      if (op !== 'all') events = events.filter(event => event.change === op)
      // Publish the count and rows together only after the complete read succeeds.
      const rows = eventsToResult(events)
      if (!owns()) return
      totalCount.value = count; tables.value = Array.from(seen).sort(); result.value = rows
    } catch (err) {
      if (owns()) {
        const message = err instanceof Error ? err.message : String(err)
        unavailable.value = message; rlsDenied.value = isRlsDenied(message) ? message : null
        totalCount.value = null; result.value = null
      }
    } finally {
      if (owns()) {
        loading.value = false
        frame.current = requestAnimationFrame(() => {
          frame.current = null
          if (owns() && gridRef.current) gridRef.current.scrollTop = scrollTop
        })
      }
    }
  }

  const isLive = refreshInterval.value !== 'off'

  return (
    <div class={s.layout}>
      {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}
      {unavailable.value && <div role="alert">CDC data unavailable: {unavailable.value}</div>}
      <div class={s.header}>
        <span class={s.title}>Change Data Capture</span>
        <span
          class={s.walPos}
          title={'Verified on Nucleus 1.0.2: only INSERT statements emit CDC events - UPDATE and DELETE statements do not reach the log (engine defect, recorded upstream). Events carry metadata only (seq, table, change, ts) and are emitted at statement time, so rolled-back transactions appear too.'}
        >
          metadata only - INSERT events only (Nucleus 1.0.2)
        </span>
        {totalCount.value != null && (
          <span class={s.walPos} title="Total change events">{totalCount.value.toLocaleString()} events</span>
        )}
        <div class={s.refreshControl}>
          <label class={s.refreshLabel} htmlFor={`${controlsId}-refresh`}>Auto-refresh</label>
          <select
            id={`${controlsId}-refresh`}
            class={s.refreshSelect}
            value={refreshInterval.value}
            onChange={e => { invalidate(); refreshInterval.value = (e.target as HTMLSelectElement).value as RefreshInterval }}
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
          <label class={s.filterLabel} htmlFor={`${controlsId}-table`}>Table</label>
          <select id={`${controlsId}-table`} class={s.filterSelect} value={filterTable.value}
            onChange={e => { invalidate(); filterTable.value = (e.target as HTMLSelectElement).value }}>
            <option value="all">All tables</option>
            {tables.value.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div class={s.filterGroup}>
          <label class={s.filterLabel} htmlFor={`${controlsId}-operation`}>Operation</label>
          <select id={`${controlsId}-operation`} class={s.filterSelect} value={filterOp.value}
            onChange={e => { invalidate(); filterOp.value = (e.target as HTMLSelectElement).value as Op }}>
            <option value="all">All</option>
            <option value="INSERT">INSERT</option>
            <option value="UPDATE">UPDATE</option>
            <option value="DELETE">DELETE</option>
          </select>
        </div>
        <div class={s.filterGroup}>
          <label class={s.filterLabel} htmlFor={`${controlsId}-limit`}>Limit</label>
          <select id={`${controlsId}-limit`} class={s.filterSelect} value={limit.value}
            onChange={e => { invalidate(); limit.value = parseInt((e.target as HTMLSelectElement).value) }}>
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
          : <div class={s.hint}>{unavailable.value ? 'CDC changes unavailable' : loading.value ? 'Loading CDC changes...' : 'Refresh CDC changes'}</div>
        }
      </div>
    </div>
  )
}
