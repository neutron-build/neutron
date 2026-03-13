import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './CDCModule.module.css'

type Op = 'all' | 'INSERT' | 'UPDATE' | 'DELETE'
type RefreshInterval = 'off' | '1' | '2' | '5' | '10'

export function CDCModule() {
  const walPosition = useSignal<string | null>(null)
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
    async function loadMeta() {
      try {
        const posR = await api.query(`SELECT cdc_wal_position()`, conn.id)
        if (!posR.error && posR.rows.length > 0) walPosition.value = String(posR.rows[0][0])

        const tabR = await api.query(`SELECT DISTINCT table_name FROM cdc_changes(${limit.value}) ORDER BY 1`, conn.id)
        if (!tabR.error) tables.value = tabR.rows.map(r => String(r[0]))
      } catch { /* non-critical */ }
    }
    loadMeta()
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
      const tableCond = filterTable.value !== 'all'
        ? `AND table_name = '${filterTable.value}'` : ''
      const opCond = filterOp.value !== 'all'
        ? `AND operation = '${filterOp.value}'` : ''

      const r = await api.query(
        `SELECT lsn, operation, table_name, old_data, new_data, changed_at
         FROM cdc_changes(${limit.value})
         WHERE 1=1 ${tableCond} ${opCond}
         ORDER BY changed_at DESC`,
        conn.id
      )
      result.value = r
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
        {walPosition.value && (
          <span class={s.walPos} title="Current WAL position">LSN {walPosition.value}</span>
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
            onChange={e => { limit.value = parseInt((e.target as HTMLSelectElement).value) }}>
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
