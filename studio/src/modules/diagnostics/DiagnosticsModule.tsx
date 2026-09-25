import { useEffect } from 'preact/hooks'
import { useSignal } from '@preact/signals'
import { activeConnection, openTab } from '../../lib/store'
import { api, ApiError } from '../../lib/api'
import type { DiagnosticsQueriesResponse, TableStatsResponse } from '../../lib/types'
import s from './Diagnostics.module.css'

function fmtMs(ms: number): string {
  return ms >= 100 ? ms.toFixed(0) : ms >= 10 ? ms.toFixed(1) : ms.toFixed(2)
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}

/** Performance diagnosis (S05): slow-query identification over the server's
 * duration log, pg_stat_statements when the server exposes it, and
 * per-table/index usage statistics. Every unavailable surface says exactly
 * why, instead of pretending. */
export function DiagnosticsModule({ schema, table }: { schema?: string; table?: string }) {
  const conn = activeConnection.value
  const minMs = useSignal(100)
  const queries = useSignal<DiagnosticsQueriesResponse | null>(null)
  const queriesError = useSignal<string | null>(null)
  const queriesTick = useSignal(0)
  // The table picker's inputs are separate from the loaded target: typing
  // never fires a request; Load (or an initial deep link) does.
  const inputSchema = useSignal(schema ?? 'public')
  const inputTable = useSignal(table ?? '')
  const statsSchema = useSignal(schema ?? '')
  const statsTable = useSignal(table ?? '')
  const stats = useSignal<TableStatsResponse | null>(null)
  const statsError = useSignal<string | null>(null)

  useEffect(() => {
    if (!conn || conn.isNucleus) return
    queriesError.value = null
    api.diagnosticsQueries(conn.id, minMs.value)
      .then(r => { queries.value = r })
      .catch(e => { queriesError.value = e instanceof Error ? e.message : String(e) })
  }, [conn?.id, minMs.value, queriesTick.value])

  useEffect(() => {
    if (!conn || conn.isNucleus || !statsSchema.value || !statsTable.value) return
    stats.value = null
    statsError.value = null
    api.tableStats(conn.id, statsSchema.value, statsTable.value)
      .then(r => { stats.value = r })
      .catch(e => {
        statsError.value = e instanceof ApiError && e.status === 404
          ? `${statsSchema.value}.${statsTable.value} has no statistics row — dropped, renamed, or not visible; refresh the schema.`
          : e instanceof Error ? e.message : String(e)
      })
  }, [conn?.id, statsSchema.value, statsTable.value])

  if (!conn) return <div class={s.hint}>Connect to a database first</div>
  if (conn.isNucleus) {
    return (
      <div class={s.hint}>
        Diagnostics use PostgreSQL's cumulative statistics system and the schema
        contract's introspection; neither is verified on Nucleus yet (X00
        conformance), so the view is unavailable rather than approximate.
      </div>
    )
  }

  return (
    <div class={s.wrap}>
      <section class={s.section} aria-label="Slow queries">
        <h3>Slow queries</h3>
        <p class={s.meta}>{queries.value?.scope ?? 'loading…'}</p>
        <button class={s.loadBtn} onClick={() => { queriesTick.value++ }}>Refresh</button>
        <label class={s.threshold}>
          threshold ≥ <input
            type="number" min="0" step="50"
            aria-label="minimum duration in milliseconds"
            value={minMs.value}
            onChange={e => { minMs.value = Math.max(0, Number((e.target as HTMLInputElement).value) || 0) }}
          /> ms
        </label>
        {queriesError.value && <div class={s.error} role="alert">{queriesError.value}</div>}
        {queries.value && (
          <>
            <div class={s.statline} role="status">
              {queries.value.stats.count} statements · p50 {fmtMs(queries.value.stats.p50Ms)} ms · p95 {fmtMs(queries.value.stats.p95Ms)} ms · max {fmtMs(queries.value.stats.maxMs)} ms
            </div>
            <table class={s.grid}>
              <thead><tr><th>When</th><th>Surface</th><th>ms</th><th>Rows</th><th>State</th><th>Statement</th><th></th></tr></thead>
              <tbody>
                {queries.value.entries.map((e, i) => (
                  <tr key={i} data-state={e.state}>
                    <td title={e.at}>{new Date(e.at).toLocaleTimeString()}</td>
                    <td>{e.surface}</td>
                    <td class={s.num}>{fmtMs(e.durationMs)}</td>
                    <td class={s.num}>{e.rowCount ?? ''}</td>
                    <td>{e.state}{e.error && <span class={s.errDetail} title={e.error}> ⚠</span>}</td>
                    <td><code class={s.sql}>{e.sql}</code></td>
                    <td>
                      <button class={s.openBtn} title="Open this statement in the SQL editor"
                        onClick={() => openTab({
                          id: crypto.randomUUID(), kind: 'sql-editor', label: 'SQL', initialSql: e.sql,
                        })}>open</button>
                    </td>
                  </tr>
                ))}
                {queries.value.entries.length === 0 && (
                  <tr><td colspan={7} class={s.none}>No statements at or above {minMs.value} ms in this Studio process's log.</td></tr>
                )}
              </tbody>
            </table>

            {queries.value.pgStatStatements && (
              <div class={s.pss}>
                {queries.value.pgStatStatements.available ? (
                  <>
                    <h4>pg_stat_statements — server-wide top statements</h4>
                    <p class={s.meta}>{queries.value.pgStatStatements.note}</p>
                    <table class={s.grid}>
                      <thead><tr><th>ms total</th><th>ms mean</th><th>calls</th><th>Query</th></tr></thead>
                      <tbody>
                        {queries.value.pgStatStatements.statements.map((st, i) => (
                          <tr key={i}>
                            <td class={s.num}>{fmtMs(st.totalMs)}</td>
                            <td class={s.num}>{fmtMs(st.meanMs)}</td>
                            <td class={s.num}>{st.calls}</td>
                            <td><code class={s.sql}>{st.query}</code></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <>
                    <h4>pg_stat_statements unavailable</h4>
                    <p class={s.meta}>{queries.value.pgStatStatements.reason}</p>
                    <p class={s.meta}>The Studio duration log above covers this process's own statements without it.</p>
                  </>
                )}
              </div>
            )}
          </>
        )}
      </section>

      <section class={s.section} aria-label="Table statistics">
        <h3>Table statistics and index usage</h3>
        <form class={s.picker} onSubmit={e => {
          e.preventDefault()
          statsSchema.value = inputSchema.value.trim()
          statsTable.value = inputTable.value.trim()
        }}>
          <input
            class={s.input} placeholder="schema" aria-label="schema"
            value={inputSchema.value}
            onInput={e => { inputSchema.value = (e.target as HTMLInputElement).value }}
          />
          <span>.</span>
          <input
            class={s.input} placeholder="table" aria-label="table"
            value={inputTable.value}
            onInput={e => { inputTable.value = (e.target as HTMLInputElement).value }}
          />
          <button class={s.loadBtn} type="submit"
            disabled={!inputSchema.value.trim() || !inputTable.value.trim()}>Load</button>
        </form>
        {statsError.value && <div class={s.error} role="alert">{statsError.value}</div>}
        {stats.value && (
          <>
            <div class={s.statline}>
              seq scans {stats.value.stats.seqScan.toLocaleString()} · index scans {stats.value.stats.idxScan.toLocaleString()} ·
              live rows {stats.value.stats.nLiveTup.toLocaleString()} · dead rows {stats.value.stats.nDeadTup.toLocaleString()}
              {stats.value.totalSizeBytes !== undefined && ` · size ${fmtBytes(stats.value.totalSizeBytes)}`}
            </div>
            <div class={s.statline}>
              last analyze {stats.value.stats.lastAnalyze ?? 'never'} · last autoanalyze {stats.value.stats.lastAutoAnalyze ?? 'never'}
            </div>
            <table class={s.grid}>
              <thead><tr><th>Index</th><th>Unique</th><th>Scans</th><th>Tuples read</th><th>Size</th><th>Definition</th><th>Observation</th></tr></thead>
              <tbody>
                {stats.value.indexes.map(ix => (
                  <tr key={ix.name} data-unused={ix.idxScan === 0 && ix.idxTupRead === 0}>
                    <td>{ix.name}</td>
                    <td>{ix.unique ? 'yes' : 'no'}</td>
                    <td class={s.num}>{ix.idxScan.toLocaleString()}</td>
                    <td class={s.num}>{ix.idxTupRead.toLocaleString()}</td>
                    <td class={s.num}>{fmtBytes(ix.sizeBytes)}</td>
                    <td><code class={s.sql}>{ix.definition}</code></td>
                    <td>{ix.idxScan === 0 ? 'no scans since the statistics reset' : ''}</td>
                  </tr>
                ))}
                {stats.value.indexes.length === 0 && (
                  <tr><td colspan={7} class={s.none}>No indexes on this table.</td></tr>
                )}
              </tbody>
            </table>
            <ul class={s.notes}>
              {stats.value.notes.map((n, i) => <li key={i}>{n}</li>)}
            </ul>
            {stats.value.sizeUnavailableReason && <p class={s.meta}>{stats.value.sizeUnavailableReason}</p>}
            <button class={s.action} onClick={() => openTab({
              id: crypto.randomUUID(), kind: 'sql-editor', label: 'SQL',
              initialSql: `SELECT * FROM ${q(stats.value!.schema)}.${q(stats.value!.table)} LIMIT 100;`,
            })}>Open a query on this table (use Explain in the editor)</button>
          </>
        )}
      </section>
    </div>
  )
}

function q(name: string): string {
  return `"${name.replaceAll('"', '""')}"`
}
