import { useEffect } from 'preact/hooks'
import { useSignal } from '@preact/signals'
import { activeConnection, openTab, limitsReport } from '../../lib/store'
import { api } from '../../lib/api'
import { ModelLimits } from '../../components/ModelLimits'
import type {
  JourneyResponse, JourneyStage, JourneySchemaData, JourneyMigrationsData, JourneyQueriesData,
  JourneyPlanData, JourneyRowsData, JourneyModelsData, JourneyChangeEventsData,
} from '../../lib/types'
import s from './JourneyModule.module.css'

// X06: one table followed across the stack —
//   schema -> migrations -> queries -> SQL/plan -> rows -> models -> change events.
// Every stage says whether this engine supports it and shows the limits of
// the model it belongs to. Nothing here mutates: each action navigates to
// the module that acts (and shows its own limits there).

const STAGE_TITLES: Record<string, string> = {
  'schema': 'Schema',
  'migrations': 'Migrations',
  'queries': 'Queries',
  'plan': 'SQL and plan',
  'rows': 'Rows',
  'models': 'Models bound to rows',
  'change-events': 'Change events',
}

export function JourneyModule({ schema, table }: { schema: string; table: string }) {
  const conn = activeConnection.value
  const journey = useSignal<JourneyResponse | null>(null)
  const error = useSignal<string | null>(null)
  const loading = useSignal(false)

  async function load() {
    if (!conn) return
    loading.value = true
    error.value = null
    try {
      const j = await api.journey(conn.id, schema, table)
      journey.value = j
      // The journey carries the limits it was built with; keep the shared
      // report current for every other surface.
      limitsReport.value = j.limits
    } catch (e) {
      error.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
  }

  useEffect(() => { void load() }, [conn?.id, schema, table])

  if (!conn) return <div class={s.hint}>Connect to a database first</div>
  const j = journey.value
  const qualified = `${schema}.${table}`

  return (
    <div class={s.wrap}>
      <header class={s.header}>
        <h2 class={s.title}>Journey: {qualified}</h2>
        {j && <span class={s.engine}>{j.engine.product === 'nucleus' ? `Nucleus ${j.engine.version}` : j.engine.product === 'postgres' ? `PostgreSQL ${j.engine.version}` : 'unrecognized engine'}</span>}
        <button class={s.action} onClick={() => void load()} disabled={loading.value}>{loading.value ? 'Loading…' : 'Refresh'}</button>
      </header>
      {error.value && <div class={s.error} role="alert">{error.value}</div>}
      {j && !j.limits.current && j.limits.currentNote && (
        <div class={s.error} role="alert">{j.limits.currentNote}</div>
      )}
      {j && (
        <ol class={s.stages}>
          {j.stages.map((st, i) => (
            <StageView key={st.stage} index={i + 1} stage={st} journey={j} />
          ))}
        </ol>
      )}
    </div>
  )
}

function StageView({ index, stage, journey }: { index: number; stage: JourneyStage; journey: JourneyResponse }) {
  return (
    <li class={s.stage} data-stage={stage.stage} data-status={stage.status} aria-label={STAGE_TITLES[stage.stage]}>
      <div class={s.stageHead}>
        <span class={s.stageIndex}>{index}</span>
        <h3 class={s.stageTitle}>{STAGE_TITLES[stage.stage] ?? stage.stage}</h3>
        <span class={s.status} data-status={stage.status}>{stage.status}</span>
      </div>
      <ModelLimits model={stage.model} compact />
      {stage.status === 'unavailable'
        ? <p class={s.reason}>{stage.reason}</p>
        : <StageBody stage={stage} journey={journey} />}
    </li>
  )
}

function open(kind: Parameters<typeof openTab>[0]['kind'], label: string, extra: Partial<Parameters<typeof openTab>[0]> = {}) {
  openTab({ id: crypto.randomUUID(), kind, label, ...extra })
}

function StageBody({ stage, journey }: { stage: JourneyStage; journey: JourneyResponse }) {
  const { schema, table } = journey
  switch (stage.stage) {
    case 'schema': {
      const d = stage.data as JourneySchemaData
      return (
        <div class={s.body}>
          <div class={s.actions}>
            <button class={s.action} onClick={() => open('schema-inspector', table, { objectSchema: schema, objectName: table })}>Inspect structure</button>
            <button class={s.action} onClick={() => open('schema-designer', table, { objectSchema: schema, objectName: table })}>Design</button>
          </div>
          <p class={s.meta}>{d.columns.length} columns · key ({d.keyColumns.join(', ') || 'none'}){d.documentSHA256 && <> · doc <code>{d.documentSHA256.slice(0, 12)}</code></>}</p>
          <ul class={s.list}>
            {d.columns.map(c => (
              <li key={c.name}><code>{c.name}</code> {c.type}{c.primaryKey ? ' (primary key)' : c.notNull ? ' not null' : ''}</li>
            ))}
          </ul>
          {[...d.references.map(r => ({ r, dir: 'references' })), ...d.referencedBy.map(r => ({ r, dir: 'referenced by' }))].map(({ r, dir }) => (
            <div key={`${dir}:${r.schema}.${r.name}.${r.constraint}`} class={s.linkRow}>
              <span>{dir} <code>{r.schema}.{r.name}</code> ({r.columns.join(', ')} → {r.refColumns.join(', ')})</span>
              <button class={s.link} onClick={() => open('journey', `Journey: ${r.name}`, { objectSchema: r.schema, objectName: r.name })}>Journey →</button>
            </div>
          ))}
        </div>
      )
    }
    case 'migrations': {
      const d = stage.data as JourneyMigrationsData
      const mentioning = d.entries.filter(e => e.mentions)
      return (
        <div class={s.body}>
          <p class={s.meta}>history: {d.history}{d.directory && <> · files: <code>{d.directory}</code></>} · {mentioning.length} of {d.entries.length} name this table (text match, not a parse)</p>
          {d.filesNote && <p class={s.meta}>{d.filesNote}</p>}
          <ul class={s.list}>
            {(mentioning.length > 0 ? mentioning : d.entries).map(e => (
              <li key={e.version}>
                <code>{e.version}</code> {e.name} — {e.applied ? `applied${e.appliedAt ? ' ' + e.appliedAt : ''}` : 'pending'} · checksum <span class={s.checksum} data-checksum={e.checksum}>{e.checksum}</span>
                {e.lines && e.lines.map(l => <pre key={l} class={s.code}>{l}</pre>)}
              </li>
            ))}
          </ul>
        </div>
      )
    }
    case 'queries': {
      const d = stage.data as JourneyQueriesData | undefined
      return (
        <div class={s.body}>
          {d && <p class={s.meta}>{d.scope}</p>}
          {(!d || d.entries.length === 0) && <p class={s.meta}>No recorded statement names this table yet.</p>}
          <ul class={s.list}>
            {d?.entries.map((q, i) => (
              <li key={`${q.at}-${i}`} class={s.linkRow}>
                <span><code>{q.sql.length > 140 ? q.sql.slice(0, 140) + ' …' : q.sql}</code> · {q.durationMs.toFixed(1)} ms · {q.state}</span>
                <button class={s.link} onClick={() => open('sql-editor', 'SQL', { initialSql: q.sql })}>Open in editor</button>
              </li>
            ))}
          </ul>
          <button class={s.action} onClick={() => open('diagnostics', table, { objectSchema: schema, objectName: table })}>Diagnose {table}</button>
        </div>
      )
    }
    case 'plan': {
      const d = stage.data as JourneyPlanData
      const top = planTop(d.plan)
      return (
        <div class={s.body}>
          <pre class={s.code}>{d.statement}</pre>
          {top && <p class={s.meta}>{top}</p>}
          <p class={s.meta}>{d.note}</p>
          <button class={s.action} onClick={() => open('sql-editor', 'SQL', { initialSql: d.statement })}>Open in SQL editor (EXPLAIN there)</button>
        </div>
      )
    }
    case 'rows': {
      const d = stage.data as JourneyRowsData
      const models = journey.stages.find(x => x.stage === 'models')
      const nodes = models?.status === 'available' ? (models.data as JourneyModelsData).graphNodes : []
      const idIdx = d.idColumn ? d.columns.indexOf(d.idColumn) : -1
      return (
        <div class={s.body}>
          <div class={s.actions}>
            <button class={s.action} onClick={() => open('sql-browser', table, { objectSchema: schema, objectName: table })}>Browse rows</button>
          </div>
          <p class={s.meta}>{d.note}</p>
          <div class={s.gridWrap}>
            <table class={s.grid}>
              <thead><tr>{d.columns.map(c => <th key={c}>{c}</th>)}{nodes.length > 0 && <th>graph node</th>}</tr></thead>
              <tbody>
                {d.rows.map((r, i) => {
                  const rowId = idIdx >= 0 ? String(r[idIdx]) : null
                  const bound = rowId === null ? [] : nodes.filter(n => String(n.rowId) === rowId)
                  return (
                    <tr key={i}>
                      {r.map((v, ci) => <td key={ci}>{v === null ? <span class={s.null}>NULL</span> : typeof v === 'object' ? JSON.stringify(v) : String(v)}</td>)}
                      {nodes.length > 0 && <td>{bound.map(n => n.nodeId).join(', ')}</td>}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )
    }
    case 'models': {
      const d = stage.data as JourneyModelsData
      return (
        <div class={s.body}>
          <p class={s.meta}>{d.graphNodes.length} graph node(s) stamped with a row of {table}{d.truncated ? ' (truncated)' : ''}</p>
          <ul class={s.list}>
            {d.graphNodes.map(n => (
              <li key={n.nodeId}>node <code>{n.nodeId}</code> → row {String(n.rowId)}{n.rowSchema ? ` (${n.rowSchema})` : ''}{n.inSample ? ' · in the row sample' : ''}</li>
            ))}
          </ul>
          <button class={s.action} onClick={() => open('graph', 'Graph', { objectName: 'graph', focus: d.query })}>Open in Graph</button>
          <p class={s.meta}>{d.documents}</p>
        </div>
      )
    }
    case 'change-events': {
      const d = stage.data as JourneyChangeEventsData
      return (
        <div class={s.body}>
          <p class={s.meta}>{d.note} · {d.retained} events retained engine-wide</p>
          <ul class={s.list}>
            {d.events.map(e => (
              <li key={e.seq}><code>#{e.seq}</code> {e.change} {new Date(e.ts).toISOString()}</li>
            ))}
          </ul>
          <button class={s.action} onClick={() => open('cdc', 'CDC', { objectName: 'changes', focus: table })}>Open CDC for {table}</button>
        </div>
      )
    }
    default:
      return null
  }
}

/** One-line summary of an EXPLAIN (FORMAT JSON) document's top node. */
export function planTop(plan: unknown): string | null {
  const root = Array.isArray(plan) ? (plan[0] as { Plan?: Record<string, unknown> } | undefined)?.Plan : undefined
  if (!root) return null
  const parts = [String(root['Node Type'] ?? '?')]
  if (root['Relation Name']) parts.push(`on ${String(root['Relation Name'])}`)
  if (root['Total Cost'] !== undefined) parts.push(`cost ${String(root['Total Cost'])}`)
  if (root['Plan Rows'] !== undefined) parts.push(`~${String(root['Plan Rows'])} rows`)
  return parts.join(' · ')
}
