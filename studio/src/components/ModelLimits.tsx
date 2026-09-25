import { useSignal } from '@preact/signals'
import { limitsFor, limitsReport, limitsError } from '../lib/store'
import type { ModelLimits as Limits } from '../lib/types'
import s from './ModelLimits.module.css'

// X06: a model's actual limits, shown where the user acts. The wording is
// derived only from the server's limits report (itself checked against the
// capability report and conformance legs); nothing here upgrades a status.

type Tone = 'ok' | 'warn' | 'bad' | 'unknown'

export function availabilityText(l: Limits): [string, Tone] {
  switch (l.availability) {
    case 'supported': return ['available', 'ok']
    case 'unsupported': return ['unavailable on this engine', 'bad']
    default: return ['availability unknown', 'unknown']
  }
}

export function transactionText(l: Limits): [string, Tone] {
  switch (l.transaction) {
    case 'atomic': return ['transactions: atomic', 'ok']
    case 'partial': return ['transactions: partial', 'warn']
    case 'rollback-not-isolated': return ['rollback undoes writes, not isolated', 'warn']
    case 'refused-in-transaction': return ['writes refused inside transactions', 'warn']
    case 'not-transactional': return ['not transactional', 'bad']
    case 'not-applicable': return ['no transactional state', 'unknown']
    default: return ['transactions: unknown', 'unknown']
  }
}

export function durabilityText(l: Limits): [string, Tone] {
  switch (l.durability) {
    case 'survives-restart': return ['survived engine kill + restart (measured)', 'ok']
    case 'engine-documented': return ['durable per engine documentation', 'ok']
    case 'not-durable': return ['not durable', 'bad']
    case 'not-applicable': return ['no stored state', 'unknown']
    default: return ['durability: unknown', 'unknown']
  }
}

export function atomicWithSqlText(l: Limits): [string, Tone] | null {
  if (l.model === 'sql') return null
  switch (l.atomicWithSql) {
    case 'supported': return ['atomic with SQL rows', 'ok']
    case 'unsupported': return ['not atomic with SQL rows', 'warn']
    default: return ['atomicity with SQL unknown', 'unknown']
  }
}

interface Props {
  model: string
  /** Compact: chips + warnings only (journey stages). */
  compact?: boolean
}

export function ModelLimits({ model, compact }: Props) {
  const open = useSignal(false)
  const l = limitsFor(model)
  const report = limitsReport.value

  if (!l) {
    return (
      <div class={s.strip} role="note" aria-label={`${model} limits`} data-state="missing">
        <span class={s.chip} data-tone="unknown">limits not loaded</span>
        <span class={s.note}>
          {limitsError.value
            ? `Could not load this model's limits (${limitsError.value}); no transaction or durability guarantee is assumed.`
            : 'No transaction or durability guarantee is assumed until the limits load.'}
        </span>
      </div>
    )
  }

  const chips: Array<[string, Tone]> = [availabilityText(l), transactionText(l), durabilityText(l)]
  const atomic = atomicWithSqlText(l)
  if (atomic) chips.push(atomic)

  return (
    <div class={s.strip} role="note" aria-label={`${l.label} limits`} data-compact={compact ? 'true' : 'false'}>
      <div class={s.row}>
        <span class={s.label}>{l.label} on {report?.engine.product === 'nucleus' ? `Nucleus ${report.engine.version}` : report?.engine.product === 'postgres' ? `PostgreSQL ${report.engine.version}` : 'this engine'}</span>
        {chips.map(([text, tone]) => (
          <span key={text} class={s.chip} data-tone={tone}>{text}</span>
        ))}
        {!compact && (
          <button class={s.toggle} aria-expanded={open.value} onClick={() => { open.value = !open.value }}>
            {open.value ? 'Hide evidence' : 'Evidence'}
          </button>
        )}
      </div>
      {report && !report.current && report.currentNote && (
        <div class={s.warning} role="alert">{report.currentNote}</div>
      )}
      {l.warnings.map(w => (
        <div key={w} class={s.warning}>{w}</div>
      ))}
      {!compact && (
        <div class={s.notes}>
          <span class={s.note}>{l.transactionNote}</span>
          <span class={s.note}>{l.durabilityNote}</span>
          {l.availability !== 'supported' && <span class={s.note}>{l.availabilityReason}</span>}
        </div>
      )}
      {!compact && open.value && (
        <ul class={s.evidence} aria-label="Evidence">
          {l.evidence.length === 0 && <li>No evidence recorded: nothing is claimed beyond the statuses above.</li>}
          {l.evidence.map(e => (
            <li key={`${e.source}:${e.ref}`}><code>{e.ref}</code> — {e.observed} ({e.source}); supports {e.supports.join(', ')}</li>
          ))}
          {report && report.engine.product === 'nucleus' && (
            <li>Measured on Nucleus {report.measured.nucleusVersion}, nucleus/ tree {report.measured.nucleusTree.slice(0, 12)}, recorded {report.measured.recorded}.{report.current && report.currentNote ? ` ${report.currentNote}.` : ''}</li>
          )}
        </ul>
      )}
    </div>
  )
}

/** The Studio surface each tab kind acts on. */
export function modelForTabKind(kind: string): string | null {
  switch (kind) {
    case 'sql-browser':
    case 'sql-editor':
    case 'schema-designer':
      return 'sql'
    case 'kv': case 'vector': case 'timeseries': case 'document': case 'graph':
    case 'fts': case 'geo': case 'blob': case 'pubsub': case 'streams':
    case 'columnar': case 'datalog': case 'cdc':
      return kind
    default:
      return null
  }
}
