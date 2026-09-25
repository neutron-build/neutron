import { useSignal } from '@preact/signals'
import type { SchemaPlanResponse } from '../../lib/types'
import s from './ObjectInspector.module.css'

/** Text of the reviewable SQL download: the plan's up statements in one
 * transaction, with its identity, notes and down statements as comments. */
export function planSqlText(plan: SchemaPlanResponse): string {
  const lines = [
    `-- Neutron Studio migration plan ${plan.planId.slice(0, 12)}`,
    `-- Planned by the CLI planner from schema document ${plan.baseSha256.slice(0, 12)} to ${plan.targetSha256.slice(0, 12)}.`,
    `-- Reproduce: ${plan.cliEquivalent}`,
    ...plan.designerNotes.map(n => `-- NOTE: ${n}`),
    ...plan.warnings.map(w => `-- WARNING: ${w}`),
    '',
    'BEGIN;',
    ...plan.up.map(stmt => `${stmt};`),
    'COMMIT;',
    '',
    '-- Down statements (structure only; down SQL never restores data):',
    ...plan.down.map(stmt => stmt.split('\n').map(l => `-- ${l}`).join('\n') + ';'),
    '',
  ]
  return lines.join('\n')
}

function download(name: string, content: string, type: string) {
  const blob = new Blob([content], { type })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

/** Reviewable migration plan (S05): the CLI planner's own statements for
 * the designer's visual changes, each with the M03 risk classification.
 * Nothing executes from this view until Apply; Apply sends the plan id, so
 * the server runs exactly these statements or refuses as stale. A plan that
 * drops objects or can lose data needs an explicit acknowledgement. */
export function PlanReview({
  plan,
  applying,
  error,
  onApply,
  onCancel,
}: {
  plan: SchemaPlanResponse
  applying: boolean
  error?: string | null
  onApply: (acknowledged: boolean) => void
  onCancel: () => void
}) {
  const showDown = useSignal(false)
  const ack = useSignal(false)
  const risky = plan.risk.hasDestructive || plan.risk.hasDataLoss
  const count = plan.up.length
  return (
    <div class={s.planReview} role="region" aria-label="Migration plan review">
      <div class={s.planReviewHeader}>
        <h3>Migration plan — {count} statement{count === 1 ? '' : 's'}</h3>
        {plan.risk.hasDestructive && <span class={s.planDestructive}>destructive</span>}
        {plan.risk.hasDataLoss && <span class={s.planDestructive}>data loss</span>}
        {count > 0 && <span class={s.rowTag}>{plan.risk.overallReversibility}</span>}
      </div>
      <p class={s.planSource}>
        Planned by the CLI's planner against the live catalog
        (schema document <code title={plan.baseSha256}>{plan.baseSha256.slice(0, 12)}</code>).
        The same plan from the command line, with the target document saved as
        target.schema.json: <code>{plan.cliEquivalent}</code>
      </p>
      {plan.designerNotes.length > 0 && (
        <ul class={s.planWarnings} aria-label="Dependent objects">
          {plan.designerNotes.map((n, i) => <li key={i}>{n}</li>)}
        </ul>
      )}
      {plan.warnings.length > 0 && (
        <ul class={s.planWarnings} aria-label="Planner warnings">
          {plan.warnings.map((w, i) => <li key={i}>{w}</li>)}
        </ul>
      )}
      {count === 0 ? (
        <p class={s.planSource} role="status">No statements: the live catalog already matches these changes.</p>
      ) : (
        <ol class={s.planUp} aria-label="Up statements">
          {plan.operations.map(op => (
            <li key={op.index} data-destructive={op.destructive} data-data-loss={op.dataLoss}>
              <code>{op.sql}</code>
              {(op.destructive || op.dataLoss) && (
                <span class={s.planOpRisk}>
                  {op.destructive && 'destructive'}{op.destructive && op.dataLoss && ' · '}{op.dataLoss && 'data loss'}
                  {' · '}{op.reversibility}
                </span>
              )}
            </li>
          ))}
        </ol>
      )}
      {plan.down.length > 0 && (
        <>
          <button class={s.planDownToggle} aria-expanded={showDown.value} onClick={() => { showDown.value = !showDown.value }}>
            {showDown.value ? 'Hide' : 'Show'} reverse (down) statements
          </button>
          {showDown.value && (
            <ol class={s.planUp} aria-label="Down statements">
              {plan.down.map((stmt, i) => <li key={i}><code>{stmt}</code></li>)}
            </ol>
          )}
        </>
      )}
      {error && <div class={s.error} role="alert">{error}</div>}
      {risky && count > 0 && (
        <label class={s.planAck}>
          <input
            type="checkbox"
            checked={ack.value}
            onChange={e => { ack.value = (e.target as HTMLInputElement).checked }}
          />
          I understand this plan {plan.risk.hasDataLoss ? 'can lose data' : 'drops database objects'} and cannot be undone by its down statements alone.
        </label>
      )}
      <div class={s.planActions}>
        <button class={s.planCancel} onClick={onCancel} disabled={applying}>Cancel</button>
        <button
          class={s.planCancel}
          onClick={() => download('target.schema.json', JSON.stringify(plan.target, null, 2) + '\n', 'application/json')}
        >Download target document</button>
        {count > 0 && (
          <button
            class={s.planCancel}
            onClick={() => download(`plan-${plan.planId.slice(0, 12)}.sql`, planSqlText(plan), 'text/plain')}
          >Download SQL</button>
        )}
        {count > 0 && (
          <button
            class={risky ? s.planApplyDestructive : s.planApply}
            disabled={applying || (risky && !ack.value)}
            onClick={() => onApply(risky && ack.value)}
          >
            {applying ? 'Applying…' : 'Apply plan'}
          </button>
        )}
      </div>
    </div>
  )
}
