import type { ExplainOutcome } from '../../lib/types'
import { flattenPlan } from './sqlTools'
import s from './SQLEditor.module.css'

// Renders an EXPLAIN outcome from /api/query/explain. The header states the
// execution semantics explicitly: a plain EXPLAIN never ran the statement;
// ANALYZE did (read-only, or with writes) and nothing was committed.

const REFUSAL_TITLES: Record<string, string> = {
  unsupported: 'No plan available',
  'write-blocked': 'EXPLAIN ANALYZE refused: the statement writes',
  canceled: 'EXPLAIN canceled',
  'sql-error': 'EXPLAIN failed',
}

function fmt(n: number | undefined, digits = 2): string {
  if (n === undefined) return ''
  return Number.isInteger(n) ? String(n) : n.toFixed(digits)
}

export function explainSemantics(outcome: ExplainOutcome & { ok: true }): string {
  if (!outcome.analyze) return 'Estimated plan. The statement was not executed.'
  if (outcome.writesAllowed) {
    return 'EXPLAIN ANALYZE executed the statement with writes allowed, then rolled back. Nothing was committed; sequence increments and effects outside the database are not undone.'
  }
  return 'EXPLAIN ANALYZE executed the statement in a read-only transaction, then rolled back.'
}

export function ExplainView({ outcome }: { outcome: ExplainOutcome }) {
  if (!outcome.ok) {
    return (
      <div class={s.explainRefusal} role="alert" data-state={outcome.state}>
        <div class={s.explainTitle}>{REFUSAL_TITLES[outcome.state] ?? 'EXPLAIN failed'}</div>
        <div class={s.explainMessage}>{outcome.error}</div>
        {outcome.sqlState && <div class={s.explainMeta}>SQLSTATE {outcome.sqlState}</div>}
        {outcome.executed === false && <div class={s.explainMeta}>Nothing was executed.</div>}
      </div>
    )
  }

  const summary = flattenPlan(outcome.plan)
  const analyzed = outcome.analyze && summary?.analyzed
  return (
    <div class={s.explain}>
      <div
        class={`${s.explainBanner} ${outcome.analyze ? s.explainBannerExecuted : ''}`}
        data-executed={outcome.executed ? 'true' : 'false'}
      >
        {explainSemantics(outcome)}
      </div>
      {summary ? (
        <table class={s.planTable} aria-label="Query plan">
          <thead>
            <tr>
              <th scope="col">Node</th>
              <th scope="col">Target</th>
              <th scope="col">Cost</th>
              <th scope="col">Est. rows</th>
              {analyzed && <th scope="col">Actual rows</th>}
              {analyzed && <th scope="col">Time (ms)</th>}
              {analyzed && <th scope="col">Loops</th>}
            </tr>
          </thead>
          <tbody>
            {summary.rows.map((r, i) => (
              <tr key={i}>
                <td style={{ paddingLeft: `${8 + r.depth * 16}px` }}>
                  <span class={s.planNode}>{r.nodeType}</span>
                  {r.detail && <div class={s.planDetail}>{r.detail}</div>}
                </td>
                <td>{r.target ?? ''}</td>
                <td>{r.totalCost === undefined ? '' : `${fmt(r.startupCost)}..${fmt(r.totalCost)}`}</td>
                <td>{fmt(r.planRows)}</td>
                {analyzed && <td>{fmt(r.actualRows)}</td>}
                {analyzed && <td>{fmt(r.actualTotalTime, 3)}</td>}
                {analyzed && <td>{fmt(r.actualLoops)}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div class={s.explainMessage}>The server returned a plan document Studio does not recognize; it is shown below unmodified.</div>
      )}
      {summary && (summary.planningTime !== undefined || summary.executionTime !== undefined) && (
        <div class={s.explainMeta}>
          {summary.planningTime !== undefined && `Planning ${fmt(summary.planningTime, 3)} ms`}
          {summary.planningTime !== undefined && summary.executionTime !== undefined && ' · '}
          {summary.executionTime !== undefined && `Execution ${fmt(summary.executionTime, 3)} ms`}
        </div>
      )}
      <details class={s.planRaw} open={!summary}>
        <summary>Plan JSON</summary>
        <pre>{JSON.stringify(outcome.plan, null, 2)}</pre>
      </details>
    </div>
  )
}
