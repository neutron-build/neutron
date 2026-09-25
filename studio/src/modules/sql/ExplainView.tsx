import { useSignal } from '@preact/signals'
import type { ExplainOutcome } from '../../lib/types'
import { flattenPlan, planTree, type PlanTreeNode } from './sqlTools'
import s from './SQLEditor.module.css'

// Renders an EXPLAIN outcome from /api/query/explain. The header states the
// execution semantics explicitly: a plain EXPLAIN never ran the statement;
// ANALYZE did (read-only, or with writes) and nothing was committed.
//
// S05: the plan renders as a TREE (PG's own nesting), honest to the engine's
// output — every field PostgreSQL emits on a node is shown, either as a
// curated metric or verbatim under the node's extras. The flat table and
// the raw JSON document remain available.

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
  const view = useSignal<'tree' | 'table'>('tree')
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
  const tree = planTree(outcome.plan)
  const analyzed = outcome.analyze && summary?.analyzed
  return (
    <div class={s.explain}>
      <div
        class={`${s.explainBanner} ${outcome.analyze ? s.explainBannerExecuted : ''}`}
        data-executed={outcome.executed ? 'true' : 'false'}
      >
        {explainSemantics(outcome)}
      </div>
      {(tree || summary) && (
        <div class={s.planViewToggle} role="tablist" aria-label="Plan view">
          {(['tree', 'table'] as const).map(v => (
            <button
              key={v}
              role="tab"
              aria-selected={view.value === v}
              class={`${s.planViewTab} ${view.value === v ? s.planViewTabActive : ''}`}
              onClick={() => { view.value = v }}
            >{v === 'tree' ? 'Tree' : 'Table'}</button>
          ))}
        </div>
      )}
      {view.value === 'tree' && tree && <PlanTreeView root={tree} analyzed={Boolean(analyzed)} />}
      {view.value === 'table' && summary && (
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
      )}
      {(view.value === 'tree' && !tree || view.value === 'table' && !summary) && (
        <div class={s.explainMessage}>The server returned a plan document Studio does not recognize; it is shown below unmodified.</div>
      )}
      {summary && (summary.planningTime !== undefined || summary.executionTime !== undefined) && (
        <div class={s.explainMeta}>
          {summary.planningTime !== undefined && `Planning ${fmt(summary.planningTime, 3)} ms`}
          {summary.planningTime !== undefined && summary.executionTime !== undefined && ' · '}
          {summary.executionTime !== undefined && `Execution ${fmt(summary.executionTime, 3)} ms`}
        </div>
      )}
      <details class={s.planRaw} open={!tree && !summary}>
        <summary>Plan JSON</summary>
        <pre>{JSON.stringify(outcome.plan, null, 2)}</pre>
      </details>
    </div>
  )
}

function PlanTreeView({ root, analyzed }: { root: PlanTreeNode; analyzed: boolean }) {
  return (
    <ul class={s.planTree} role="tree" aria-label="Query plan tree">
      <PlanTreeNodeView node={root} analyzed={analyzed} />
    </ul>
  )
}

function PlanTreeNodeView({ node, analyzed }: { node: PlanTreeNode; analyzed: boolean }) {
  const open = useSignal(false)
  return (
    <li role="treeitem" aria-expanded={node.extras.length > 0 ? open.value : undefined} class={s.planTreeItem}>
      <div class={s.planTreeNode}>
        <span class={s.planNode}>{node.nodeType}</span>
        {node.target && <span class={s.planTreeTarget}>on {node.target}</span>}
        {node.metrics.length > 0 && (
          <span class={s.planTreeMetrics}>
            {node.metrics.map(([k, v]) => (
              <span key={k} class={s.planTreeMetric} data-kind={k}>
                <span class={s.planTreeMetricK}>{k}</span> {v}
              </span>
            ))}
          </span>
        )}
        {node.detail && <div class={s.planDetail}>{node.detail}</div>}
        {node.extras.length > 0 && (
          <button
            class={s.planTreeExtras}
            aria-expanded={open.value}
            onClick={() => { open.value = !open.value }}
          >
            {open.value ? 'hide' : `+ ${node.extras.length} more field${node.extras.length === 1 ? '' : 's'}`}
          </button>
        )}
        {open.value && (
          <dl class={s.planTreeExtrasList}>
            {node.extras.map(([k, v]) => (
              <div key={k}><dt>{k}</dt><dd><code>{v}</code></dd></div>
            ))}
          </dl>
        )}
      </div>
      {node.children.length > 0 && (
        <ul class={s.planTreeChildren} role="group">
          {node.children.map((child, i) => (
            <PlanTreeNodeView key={i} node={child} analyzed={analyzed} />
          ))}
        </ul>
      )}
    </li>
  )
}
