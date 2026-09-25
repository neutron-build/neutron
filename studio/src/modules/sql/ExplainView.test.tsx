import { describe, it, expect, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/preact'
import type { ExplainPlan } from '../../lib/types'
import { ExplainView } from './ExplainView'

// Rendered ExplainView: the execution semantics must be stated explicitly
// for every outcome, and refusals surface as alerts.

afterEach(cleanup)

function planOutcome(p: Partial<ExplainPlan>): ExplainPlan {
  return {
    ok: true, requestId: 'r1', engine: 'postgresql', format: 'json',
    plan: [{ Plan: { 'Node Type': 'ModifyTable', Operation: 'Delete', 'Relation Name': 'memo', 'Startup Cost': 0, 'Total Cost': 1.04, 'Plan Rows': 0,
      Plans: [{ 'Node Type': 'Seq Scan', 'Relation Name': 'memo', 'Startup Cost': 0, 'Total Cost': 1.04, 'Plan Rows': 4 }] } }],
    analyze: false, executed: false, writesAllowed: false, readOnly: true, committed: false, duration: 1,
    ...p,
  }
}

describe('ExplainView', () => {
  it('states that a plain EXPLAIN did not execute the statement, and renders the plan tree', () => {
    render(<ExplainView outcome={planOutcome({})} />)
    expect(screen.getByText('Estimated plan. The statement was not executed.')).toBeTruthy()
    // S05: the default view is the TREE; PG's node order is preserved.
    const tree = screen.getByRole('tree', { name: 'Query plan tree' })
    const nodes = Array.from(tree.querySelectorAll('.planTreeNode > .planNode, li > div > .planNode')).map(el => el.textContent)
    expect(nodes[0]).toBe('ModifyTable')
    expect(nodes).toContain('Seq Scan')
    // Estimated plans have no actual-stat columns in the table view either.
    fireEvent.click(screen.getByRole('tab', { name: 'Table' }))
    const table = screen.getByRole('table', { name: 'Query plan' })
    const cells = Array.from(table.querySelectorAll('tbody tr')).map(tr => tr.querySelector('td')!.textContent)
    expect(cells).toEqual(['ModifyTableDelete', 'Seq Scan'])
    expect(screen.queryByText('Actual rows')).toBeNull()
  })

  it('states that ANALYZE executed the statement read-only and shows actual stats', () => {
    const outcome = planOutcome({
      analyze: true, executed: true,
      plan: [{ Plan: { 'Node Type': 'Seq Scan', 'Relation Name': 'memo', 'Startup Cost': 0, 'Total Cost': 1, 'Plan Rows': 3, 'Actual Rows': 3, 'Actual Total Time': 0.01, 'Actual Loops': 1 }, 'Planning Time': 0.1, 'Execution Time': 0.2 }],
    })
    render(<ExplainView outcome={outcome} />)
    expect(screen.getByText(/executed the statement in a read-only transaction, then rolled back/)).toBeTruthy()
    // Tree carries the actual metrics on the node.
    const tree = screen.getByRole('tree', { name: 'Query plan tree' })
    expect(tree.textContent).toContain('actual rows')
    expect(tree.textContent).toContain('3')
    fireEvent.click(screen.getByRole('tab', { name: 'Table' }))
    expect(screen.getByText('Actual rows')).toBeTruthy()
    expect(screen.getByText(/Execution 0.200 ms/)).toBeTruthy()
  })

  it('renders buffers and uncurated node fields honestly in the tree', () => {
    const outcome = planOutcome({
      analyze: true, executed: true,
      plan: [{ Plan: {
        'Node Type': 'Seq Scan', 'Relation Name': 'orders', 'Startup Cost': 0, 'Total Cost': 10, 'Plan Rows': 2,
        'Actual Rows': 2, 'Actual Total Time': 0.05, 'Actual Loops': 1,
        'Shared Hit Blocks': 3, 'Shared Read Blocks': 7,
        'Sort Method' : 'quicksort', 'Sort Space Used': 25,
      } }],
    })
    render(<ExplainView outcome={outcome} />)
    const tree = screen.getByRole('tree', { name: 'Query plan tree' })
    expect(tree.textContent).toContain('buffers')
    expect(tree.textContent).toContain('shared: hit=3 read=7')
    // Fields outside the curated set surface verbatim behind the extras
    // toggle — the tree never drops engine evidence.
    const extrasBtn = tree.querySelector('button[class*="planTreeExtras"]') as HTMLElement
    expect(extrasBtn).toBeTruthy()
    fireEvent.click(extrasBtn)
    expect(tree.textContent).toContain('Sort Method')
    expect(tree.textContent).toContain('quicksort')
  })

  it('states that allowed writes executed and were rolled back', () => {
    render(<ExplainView outcome={planOutcome({ analyze: true, executed: true, writesAllowed: true, readOnly: false })} />)
    expect(screen.getByText(/with writes allowed, then rolled back. Nothing was committed/)).toBeTruthy()
  })

  it('renders refusals as alerts with the reason and SQLSTATE', () => {
    render(<ExplainView outcome={{ ok: false, state: 'write-blocked', error: 'EXPLAIN ANALYZE executes the statement, and this statement writes.', sqlState: '25006', executed: false }} />)
    const alert = screen.getByRole('alert')
    expect(alert.textContent).toContain('refused: the statement writes')
    expect(alert.textContent).toContain('SQLSTATE 25006')
    expect(alert.textContent).toContain('Nothing was executed.')
  })

  it('shows an unrecognized plan document raw instead of inventing a tree', () => {
    render(<ExplainView outcome={planOutcome({ plan: { something: 'else' } })} />)
    expect(screen.queryByRole('table')).toBeNull()
    expect(screen.getByText(/does not recognize/)).toBeTruthy()
    expect(screen.getByText(/"something": "else"/)).toBeTruthy()
  })
})
