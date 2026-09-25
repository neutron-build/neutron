import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent, waitFor } from '@testing-library/preact'

// X06 journey rendering: stage order, per-stage limits, honest
// unavailability, and navigation into the modules that act. Only the
// fetch boundary is mocked; the Go E2E leg drives the real endpoint.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return { ...orig, api: { ...orig.api, journey: vi.fn() } }
})

import { api } from '../../lib/api'
import { activeConnection, tabs, activeTabId, limitsReport } from '../../lib/store'
import limitsFixture from '../../lib/limits.fixture.json'
import type { JourneyResponse, LimitsReport } from '../../lib/types'
import { JourneyModule, planTop } from './JourneyModule'
import { parseDeepLink, serializeDeepLink } from '../../lib/router'

const journeyMock = vi.mocked(api.journey)

function nucleusJourney(): JourneyResponse {
  const limits = limitsFixture.nucleus as LimitsReport
  return {
    engine: limits.engine, schema: 'public', table: 'orders', limits,
    stages: [
      { stage: 'schema', model: 'sql', status: 'unavailable', reason: 'structure comes from the schema contract v2 introspection, verified on PostgreSQL only' },
      { stage: 'migrations', model: 'sql', status: 'unavailable', reason: 'the CLI migration workflow targets PostgreSQL' },
      { stage: 'queries', model: 'sql', status: 'available', data: { scope: 'statements this Studio process executed', entries: [{ at: '2026-09-25T00:00:00Z', surface: 'editor', sql: 'SELECT * FROM orders', durationMs: 1.5, state: 'ok' }] } },
      { stage: 'plan', model: 'sql', status: 'unavailable', reason: 'EXPLAIN output and its read-only guarantee are not verified on this engine' },
      { stage: 'rows', model: 'sql', status: 'available', data: { statement: 'SELECT * FROM "public"."orders" LIMIT 5', columns: ['id', 'total'], rows: [[1, '9.50'], [2, '12.00']], keyColumns: [], idColumn: 'id', note: 'sample read' } },
      { stage: 'models', model: 'graph', status: 'available', data: { query: "MATCH (n) WHERE n.sqlref_table = 'orders' RETURN n, n.sqlref_row, n.sqlref_schema", graphNodes: [{ nodeId: '41', rowId: 2, inSample: true }], truncated: false, documents: 'collections bound to a table exist only in client code' } },
      { stage: 'change-events', model: 'cdc', status: 'available', data: { retained: 9, events: [{ seq: 7, table: 'orders', change: 'INSERT', ts: 1790000000000 }], note: 'metadata only' } },
    ],
  }
}

beforeEach(() => {
  cleanup()
  activeConnection.value = { id: 'c1', name: 'nuc', url: 'postgres://n', isNucleus: true }
  tabs.value = []
  activeTabId.value = null
  limitsReport.value = null
})
afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('JourneyModule (X06)', () => {
  it('renders all seven stages in order, each with its model limits', async () => {
    journeyMock.mockResolvedValueOnce(nucleusJourney())
    const { container } = render(<JourneyModule schema="public" table="orders" />)
    await waitFor(() => expect(container.querySelectorAll('[data-stage]').length).toBe(7))
    const order = Array.from(container.querySelectorAll('[data-stage]')).map(el => el.getAttribute('data-stage'))
    expect(order).toEqual(['schema', 'migrations', 'queries', 'plan', 'rows', 'models', 'change-events'])
    // Each stage renders its model's limits; the journey refreshes the shared report.
    expect(limitsReport.value?.engine.product).toBe('nucleus')
    expect(container.querySelectorAll('[aria-label="SQL limits"]').length).toBe(5)
    expect(screen.getByLabelText('Graph limits').textContent).toMatch(/not atomic with SQL rows/)
    expect(screen.getByLabelText('CDC limits').textContent).toMatch(/not transactional/)
    expect(screen.getByLabelText('CDC limits').textContent).toMatch(/only INSERT statements emit events/)
    // Unavailable stages say why instead of pretending.
    expect(screen.getByText(/verified on PostgreSQL only/)).toBeTruthy()
    expect(container.textContent).not.toMatch(/transactions: atomic/)
  })

  it('links rows to their bound graph nodes and navigates into the acting modules', async () => {
    journeyMock.mockResolvedValueOnce(nucleusJourney())
    const { container } = render(<JourneyModule schema="public" table="orders" />)
    await waitFor(() => expect(container.querySelector('[data-stage="rows"]')).toBeTruthy())
    const rows = container.querySelector('[data-stage="rows"] tbody')!
    expect(rows.querySelectorAll('tr')[1].textContent).toContain('41')
    expect(rows.querySelectorAll('tr')[0].textContent).not.toContain('41')

    fireEvent.click(screen.getByText('Open CDC for orders'))
    expect(tabs.value.at(-1)).toMatchObject({ kind: 'cdc', focus: 'orders' })
    fireEvent.click(screen.getByText('Open in Graph'))
    expect(tabs.value.at(-1)).toMatchObject({ kind: 'graph', focus: "MATCH (n) WHERE n.sqlref_table = 'orders' RETURN n, n.sqlref_row, n.sqlref_schema" })
    fireEvent.click(screen.getByText('Browse rows'))
    expect(tabs.value.at(-1)).toMatchObject({ kind: 'sql-browser', objectSchema: 'public', objectName: 'orders' })
    fireEvent.click(screen.getByText('Open in editor'))
    expect(tabs.value.at(-1)).toMatchObject({ kind: 'sql-editor', initialSql: 'SELECT * FROM orders' })
  })

  it('follows foreign keys into the referenced table\'s journey and shows migration checksums', async () => {
    activeConnection.value = { id: 'c1', name: 'pg', url: 'postgres://p', isNucleus: false }
    const limits = limitsFixture.postgres as LimitsReport
    journeyMock.mockResolvedValueOnce({
      engine: limits.engine, schema: 'public', table: 'orders', limits,
      stages: [
        { stage: 'schema', model: 'sql', status: 'available', data: { source: 'introspection-v2', documentSHA256: 'abcdef0123456789', columns: [{ name: 'id', type: 'bigint', notNull: true, primaryKey: true }, { name: 'customer_id', type: 'bigint', notNull: true, primaryKey: false }], keyColumns: ['id'], references: [{ constraint: 'orders_customer_fk', schema: 'public', name: 'customers', columns: ['customer_id'], refColumns: ['id'] }], referencedBy: [] } },
        { stage: 'migrations', model: 'sql', status: 'available', data: { directory: 'migrations', history: 'v2', mentioning: 1, entries: [{ version: '001', name: 'orders', applied: true, appliedAt: '2026-09-25T00:00:00Z', checksum: 'verified', mentions: true, lines: ['CREATE TABLE orders ('] }, { version: '002', name: 'other', applied: false, checksum: 'pending', mentions: false }] } },
        { stage: 'queries', model: 'sql', status: 'empty', data: { scope: 's', entries: [] } },
        { stage: 'plan', model: 'sql', status: 'available', data: { statement: 'SELECT * FROM "public"."orders" LIMIT 5', plan: [{ Plan: { 'Node Type': 'Limit', 'Total Cost': 0.4, 'Plan Rows': 5 } }], executed: false, note: 'EXPLAIN without ANALYZE' } },
        { stage: 'rows', model: 'sql', status: 'empty', data: { statement: 's', columns: ['id'], rows: [], keyColumns: ['id'], note: 'n' } },
        { stage: 'models', model: 'graph', status: 'unavailable', reason: 'the connected engine is not Nucleus' },
        { stage: 'change-events', model: 'cdc', status: 'unavailable', reason: 'Studio has no change-event source on this engine' },
      ],
    })
    const { container } = render(<JourneyModule schema="public" table="orders" />)
    await waitFor(() => expect(container.querySelector('[data-stage="schema"]')).toBeTruthy())
    expect(container.querySelector('[data-checksum="verified"]')).toBeTruthy()
    expect(screen.getByText('CREATE TABLE orders (')).toBeTruthy()
    expect(screen.getByText(/Limit · cost 0\.4 · ~5 rows/)).toBeTruthy()
    expect(screen.getAllByText('transactions: atomic').length).toBeGreaterThan(0)
    fireEvent.click(screen.getByText('Journey →'))
    expect(tabs.value.at(-1)).toMatchObject({ kind: 'journey', objectSchema: 'public', objectName: 'customers' })
  })

  it('surfaces a failed load', async () => {
    journeyMock.mockRejectedValueOnce(new Error('not connected'))
    render(<JourneyModule schema="public" table="orders" />)
    await waitFor(() => expect(screen.getByRole('alert').textContent).toContain('not connected'))
  })

  it('deep-links the journey', () => {
    const link = parseDeepLink('#/c/c1/journey/public/Order%20Lines')
    expect(link).toMatchObject({ connectionId: 'c1', tab: { kind: 'journey', objectSchema: 'public', objectName: 'Order Lines' } })
    expect(serializeDeepLink({ id: 'x', ...link!.tab }, 'c1')).toBe('#/c/c1/journey/public/Order%20Lines')
  })

  it('summarizes a plan top node', () => {
    expect(planTop([{ Plan: { 'Node Type': 'Seq Scan', 'Relation Name': 'orders', 'Total Cost': 1 } }])).toBe('Seq Scan · on orders · cost 1')
    expect(planTop(null)).toBeNull()
  })
})
