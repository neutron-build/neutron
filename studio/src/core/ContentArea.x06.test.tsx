import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, waitFor } from '@testing-library/preact'

// X06: every tab that acts on a model shows that model's limits above the
// module; non-acting tabs (journey, inspector, diagnostics) do not. The
// modules themselves are stubbed: this pins the wiring, not the modules.

vi.mock('../modules/kv/KVModule', () => ({ KVModule: () => <div>kv module</div> }))
vi.mock('../modules/sql/SQLEditor', () => ({ SQLEditor: () => <div>sql editor</div> }))
vi.mock('../modules/cdc/CDCModule', () => ({ CDCModule: ({ initialTable }: { initialTable?: string }) => <div>cdc module {initialTable}</div> }))
vi.mock('../modules/journey/JourneyModule', () => ({ JourneyModule: () => <div>journey module</div> }))

import { tabs, activeTabId, limitsReport, activeConnection } from '../lib/store'
import limitsFixture from '../lib/limits.fixture.json'
import type { LimitsReport, Tab } from '../lib/types'
import { ContentArea } from './ContentArea'

function show(tab: Tab) {
  tabs.value = [tab]
  activeTabId.value = tab.id
}

beforeEach(() => {
  cleanup()
  activeConnection.value = { id: 'c1', name: 'nuc', url: 'postgres://n', isNucleus: true }
  limitsReport.value = limitsFixture.nucleus as LimitsReport
})
afterEach(() => {
  cleanup()
  tabs.value = []
  activeTabId.value = null
  limitsReport.value = null
})

describe('ContentArea limits wiring (X06)', () => {
  it('shows KV limits above the KV module', async () => {
    show({ id: 't1', kind: 'kv', label: 'kv', objectName: 'keyspace' })
    render(<ContentArea />)
    expect(screen.getByLabelText('Key-Value limits').textContent).toMatch(/not atomic with SQL rows/)
    await waitFor(() => expect(screen.getByText('kv module')).toBeTruthy())
  })

  it('shows SQL limits above the SQL editor', async () => {
    show({ id: 't2', kind: 'sql-editor', label: 'SQL' })
    render(<ContentArea />)
    expect(screen.getByLabelText('SQL limits').textContent).toMatch(/transactions: partial/)
    await waitFor(() => expect(screen.getByText('sql editor')).toBeTruthy())
  })

  it('passes journey focus into CDC and shows CDC limits', async () => {
    show({ id: 't3', kind: 'cdc', label: 'CDC', objectName: 'changes', focus: 'orders' })
    render(<ContentArea />)
    expect(screen.getByLabelText('CDC limits').textContent).toMatch(/not transactional/)
    await waitFor(() => expect(screen.getByText('cdc module orders')).toBeTruthy())
  })

  it('renders the journey without a single-model strip', async () => {
    show({ id: 't4', kind: 'journey', label: 'Journey: orders', objectSchema: 'public', objectName: 'orders' })
    const { container } = render(<ContentArea />)
    await waitFor(() => expect(screen.getByText('journey module')).toBeTruthy())
    expect(container.querySelector('[role="note"]')).toBeNull()
  })
})
