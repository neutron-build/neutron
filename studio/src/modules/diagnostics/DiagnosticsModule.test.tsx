import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent, waitFor } from '@testing-library/preact'

// S05 diagnosis view: slow queries from the duration log with an honest
// scope statement, pg_stat_statements surfaced when available and explained
// when not, table statistics with index usage, and honest 404 handling.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      diagnosticsQueries: vi.fn(),
      tableStats: vi.fn(),
    },
  }
})

import { api, ApiError } from '../../lib/api'
import { activeConnection, tabs } from '../../lib/store'
import { DiagnosticsModule } from './DiagnosticsModule'

const queriesMock = vi.mocked(api.diagnosticsQueries)
const statsMock = vi.mocked(api.tableStats)

beforeEach(() => {
  cleanup()
  activeConnection.value = { id: 'c1', name: 'test', url: 'postgres://x', isNucleus: false }
  tabs.value = []
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('DiagnosticsModule (S05)', () => {
  it('lists slow statements with durations and opens one in the SQL editor', async () => {
    queriesMock.mockResolvedValueOnce({
      entries: [
        { at: '2026-09-24T12:00:00Z', connectionId: 'c1', surface: 'editor', requestId: 'q1', sql: 'SELECT pg_sleep(0.4) FROM big', durationMs: 412.5, rowCount: 1, state: 'ok' },
        { at: '2026-09-24T12:00:01Z', connectionId: 'c1', surface: 'table-read', sql: 'SELECT * FROM wide', durationMs: 155, rowCount: 200, state: 'error', error: 'boom' },
      ],
      stats: { count: 10, p50Ms: 5, p95Ms: 400, maxMs: 412.5 },
      scope: 'statements executed through this Studio server process since its launch',
      pgStatStatements: { available: false, reason: 'the pg_stat_statements extension is not installed on this server' },
    })
    render(<DiagnosticsModule />)
    await waitFor(() => screen.getByText('SELECT pg_sleep(0.4) FROM big'))
    expect(screen.getByText(/p95 400 ms/)).toBeTruthy()
    expect(screen.getByText(/statements executed through this Studio server process/)).toBeTruthy()
    // Honest unavailability with the reason, not silence.
    expect(screen.getByText('pg_stat_statements unavailable')).toBeTruthy()
    expect(screen.getByText(/extension is not installed/)).toBeTruthy()
    fireEvent.click(screen.getAllByText('open')[0])
    const editorTab = tabs.value.find(t => t.kind === 'sql-editor')
    expect(editorTab?.initialSql).toBe('SELECT pg_sleep(0.4) FROM big')
  })

  it('shows pg_stat_statements data when the probe succeeds', async () => {
    queriesMock.mockResolvedValueOnce({
      entries: [],
      stats: { count: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 },
      scope: 'statements executed through this Studio server process since its launch',
      pgStatStatements: {
        available: true,
        statements: [{ query: 'SELECT $1 FROM t', calls: 1200, totalMs: 9000.5, meanMs: 7.5 }],
        note: 'server-wide, per-database, since the last statistics reset',
      },
    })
    render(<DiagnosticsModule />)
    await waitFor(() => screen.getByText('SELECT $1 FROM t'))
    expect(screen.getByText(/server-wide, per-database/)).toBeTruthy()
  })

  it('renders table statistics with per-index usage and caveats', async () => {
    queriesMock.mockResolvedValueOnce({
      entries: [], stats: { count: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 }, scope: 'scope',
    })
    statsMock.mockResolvedValueOnce({
      schema: 'public', table: 'orders',
      stats: {
        seqScan: 4, idxScan: 9, idxTupFetch: 90, nLiveTup: 2, nDeadTup: 1, nModSinceAnalyze: 3,
        lastAnalyze: null, lastAutoAnalyze: '2026-09-24T10:00:00Z', lastVacuum: null, lastAutoVacuum: null,
      },
      indexes: [
        { name: 'orders_note_idx', definition: 'CREATE INDEX orders_note_idx ON public.orders USING btree (note)', unique: false, idxScan: 9, idxTupRead: 90, sizeBytes: 8192 },
        { name: 'orders_unused_idx', definition: 'CREATE INDEX orders_unused_idx ON public.orders USING btree (total)', unique: false, idxScan: 0, idxTupRead: 0, sizeBytes: 8192 },
      ],
      notes: ['counters are cumulative since the last statistics reset (pg_stat_reset) or server restart'],
      relSizeBytes: 8192, totalSizeBytes: 32768,
    })
    render(<DiagnosticsModule schema="public" table="orders" />)
    await waitFor(() => screen.getByText('orders_note_idx'))
    expect(screen.getByText(/seq scans 4 · index scans 9/)).toBeTruthy()
    expect(screen.getByText(/last analyze never · last autoanalyze 2026-09-24T10:00:00Z/)).toBeTruthy()
    expect(screen.getByText(/counters are cumulative/)).toBeTruthy()
    // Unused index visible with its zero scans — a signal, not a verdict.
    expect(screen.getByText('orders_unused_idx')).toBeTruthy()
    expect(screen.getAllByText('no scans since the statistics reset')).toHaveLength(1)
  })

  it('loads statistics for a typed table only on submit, not per keystroke', async () => {
    queriesMock.mockResolvedValue({
      entries: [], stats: { count: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 }, scope: 'scope',
    })
    statsMock.mockRejectedValue(new ApiError(404, 'no statistics row', { state: undefined }))
    render(<DiagnosticsModule />)
    const tableInput = screen.getByRole('textbox', { name: 'table' })
    for (const partial of ['o', 'or', 'ord', 'orders']) {
      fireEvent.input(tableInput, { target: { value: partial } })
    }
    expect(statsMock).not.toHaveBeenCalled()
    fireEvent.click(screen.getByText('Load'))
    await waitFor(() => expect(statsMock).toHaveBeenCalledTimes(1))
    expect(statsMock).toHaveBeenCalledWith('c1', 'public', 'orders')
  })

  it('states honestly when a table has no statistics row', async () => {
    queriesMock.mockResolvedValueOnce({
      entries: [], stats: { count: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 }, scope: 'scope',
    })
    statsMock.mockRejectedValueOnce(new ApiError(404, 'no statistics row', { state: undefined }))
    render(<DiagnosticsModule schema="public" table="nope" />)
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('no statistics row')
    expect(alert.textContent).toContain('refresh the schema')
  })

  it('is unavailable on Nucleus with the honest reason', () => {
    activeConnection.value = { id: 'c1', name: 'n', url: 'postgres://x', isNucleus: true }
    render(<DiagnosticsModule />)
    expect(screen.getByText(/neither is verified on Nucleus yet/)).toBeTruthy()
  })
})
