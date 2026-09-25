import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection, toast } from '../../lib/store'
import { DocModule } from './DocModule'

// X02: the document module scopes every DOC_* statement to the selected
// collection (empty = default). The engine scopes each statement to the
// named collection — a document in another collection reads as absent. The /api/query endpoint does not bind parameters yet, so the UI
// constrains the collection name to characters that cannot break out of a
// single-quoted literal; these tests pin that constraint and the scoped
// SQL forms through the real component.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      query: vi.fn(),
    },
  }
})

import { api } from '../../lib/api'

const query = vi.mocked(api.query)

function docListResult(ids: number[], docFor: (id: number) => unknown) {
  return {
    columns: ids.map(String),
    rows: [ids.map(docFor)],
    rowCount: 1,
    duration: 0,
  }
}

beforeEach(() => {
  activeConnection.value = { id: 'c1', name: 'test', type: 'nucleus' } as never
  query.mockReset()
  query.mockResolvedValue({ columns: [], rows: [], rowCount: 0, duration: 0 } as never)
})

afterEach(cleanup)

describe('X02 DocModule collection scoping', () => {
  it('loads the default collection with unscoped forms', async () => {
    query.mockResolvedValueOnce({ columns: ['q'], rows: [['']], rowCount: 1, duration: 0 } as never)
    render(<DocModule name="Documents" />)
    await waitFor(() => expect(query).toHaveBeenCalled())
    expect(query.mock.calls[0][0]).toBe("SELECT DOC_QUERY('{}')")
  })

  it('a collection name re-loads with scoped forms for list and fetch', async () => {
    query.mockResolvedValue({ columns: [], rows: [], rowCount: 0, duration: 0 } as never)
    render(<DocModule name="Documents" />)
    const input = screen.getByTitle(/Document collection/) as HTMLInputElement
    await fireEvent.input(input, { target: { value: 'tenants_a' } })
    await fireEvent.keyDown(input, { key: 'Enter' })
    await waitFor(() =>
      expect(query.mock.calls.some(([sql]) => sql === "SELECT DOC_QUERY('tenants_a', '{}')")).toBe(true),
    )
    // Document fetches are scoped too: ids 7 -> DOC_GET('tenants_a', 7).
    query.mockClear()
    query.mockResolvedValueOnce({ columns: ['q'], rows: [['7']], rowCount: 1, duration: 0 } as never)
    query.mockResolvedValueOnce(docListResult([7], () => JSON.stringify({ a: 1 })) as never)
    const refresh = document.querySelector('button[class*="refreshBtn"]') as HTMLButtonElement
    await fireEvent.click(refresh)
    await waitFor(() =>
      expect(query.mock.calls.some(([sql]) => sql.includes("DOC_GET('tenants_a', 7)"))).toBe(true),
    )
  })

  it('rejects characters that could break out of a literal', async () => {
    query.mockResolvedValue({ columns: [], rows: [], rowCount: 0, duration: 0 } as never)
    render(<DocModule name="Documents" />)
    const input = screen.getByTitle(/Document collection/) as HTMLInputElement
    // The whole hostile string arrives in one event and is rejected whole —
    // nothing after the first bad character ever reaches the signal.
    await fireEvent.input(input, { target: { value: "x'; drop--" } })
    expect(input.value).toBe('')
    // Incremental typing keeps only the safe prefix characters.
    await fireEvent.input(input, { target: { value: 'x' } })
    expect(input.value).toBe('x')
    await fireEvent.input(input, { target: { value: "x'" } })
    expect(input.value).toBe('x')
    expect(query.mock.calls.every(([sql]) => !sql.includes('drop'))).toBe(true)
  })

  it('interpolates only digit ids returned by the engine', async () => {
    query.mockResolvedValueOnce({ columns: ['q'], rows: [['3, 1,x) or (1,7']], rowCount: 1, duration: 0 } as never)
    query.mockResolvedValueOnce(docListResult([1, 3, 7], () => JSON.stringify({ a: 1 })) as never)
    render(<DocModule name="Documents" />)
    await waitFor(() => expect(query.mock.calls.length).toBeGreaterThanOrEqual(2))
    expect(query.mock.calls[1][0]).toBe('SELECT DOC_GET(1), DOC_GET(3), DOC_GET(7)')
  })
})
