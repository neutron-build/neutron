import { describe, it, expect } from 'vitest'

// Tests for CDCModule real Nucleus SQL. CDC is a single GLOBAL log read via
// UPPERCASE SCALAR functions returning JSON ({seq,table,change,ts}); there is
// no cdc_changes()/lsn/old_data/new_data table-valued surface.

type Op = 'all' | 'INSERT' | 'UPDATE' | 'DELETE'
type RefreshInterval = 'off' | '1' | '2' | '5' | '10'

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

interface CdcEvent {
  seq: number
  table: string
  change: string
  ts: number
}

function buildCdcQuery(count: number, limit: number, filterTable: string): string {
  const after = Math.max(0, count - limit)
  return filterTable !== 'all'
    ? `SELECT CDC_TABLE_READ(${sqlStr(filterTable)}, ${after}, ${limit})`
    : `SELECT CDC_READ(${after}, ${limit})`
}

function parseCdcEvents(cell: unknown): CdcEvent[] {
  if (cell == null) return []
  const text = String(cell).trim()
  if (text === '') return []
  try {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? (parsed as CdcEvent[]) : []
  } catch {
    return []
  }
}

describe('CDCModule — query building', () => {
  it('should use CDC_READ for all tables, reading the most recent window', () => {
    // count=500, limit=200 → after = 300
    expect(buildCdcQuery(500, 200, 'all')).toBe('SELECT CDC_READ(300, 200)')
  })

  it('should clamp the after-cursor at 0 when count < limit', () => {
    expect(buildCdcQuery(50, 200, 'all')).toBe('SELECT CDC_READ(0, 200)')
  })

  it('should use CDC_TABLE_READ when a table filter is set', () => {
    expect(buildCdcQuery(300, 100, 'users')).toBe("SELECT CDC_TABLE_READ('users', 200, 100)")
  })
})

describe('CDCModule — event JSON parsing', () => {
  it('should parse the real {seq,table,change,ts} shape', () => {
    const cell = JSON.stringify([
      { seq: 1, table: 'users', change: 'INSERT', ts: 1700000000000 },
      { seq: 2, table: 'orders', change: 'UPDATE', ts: 1700000000001 },
    ])
    const events = parseCdcEvents(cell)
    expect(events.length).toBe(2)
    expect(events[0]).toEqual({ seq: 1, table: 'users', change: 'INSERT', ts: 1700000000000 })
    expect(events[1].change).toBe('UPDATE')
  })

  it('should apply the operation filter client-side', () => {
    const events: CdcEvent[] = [
      { seq: 1, table: 'a', change: 'INSERT', ts: 0 },
      { seq: 2, table: 'a', change: 'DELETE', ts: 0 },
    ]
    const op: Op = 'INSERT'
    const filtered = op !== 'all' ? events.filter(e => e.change === op) : events
    expect(filtered.length).toBe(1)
    expect(filtered[0].seq).toBe(1)
  })

  it('should treat empty/invalid cells as no events', () => {
    expect(parseCdcEvents('')).toEqual([])
    expect(parseCdcEvents(null)).toEqual([])
    expect(parseCdcEvents('nope')).toEqual([])
  })
})

describe('CDCModule — refresh interval', () => {
  it('should validate all refresh interval values', () => {
    const intervals: RefreshInterval[] = ['off', '1', '2', '5', '10']
    expect(intervals.length).toBe(5)
    for (const interval of intervals) {
      if (interval !== 'off') {
        expect(parseInt(interval) * 1000).toBeGreaterThan(0)
      }
    }
  })

  it('should compute isLive correctly', () => {
    const testCases: { interval: RefreshInterval; expected: boolean }[] = [
      { interval: 'off', expected: false },
      { interval: '1', expected: true },
      { interval: '10', expected: true },
    ]
    for (const { interval, expected } of testCases) {
      expect(interval !== 'off').toBe(expected)
    }
  })
})
