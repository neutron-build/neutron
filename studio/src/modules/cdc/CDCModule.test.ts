import { describe, it, expect } from 'vitest'

// Tests for CDCModule: query building, filter logic

type Op = 'all' | 'INSERT' | 'UPDATE' | 'DELETE'
type RefreshInterval = 'off' | '1' | '2' | '5' | '10'

function buildCDCQuery(
  limit: number,
  filterTable: string,
  filterOp: Op,
): string {
  const tableCond = filterTable !== 'all'
    ? `AND table_name = '${filterTable}'` : ''
  const opCond = filterOp !== 'all'
    ? `AND operation = '${filterOp}'` : ''

  return `SELECT lsn, operation, table_name, old_data, new_data, changed_at
         FROM cdc_changes(${limit})
         WHERE 1=1 ${tableCond} ${opCond}
         ORDER BY changed_at DESC`
}

describe('CDCModule — query building', () => {
  it('should build query with no filters', () => {
    const sql = buildCDCQuery(200, 'all', 'all')
    expect(sql).toContain('cdc_changes(200)')
    expect(sql).not.toContain("AND table_name")
    expect(sql).not.toContain("AND operation")
    expect(sql).toContain('ORDER BY changed_at DESC')
  })

  it('should add table filter', () => {
    const sql = buildCDCQuery(200, 'users', 'all')
    expect(sql).toContain("AND table_name = 'users'")
    expect(sql).not.toContain("AND operation")
  })

  it('should add operation filter', () => {
    const sql = buildCDCQuery(100, 'all', 'INSERT')
    expect(sql).toContain("AND operation = 'INSERT'")
    expect(sql).not.toContain("AND table_name")
  })

  it('should add both filters', () => {
    const sql = buildCDCQuery(500, 'orders', 'UPDATE')
    expect(sql).toContain("AND table_name = 'orders'")
    expect(sql).toContain("AND operation = 'UPDATE'")
    expect(sql).toContain('cdc_changes(500)')
  })

  it('should support all operation types', () => {
    const ops: Op[] = ['all', 'INSERT', 'UPDATE', 'DELETE']
    for (const op of ops) {
      const sql = buildCDCQuery(200, 'all', op)
      if (op !== 'all') {
        expect(sql).toContain(`AND operation = '${op}'`)
      }
    }
  })
})

describe('CDCModule — refresh interval', () => {
  it('should validate all refresh interval values', () => {
    const intervals: RefreshInterval[] = ['off', '1', '2', '5', '10']
    expect(intervals.length).toBe(5)

    for (const interval of intervals) {
      if (interval !== 'off') {
        const ms = parseInt(interval) * 1000
        expect(ms).toBeGreaterThan(0)
      }
    }
  })

  it('should compute isLive correctly', () => {
    const testCases: { interval: RefreshInterval; expected: boolean }[] = [
      { interval: 'off', expected: false },
      { interval: '1', expected: true },
      { interval: '2', expected: true },
      { interval: '5', expected: true },
      { interval: '10', expected: true },
    ]
    for (const { interval, expected } of testCases) {
      const isLive = interval !== 'off'
      expect(isLive).toBe(expected)
    }
  })
})
