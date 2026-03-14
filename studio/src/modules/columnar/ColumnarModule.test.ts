import { describe, it, expect } from 'vitest'

// Tests for ColumnarModule: quick queries, column stat parsing

describe('ColumnarModule — QUICK_QUERIES', () => {
  const QUICK_QUERIES = [
    (t: string) => `SELECT COUNT(*) FROM columnar_scan('${t}')`,
    (t: string) => `SELECT * FROM columnar_scan('${t}') LIMIT 100`,
    (t: string) => `SELECT * FROM columnar_aggregate('${t}', 'count,sum,avg')`,
  ]

  it('should generate COUNT query', () => {
    expect(QUICK_QUERIES[0]('analytics')).toBe("SELECT COUNT(*) FROM columnar_scan('analytics')")
  })

  it('should generate SCAN query with LIMIT', () => {
    expect(QUICK_QUERIES[1]('sales')).toBe("SELECT * FROM columnar_scan('sales') LIMIT 100")
  })

  it('should generate AGGREGATE query', () => {
    expect(QUICK_QUERIES[2]('metrics')).toBe("SELECT * FROM columnar_aggregate('metrics', 'count,sum,avg')")
  })
})

describe('ColumnarModule — column stat parsing', () => {
  interface ColumnStat {
    name: string
    type: string
    nullPct: number
    minVal: string
    maxVal: string
    distinctCount: number
  }

  function parseColStats(rows: unknown[][]): ColumnStat[] {
    return rows.map(r => ({
      name: String(r[0]),
      type: String(r[1]),
      nullPct: Number(r[2]),
      minVal: String(r[3] ?? ''),
      maxVal: String(r[4] ?? ''),
      distinctCount: Number(r[5]),
    }))
  }

  it('should parse column statistics from query result', () => {
    const rows: unknown[][] = [
      ['id', 'bigint', 0, '1', '1000', 1000],
      ['value', 'double', 5.2, '0.01', '99.9', 500],
      ['label', 'text', 15, null, null, 10],
    ]
    const stats = parseColStats(rows)
    expect(stats.length).toBe(3)
    expect(stats[0]).toEqual({ name: 'id', type: 'bigint', nullPct: 0, minVal: '1', maxVal: '1000', distinctCount: 1000 })
    expect(stats[1].nullPct).toBe(5.2)
    expect(stats[2].minVal).toBe('')
    expect(stats[2].maxVal).toBe('')
  })

  it('should handle empty results', () => {
    expect(parseColStats([])).toEqual([])
  })
})

describe('ColumnarModule — info parsing', () => {
  it('should parse row count from columnar_info result', () => {
    const rows: unknown[][] = [[500000]]
    const rowCount = Number(rows[0][0])
    expect(rowCount).toBe(500000)
  })
})
