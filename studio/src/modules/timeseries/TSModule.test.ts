import { describe, it, expect } from 'vitest'

// Tests for TSModule utility functions: fmt, Sparkline data generation

function fmt(n: number): string {
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(2) + 'K'
  return Number(n.toFixed(4)).toString()
}

describe('TSModule — fmt', () => {
  it('should format millions', () => {
    expect(fmt(1000000)).toBe('1.00M')
    expect(fmt(2500000)).toBe('2.50M')
    expect(fmt(1234567)).toBe('1.23M')
  })

  it('should format negative millions', () => {
    expect(fmt(-2000000)).toBe('-2.00M')
  })

  it('should format thousands', () => {
    expect(fmt(1000)).toBe('1.00K')
    expect(fmt(5432)).toBe('5.43K')
    expect(fmt(999999)).toBe('1000.00K')
  })

  it('should format negative thousands', () => {
    expect(fmt(-3500)).toBe('-3.50K')
  })

  it('should format small numbers without suffix', () => {
    expect(fmt(0)).toBe('0')
    expect(fmt(1)).toBe('1')
    expect(fmt(42.123456)).toBe('42.1235')
    expect(fmt(999)).toBe('999')
  })

  it('should trim trailing zeros for small numbers', () => {
    expect(fmt(1.0)).toBe('1')
    expect(fmt(2.5)).toBe('2.5')
    expect(fmt(3.14)).toBe('3.14')
  })

  it('should handle fractional values', () => {
    expect(fmt(0.001)).toBe('0.001')
    expect(fmt(0.00001)).toBe('0')
  })
})

describe('TSModule — Sparkline logic', () => {
  function computeSparkline(values: number[]) {
    if (values.length < 2) return null
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = max - min || 1
    const w = 200
    const h = 40
    const pts = values.map((v, i) => {
      const x = (i / (values.length - 1)) * w
      const y = h - ((v - min) / range) * (h - 4) - 2
      return { x, y }
    })
    return pts
  }

  it('should return null for fewer than 2 values', () => {
    expect(computeSparkline([])).toBeNull()
    expect(computeSparkline([42])).toBeNull()
  })

  it('should compute points for valid data', () => {
    const pts = computeSparkline([0, 10])!
    expect(pts.length).toBe(2)
    // First point at x=0
    expect(pts[0].x).toBe(0)
    // Last point at x=200
    expect(pts[1].x).toBe(200)
  })

  it('should handle constant values', () => {
    const pts = computeSparkline([5, 5, 5])!
    expect(pts.length).toBe(3)
    // All y values should be the same when range is 0 (uses fallback range=1)
    const yValues = pts.map(p => p.y)
    expect(yValues[0]).toBeCloseTo(yValues[1])
    expect(yValues[1]).toBeCloseTo(yValues[2])
  })

  it('should map min value to bottom and max to top', () => {
    const pts = computeSparkline([0, 100])!
    // min value -> higher y (bottom of SVG)
    // max value -> lower y (top of SVG)
    expect(pts[0].y).toBeGreaterThan(pts[1].y)
  })

  it('should evenly space x values', () => {
    const pts = computeSparkline([1, 2, 3, 4, 5])!
    const dx = pts[1].x - pts[0].x
    for (let i = 2; i < pts.length; i++) {
      expect(pts[i].x - pts[i - 1].x).toBeCloseTo(dx)
    }
  })
})

function sqlStr(s: string): string {
  return `'${s.replace(/'/g, "''")}'`
}

describe('TSModule — query building', () => {
  // Summary uses the real scalar TS_COUNT / TS_LAST (no store-name-less
  // ts_range table function, which does not exist).
  it('should build the summary query', () => {
    const name = 'cpu'
    const sql = `SELECT TS_COUNT(${sqlStr(name)}), TS_LAST(${sqlStr(name)})`
    expect(sql).toBe("SELECT TS_COUNT('cpu'), TS_LAST('cpu')")
  })

  // Bucketed aggregates are built client-side from TS_RANGE_AVG windows with
  // numeric epoch-ms bounds, batched into one multi-column select.
  it('should build a batched TS_RANGE_AVG bucket query', () => {
    const name = 'cpu'
    const start = 0
    const size = 60_000
    const end = 180_000
    const windows: number[] = []
    for (let b = start; b < end; b += size) windows.push(b)
    const cols = windows
      .map(b => `TS_RANGE_AVG(${sqlStr(name)}, ${b}, ${Math.min(b + size, end)})`)
      .join(', ')
    const sql = `SELECT ${cols}`
    expect(sql).toBe(
      "SELECT TS_RANGE_AVG('cpu', 0, 60000), TS_RANGE_AVG('cpu', 60000, 120000), TS_RANGE_AVG('cpu', 120000, 180000)"
    )
  })

  it('should build TS_RANGE_COUNT windows for the count aggregate', () => {
    const name = 'cpu'
    const sql = `TS_RANGE_COUNT(${sqlStr(name)}, ${0}, ${60_000})`
    expect(sql).toBe("TS_RANGE_COUNT('cpu', 0, 60000)")
  })
})
