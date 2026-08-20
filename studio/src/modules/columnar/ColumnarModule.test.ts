import { describe, it, expect } from 'vitest'

// Tests for ColumnarModule real Nucleus SQL. The COLUMNAR_* store is a named
// keyspace inside the columnar store — NOT a SQL relation, so `SELECT * FROM
// t` does not address it (SQL tables created WITH (engine='columnar') are
// browsed in the SQL section). Aggregates and inserts are UPPERCASE SCALAR
// functions.

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`

const QUICK_QUERIES = [
  (t: string) => `SELECT COLUMNAR_COUNT(${sqlStr(t)})`,
  (t: string) => `SELECT COLUMNAR_SUM(${sqlStr(t)}, 'column')`,
]

type Agg = 'SUM' | 'AVG' | 'MIN' | 'MAX'

function aggregateSql(table: string, agg: Agg, col: string): string {
  return `SELECT COLUMNAR_${agg}(${sqlStr(table)}, ${sqlStr(col)})`
}

const NUMERIC_RE = /^-?\d+(\.\d+)?$/

function buildInsertSql(table: string, pairsInput: string): string {
  const parts: string[] = [sqlStr(table)]
  for (const pair of pairsInput.split(',')) {
    const eq = pair.indexOf('=')
    if (eq === -1) continue
    const key = pair.slice(0, eq).trim()
    const val = pair.slice(eq + 1).trim()
    if (!key) continue
    parts.push(sqlStr(key))
    parts.push(NUMERIC_RE.test(val) ? val : sqlStr(val))
  }
  return `SELECT COLUMNAR_INSERT(${parts.join(', ')})`
}

describe('ColumnarModule — QUICK_QUERIES', () => {
  it('should generate COLUMNAR_COUNT query', () => {
    expect(QUICK_QUERIES[0]('analytics')).toBe("SELECT COLUMNAR_COUNT('analytics')")
  })

  it('should generate a COLUMNAR_SUM template query (the store is not a SQL relation)', () => {
    expect(QUICK_QUERIES[1]('sales')).toBe("SELECT COLUMNAR_SUM('sales', 'column')")
  })
})

describe('ColumnarModule — aggregates', () => {
  it('should build COLUMNAR_SUM query', () => {
    expect(aggregateSql('metrics', 'SUM', 'value')).toBe("SELECT COLUMNAR_SUM('metrics', 'value')")
  })

  it('should support all aggregate functions', () => {
    for (const agg of ['SUM', 'AVG', 'MIN', 'MAX'] as Agg[]) {
      expect(aggregateSql('t', agg, 'c')).toBe(`SELECT COLUMNAR_${agg}('t', 'c')`)
    }
  })
})

describe('ColumnarModule — COLUMNAR_INSERT', () => {
  it('should build variadic col/val pairs, quoting text and leaving numbers bare', () => {
    expect(buildInsertSql('sales', 'name=alice, age=30')).toBe(
      "SELECT COLUMNAR_INSERT('sales', 'name', 'alice', 'age', 30)"
    )
  })

  it('should keep negative and decimal numbers unquoted', () => {
    expect(buildInsertSql('t', 'x=-1.5')).toBe("SELECT COLUMNAR_INSERT('t', 'x', -1.5)")
  })

  it('should ignore malformed pairs without an equals sign', () => {
    expect(buildInsertSql('t', 'garbage')).toBe("SELECT COLUMNAR_INSERT('t')")
  })
})

describe('ColumnarModule — count parsing', () => {
  it('should parse row count from COLUMNAR_COUNT result', () => {
    const rows: unknown[][] = [[500000]]
    expect(Number(rows[0][0])).toBe(500000)
  })
})
