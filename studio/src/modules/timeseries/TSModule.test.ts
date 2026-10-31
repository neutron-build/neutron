import { describe, it, expect } from 'vitest'
import type { IngestResult } from './TSModule'
const errorOf = (result: IngestResult) => result.ok ? undefined : result.error

// Production ingestion/retention helpers; component/chart ownership is tested separately.

describe('TSModule — ingestion SQL (X03)', () => {
  // buildInsertSql mirrors the module's exported helper through the source
  // module under test (same file the component uses).
  it('builds a batched TS_INSERT select from epoch-ms lines', async () => {
    const mod = await import('./TSModule')
    const parsed = mod.buildInsertSql('cpu', '1730000000000, 1.5\n1730000060000, 2')
    expect(parsed.ok).toBe(true)
    if (!parsed.ok) throw new Error(parsed.error)
    expect(parsed.count).toBe(2)
    expect(parsed.sql).toBe("SELECT TS_INSERT('cpu', 1730000000000, 1.5), TS_INSERT('cpu', 1730000060000, 2)")
  })

  it('converts ISO-8601 timestamps to epoch ms', async () => {
    const mod = await import('./TSModule')
    const parsed = mod.buildInsertSql('cpu', '2026-09-24T05:00:00Z, 3.5')
    expect(parsed.ok).toBe(true)
    if (!parsed.ok) throw new Error(parsed.error)
    expect(parsed.count).toBe(1)
    expect(parsed.sql).toBe(`SELECT TS_INSERT('cpu', ${Date.parse('2026-09-24T05:00:00Z')}, 3.5)`)
  })

  it('rejects bad lines with a precise error and sends NOTHING', async () => {
    const mod = await import('./TSModule')
    expect(errorOf(mod.buildInsertSql('cpu', 'not-a-time, 1'))).toMatch(/neither epoch-ms nor a parseable ISO-8601/)
    expect(errorOf(mod.buildInsertSql('cpu', '1730000000000, abc'))).toMatch(/not a finite number/)
    expect(errorOf(mod.buildInsertSql('cpu', '1730000000000'))).toMatch(/expected "timestamp, value"/)
    expect(errorOf(mod.buildInsertSql('cpu', '   '))).toMatch(/at least one/)
  })

  it('escapes series names (single quotes) and bounds batch size', async () => {
    const mod = await import('./TSModule')
    const parsed = mod.buildInsertSql("o'brien", '0, 1')
    if (!parsed.ok) throw new Error(parsed.error)
    expect(parsed.sql).toBe("SELECT TS_INSERT('o''brien', 0, 1)")
    const big = Array.from({ length: 501 }, (_, i) => `${i}, 1`).join('\n')
    expect(errorOf(mod.buildInsertSql('cpu', big))).toMatch(/exceeds the 500-point batch limit/)
  })
})

describe('TSModule — retention SQL (X03)', () => {
  it('converts days to the engine\'s global max_age_ms policy', async () => {
    const mod = await import('./TSModule')
    expect(mod.buildRetentionSql(30)).toBe(`SELECT TS_RETENTION(${30 * 86_400_000})`)
    expect(mod.buildRetentionSql(0.5)).toBe(`SELECT TS_RETENTION(${12 * 3_600_000})`)
  })
})
