import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { QueryResult } from '../lib/types'

// Tests for DataGrid's export helper functions (extractable logic).

function escapeCSV(v: unknown): string {
  const s = v === null || v === undefined ? '' : String(v)
  return s.includes(',') || s.includes('"') || s.includes('\n')
    ? `"${s.replace(/"/g, '""')}"` : s
}

function buildCSV(result: QueryResult): string {
  const header = result.columns.map(escapeCSV).join(',')
  const rows = result.rows.map(row => (row as unknown[]).map(escapeCSV).join(','))
  return [header, ...rows].join('\n')
}

function buildJSON(result: QueryResult): object[] {
  return result.rows.map(row =>
    Object.fromEntries(result.columns.map((col, i) => [col, (row as unknown[])[i]]))
  )
}

describe('DataGrid CSV escape', () => {
  it('should return empty string for null', () => {
    expect(escapeCSV(null)).toBe('')
  })

  it('should return empty string for undefined', () => {
    expect(escapeCSV(undefined)).toBe('')
  })

  it('should not quote simple strings', () => {
    expect(escapeCSV('hello')).toBe('hello')
  })

  it('should quote strings with commas', () => {
    expect(escapeCSV('hello, world')).toBe('"hello, world"')
  })

  it('should quote strings with double quotes and escape them', () => {
    expect(escapeCSV('he said "hi"')).toBe('"he said ""hi"""')
  })

  it('should quote strings with newlines', () => {
    expect(escapeCSV('line1\nline2')).toBe('"line1\nline2"')
  })

  it('should handle numbers', () => {
    expect(escapeCSV(42)).toBe('42')
    expect(escapeCSV(3.14)).toBe('3.14')
  })

  it('should handle booleans', () => {
    expect(escapeCSV(true)).toBe('true')
    expect(escapeCSV(false)).toBe('false')
  })
})

describe('DataGrid CSV builder', () => {
  it('should build correct CSV from QueryResult', () => {
    const result: QueryResult = {
      columns: ['id', 'name'],
      rows: [[1, 'Alice'], [2, 'Bob']],
      rowCount: 2,
      duration: 1,
    }
    const csv = buildCSV(result)
    expect(csv).toBe('id,name\n1,Alice\n2,Bob')
  })

  it('should handle empty rows', () => {
    const result: QueryResult = {
      columns: ['id'],
      rows: [],
      rowCount: 0,
      duration: 0,
    }
    const csv = buildCSV(result)
    expect(csv).toBe('id')
  })

  it('should handle null values in rows', () => {
    const result: QueryResult = {
      columns: ['id', 'name'],
      rows: [[1, null]],
      rowCount: 1,
      duration: 0,
    }
    const csv = buildCSV(result)
    expect(csv).toBe('id,name\n1,')
  })

  it('should escape column headers that need quoting', () => {
    const result: QueryResult = {
      columns: ['user,id', 'full "name"'],
      rows: [[1, 'Test']],
      rowCount: 1,
      duration: 0,
    }
    const csv = buildCSV(result)
    expect(csv).toContain('"user,id"')
    expect(csv).toContain('"full ""name"""')
  })
})

describe('DataGrid JSON builder', () => {
  it('should convert QueryResult rows to objects', () => {
    const result: QueryResult = {
      columns: ['id', 'name', 'email'],
      rows: [[1, 'Alice', 'a@b.com'], [2, 'Bob', 'b@c.com']],
      rowCount: 2,
      duration: 0,
    }
    const json = buildJSON(result)
    expect(json).toEqual([
      { id: 1, name: 'Alice', email: 'a@b.com' },
      { id: 2, name: 'Bob', email: 'b@c.com' },
    ])
  })

  it('should handle empty result', () => {
    const result: QueryResult = { columns: ['id'], rows: [], rowCount: 0, duration: 0 }
    const json = buildJSON(result)
    expect(json).toEqual([])
  })

  it('should preserve null values', () => {
    const result: QueryResult = {
      columns: ['a', 'b'],
      rows: [[null, undefined]],
      rowCount: 1,
      duration: 0,
    }
    const json = buildJSON(result)
    expect(json[0].a).toBeNull()
    expect(json[0].b).toBeUndefined()
  })
})
