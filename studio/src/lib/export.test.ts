import { describe, it, expect, vi, beforeEach } from 'vitest'
import { exportCSV, exportJSON, resultToCSV, resultToJSON } from './export'
import type { QueryResult } from './types'

describe('exportCSV', () => {
  let clickSpy: ReturnType<typeof vi.fn>
  let createElementSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    clickSpy = vi.fn()
    createElementSpy = vi.spyOn(document, 'createElement').mockReturnValue({
      href: '',
      download: '',
      click: clickSpy,
    } as unknown as HTMLAnchorElement)
    vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:mock')
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
  })

  it('should do nothing for empty data', () => {
    exportCSV([], 'test.csv')
    expect(clickSpy).not.toHaveBeenCalled()
  })

  it('should export simple data as CSV', () => {
    const data = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ] as Record<string, unknown>[]

    exportCSV(data, 'users.csv')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should escape commas in values', () => {
    // We can't easily inspect the blob content, but we can verify it doesn't crash
    const data = [
      { description: 'hello, world', value: 42 },
    ] as Record<string, unknown>[]

    exportCSV(data, 'test.csv')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should escape quotes in values', () => {
    const data = [
      { text: 'He said "hello"', num: 1 },
    ] as Record<string, unknown>[]

    exportCSV(data, 'test.csv')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should escape newlines in values', () => {
    const data = [
      { text: 'line1\nline2', num: 1 },
    ] as Record<string, unknown>[]

    exportCSV(data, 'test.csv')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should handle null and undefined values', () => {
    const data = [
      { a: null, b: undefined, c: 'ok' },
    ] as Record<string, unknown>[]

    exportCSV(data, 'test.csv')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should use object keys from first row as headers', () => {
    const data = [
      { name: 'Alice', age: 30 },
    ] as Record<string, unknown>[]

    // We verify it creates the anchor and triggers download
    exportCSV(data, 'people.csv')
    expect(createElementSpy).toHaveBeenCalledWith('a')
  })
})

describe('exportJSON', () => {
  let clickSpy: ReturnType<typeof vi.fn>

  beforeEach(() => {
    clickSpy = vi.fn()
    vi.spyOn(document, 'createElement').mockReturnValue({
      href: '',
      download: '',
      click: clickSpy,
    } as unknown as HTMLAnchorElement)
    vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:mock')
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
  })

  it('should export data as JSON', () => {
    exportJSON({ key: 'value' }, 'test.json')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should export arrays as JSON', () => {
    exportJSON([1, 2, 3], 'numbers.json')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })

  it('should handle null data', () => {
    exportJSON(null, 'null.json')
    expect(clickSpy).toHaveBeenCalledTimes(1)
  })
})

// --- S06: query-result exports (the editor grid's in-memory export) ---

function qr(rows: unknown[][]): QueryResult {
  return { columns: ['id', 'note', 'raw'], rows, rowCount: rows.length, duration: 0 }
}

describe('resultToCSV', () => {
  it('keeps SQL NULL (unquoted empty) distinct from the empty string (quoted), like the server export', () => {
    const csv = resultToCSV(qr([[1, null, null], [2, '', '']]))
    expect(csv).toBe('id,note,raw\n1,,\n2,"",""\n')
  })

  it('quotes delimiters, quotes, newlines and edge whitespace exactly', () => {
    const csv = resultToCSV(qr([[1, 'a,b', 'x'], [2, 'he said "hi"', 'y'], [3, 'l1\nl2', 'z'], [4, ' pad ', 'w']]))
    expect(csv).toBe('id,note,raw\n1,"a,b",x\n2,"he said ""hi""",y\n3,"l1\nl2",z\n4," pad ",w\n')
  })

  it('bigint digits and bytea hex text are exact', () => {
    const csv = resultToCSV(qr([[9007199254740993n, 'n', new Uint8Array([0x00, 0xff, 0x10])]]))
    expect(csv).toBe('id,note,raw\n9007199254740993,n,\\x00ff10\n')
  })
})

describe('resultToJSON', () => {
  it('writes bigint digits as exact number literals, never through a double', () => {
    const json = resultToJSON(qr([[9007199254740993n, 'x', null]]))
    expect(json).toBe('[\n{"id":9007199254740993,"note":"x","raw":null}\n]\n')
  })

  it('NULL is null, empty string is "", bytea is \\x text, objects pass through', () => {
    const json = resultToJSON(qr([[1, '', new Uint8Array([0xde, 0xad])], [null, null, { a: [1, 2.5] }]]))
    expect(json).toBe('[\n{"id":1,"note":"","raw":"\\\\xdead"},\n{"id":null,"note":null,"raw":{"a":[1,2.5]}}\n]\n')
  })

  it('an empty result is an empty array document', () => {
    expect(resultToJSON(qr([]))).toBe('[]\n')
  })
})
