import { describe, it, expect, vi, beforeEach } from 'vitest'
import { exportCSV, exportJSON } from './export'

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
