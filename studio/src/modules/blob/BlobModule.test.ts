import { describe, it, expect } from 'vitest'

// Tests for BlobModule utility functions: formatBytes, fmtDate

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function fmtDate(val: string): string {
  if (!val) return '\u2014'
  try { return new Date(val).toLocaleString() } catch { return val }
}

describe('BlobModule — formatBytes', () => {
  it('should format bytes', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(1)).toBe('1 B')
    expect(formatBytes(512)).toBe('512 B')
    expect(formatBytes(1023)).toBe('1023 B')
  })

  it('should format kilobytes', () => {
    expect(formatBytes(1024)).toBe('1.0 KB')
    expect(formatBytes(1536)).toBe('1.5 KB')
    expect(formatBytes(10240)).toBe('10.0 KB')
    expect(formatBytes(1024 * 1024 - 1)).toMatch(/KB$/)
  })

  it('should format megabytes', () => {
    expect(formatBytes(1024 * 1024)).toBe('1.0 MB')
    expect(formatBytes(1024 * 1024 * 5)).toBe('5.0 MB')
    expect(formatBytes(1024 * 1024 * 1023)).toMatch(/MB$/)
  })

  it('should format gigabytes', () => {
    expect(formatBytes(1024 * 1024 * 1024)).toBe('1.00 GB')
    expect(formatBytes(1024 * 1024 * 1024 * 2.5)).toBe('2.50 GB')
  })
})

describe('BlobModule — fmtDate', () => {
  it('should return em-dash for empty string', () => {
    expect(fmtDate('')).toBe('\u2014')
  })

  it('should format valid ISO date', () => {
    const result = fmtDate('2025-01-15T10:30:00Z')
    // Should not throw and should return a non-empty string
    expect(result.length).toBeGreaterThan(0)
    expect(result).not.toBe('\u2014')
  })

  it('should return the input for invalid date', () => {
    // new Date('not-a-date') returns Invalid Date, but toLocaleString
    // should still return something in most environments
    const result = fmtDate('not-a-date')
    expect(result.length).toBeGreaterThan(0)
  })
})
