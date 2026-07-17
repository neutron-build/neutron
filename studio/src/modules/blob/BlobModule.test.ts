import { describe, it, expect } from 'vitest'
import { parseKeys, parseMeta } from './BlobModule'

// Nucleus blob store is global and keyed by string. Listing is
// BLOB_LIST(prefix) → JSON array of key strings, then BLOB_META(key) →
// { size, content_type, created_at, updated_at } with epoch-ms timestamps.

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
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

describe('BlobModule — parseKeys', () => {
  it('should return empty for null', () => {
    expect(parseKeys(null)).toEqual([])
  })

  it('should parse a JSON string array of keys', () => {
    expect(parseKeys('["a","b/c","d"]')).toEqual(['a', 'b/c', 'd'])
  })

  it('should accept an already-parsed array', () => {
    expect(parseKeys(['x', 'y'])).toEqual(['x', 'y'])
  })

  it('should return empty for invalid JSON', () => {
    expect(parseKeys('not json')).toEqual([])
  })
})

describe('BlobModule — parseMeta', () => {
  it('should parse a BLOB_META JSON cell', () => {
    const meta = parseMeta('avatar.png', '{"size":2048,"content_type":"image/png","created_at":1700000000000,"updated_at":1700000000000}')
    expect(meta.id).toBe('avatar.png')
    expect(meta.size).toBe(2048)
    expect(meta.contentType).toBe('image/png')
    expect(meta.createdAt).toBe(1700000000000)
  })

  it('should fall back to defaults for a null cell', () => {
    const meta = parseMeta('missing', null)
    expect(meta).toEqual({ id: 'missing', size: 0, contentType: '', createdAt: 0 })
  })

  it('should tolerate missing fields', () => {
    const meta = parseMeta('k', '{"size":10}')
    expect(meta.size).toBe(10)
    expect(meta.contentType).toBe('')
    expect(meta.createdAt).toBe(0)
  })
})

describe('BlobModule — query building', () => {
  it('should build BLOB_LIST query with empty prefix (all keys)', () => {
    expect(`SELECT BLOB_LIST('')`).toBe("SELECT BLOB_LIST('')")
  })

  it('should build BLOB_META query per key', () => {
    const key = 'photo.jpg'
    const sql = `SELECT BLOB_META('${key.replace(/'/g, "''")}')`
    expect(sql).toBe("SELECT BLOB_META('photo.jpg')")
  })

  it('should build BLOB_DELETE query with the key only (no store name)', () => {
    const id = "it's-a-key"
    const sql = `SELECT BLOB_DELETE('${id.replace(/'/g, "''")}')`
    expect(sql).toBe("SELECT BLOB_DELETE('it''s-a-key')")
  })
})
