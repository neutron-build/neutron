import { describe, it, expect } from 'vitest'

// Tests for KV module utility functions extracted from KVModule.tsx

function sqlStr(s: string): string {
  return `'${s.replace(/'/g, "''")}'`
}

function formatTTL(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  return `${Math.floor(seconds / 3600)}h`
}

describe('KVModule — sqlStr', () => {
  it('should wrap a simple string in single quotes', () => {
    expect(sqlStr('hello')).toBe("'hello'")
  })

  it('should escape single quotes by doubling them', () => {
    expect(sqlStr("it's")).toBe("'it''s'")
  })

  it('should handle multiple single quotes', () => {
    expect(sqlStr("it's a 'test'")).toBe("'it''s a ''test'''")
  })

  it('should handle empty string', () => {
    expect(sqlStr('')).toBe("''")
  })

  it('should handle string with no quotes', () => {
    expect(sqlStr('key123')).toBe("'key123'")
  })
})

describe('KVModule — formatTTL', () => {
  it('should format seconds under 60 as seconds', () => {
    expect(formatTTL(0)).toBe('0s')
    expect(formatTTL(1)).toBe('1s')
    expect(formatTTL(59)).toBe('59s')
  })

  it('should format 60-3599 as minutes', () => {
    expect(formatTTL(60)).toBe('1m')
    expect(formatTTL(120)).toBe('2m')
    expect(formatTTL(90)).toBe('1m')
    expect(formatTTL(3599)).toBe('59m')
  })

  it('should format 3600+ as hours', () => {
    expect(formatTTL(3600)).toBe('1h')
    expect(formatTTL(7200)).toBe('2h')
    expect(formatTTL(86400)).toBe('24h')
  })
})

describe('KVModule — filter logic', () => {
  interface KVEntry { key: string; value: string; ttl: number | null }

  const entries: KVEntry[] = [
    { key: 'user:1', value: 'Alice', ttl: null },
    { key: 'user:2', value: 'Bob', ttl: 300 },
    { key: 'session:abc', value: 'data', ttl: 3600 },
    { key: 'config:theme', value: 'dark', ttl: null },
  ]

  function filterEntries(entries: KVEntry[], filterText: string): KVEntry[] {
    return filterText
      ? entries.filter(e => e.key.includes(filterText))
      : entries
  }

  it('should return all entries when filter is empty', () => {
    expect(filterEntries(entries, '')).toEqual(entries)
  })

  it('should filter by key prefix', () => {
    const result = filterEntries(entries, 'user:')
    expect(result.length).toBe(2)
    expect(result.every(e => e.key.startsWith('user:'))).toBe(true)
  })

  it('should return empty when no match', () => {
    expect(filterEntries(entries, 'nonexistent')).toEqual([])
  })

  it('should match partial key', () => {
    const result = filterEntries(entries, 'session')
    expect(result.length).toBe(1)
    expect(result[0].key).toBe('session:abc')
  })
})

describe('KVModule — query building', () => {
  it('should build kv_scan query', () => {
    const name = 'cache'
    const query = `SELECT key, value, ttl FROM kv_scan(${sqlStr(name)}, '*', 500)`
    expect(query).toBe("SELECT key, value, ttl FROM kv_scan('cache', '*', 500)")
  })

  it('should build kv_set query without TTL', () => {
    const name = 'cache'
    const key = 'mykey'
    const value = 'myvalue'
    const query = `SELECT kv_set(${sqlStr(name)}, ${sqlStr(key)}, ${sqlStr(value)})`
    expect(query).toBe("SELECT kv_set('cache', 'mykey', 'myvalue')")
  })

  it('should build kv_set query with TTL', () => {
    const name = 'cache'
    const key = 'mykey'
    const value = 'myvalue'
    const ttl = 300
    const query = `SELECT kv_set(${sqlStr(name)}, ${sqlStr(key)}, ${sqlStr(value)}, ${ttl})`
    expect(query).toBe("SELECT kv_set('cache', 'mykey', 'myvalue', 300)")
  })

  it('should build kv_delete query', () => {
    const name = 'cache'
    const key = "key'with'quotes"
    const query = `SELECT kv_delete(${sqlStr(name)}, ${sqlStr(key)})`
    expect(query).toBe("SELECT kv_delete('cache', 'key''with''quotes')")
  })
})
