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
  // The KV store is a single global keyspace: no store-name argument.
  it('should build KV_KEYS enumeration query', () => {
    const query = `SELECT KV_KEYS('*')`
    expect(query).toBe("SELECT KV_KEYS('*')")
  })

  it('should build a KV_GET value fetch query', () => {
    const keys = ['mykey', "key'q"]
    const cols = keys.map(k => `KV_GET(${sqlStr(k)})`).join(', ')
    const query = `SELECT ${cols}`
    expect(query).toBe("SELECT KV_GET('mykey'), KV_GET('key''q')")
  })

  it('should fetch value and remaining TTL per key in one select', () => {
    // KV_TTL(key) → remaining seconds; -1 = no TTL, -2 = missing
    const keys = ['a', 'b']
    const cols = keys.flatMap(k => [`KV_GET(${sqlStr(k)})`, `KV_TTL(${sqlStr(k)})`]).join(', ')
    const query = `SELECT ${cols}`
    expect(query).toBe("SELECT KV_GET('a'), KV_TTL('a'), KV_GET('b'), KV_TTL('b')")
  })

  it('should map engine TTL sentinels to null (no expiry / missing)', () => {
    function ttlFromEngine(v: unknown): number | null {
      if (v == null || v === '') return null
      const n = Number(v)
      return Number.isFinite(n) && n >= 0 ? n : null
    }
    expect(ttlFromEngine(3563)).toBe(3563)
    expect(ttlFromEngine(0)).toBe(0)
    expect(ttlFromEngine(-1)).toBe(null) // no TTL
    expect(ttlFromEngine(-2)).toBe(null) // missing key
    expect(ttlFromEngine(null)).toBe(null)
  })

  it('should build KV_SET query without TTL', () => {
    const key = 'mykey'
    const value = 'myvalue'
    const query = `SELECT KV_SET(${sqlStr(key)}, ${sqlStr(value)})`
    expect(query).toBe("SELECT KV_SET('mykey', 'myvalue')")
  })

  it('should build KV_SET query with TTL', () => {
    const key = 'mykey'
    const value = 'myvalue'
    const ttl = 300
    const query = `SELECT KV_SET(${sqlStr(key)}, ${sqlStr(value)}, ${ttl})`
    expect(query).toBe("SELECT KV_SET('mykey', 'myvalue', 300)")
  })

  it('should build KV_DEL query', () => {
    const key = "key'with'quotes"
    const query = `SELECT KV_DEL(${sqlStr(key)})`
    expect(query).toBe("SELECT KV_DEL('key''with''quotes')")
  })
})
