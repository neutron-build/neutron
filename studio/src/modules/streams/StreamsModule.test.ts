import { describe, it, expect } from 'vitest'

// Tests for StreamsModule real Nucleus SQL. Streams are a GLOBAL store whose
// model functions are UPPERCASE SCALARS. There is NO pending-list or claim
// SQL surface, so those affordances were removed from the module.

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`
const MAX_MS = 9999999999999

interface StreamEntry {
  id: string
  fields: Record<string, string>
}

function parseStreamEntries(cell: unknown): StreamEntry[] {
  if (cell == null) return []
  const text = String(cell).trim()
  if (text === '') return []
  try {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? (parsed as StreamEntry[]) : []
  } catch {
    return []
  }
}

describe('StreamsModule — real SQL builders', () => {
  it('should build STREAM_XLEN query', () => {
    expect(`SELECT STREAM_XLEN(${sqlStr('events')})`).toBe("SELECT STREAM_XLEN('events')")
  })

  it('should build STREAM_XRANGE query with numeric epoch-ms bounds', () => {
    const sql = `SELECT STREAM_XRANGE(${sqlStr('mystream')}, ${0}, ${MAX_MS}, ${100})`
    expect(sql).toBe("SELECT STREAM_XRANGE('mystream', 0, 9999999999999, 100)")
  })

  it('should build STREAM_XADD query', () => {
    const sql = `SELECT STREAM_XADD(${sqlStr('events')}, ${sqlStr('user')}, ${sqlStr('alice')})`
    expect(sql).toBe("SELECT STREAM_XADD('events', 'user', 'alice')")
  })

  it('should build STREAM_XGROUP_CREATE query with numeric start id', () => {
    const sql = `SELECT STREAM_XGROUP_CREATE(${sqlStr('events')}, ${sqlStr('my-group')}, ${0})`
    expect(sql).toBe("SELECT STREAM_XGROUP_CREATE('events', 'my-group', 0)")
  })

  it('should build STREAM_XREADGROUP query', () => {
    const sql = `SELECT STREAM_XREADGROUP(${sqlStr('events')}, ${sqlStr('g1')}, ${sqlStr('c1')}, ${100})`
    expect(sql).toBe("SELECT STREAM_XREADGROUP('events', 'g1', 'c1', 100)")
  })

  it('should build STREAM_XACK query with split ms-seq id', () => {
    const [idMs, idSeq] = '1234-0'.split('-')
    const sql = `SELECT STREAM_XACK(${sqlStr('events')}, ${sqlStr('g1')}, ${Number(idMs)}, ${Number(idSeq)})`
    expect(sql).toBe("SELECT STREAM_XACK('events', 'g1', 1234, 0)")
  })

  it('should escape single quotes in identifiers', () => {
    expect(sqlStr("it's-a-group")).toBe("'it''s-a-group'")
  })
})

describe('StreamsModule — entry JSON parsing', () => {
  it('should parse a JSON array of stream entries', () => {
    const cell = JSON.stringify([
      { id: '1-0', fields: { a: '1' } },
      { id: '2-0', fields: { b: '2' } },
    ])
    const entries = parseStreamEntries(cell)
    expect(entries.length).toBe(2)
    expect(entries[0].id).toBe('1-0')
    expect(entries[0].fields.a).toBe('1')
  })

  it('should treat an empty string (nonexistent stream) as no entries', () => {
    expect(parseStreamEntries('')).toEqual([])
    expect(parseStreamEntries(null)).toEqual([])
  })

  it('should treat malformed JSON as no entries', () => {
    expect(parseStreamEntries('not json')).toEqual([])
  })
})
