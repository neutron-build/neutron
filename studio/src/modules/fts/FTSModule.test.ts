import { describe, it, expect } from 'vitest'
import { parseHits } from './FTSModule'

// Tests for FTSModule: JSON hit parsing + real SQL query building.
// Nucleus FTS is global — FTS_SEARCH/FTS_FUZZY_SEARCH take no index name and
// return a JSON array of { doc_id, score } (no snippet).

describe('FTSModule — parseHits', () => {
  it('should return empty for null', () => {
    expect(parseHits(null)).toEqual([])
    expect(parseHits(undefined)).toEqual([])
  })

  it('should parse a JSON string array of doc_id/score', () => {
    const hits = parseHits('[{"doc_id":42,"score":1.5},{"doc_id":7,"score":0.25}]')
    expect(hits.length).toBe(2)
    expect(hits[0].docId).toBe('42')
    expect(hits[0].score).toBeCloseTo(1.5)
    expect(hits[1].docId).toBe('7')
    expect(hits[1].score).toBeCloseTo(0.25)
  })

  it('should accept an already-parsed array', () => {
    const hits = parseHits([{ doc_id: 1, score: 3 }])
    expect(hits.length).toBe(1)
    expect(hits[0].docId).toBe('1')
    expect(hits[0].score).toBe(3)
  })

  it('should return empty for invalid JSON', () => {
    expect(parseHits('not json')).toEqual([])
  })

  it('should return empty for a non-array cell', () => {
    expect(parseHits('{"doc_id":1}')).toEqual([])
  })
})

describe('FTSModule — query building', () => {
  it('should build FTS_SEARCH query with no index name', () => {
    const query = 'hello world'
    const limit = 25
    const sql = `SELECT FTS_SEARCH('${query.replace(/'/g, "''")}', ${limit})`
    expect(sql).toBe("SELECT FTS_SEARCH('hello world', 25)")
  })

  it('should build FTS_FUZZY_SEARCH query with max distance', () => {
    const query = 'helo'
    const maxDistance = 2
    const limit = 10
    const sql = `SELECT FTS_FUZZY_SEARCH('${query.replace(/'/g, "''")}', ${maxDistance}, ${limit})`
    expect(sql).toBe("SELECT FTS_FUZZY_SEARCH('helo', 2, 10)")
  })

  it('should build FTS_DOC_COUNT query with no args', () => {
    expect(`SELECT FTS_DOC_COUNT()`).toBe('SELECT FTS_DOC_COUNT()')
  })

  it('should escape single quotes in search query', () => {
    const query = "it's a test"
    const escaped = query.replace(/'/g, "''")
    expect(escaped).toBe("it''s a test")
  })
})
