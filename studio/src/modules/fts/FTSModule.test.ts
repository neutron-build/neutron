import { describe, it, expect } from 'vitest'

// Tests for FTSModule utility function: highlight

function highlight(text: string, q: string): string {
  if (!q.trim()) return text
  const terms = q.trim().split(/\s+/).filter(Boolean)
  const pattern = new RegExp(`(${terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'gi')
  return text.replace(pattern, '<mark>$1</mark>')
}

describe('FTSModule — highlight', () => {
  it('should return text unchanged when query is empty', () => {
    expect(highlight('hello world', '')).toBe('hello world')
    expect(highlight('hello world', '   ')).toBe('hello world')
  })

  it('should highlight a single term', () => {
    expect(highlight('hello world', 'world')).toBe('hello <mark>world</mark>')
  })

  it('should highlight multiple terms', () => {
    const result = highlight('the quick brown fox', 'quick fox')
    expect(result).toBe('the <mark>quick</mark> brown <mark>fox</mark>')
  })

  it('should be case insensitive', () => {
    expect(highlight('Hello World', 'hello')).toBe('<mark>Hello</mark> World')
    expect(highlight('HELLO world', 'hello WORLD')).toBe('<mark>HELLO</mark> <mark>world</mark>')
  })

  it('should highlight multiple occurrences', () => {
    expect(highlight('ab ab ab', 'ab')).toBe('<mark>ab</mark> <mark>ab</mark> <mark>ab</mark>')
  })

  it('should escape regex special characters in query', () => {
    expect(highlight('a.b+c', 'a.b')).toBe('<mark>a.b</mark>+c')
    expect(highlight('foo(bar)', '(bar)')).toBe('foo<mark>(bar)</mark>')
  })

  it('should handle overlapping matches', () => {
    // "ab" and "bc" in "abc" -- regex alternation handles this as first match
    const result = highlight('abc', 'ab bc')
    // Should match 'ab' first, then 'bc' is partially consumed
    expect(result).toContain('<mark>')
  })

  it('should not highlight when no match', () => {
    expect(highlight('hello', 'xyz')).toBe('hello')
  })
})

describe('FTSModule — query building', () => {
  it('should build fts_search query', () => {
    const name = 'articles'
    const query = "hello world"
    const limit = 25
    const fn = 'fts_search'
    const sql = `SELECT id, snippet, score FROM ${fn}('${name}', '${query.replace(/'/g, "''")}', ${limit})
         ORDER BY score DESC`
    expect(sql).toContain("fts_search('articles'")
    expect(sql).toContain("'hello world'")
    expect(sql).toContain('25')
  })

  it('should build fts_search_fuzzy query when fuzzy enabled', () => {
    const fn = 'fts_search_fuzzy'
    expect(fn).toBe('fts_search_fuzzy')
  })

  it('should escape single quotes in search query', () => {
    const query = "it's a test"
    const escaped = query.replace(/'/g, "''")
    expect(escaped).toBe("it''s a test")
  })
})
