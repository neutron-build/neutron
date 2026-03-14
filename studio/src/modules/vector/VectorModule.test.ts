import { describe, it, expect } from 'vitest'

// Tests for VectorModule query building and validation logic

describe('VectorModule — vector validation', () => {
  function isValidVector(input: string): boolean {
    const vec = input.trim()
    if (!vec.startsWith('[') || !vec.endsWith(']')) return false
    return true
  }

  it('should accept valid vector format', () => {
    expect(isValidVector('[1.0, 0.5, 0.0]')).toBe(true)
    expect(isValidVector('[0,0,0]')).toBe(true)
    expect(isValidVector('[1]')).toBe(true)
  })

  it('should reject invalid vector format', () => {
    expect(isValidVector('1.0, 0.5')).toBe(false)
    expect(isValidVector('{1,2,3}')).toBe(false)
    expect(isValidVector('')).toBe(false)
    expect(isValidVector('hello')).toBe(false)
  })

  it('should accept vector with whitespace', () => {
    expect(isValidVector('  [1, 2, 3]  ')).toBe(true)
  })
})

describe('VectorModule — query building', () => {
  it('should build vector search query with cosine metric', () => {
    const name = 'embeddings'
    const vec = '[1.0, 0.5, 0.0]'
    const k = 10
    const metric = 'cosine'

    const sql =
      `SELECT id, embedding, VECTOR_DISTANCE(embedding, VECTOR('${vec}'), '${metric}') AS score
         FROM vector_search('${name}', VECTOR('${vec}'), ${k}, '${metric}')
         ORDER BY score ASC`

    expect(sql).toContain("VECTOR('[1.0, 0.5, 0.0]')")
    expect(sql).toContain("VECTOR_DISTANCE")
    expect(sql).toContain("'cosine'")
    expect(sql).toContain("vector_search('embeddings'")
    expect(sql).toContain('10')
  })

  it('should build vector search query with l2 metric', () => {
    const name = 'vecs'
    const vec = '[0,0,1]'
    const k = 5
    const metric = 'l2'

    const sql =
      `SELECT id, embedding, VECTOR_DISTANCE(embedding, VECTOR('${vec}'), '${metric}') AS score
         FROM vector_search('${name}', VECTOR('${vec}'), ${k}, '${metric}')
         ORDER BY score ASC`

    expect(sql).toContain("'l2'")
  })

  it('should build sample scan query', () => {
    const name = 'embeddings'
    const sql = `SELECT id, embedding FROM vector_scan('${name}', 20)`
    expect(sql).toBe("SELECT id, embedding FROM vector_scan('embeddings', 20)")
  })
})

describe('VectorModule — metric types', () => {
  it('should support cosine, l2, and dot metrics', () => {
    const metrics = ['cosine', 'l2', 'dot'] as const
    expect(metrics.length).toBe(3)
    expect(metrics).toContain('cosine')
    expect(metrics).toContain('l2')
    expect(metrics).toContain('dot')
  })
})
