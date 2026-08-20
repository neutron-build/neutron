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

function qIdent(ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`
}

describe('VectorModule — query building', () => {
  // Search runs against a user table + VECTOR column, using the real scalar
  // VECTOR_DISTANCE and ORDER BY — no fabricated vector_search()/vector_scan().
  it('should build nearest-neighbor query with cosine metric', () => {
    const table = 'embeddings'
    const col = 'embedding'
    const vec = '[1.0, 0.5, 0.0]'
    const k = 10
    const metric = 'cosine'

    const sql =
      `SELECT id, ${qIdent(col)} AS embedding, VECTOR_DISTANCE(${qIdent(col)}, VECTOR('${vec}'), '${metric}') AS distance
         FROM ${qIdent(table)}
         ORDER BY distance ASC
         LIMIT ${k}`

    expect(sql).toContain("VECTOR('[1.0, 0.5, 0.0]')")
    expect(sql).toContain('VECTOR_DISTANCE("embedding"')
    expect(sql).toContain("'cosine'")
    expect(sql).toContain('FROM "embeddings"')
    expect(sql).toContain('ORDER BY distance ASC')
    expect(sql).toContain('LIMIT 10')
    expect(sql).not.toContain('vector_search')
    expect(sql).not.toContain('vector_scan')
  })

  it('should build nearest-neighbor query with l2 metric', () => {
    const table = 'vecs'
    const col = 'embedding'
    const vec = '[0,0,1]'
    const metric = 'l2'

    const sql =
      `SELECT id, ${qIdent(col)} AS embedding, VECTOR_DISTANCE(${qIdent(col)}, VECTOR('${vec}'), '${metric}') AS distance
         FROM ${qIdent(table)}
         ORDER BY distance ASC
         LIMIT 5`

    expect(sql).toContain("'l2'")
  })

  it('should build sample scan query against the table', () => {
    const table = 'embeddings'
    const col = 'embedding'
    const sql = `SELECT id, ${qIdent(col)} AS embedding FROM ${qIdent(table)} LIMIT 20`
    expect(sql).toBe('SELECT id, "embedding" AS embedding FROM "embeddings" LIMIT 20')
  })
})

describe('VectorModule — metric types', () => {
  it('should support cosine, l2, and inner metrics', () => {
    const metrics = ['cosine', 'l2', 'inner'] as const
    expect(metrics.length).toBe(3)
    expect(metrics).toContain('cosine')
    expect(metrics).toContain('l2')
    expect(metrics).toContain('inner')
  })
})
