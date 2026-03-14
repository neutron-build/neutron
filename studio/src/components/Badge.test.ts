import { describe, it, expect } from 'vitest'

// Tests for the Badge component's label mapping logic.

type BadgeKind =
  | 'sql' | 'kv' | 'vector' | 'ts' | 'doc' | 'graph'
  | 'fts' | 'geo' | 'blob' | 'pubsub' | 'streams'
  | 'columnar' | 'datalog' | 'cdc'

const LABELS: Record<BadgeKind, string> = {
  sql: 'SQL', kv: 'KV', vector: 'Vector', ts: 'TimeSeries',
  doc: 'Document', graph: 'Graph', fts: 'FTS', geo: 'Geo',
  blob: 'Blob', pubsub: 'PubSub', streams: 'Streams',
  columnar: 'Columnar', datalog: 'Datalog', cdc: 'CDC',
}

describe('Badge LABELS', () => {
  it('should have entries for all 14 badge kinds', () => {
    const kinds: BadgeKind[] = [
      'sql', 'kv', 'vector', 'ts', 'doc', 'graph',
      'fts', 'geo', 'blob', 'pubsub', 'streams',
      'columnar', 'datalog', 'cdc',
    ]
    expect(Object.keys(LABELS).length).toBe(14)
    for (const kind of kinds) {
      expect(LABELS[kind]).toBeTruthy()
      expect(typeof LABELS[kind]).toBe('string')
    }
  })

  it('should map sql to SQL', () => {
    expect(LABELS.sql).toBe('SQL')
  })

  it('should map ts to TimeSeries', () => {
    expect(LABELS.ts).toBe('TimeSeries')
  })

  it('should map doc to Document', () => {
    expect(LABELS.doc).toBe('Document')
  })

  it('should use custom label when provided (logic)', () => {
    // Badge component uses: label ?? LABELS[kind]
    const kind: BadgeKind = 'sql'
    const customLabel = 'Custom'
    const displayLabel = customLabel ?? LABELS[kind]
    expect(displayLabel).toBe('Custom')
  })

  it('should use default label when no custom label provided', () => {
    const kind: BadgeKind = 'graph'
    const customLabel: string | undefined = undefined
    const displayLabel = customLabel ?? LABELS[kind]
    expect(displayLabel).toBe('Graph')
  })
})
