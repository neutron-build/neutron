import { describe, it, expect } from 'vitest'
import type { TabKind, Schema, NucleusFeatures } from '../lib/types'

// Tests the SchemaTree section-building logic extracted from the component.

interface Section {
  model: string
  label: string
  kind: TabKind
  items: { name: string; sub?: string }[]
  nucleusOnly: boolean
}

function buildSections(sc: Schema): Section[] {
  return [
    {
      model: 'sql',
      label: 'SQL',
      kind: 'sql-browser',
      items: sc.sql.map(t => ({ name: t.name, sub: t.schema !== 'public' ? t.schema : undefined })),
      nucleusOnly: false,
    },
    { model: 'kv', label: 'Key-Value', kind: 'kv', items: sc.kv.map(k => ({ name: k.name })), nucleusOnly: true },
    { model: 'vector', label: 'Vector', kind: 'vector', items: sc.vector.map(v => ({ name: v.name })), nucleusOnly: true },
    { model: 'timeseries', label: 'TimeSeries', kind: 'timeseries', items: sc.timeseries.map(t => ({ name: t.name })), nucleusOnly: true },
    { model: 'document', label: 'Document', kind: 'document', items: sc.document.map(d => ({ name: d.name })), nucleusOnly: true },
    { model: 'graph', label: 'Graph', kind: 'graph', items: sc.graph.map(g => ({ name: g.name })), nucleusOnly: true },
    { model: 'fts', label: 'Full-Text', kind: 'fts', items: sc.fts.map(f => ({ name: f.name })), nucleusOnly: true },
    { model: 'geo', label: 'Geo', kind: 'geo', items: sc.geo.map(g => ({ name: g.name })), nucleusOnly: true },
    { model: 'blob', label: 'Blob', kind: 'blob', items: sc.blob.map(b => ({ name: b.name })), nucleusOnly: true },
    { model: 'pubsub', label: 'PubSub', kind: 'pubsub', items: sc.pubsub.map(p => ({ name: p.name })), nucleusOnly: true },
    { model: 'streams', label: 'Streams', kind: 'streams', items: sc.streams.map(st => ({ name: st.name })), nucleusOnly: true },
    { model: 'columnar', label: 'Columnar', kind: 'columnar', items: sc.columnar.map(c => ({ name: c.name })), nucleusOnly: true },
    { model: 'datalog', label: 'Datalog', kind: 'datalog', items: sc.datalog ? [{ name: 'datalog' }] : [], nucleusOnly: true },
    { model: 'cdc', label: 'CDC', kind: 'cdc', items: sc.cdc ? [{ name: 'changes' }] : [], nucleusOnly: true },
  ]
}

function filterVisible(sections: Section[], ft: NucleusFeatures): Section[] {
  return sections.filter(sec => !sec.nucleusOnly || ft.isNucleus)
}

const EMPTY_SCHEMA: Schema = {
  sql: [], kv: [], vector: [], timeseries: [], document: [], graph: [],
  fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [],
  datalog: null, cdc: false,
}

describe('SchemaTree section building', () => {
  it('should build 14 sections for a full schema', () => {
    const sections = buildSections(EMPTY_SCHEMA)
    expect(sections.length).toBe(14)
  })

  it('should only include SQL section for non-Nucleus connections', () => {
    const sections = buildSections(EMPTY_SCHEMA)
    const visible = filterVisible(sections, { isNucleus: false, version: '', models: [] })
    expect(visible.length).toBe(1)
    expect(visible[0].model).toBe('sql')
  })

  it('should include all sections for Nucleus connections', () => {
    const sections = buildSections(EMPTY_SCHEMA)
    const visible = filterVisible(sections, { isNucleus: true, version: '0.1.0', models: [] })
    expect(visible.length).toBe(14)
  })

  it('should populate SQL items with name and sub schema', () => {
    const schema: Schema = {
      ...EMPTY_SCHEMA,
      sql: [
        { schema: 'public', name: 'users', columns: [] },
        { schema: 'audit', name: 'logs', columns: [] },
      ],
    }
    const sections = buildSections(schema)
    const sqlSection = sections[0]
    expect(sqlSection.items.length).toBe(2)
    expect(sqlSection.items[0]).toEqual({ name: 'users', sub: undefined })
    expect(sqlSection.items[1]).toEqual({ name: 'logs', sub: 'audit' })
  })

  it('should show datalog section only when datalog is not null', () => {
    const withDatalog: Schema = { ...EMPTY_SCHEMA, datalog: { predicateCount: 5, ruleCount: 3 } }
    const without: Schema = { ...EMPTY_SCHEMA, datalog: null }

    const sectionsWithDL = buildSections(withDatalog)
    const datalogSection = sectionsWithDL.find(s => s.model === 'datalog')!
    expect(datalogSection.items.length).toBe(1)

    const sectionsWithoutDL = buildSections(without)
    const emptyDL = sectionsWithoutDL.find(s => s.model === 'datalog')!
    expect(emptyDL.items.length).toBe(0)
  })

  it('should show CDC section only when cdc is true', () => {
    const withCDC: Schema = { ...EMPTY_SCHEMA, cdc: true }
    const without: Schema = { ...EMPTY_SCHEMA, cdc: false }

    const cdcWith = buildSections(withCDC).find(s => s.model === 'cdc')!
    expect(cdcWith.items.length).toBe(1)
    expect(cdcWith.items[0].name).toBe('changes')

    const cdcWithout = buildSections(without).find(s => s.model === 'cdc')!
    expect(cdcWithout.items.length).toBe(0)
  })

  it('should populate KV, Vector, TimeSeries items', () => {
    const schema: Schema = {
      ...EMPTY_SCHEMA,
      kv: [{ name: 'cache', keyCount: 100 }],
      vector: [{ name: 'embeddings', dimensions: 384, metric: 'cosine', count: 500 }],
      timeseries: [{ name: 'cpu', count: 1000 }],
    }
    const sections = buildSections(schema)
    expect(sections.find(s => s.model === 'kv')!.items).toEqual([{ name: 'cache' }])
    expect(sections.find(s => s.model === 'vector')!.items).toEqual([{ name: 'embeddings' }])
    expect(sections.find(s => s.model === 'timeseries')!.items).toEqual([{ name: 'cpu' }])
  })
})
