import { describe, it, expect } from 'vitest'
import type { TabKind, Schema, NucleusFeatures } from '../lib/types'

// Tests the SchemaTree section-building logic extracted from the component.

interface Section {
  model: string
  label: string
  kind: TabKind
  items: { name: string; sub?: string }[]
  nucleusOnly: boolean
  fallback: string
}

function buildSections(sc: Schema): Section[] {
  return [
    {
      model: 'sql',
      label: 'SQL',
      kind: 'sql-browser',
      items: sc.sql.map(t => ({ name: t.name, sub: t.schema !== 'public' ? t.schema : undefined })),
      nucleusOnly: false,
      fallback: '',
    },
    { model: 'kv', label: 'Key-Value', kind: 'kv', items: sc.kv.map(k => ({ name: k.name })), nucleusOnly: true, fallback: 'keyspace' },
    { model: 'vector', label: 'Vector', kind: 'vector', items: sc.vector.map(v => ({ name: v.name })), nucleusOnly: true, fallback: 'search' },
    { model: 'timeseries', label: 'TimeSeries', kind: 'timeseries', items: sc.timeseries.map(t => ({ name: t.name })), nucleusOnly: true, fallback: 'series' },
    { model: 'document', label: 'Document', kind: 'document', items: sc.document.map(d => ({ name: d.name })), nucleusOnly: true, fallback: 'documents' },
    { model: 'graph', label: 'Graph', kind: 'graph', items: sc.graph.map(g => ({ name: g.name })), nucleusOnly: true, fallback: 'graph' },
    { model: 'fts', label: 'Full-Text', kind: 'fts', items: sc.fts.map(f => ({ name: f.name })), nucleusOnly: true, fallback: 'index' },
    { model: 'geo', label: 'Geo', kind: 'geo', items: sc.geo.map(g => ({ name: g.name })), nucleusOnly: true, fallback: 'calculator' },
    { model: 'blob', label: 'Blob', kind: 'blob', items: sc.blob.map(b => ({ name: b.name })), nucleusOnly: true, fallback: 'blobs' },
    { model: 'pubsub', label: 'PubSub', kind: 'pubsub', items: sc.pubsub.map(p => ({ name: p.name })), nucleusOnly: true, fallback: 'channel' },
    { model: 'streams', label: 'Streams', kind: 'streams', items: sc.streams.map(st => ({ name: st.name })), nucleusOnly: true, fallback: 'stream' },
    { model: 'columnar', label: 'Columnar', kind: 'columnar', items: sc.columnar.map(c => ({ name: c.name })), nucleusOnly: true, fallback: 'table' },
    { model: 'datalog', label: 'Datalog', kind: 'datalog', items: sc.datalog ? [{ name: 'datalog' }] : [], nucleusOnly: true, fallback: 'datalog' },
    { model: 'cdc', label: 'CDC', kind: 'cdc', items: sc.cdc ? [{ name: 'changes' }] : [], nucleusOnly: true, fallback: 'changes' },
  ]
}

function filterVisible(sections: Section[], ft: NucleusFeatures): Section[] {
  return sections.filter(sec => !sec.nucleusOnly || ft.isNucleus)
}

// Mirrors TreeSection: an empty Nucleus model section still opens its browser
// via the fallback entry — the engine has no enumeration surface for several
// models (vector, timeseries, geo, streams, columnar, datalog), and an empty
// or RLS-sealed store must not make the browser unreachable.
function sectionItems(section: Section): { name: string; sub?: string }[] {
  return section.items.length > 0
    ? section.items
    : section.nucleusOnly && section.fallback
      ? [{ name: section.fallback }]
      : []
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

  it('should still open every Nucleus browser when nothing is enumerable', () => {
    const sections = buildSections(EMPTY_SCHEMA)
    for (const sec of sections.filter(s => s.nucleusOnly)) {
      const items = sectionItems(sec)
      expect(items.length, sec.model).toBe(1)
      expect(items[0].name, sec.model).toBe(sec.fallback)
    }
  })

  it('should show no fallback entry for the SQL section (not a Nucleus model)', () => {
    const sqlSection = buildSections(EMPTY_SCHEMA)[0]
    expect(sectionItems(sqlSection)).toEqual([])
  })

  it('should prefer real items over the fallback when the backend listed objects', () => {
    const schema: Schema = {
      ...EMPTY_SCHEMA,
      kv: [{ name: 'keyspace', keyCount: 3 }],
      cdc: true,
    }
    const sections = buildSections(schema)
    expect(sectionItems(sections.find(s => s.model === 'kv')!)).toEqual([{ name: 'keyspace' }])
    expect(sectionItems(sections.find(s => s.model === 'cdc')!)).toEqual([{ name: 'changes' }])
  })

  it('should show datalog items when datalog is not null', () => {
    const withDatalog: Schema = { ...EMPTY_SCHEMA, datalog: { predicateCount: 5, ruleCount: 3 } }
    const without: Schema = { ...EMPTY_SCHEMA, datalog: null }

    const sectionsWithDL = buildSections(withDatalog)
    const datalogSection = sectionsWithDL.find(s => s.model === 'datalog')!
    expect(datalogSection.items.length).toBe(1)

    const sectionsWithoutDL = buildSections(without)
    const emptyDL = sectionsWithoutDL.find(s => s.model === 'datalog')!
    expect(emptyDL.items.length).toBe(0)
    // The browser stays reachable through the fallback entry
    expect(sectionItems(emptyDL)).toEqual([{ name: 'datalog' }])
  })

  it('should show CDC items when cdc is true', () => {
    const withCDC: Schema = { ...EMPTY_SCHEMA, cdc: true }
    const without: Schema = { ...EMPTY_SCHEMA, cdc: false }

    const cdcWith = buildSections(withCDC).find(s => s.model === 'cdc')!
    expect(cdcWith.items.length).toBe(1)
    expect(cdcWith.items[0].name).toBe('changes')

    const cdcWithout = buildSections(without).find(s => s.model === 'cdc')!
    expect(cdcWithout.items.length).toBe(0)
    expect(sectionItems(cdcWithout)).toEqual([{ name: 'changes' }])
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
