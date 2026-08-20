import { useSignal } from '@preact/signals'
import { schema, features, openTab } from '../lib/store'
import { Badge } from '../components/Badge'
import type { TabKind } from '../lib/types'
import s from './SchemaTree.module.css'

interface Section {
  model: string
  label: string
  kind: TabKind
  items: { name: string; sub?: string }[]
  nucleusOnly: boolean
  // Entry shown when the engine has no enumeration surface for the model
  // (vector, timeseries, geo, streams, columnar, datalog never list; the
  // others list nothing while empty or when RLS seals the store). The
  // browser itself is still usable — the object name is supplied inside it.
  fallback: string
}

export function SchemaTree() {
  const sc = schema.value
  const ft = features.value

  if (!sc) {
    return (
      <div class={s.empty}>
        <span class={s.emptyText}>Loading schema…</span>
      </div>
    )
  }

  const sections: Section[] = [
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

  const visible = sections.filter(sec =>
    !sec.nucleusOnly || ft.isNucleus
  )

  return (
    <div class={s.tree}>
      <button
        class={s.designerBtn}
        onClick={() => openTab({
          id: 'schema-designer',
          kind: 'schema-designer',
          label: 'Schema Designer',
        })}
      >
        ⬡ Schema Designer
      </button>
      {visible.map(sec => (
        <TreeSection key={sec.model} section={sec} />
      ))}
    </div>
  )
}

function TreeSection({ section }: { section: Section }) {
  const open = useSignal(section.model === 'sql')

  // Every Nucleus model keeps its browser reachable: when there is nothing to
  // enumerate (no listing surface, an empty store, or RLS sealing the counts)
  // the fallback entry opens the browser directly.
  const items = section.items.length > 0
    ? section.items
    : section.nucleusOnly && section.fallback
      ? [{ name: section.fallback }]
      : []

  return (
    <div class={s.section}>
      <button
        class={s.sectionHeader}
        onClick={() => { open.value = !open.value }}
      >
        <span class={s.chevron} data-open={open.value}>›</span>
        <Badge kind={section.model as any} />
        <span class={s.sectionLabel}>{section.label}</span>
        <span class={s.count}>{section.items.length}</span>
      </button>

      {open.value && section.items.length === 0 && !section.nucleusOnly && (
        <div class={s.empty}>
          <span class={s.emptyText}>No {section.label.toLowerCase()} objects</span>
        </div>
      )}

      {open.value && items.map(item => (
        <button
          key={item.name}
          class={s.item}
          onClick={() => openTab({
            id: crypto.randomUUID(),
            kind: section.kind,
            label: item.name,
            objectSchema: item.sub ?? 'public',
            objectName: item.name,
          })}
        >
          {item.sub && <span class={s.schema}>{item.sub}.</span>}
          <span class={s.itemName}>{item.name}</span>
        </button>
      ))}
    </div>
  )
}
