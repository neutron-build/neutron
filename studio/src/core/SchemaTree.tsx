import { useSignal } from '@preact/signals'
import { schema, features, openTab, refreshSchema, schemaRefreshing, activeConnection } from '../lib/store'
import { Badge } from '../components/Badge'
import type { TabKind } from '../lib/types'
import s from './SchemaTree.module.css'

interface Section {
  model: string
  label: string
  kind: TabKind
  items: { name: string; sub?: string; isView?: boolean }[]
  nucleusOnly: boolean
  // Entry shown when the engine has no enumeration surface for the model
  // (vector, timeseries, geo, streams, columnar, datalog never list; the
  // others list nothing while empty or when RLS seals the store). The
  // browser itself is still usable — the object name is supplied inside it.
  fallback: string
}

/** Case-insensitive substring search over the schema-qualified name. */
function matches(query: string, name: string, schema: string): boolean {
  if (!query) return true
  const q = query.toLowerCase()
  return name.toLowerCase().includes(q) || `${schema}.${name}`.toLowerCase().includes(q)
}

export function SchemaTree() {
  const sc = schema.value
  const ft = features.value
  const filter = useSignal('')

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
      items: sc.sql
        .filter(t => matches(filter.value, t.name, t.schema))
        .map(t => ({ name: t.name, sub: t.schema !== 'public' ? t.schema : undefined })),
      nucleusOnly: false,
      fallback: '',
    },
    { model: 'kv', label: 'Key-Value', kind: 'kv', items: sc.kv.filter(k => matches(filter.value, k.name, 'kv')).map(k => ({ name: k.name })), nucleusOnly: true, fallback: 'keyspace' },
    { model: 'vector', label: 'Vector', kind: 'vector', items: sc.vector.filter(v => matches(filter.value, v.name, 'vector')).map(v => ({ name: v.name })), nucleusOnly: true, fallback: 'search' },
    { model: 'timeseries', label: 'TimeSeries', kind: 'timeseries', items: sc.timeseries.filter(t => matches(filter.value, t.name, 'ts')).map(t => ({ name: t.name })), nucleusOnly: true, fallback: 'series' },
    { model: 'document', label: 'Document', kind: 'document', items: sc.document.filter(d => matches(filter.value, d.name, 'doc')).map(d => ({ name: d.name })), nucleusOnly: true, fallback: 'documents' },
    { model: 'graph', label: 'Graph', kind: 'graph', items: sc.graph.filter(g => matches(filter.value, g.name, 'graph')).map(g => ({ name: g.name })), nucleusOnly: true, fallback: 'graph' },
    { model: 'fts', label: 'Full-Text', kind: 'fts', items: sc.fts.filter(f => matches(filter.value, f.name, 'fts')).map(f => ({ name: f.name })), nucleusOnly: true, fallback: 'index' },
    { model: 'geo', label: 'Geo', kind: 'geo', items: sc.geo.filter(g => matches(filter.value, g.name, 'geo')).map(g => ({ name: g.name })), nucleusOnly: true, fallback: 'calculator' },
    { model: 'blob', label: 'Blob', kind: 'blob', items: sc.blob.filter(b => matches(filter.value, b.name, 'blob')).map(b => ({ name: b.name })), nucleusOnly: true, fallback: 'blobs' },
    { model: 'pubsub', label: 'PubSub', kind: 'pubsub', items: sc.pubsub.filter(p => matches(filter.value, p.name, 'pubsub')).map(p => ({ name: p.name })), nucleusOnly: true, fallback: 'channel' },
    { model: 'streams', label: 'Streams', kind: 'streams', items: sc.streams.filter(st => matches(filter.value, st.name, 'streams')).map(st => ({ name: st.name })), nucleusOnly: true, fallback: 'stream' },
    { model: 'columnar', label: 'Columnar', kind: 'columnar', items: sc.columnar.filter(c => matches(filter.value, c.name, 'columnar')).map(c => ({ name: c.name })), nucleusOnly: true, fallback: 'table' },
    { model: 'datalog', label: 'Datalog', kind: 'datalog', items: sc.datalog ? [{ name: 'datalog' }] : [], nucleusOnly: true, fallback: 'datalog' },
    { model: 'cdc', label: 'CDC', kind: 'cdc', items: sc.cdc ? [{ name: 'changes' }] : [], nucleusOnly: true, fallback: 'changes' },
  ]

  // Views share the SQL section (S05): browsed read-only, inspected through
  // the object detail endpoint. Filtered by the same query.
  const viewItems = (sc.views ?? [])
    .filter(v => matches(filter.value, v.name, v.schema))
    .map(v => ({ name: v.name, sub: v.schema !== 'public' ? v.schema : undefined, isView: true }))
  sections[0].items = [...sections[0].items, ...viewItems]

  const visible = sections.filter(sec =>
    !sec.nucleusOnly || ft.isNucleus
  )
  // While searching, sections that match nothing hide entirely.
  const searching = filter.value.trim() !== ''

  return (
    <div class={s.tree}>
      <div class={s.tools}>
        <input
          class={s.search}
          type="search"
          placeholder="Search schema…"
          aria-label="Search schema objects"
          value={filter.value}
          onInput={e => { filter.value = (e.target as HTMLInputElement).value }}
        />
        <button
          class={s.refreshBtn}
          title="Re-fetch the live catalog (handles concurrent schema changes)"
          aria-label="Refresh schema"
          disabled={schemaRefreshing.value || !activeConnection.value}
          onClick={() => {
            const conn = activeConnection.value
            if (conn) void refreshSchema(conn.id)
          }}
        >
          {schemaRefreshing.value ? '…' : '↻'}
        </button>
      </div>
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
      {activeConnection.value && !activeConnection.value.isNucleus && (
        <button
          class={s.designerBtn}
          onClick={() => openTab({
            id: 'diagnostics',
            kind: 'diagnostics',
            label: 'Diagnostics',
          })}
        >
          ◷ Diagnostics
        </button>
      )}
      {searching && (
        <div class={s.searchSummary} role="status">
          {visible.reduce((n, sec) => n + sec.items.length, 0)} match(es)
        </div>
      )}
      {visible.map(sec => (
        <TreeSection key={sec.model} section={sec} searching={searching} />
      ))}
      {searching && visible.every(sec => sec.items.length === 0) && (
        <div class={s.empty}>
          <span class={s.emptyText}>No objects match “{filter.value}”</span>
        </div>
      )}
    </div>
  )
}

function TreeSection({ section, searching }: { section: Section; searching: boolean }) {
  const open = useSignal(section.model === 'sql')

  // Every Nucleus model keeps its browser reachable: when there is nothing to
  // enumerate (no listing surface, an empty store, or RLS sealing the counts)
  // the fallback entry opens the browser directly.
  const items = section.items.length > 0
    ? section.items
    : section.nucleusOnly && section.fallback
      ? [{ name: section.fallback }]
      : []

  if (searching && section.items.length === 0) {
    return null
  }

  return (
    <div class={s.section}>
      <button
        class={s.sectionHeader}
        onClick={() => { open.value = !open.value }}
        aria-expanded={open.value}
      >
        <span class={s.chevron} data-open={open.value}>›</span>
        <Badge kind={section.model as any} />
        <span class={s.sectionLabel}>{section.label}</span>
        <span class={s.count}>{section.items.length}</span>
      </button>

      {open.value && section.items.length === 0 && !section.nucleusOnly && !searching && (
        <div class={s.empty}>
          <span class={s.emptyText}>No {section.label.toLowerCase()} objects</span>
        </div>
      )}

      {open.value && items.map(item => (
        <span key={`${item.sub ?? 'public'}.${item.name}${item.isView ? ':view' : ''}`} class={s.itemRow}>
          <button
            class={s.item}
            title={`Browse ${item.sub ? item.sub + '.' : ''}${item.name}`}
            onClick={() => openTab({
              id: crypto.randomUUID(),
              kind: section.kind,
              label: item.name,
              objectSchema: item.sub ?? 'public',
              objectName: item.name,
            })}
          >
            {item.isView && <span class={s.viewBadge} title="view (browsed read-only)">▤</span>}
            {item.sub && <span class={s.schema}>{item.sub}.</span>}
            <span class={s.itemName}>{item.name}</span>
          </button>
          {!item.isView && section.kind === 'sql-browser' && (
            <button
              class={s.inspectBtn}
              title={`Structure of ${item.sub ? item.sub + '.' : ''}${item.name}`}
              aria-label={`Inspect structure of ${item.name}`}
              onClick={() => openTab({
                id: crypto.randomUUID(),
                kind: 'schema-inspector',
                label: item.name,
                objectSchema: item.sub ?? 'public',
                objectName: item.name,
              })}
            >
              ⓘ
            </button>
          )}
          {!item.isView && section.kind === 'sql-browser' && (
            <button
              class={s.inspectBtn}
              title={`Journey: follow ${item.sub ? item.sub + '.' : ''}${item.name} through migrations, queries, plan, rows, models and change events`}
              aria-label={`Journey for ${item.name}`}
              onClick={() => openTab({
                id: crypto.randomUUID(),
                kind: 'journey',
                label: `Journey: ${item.name}`,
                objectSchema: item.sub ?? 'public',
                objectName: item.name,
              })}
            >
              ⇢
            </button>
          )}
        </span>
      ))}
    </div>
  )
}
