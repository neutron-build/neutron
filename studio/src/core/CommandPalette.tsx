import { useEffect, useRef } from 'preact/hooks'
import { computed } from '@preact/signals'
import {
  paletteOpen, paletteQuery, closePalette, openTab,
  schema, activeConnection,
} from '../lib/store'
import type { Tab } from '../lib/types'
import s from './CommandPalette.module.css'

interface PaletteItem {
  id: string
  icon: string
  label: string
  sub: string
  tab: Tab
}

const allItems = computed<PaletteItem[]>(() => {
  const sc = schema.value
  const conn = activeConnection.value
  if (!sc || !conn) return []

  const items: PaletteItem[] = []

  for (const t of sc.sql) {
    items.push({
      id: `sql:${t.schema}.${t.name}`,
      icon: '▤',
      label: t.name,
      sub: t.schema,
      tab: { id: `sql:${t.schema}.${t.name}`, kind: 'sql-browser', label: t.name, objectSchema: t.schema, objectName: t.name },
    })
  }
  for (const k of sc.kv) {
    items.push({ id: `kv:${k.name}`, icon: '⬡', label: k.name, sub: 'kv', tab: { id: `kv:${k.name}`, kind: 'kv', label: k.name, objectName: k.name } })
  }
  for (const v of sc.vector) {
    items.push({ id: `vec:${v.name}`, icon: '⬡', label: v.name, sub: 'vector', tab: { id: `vec:${v.name}`, kind: 'vector', label: v.name, objectName: v.name } })
  }
  for (const m of sc.timeseries) {
    items.push({ id: `ts:${m.name}`, icon: '⬡', label: m.name, sub: 'timeseries', tab: { id: `ts:${m.name}`, kind: 'timeseries', label: m.name, objectName: m.name } })
  }
  for (const d of sc.document) {
    items.push({ id: `doc:${d.name}`, icon: '⬡', label: d.name, sub: 'document', tab: { id: `doc:${d.name}`, kind: 'document', label: d.name, objectName: d.name } })
  }
  for (const g of sc.graph) {
    items.push({ id: `graph:${g.name}`, icon: '⬡', label: g.name, sub: 'graph', tab: { id: `graph:${g.name}`, kind: 'graph', label: g.name, objectName: g.name } })
  }
  for (const f of sc.fts) {
    items.push({ id: `fts:${f.name}`, icon: '⬡', label: f.name, sub: 'fts', tab: { id: `fts:${f.name}`, kind: 'fts', label: f.name, objectName: f.name } })
  }

  // Static actions
  items.push({
    id: 'sql-editor',
    icon: '⌨',
    label: 'New SQL Query',
    sub: 'editor',
    tab: { id: `sql-editor:${Date.now()}`, kind: 'sql-editor', label: 'Query' },
  })

  return items
})

const filtered = computed(() => {
  const q = paletteQuery.value.toLowerCase().trim()
  if (!q) return allItems.value.slice(0, 20)
  return allItems.value
    .filter(i => i.label.toLowerCase().includes(q) || i.sub.toLowerCase().includes(q))
    .slice(0, 20)
})

export function CommandPalette() {
  if (!paletteOpen.value) return null

  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') closePalette()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  function select(item: PaletteItem) {
    openTab(item.tab)
    closePalette()
  }

  return (
    <div class={s.overlay} onClick={closePalette}>
      <div class={s.panel} onClick={(e) => e.stopPropagation()}>
        <div class={s.inputWrap}>
          <span class={s.searchIcon}>⌕</span>
          <input
            ref={inputRef}
            class={s.input}
            placeholder="Go to table, collection, query..."
            value={paletteQuery.value}
            onInput={(e) => { paletteQuery.value = (e.target as HTMLInputElement).value }}
          />
          <kbd class={s.esc}>Esc</kbd>
        </div>
        <div class={s.list}>
          {filtered.value.length === 0 && (
            <div class={s.empty}>No results</div>
          )}
          {filtered.value.map((item) => (
            <button key={item.id} class={s.item} onClick={() => select(item)}>
              <span class={s.itemIcon}>{item.icon}</span>
              <span class={s.itemLabel}>{item.label}</span>
              <span class={s.itemSub}>{item.sub}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
