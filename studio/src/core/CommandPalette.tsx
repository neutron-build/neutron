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

const MAX_VISIBLE_MATCHES = 20
const matching = computed(() => {
  const q = paletteQuery.value.toLowerCase().trim()
  const items: PaletteItem[] = []
  let total = 0
  for (const item of allItems.value) {
    if (q && !item.label.toLowerCase().includes(q) && !item.sub.toLowerCase().includes(q)) continue
    total++
    if (items.length < MAX_VISIBLE_MATCHES) items.push(item)
  }
  return { items, total }
})

export function CommandPalette() {
  const isOpen = paletteOpen.value
  const panelRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  // Hooks remain active while the overlay is closed. The App keeps this
  // component mounted; a mount-only effect would not focus it on reopening
  // or clean up the previous keyboard listener when the overlay disappears.
  useEffect(() => {
    if (!isOpen) return
    const opener = document.activeElement as HTMLElement | null
    inputRef.current?.focus()
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        e.preventDefault()
        closePalette()
      } else if (e.key === 'Tab') {
        const controls = Array.from(panelRef.current?.querySelectorAll<HTMLElement>('input:not(:disabled), button:not(:disabled)') ?? [])
        const first = controls[0], last = controls[controls.length - 1]
        if (!first || !last) return
        const active = document.activeElement
        if (!panelRef.current?.contains(active)) {
          e.preventDefault()
          ;(e.shiftKey ? last : first).focus()
        } else if (e.shiftKey && active === first) {
          e.preventDefault()
          last.focus()
        } else if (!e.shiftKey && active === last) {
          e.preventDefault()
          first.focus()
        }
      }
    }
    window.addEventListener('keydown', onKey)
    return () => {
      window.removeEventListener('keydown', onKey)
      if (opener?.isConnected) opener.focus()
    }
  }, [isOpen])

  if (!isOpen) return null
  const matches = matching.value

  function select(item: PaletteItem) {
    openTab(item.tab)
    closePalette()
  }

  return (
    <div class={s.overlay} onClick={closePalette}>
      <div ref={panelRef} class={s.panel} role="dialog" aria-modal="true" aria-label="Find database objects and commands" onClick={(e) => e.stopPropagation()}>
        <div class={s.inputWrap}>
          <span class={s.searchIcon} aria-hidden="true">⌕</span>
          <input
            ref={inputRef}
            class={s.input}
            aria-label="Search database objects and commands"
            placeholder="Go to table, collection, query..."
            value={paletteQuery.value}
            onInput={(e) => { paletteQuery.value = (e.target as HTMLInputElement).value }}
          />
          <kbd class={s.esc}>Esc</kbd>
        </div>
        <div class={s.list}>
          <div class={s.empty} role="status" aria-live="polite">
            {matches.total === 0 ? 'No results' : matches.total > MAX_VISIBLE_MATCHES
              ? `Showing first ${MAX_VISIBLE_MATCHES} of ${matches.total} matches. Narrow your search to see more.`
              : `${matches.total} match${matches.total === 1 ? '' : 'es'}`}
          </div>
          {matches.items.map((item) => (
            <button key={item.id} class={s.item} onClick={() => select(item)}>
              <span class={s.itemIcon} aria-hidden="true">{item.icon}</span>
              <span class={s.itemLabel}>{item.label}</span>
              <span class={s.itemSub}>{item.sub}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
