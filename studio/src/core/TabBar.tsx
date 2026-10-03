import { useLayoutEffect, useRef } from 'preact/hooks'
import { tabs, activeTabId, closeTab } from '../lib/store'
import type { Tab } from '../lib/types'
import s from './TabBar.module.css'

const KIND_COLORS: Partial<Record<string, string>> = {
  'sql-browser': 'sql',
  'sql-editor': 'sql',
  'kv': 'kv',
  'vector': 'vector',
  'timeseries': 'ts',
  'document': 'doc',
  'graph': 'graph',
  'fts': 'fts',
  'geo': 'geo',
  'blob': 'blob',
  'pubsub': 'pubsub',
  'streams': 'streams',
  'columnar': 'columnar',
  'datalog': 'datalog',
  'cdc': 'cdc',
}

function TabItem({ tab, onClose }: { tab: Tab; onClose: (id: string, restoreFocus: boolean) => void }) {
  const isActive = activeTabId.value === tab.id

  function handleClose(e: MouseEvent) {
    e.stopPropagation()
    onClose(tab.id, document.activeElement === e.currentTarget)
  }

  return (
    <div class={`${s.tab} ${isActive ? s.active : ''}`}>
      <button data-tab-select={tab.id} class={s.tabSelect} onClick={() => { activeTabId.value = tab.id }} title={tab.label} aria-pressed={isActive}>
        <span class={s.tabDot} data-kind={KIND_COLORS[tab.kind] ?? 'sql'} />
        <span class={s.tabLabel}>{tab.label}</span>
      </button>
      <button class={s.tabClose} onClick={handleClose} title="Close" aria-label={`Close ${tab.label}`}>×</button>
    </div>
  )
}

export function TabBar() {
  const list = tabs.value
  const activeId = activeTabId.value
  const barRef = useRef<HTMLDivElement>(null)
  const restoreFocus = useRef(false)
  function close(id: string, restore: boolean) {
    restoreFocus.current = restore
    closeTab(id)
  }
  // A keyboard-activated close removes its focused DOM node. Restore only
  // that interaction's focus, after the surviving selectors have rendered;
  // background/programmatic closes must not steal workspace input focus.
  useLayoutEffect(() => {
    if (!restoreFocus.current) return
    restoreFocus.current = false
    const selectors = Array.from(barRef.current?.querySelectorAll<HTMLButtonElement>('[data-tab-select]') ?? [])
    const active = selectors.find(button => button.dataset.tabSelect === activeId)
    if (active) active.focus()
    else document.getElementById('studio-content')?.focus()
  }, [list, activeId])

  if (list.length === 0) {
    return (
      <div ref={barRef} class={s.tabBar}>
        <span class={s.empty}>Open a table or run a query to get started</span>
      </div>
    )
  }

  return (
    <div ref={barRef} class={s.tabBar}>
      <div class={s.tabs} role="group" aria-label="Open workspace tabs">
        {list.map(tab => <TabItem key={tab.id} tab={tab} onClose={close} />)}
      </div>
    </div>
  )
}
