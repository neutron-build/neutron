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

function TabItem({ tab }: { tab: Tab }) {
  const isActive = activeTabId.value === tab.id

  function handleClose(e: MouseEvent) {
    e.stopPropagation()
    closeTab(tab.id)
  }

  return (
    <button
      class={`${s.tab} ${isActive ? s.active : ''}`}
      onClick={() => { activeTabId.value = tab.id }}
      title={tab.label}
    >
      <span
        class={s.tabDot}
        data-kind={KIND_COLORS[tab.kind] ?? 'sql'}
      />
      <span class={s.tabLabel}>{tab.label}</span>
      <span class={s.tabClose} onClick={handleClose} title="Close">×</span>
    </button>
  )
}

export function TabBar() {
  const list = tabs.value

  if (list.length === 0) {
    return (
      <div class={s.tabBar}>
        <span class={s.empty}>Open a table or run a query to get started</span>
      </div>
    )
  }

  return (
    <div class={s.tabBar}>
      <div class={s.tabs}>
        {list.map(tab => <TabItem key={tab.id} tab={tab} />)}
      </div>
    </div>
  )
}
