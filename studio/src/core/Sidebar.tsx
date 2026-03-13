import { activeConnection, openPalette, toggleTheme, theme } from '../lib/store'
import { SchemaTree } from './SchemaTree'
import s from './Sidebar.module.css'

export function Sidebar() {
  const conn = activeConnection.value!

  return (
    <div class={s.sidebar}>
      <div class={s.header}>
        <div class={s.connInfo}>
          <span class={s.connDot} data-nucleus={conn.isNucleus} />
          <span class={s.connName} title={conn.name}>{conn.name}</span>
          {conn.isNucleus && (
            <span class={s.nucleusBadge}>Nucleus</span>
          )}
        </div>
        <button class={s.iconBtn} onClick={openPalette} title="Command palette (⌘K)">
          ⌘
        </button>
      </div>

      <div class={s.treeWrap}>
        <SchemaTree />
      </div>

      <div class={s.footer}>
        <button class={s.footerBtn} onClick={toggleTheme} title="Toggle theme">
          {theme.value === 'dark' ? '☀' : '☾'}
        </button>
        <button
          class={s.footerBtn}
          onClick={() => { (window as any).__studioDisconnect?.() }}
          title="Disconnect"
        >
          ⏏
        </button>
      </div>
    </div>
  )
}
