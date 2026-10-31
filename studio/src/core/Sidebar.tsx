import { activeConnection, openPalette, toggleTheme, theme } from '../lib/store'
import { Icon } from '../components/Icon'
import { SchemaTree } from './SchemaTree'
import s from './Sidebar.module.css'

export function Sidebar() {
  const conn = activeConnection.value!

  return (
    <aside class={s.sidebar} aria-label="Database navigation">
      <div class={s.eyebrow}>DATABASE EXPLORER</div>
      <div class={s.header}>
        <div class={s.connInfo}>
          <span class={s.connDot} data-nucleus={conn.isNucleus} />
          <span class={s.connName} title={conn.name}>{conn.name}</span>
          {conn.isNucleus && (
            <span class={s.nucleusBadge}>Nucleus</span>
          )}
        </div>
        <button class={s.iconBtn} onClick={openPalette} title="Command palette (⌘K)" aria-label="Open command palette">
          <Icon name="search" size={16} />
        </button>
      </div>

      <div class={s.treeWrap}>
        <SchemaTree />
      </div>

      <div class={s.footer}>
        <span class={s.footerLabel}>Workspace</span>
        <button class={s.footerBtn} onClick={toggleTheme} title="Toggle theme" aria-label={`Switch to ${theme.value === 'dark' ? 'light' : 'dark'} theme`}>
          <Icon name={theme.value === 'dark' ? 'sun' : 'moon'} size={17} />
        </button>
        <button
          class={s.footerBtn}
          onClick={() => { (window as any).__studioDisconnect?.() }}
          title="Disconnect" aria-label="Disconnect database"
        >
          <Icon name="disconnect" size={17} />
        </button>
      </div>
    </aside>
  )
}
