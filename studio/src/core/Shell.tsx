import { useEffect, useRef } from 'preact/hooks'
import { useSignal } from '@preact/signals'
import { activeConnection, activeTabId, openTab, openPalette } from '../lib/store'
import { Icon } from '../components/Icon'
import { Sidebar } from './Sidebar'
import { TabBar } from './TabBar'
import { CommitBar } from './CommitBar'
import { ContentArea } from './ContentArea'
import s from './Shell.module.css'

export function Shell() {
  const navigationOpen = useSignal(false)
  const menuRef = useRef<HTMLButtonElement>(null)
  const navigationRef = useRef<HTMLDivElement>(null)
  const conn = activeConnection.value
  useEffect(() => { navigationOpen.value = false }, [activeTabId.value])
  useEffect(() => {
    if (!navigationOpen.value) return
    const navigation = navigationRef.current
    const controls = () => Array.from(navigation?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled)') ?? [])
    controls()[0]?.focus()
    function onKey(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        event.preventDefault()
        navigationOpen.value = false
        menuRef.current?.focus()
      }
      if (event.key === 'Tab') {
        const items = controls()
        const first = items[0], last = items[items.length - 1]
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus() }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
      }
    }
    const desktop = window.matchMedia('(min-width: 681px)')
    const onResize = () => { if (desktop.matches) navigationOpen.value = false }
    window.addEventListener('keydown', onKey)
    desktop.addEventListener('change', onResize)
    return () => { window.removeEventListener('keydown', onKey); desktop.removeEventListener('change', onResize) }
  }, [navigationOpen.value])
  return (
    <div class={s.shell}>
      <a class={s.skipLink} href="#studio-content" onClick={e => { e.preventDefault(); document.getElementById('studio-content')?.focus() }}>Skip to workspace</a>
      <header class={s.topbar}>
        <button ref={menuRef} class={s.menuButton} aria-label="Toggle database navigation" aria-expanded={navigationOpen.value} aria-controls="studio-navigation" onClick={() => { navigationOpen.value = !navigationOpen.value }}><Icon name="menu" /></button>
        <div class={s.brand} aria-label="Neutron Studio"><span>neutron<span class={s.brandStudio}>studio</span></span></div>
        <span class={s.environment}><span class={s.statusDot} />{conn?.isNucleus ? 'Nucleus' : 'PostgreSQL'}<span class={s.environmentName}>{conn?.name}</span></span>
        <div class={s.topActions}>
          <button class={s.searchButton} aria-label="Find anything" disabled={navigationOpen.value} onClick={openPalette}><Icon name="search" size={16} /><span>Find anything</span><kbd>⌘ K</kbd></button>
          <button class={s.queryButton} aria-label="Open SQL editor" disabled={navigationOpen.value} onClick={() => openTab({ id: `sql-editor-${Date.now()}`, kind: 'sql-editor', label: 'SQL query' })}><Icon name="query" size={16} /><span>SQL editor</span></button>
        </div>
      </header>
      <div class={s.body}>
        <div id="studio-navigation" onClick={e => {
          if (navigationOpen.value && (e.target as HTMLElement).closest('[data-workspace-navigation]')) {
            navigationOpen.value = false
            document.getElementById('studio-content')?.focus()
          }
        }} ref={navigationRef} role={navigationOpen.value ? 'dialog' : undefined} aria-modal={navigationOpen.value ? true : undefined} aria-label="Database navigation" class={`${s.navigation} ${navigationOpen.value ? s.navigationOpen : ''}`}>
          <Sidebar />
        </div>
        {navigationOpen.value && <button class={s.backdrop} aria-label="Close database navigation" onClick={() => { navigationOpen.value = false; menuRef.current?.focus() }} />}
        <main id="studio-content" tabIndex={-1} inert={navigationOpen.value} class={s.main}>
          <TabBar />
          <ContentArea />
          <CommitBar />
        </main>
      </div>
    </div>
  )
}
