import { useEffect } from 'preact/hooks'
import { Shell } from './Shell'
import { ConnectionManager } from './ConnectionManager'
import { connections, activeConnection, connectConnection, connectionError, openTab } from '../lib/store'
import { api } from '../lib/api'
import { parseDeepLink } from '../lib/router'
import { Toasts } from '../components/Toast'
import { CommandPalette } from './CommandPalette'

/** Apply a #/c/<id>/... deep link: connect the named connection (once) and
 * open the addressed tab. An unknown connection id surfaces an honest
 * connection-manager error instead of guessing. */
export async function applyDeepLink(hash: string, current: () => boolean): Promise<void> {
  const link = parseDeepLink(hash)
  if (!link) return
  if (!current()) return
  if (activeConnection.value?.id !== link.connectionId) {
    if (!connections.value.some(c => c.id === link.connectionId)) {
      // The connection list may not have loaded yet: load it here rather
      // than racing the connection manager.
      try {
        const loaded = await api.connections.list()
        if (!current()) return
        connections.value = loaded
      } catch (err: unknown) {
        if (current()) connectionError.value = err instanceof Error ? err.message : String(err)
        return
      }
      if (!connections.value.some(c => c.id === link.connectionId)) {
        connectionError.value = `The link names connection "${link.connectionId}", which is not saved in this Studio.`
        return
      }
    }
    try {
      await connectConnection(link.connectionId, current)
    } catch {
      return // connectConnection recorded the error
    }
  }
  if (!current() || activeConnection.value?.id !== link.connectionId) return
  openTab({ id: crypto.randomUUID(), ...link.tab })
}

export function App() {
  useEffect(() => {
    let generation = 0
    const navigate = () => {
      const own = ++generation
      void applyDeepLink(window.location.hash, () => own === generation)
    }
    navigate()
    window.addEventListener('hashchange', navigate)
    return () => { generation++; window.removeEventListener('hashchange', navigate) }
  }, [])

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {activeConnection.value ? <Shell /> : <ConnectionManager />}
      <CommandPalette />
      <Toasts />
    </div>
  )
}
