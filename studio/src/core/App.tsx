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
async function applyDeepLink(hash: string, handled: Set<string>): Promise<void> {
  const link = parseDeepLink(hash)
  if (!link) return
  handled.add(hash)
  if (activeConnection.value?.id !== link.connectionId) {
    if (!connections.value.some(c => c.id === link.connectionId)) {
      // The connection list may not have loaded yet: load it here rather
      // than racing the connection manager.
      try {
        connections.value = await api.connections.list()
      } catch (err: unknown) {
        connectionError.value = err instanceof Error ? err.message : String(err)
        return
      }
      if (!connections.value.some(c => c.id === link.connectionId)) {
        connectionError.value = `The link names connection "${link.connectionId}", which is not saved in this Studio.`
        return
      }
    }
    try {
      await connectConnection(link.connectionId)
    } catch {
      return // connectConnection recorded the error
    }
  }
  openTab({ id: crypto.randomUUID(), ...link.tab })
}

export function App() {
  // Deep links: on load and on every hash change (browser back/forward,
  // pasted URLs). Each hash is applied at most once per load.
  const handled = new Set<string>()
  useEffect(() => {
    void applyDeepLink(window.location.hash, handled)
    const onHash = () => {
      if (handled.has(window.location.hash)) return
      void applyDeepLink(window.location.hash, handled)
    }
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {activeConnection.value ? <Shell /> : <ConnectionManager />}
      <CommandPalette />
      <Toasts />
    </div>
  )
}
