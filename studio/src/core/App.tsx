import { Shell } from './Shell'
import { ConnectionManager } from './ConnectionManager'
import { activeConnection } from '../lib/store'
import { Toasts } from '../components/Toast'
import { CommandPalette } from './CommandPalette'

export function App() {
  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {activeConnection.value ? <Shell /> : <ConnectionManager />}
      <CommandPalette />
      <Toasts />
    </div>
  )
}
