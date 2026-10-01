import { useEffect } from 'preact/hooks'
import { Icon } from '../components/Icon'
import { useSignal } from '@preact/signals'
import {
  connections, connectConnection,
  connectionLoading, connectionError, toast,
} from '../lib/store'
import { api } from '../lib/api'
import type { ConnectionInput } from '../lib/types'
import s from './ConnectionManager.module.css'

// ---- Add connection form ----

function AddForm({ onDone }: { onDone: () => void }) {
  const name = useSignal('')
  const url = useSignal('')
  const testing = useSignal(false)
  const saving = useSignal(false)
  const testResult = useSignal<string | null>(null)
  const error = useSignal<string | null>(null)

  async function handleTest() {
    if (!url.value.trim()) return
    testing.value = true
    testResult.value = null
    error.value = null
    try {
      const r = await api.connections.test(url.value.trim())
      testResult.value = r.ok
        ? `Connected — ${r.isNucleus ? `Nucleus ${r.version}` : `PostgreSQL ${r.version}`}`
        : `Failed: ${r.error}`
    } catch (err: unknown) {
      error.value = err instanceof Error ? err.message : String(err)
    } finally {
      testing.value = false
    }
  }

  async function handleAdd() {
    const input: ConnectionInput = { name: name.value.trim(), url: url.value.trim() }
    if (!input.name || !input.url || saving.value) return
    saving.value = true
    error.value = null
    try {
      const conn = await api.connections.add(input)
      connections.value = [...connections.value, conn]
      onDone()
    } catch (err: unknown) {
      error.value = err instanceof Error ? err.message : String(err)
    } finally {
      saving.value = false
    }
  }

  return (
    <form class={s.addForm} onSubmit={e => { e.preventDefault(); void handleAdd() }}>
      <div class={s.field}>
        <label class={s.label} htmlFor="connection-name">Name</label>
        <input
          id="connection-name"
          class={s.input}
          placeholder="e.g. Local development"
          autoFocus
          required
          disabled={saving.value}
          value={name.value}
          onInput={(e) => { name.value = (e.target as HTMLInputElement).value }}
        />
      </div>
      <div class={s.field}>
        <label class={s.label} htmlFor="connection-url">Connection URL</label>
        <input
          id="connection-url"
          class={s.input}
          type="password"
          autoComplete="off"
          spellcheck={false}
          required
          disabled={saving.value}
          aria-describedby="connection-url-help"
          placeholder="postgres://user:pass@host:5432/db"
          value={url.value}
          onInput={(e) => { url.value = (e.target as HTMLInputElement).value }}
        />
      </div>
      <p id="connection-url-help" class={s.fieldHelp}>Use a database role with the permissions you need. Saved credentials stay on the Studio server; editing requires database permission.</p>
      {testResult.value && (
        <div role="status" class={`${s.testResult} ${testResult.value.startsWith('Connected') ? s.ok : s.fail}`}>
          {testResult.value}
        </div>
      )}
      {error.value && <div role="alert" class={s.errorMsg}>{error.value}</div>}
      <div class={s.formActions}>
        <button type="button" class={s.btnSecondary} onClick={handleTest} disabled={testing.value || saving.value || !url.value.trim()}>
          {testing.value ? 'Testing…' : 'Test Connection'}
        </button>
        <button type="submit" class={s.btnPrimary} disabled={saving.value || !name.value.trim() || !url.value.trim()}>
          {saving.value ? 'Saving…' : 'Save'}
        </button>
      </div>
    </form>
  )
}

// ---- Connection list ----

export function ConnectionManager() {
  const showAdd = useSignal(false)
  const listLoading = useSignal(true)
  const listError = useSignal<string | null>(null)

  async function loadConnections() {
    listLoading.value = true
    listError.value = null
    try {
      connections.value = await api.connections.list()
    } catch (err: unknown) {
      listError.value = err instanceof Error ? err.message : String(err)
    } finally {
      listLoading.value = false
    }
  }

  useEffect(() => { void loadConnections() }, [])

  async function connect(id: string) {
    try {
      await connectConnection(id)
    } catch {
      // connectConnection records the error in the store signal.
    }
  }

  async function remove(id: string) {
    try {
      await api.connections.remove(id)
      connections.value = connections.value.filter(c => c.id !== id)
      toast('info', 'Connection removed')
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div class={s.page}>
      <div class={s.card}>
        <div class={s.cardHeader}>
          <div><h1 class={s.cardTitle}>Connections</h1><p class={s.cardSubtitle}>Choose a database to open your workspace.</p></div>
          <button class={s.btnAdd} onClick={() => { showAdd.value = !showAdd.value }}>
            {showAdd.value ? 'Cancel' : '+ Add'}
          </button>
        </div>

        {showAdd.value && (
          <AddForm onDone={() => { showAdd.value = false }} />
        )}

        {connectionError.value && (
          <div role="alert" class={s.errorMsg}>{connectionError.value}</div>
        )}

        {listLoading.value && <div class={s.empty} role="status">Loading saved connections…</div>}
        {listError.value && <div class={s.empty} role="alert"><p>Could not load connections: {listError.value}</p><button class={s.btnSecondary} onClick={loadConnections}>Try again</button></div>}
        {!listLoading.value && !listError.value && connections.value.length === 0 && !showAdd.value && (
          <div class={s.empty}><Icon name="database" size={30} /><strong>A workspace starts with a connection</strong><p>Add your database URL to browse tables and run your first query.</p><button class={s.btnPrimary} onClick={() => { showAdd.value = true }}>Add your first connection</button></div>
        )}

        <div class={s.connList}>
          {connections.value.map(conn => (
            <div key={conn.id} class={s.connRow}>
              <span class={s.connDot} data-nucleus={conn.isNucleus}><Icon name="database" size={20} /></span>
              <div class={s.connInfo}>
                <span class={s.connName}>{conn.name}</span>
                <span class={s.connUrl}>{conn.url}</span>
              </div>
              <div class={s.connActions}>
                <button
                  class={s.btnConnect}
                  onClick={() => connect(conn.id)}
                  disabled={connectionLoading.value}
                >
                  {connectionLoading.value ? 'Connecting…' : 'Connect'}
                </button>
                <button
                  class={s.btnRemove}
                  onClick={() => remove(conn.id)}
                  title="Remove connection"
                  aria-label={`Remove ${conn.name} connection`}
                >
                  <Icon name="close" size={16} />
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
      <p class={s.footnote}>Permissions are enforced by the connected database role.</p>
    </div>
  )
}
