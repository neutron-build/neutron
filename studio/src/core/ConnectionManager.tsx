import { useSignal } from '@preact/signals'
import {
  connections, activeConnection, features, schema,
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
    if (!input.name || !input.url) return
    error.value = null
    try {
      const conn = await api.connections.add(input)
      connections.value = [...connections.value, conn]
      onDone()
    } catch (err: unknown) {
      error.value = err instanceof Error ? err.message : String(err)
    }
  }

  return (
    <div class={s.addForm}>
      <div class={s.field}>
        <label class={s.label}>Name</label>
        <input
          class={s.input}
          placeholder="My Database"
          value={name.value}
          onInput={(e) => { name.value = (e.target as HTMLInputElement).value }}
        />
      </div>
      <div class={s.field}>
        <label class={s.label}>Connection URL</label>
        <input
          class={s.input}
          type="password"
          placeholder="postgres://user:pass@host:5432/db"
          value={url.value}
          onInput={(e) => { url.value = (e.target as HTMLInputElement).value }}
        />
      </div>
      {testResult.value && (
        <div class={`${s.testResult} ${testResult.value.startsWith('Connected') ? s.ok : s.fail}`}>
          {testResult.value}
        </div>
      )}
      {error.value && <div class={s.errorMsg}>{error.value}</div>}
      <div class={s.formActions}>
        <button class={s.btnSecondary} onClick={handleTest} disabled={testing.value}>
          {testing.value ? 'Testing…' : 'Test Connection'}
        </button>
        <button class={s.btnPrimary} onClick={handleAdd}>
          Save
        </button>
      </div>
    </div>
  )
}

// ---- Connection list ----

export function ConnectionManager() {
  const showAdd = useSignal(false)

  async function connect(id: string) {
    connectionLoading.value = true
    connectionError.value = null
    try {
      const { features: f, schema: sc } = await api.connections.connect(id)
      features.value = f
      schema.value = sc
      const conn = connections.value.find(c => c.id === id) ?? null
      if (conn) activeConnection.value = { ...conn, isNucleus: f.isNucleus }
    } catch (err: unknown) {
      connectionError.value = err instanceof Error ? err.message : String(err)
    } finally {
      connectionLoading.value = false
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

  // Load connections on first render
  const loaded = useSignal(false)
  if (!loaded.value) {
    loaded.value = true
    api.connections.list().then(list => { connections.value = list })
  }

  return (
    <div class={s.page}>
      <div class={s.hero}>
        <div class={s.logoMark}>N</div>
        <h1 class={s.title}>Neutron Studio</h1>
        <p class={s.sub}>Connect to Nucleus or any PostgreSQL-compatible database</p>
      </div>

      <div class={s.card}>
        <div class={s.cardHeader}>
          <span class={s.cardTitle}>Connections</span>
          <button class={s.btnAdd} onClick={() => { showAdd.value = !showAdd.value }}>
            {showAdd.value ? '× Cancel' : '+ Add'}
          </button>
        </div>

        {showAdd.value && (
          <AddForm onDone={() => { showAdd.value = false }} />
        )}

        {connectionError.value && (
          <div class={s.errorMsg}>{connectionError.value}</div>
        )}

        {connections.value.length === 0 && !showAdd.value && (
          <div class={s.empty}>No saved connections. Add one above.</div>
        )}

        <div class={s.connList}>
          {connections.value.map(conn => (
            <div key={conn.id} class={s.connRow}>
              <span class={s.connDot} data-nucleus={conn.isNucleus} />
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
                >
                  ×
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
