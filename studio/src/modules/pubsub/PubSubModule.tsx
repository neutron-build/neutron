import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { useRequestOwner } from '../../lib/requestOwner'
import { api, queryMutationOrThrow as runMutation } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './PubSubModule.module.css'

// Nucleus pub/sub over SQL is publish-only: PUBSUB_PUBLISH(channel, message)
// returns the subscriber count reached (always 0 from a standalone SQL
// deployment — verified live, the wire surface has no subscribe statement),
// PUBSUB_SUBSCRIBERS(channel) returns the current count, and
// PUBSUB_CHANNELS() (no args, no pattern support in the engine) returns a
// comma-separated list of active channels. Real subscription needs a live
// LISTEN connection (Nucleus delivers LISTEN/NOTIFY around the listener's own
// statement traffic — verified live), which this query UI cannot hold open.
interface PubSubMessage {
  id: string
  payload: string
  receivedAt: string
}

interface PubSubModuleProps {
  name: string
}

// Split the comma-separated channel list returned by PUBSUB_CHANNELS().
export function parseChannels(cell: unknown): string[] {
  if (cell == null) return []
  return String(cell).split(',').map(c => c.trim()).filter(Boolean)
}

export function PubSubModule({ name }: PubSubModuleProps) {
  // Channels exist only while a subscriber holds them open, so the tab label
  // is only the starting channel to publish to.
  const channelName = useSignal(name)
  const messages = useSignal<PubSubMessage[]>([])
  const payload = useSignal('')
  const publishing = useSignal(false)
  const subscriberCount = useSignal<number | null>(null)
  const channels = useSignal<string[]>([])
  const pinToBottom = useSignal(true)
  const rlsDenied = useSignal<string | null>(null)
  const listRef = useRef<HTMLDivElement>(null)

  const priorName = useRef(name)
  if (priorName.current !== name) { priorName.current = name; channelName.value = name }
  const conn = activeConnection.value
  const infoGeneration = useRef(0)
  const publishVersion = useRef(0)
  const logVersion = useRef(0)
  const requests = useRequestOwner(JSON.stringify([conn?.id, name, channelName.value]))
  const unavailable = useSignal<string | null>(null)
  const owner = useRef({ epoch: 0, alive: true, connection: conn?.id, name: name })
  function invalidate() {
    requests.invalidate()
    owner.current.epoch++
    unavailable.value = null; rlsDenied.value = null
    infoGeneration.current++; publishVersion.current++; logVersion.current++; subscriberCount.value = null; channels.value = []; messages.value = []; publishing.value = false;
  }
  if (owner.current.connection !== conn?.id || owner.current.name !== name) {
    invalidate(); owner.current.connection = conn?.id; owner.current.name = name
  }
  function capture(channel: string) {
    const ticket = requests.begin(channel)
    const epoch = owner.current.epoch
    const connectionId = conn?.id
    const target = channelName.value
    return { connectionId, owns: () => ticket() && owner.current.alive && epoch === owner.current.epoch && connectionId === activeConnection.value?.id && target === channelName.value }
  }
  function fail(err: unknown) {
    const msg = err instanceof Error ? err.message : String(err)
    unavailable.value = msg; rlsDenied.value = isRlsDenied(msg) ? msg : null
  }
  useEffect(() => {
    owner.current.alive = true
    let current = activeConnection.value
    const stop = activeConnection.subscribe(next => { if (next !== current) { current = next; invalidate() } })
    return () => { owner.current.alive = false; invalidate(); stop() }
  }, [])
  async function refreshInfo() {
    const binding = capture('info')
    if (!binding.connectionId || !binding.owns()) return
    const channel = channelName.value.trim()
    const generation = ++infoGeneration.current
    const owns = () => binding.owns() && generation === infoGeneration.current
    subscriberCount.value = null; channels.value = []; unavailable.value = null
    try {
      let count: number | null = null
      if (channel) {
        const r = await api.query(`SELECT PUBSUB_SUBSCRIBERS('${channel.replace(/'/g, "''")}')`, binding.connectionId)
        if (!owns()) return
        if (r.error || r.canceled) throw new Error(r.error || 'Subscriber read canceled')
        if (r.rows[0]?.[0] == null || !Number.isFinite(Number(r.rows[0][0]))) throw new Error('Subscriber count unavailable')
        count = Number(r.rows[0][0])
      }
      const r = await api.query(`SELECT PUBSUB_CHANNELS()`, binding.connectionId)
      if (!owns()) return
      if (r.error || r.canceled) throw new Error(r.error || 'Channels read canceled')
      if (!r.rows.length) throw new Error('Channels unavailable')
      subscriberCount.value = count; channels.value = parseChannels(r.rows[0]?.[0])
    } catch (err) { if (owns()) fail(err) }
  }
  useEffect(() => { void refreshInfo() }, [conn?.id, name])

  // Auto-scroll when pinned and new messages arrive
  useEffect(() => {
    if (pinToBottom.value && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight
    }
  }, [messages.value.length, pinToBottom.value])

  async function publish() {
    if (publishing.value) return
    const binding = capture('publish')
    if (!binding.connectionId || !binding.owns()) return
    const version = publishVersion.current
    const log = logVersion.current
    const infoRevision = infoGeneration.current
    const channel = channelName.value.trim()
    const msg = payload.value
    if (!channel) return
    if (!msg.trim()) return
    publishing.value = true
    try {
      const r = await queryMutationOrThrow(
        `SELECT PUBSUB_PUBLISH('${channel.replace(/'/g, "''")}', '${msg.replace(/'/g, "''")}')`,
        binding.connectionId
      )
      if (!binding.owns()) return
      if (r.rows[0]?.[0] == null || !Number.isSafeInteger(Number(r.rows[0][0])) || Number(r.rows[0][0]) < 0) throw new Error('Publish outcome unavailable; verify before retrying')
      const reached = Number(r.rows[0][0])
      // Record locally so the user can see what they sent
      if (log === logVersion.current) messages.value = [
        ...messages.value,
        { id: crypto.randomUUID(), payload: msg, receivedAt: new Date().toISOString() },
      ]
      if (version === publishVersion.current) payload.value = ''
      toast('success', `Published to ${channel} (${reached} subscriber${reached !== 1 ? 's' : ''})`)
      if (infoRevision === infoGeneration.current) void refreshInfo()
    } catch (err: unknown) {
      if (binding.owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) }
    } finally {
      if (binding.owns()) publishing.value = false
    }
  }

  function handleKey(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      publish()
    }
  }

  function clearLog() {
    logVersion.current++
    messages.value = []
  }

  function handleExportCSV() {
    const data = messages.value.map(m => ({
      id: m.id,
      payload: m.payload,
      receivedAt: m.receivedAt,
    }))
    exportCSV(data, `pubsub-${channelName.value || 'channel'}.csv`)
  }

  function handleExportJSON() {
    exportJSON(messages.value, `pubsub-${channelName.value || 'channel'}.json`)
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <input
          class={s.channelInput}
          value={channelName.value}
          placeholder="channel name"
          title="Channel to publish to"
          onInput={e => { invalidate(); channelName.value = (e.target as HTMLInputElement).value }}
          onKeyDown={e => { if (e.key === 'Enter') refreshInfo() }}
          onBlur={refreshInfo}
        />
        {subscriberCount.value != null && (
          <span class={s.subCount}>{subscriberCount.value} subscriber{subscriberCount.value !== 1 ? 's' : ''}</span>
        )}
        {messages.value.length > 0 && (
          <span class={s.msgBadge}>{messages.value.length}</span>
        )}
      </div>

      {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}
      {unavailable.value && <div role="alert">PubSub data unavailable: {unavailable.value}</div>}

      {/* Toolbar: refresh, pin, export, clear */}
      <div class={s.toolbar}>
        <button class={s.subscribeBtn} onClick={refreshInfo}>
          Refresh
        </button>
        <label class={s.pinLabel}>
          <input
            type="checkbox"
            checked={pinToBottom.value}
            onChange={() => { pinToBottom.value = !pinToBottom.value }}
          />
          Pin to bottom
        </label>
        <div class={s.toolbarSpacer} />
        <button class={s.exportBtn} onClick={handleExportCSV} disabled={messages.value.length === 0}>
          CSV
        </button>
        <button class={s.exportBtn} onClick={handleExportJSON} disabled={messages.value.length === 0}>
          JSON
        </button>
        <button class={s.clearBtn} onClick={clearLog} disabled={messages.value.length === 0}>
          Clear log
        </button>
      </div>

      {channels.value.length > 0 && (
        <div class={s.empty}>Active channels: {channels.value.join(', ')}</div>
      )}

      <div class={s.messageList} ref={listRef}>
        {messages.value.length === 0 && (
          <div class={s.empty}>
            Publish a message below. Live subscription uses LISTEN/NOTIFY, which the SQL query UI cannot hold open — only sent messages are logged here.
          </div>
        )}
        {messages.value.map(m => (
          <div key={m.id} class={s.message}>
            <span class={s.msgTime}>{new Date(m.receivedAt).toLocaleTimeString()}</span>
            <span class={s.msgPayload}>{m.payload}</span>
          </div>
        ))}
      </div>

      <div class={s.publishPanel}>
        <div class={s.publishLabel}>Publish message <span class={s.hint}>Cmd+Enter to send</span></div>
        <textarea
          class={s.payloadInput}
          placeholder="Message payload..."
          value={payload.value}
          onInput={e => { publishVersion.current++; payload.value = (e.target as HTMLTextAreaElement).value }}
          onKeyDown={handleKey}
          rows={3}
        />
        <div class={s.publishFooter}>
          <button class={s.publishBtn} onClick={publish} disabled={publishing.value}>
            {publishing.value ? 'Publishing...' : 'Publish'}
          </button>
        </div>
      </div>
    </div>
  )
}

const queryMutationOrThrow = (sql: string, connectionId: string, params?: unknown[]) => runMutation(sql, connectionId, params, api.query)
