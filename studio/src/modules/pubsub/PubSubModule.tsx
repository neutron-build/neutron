import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
import s from './PubSubModule.module.css'

// Nucleus pub/sub over SQL is publish-only: PUBSUB_PUBLISH(channel, message)
// returns the subscriber count reached, PUBSUB_SUBSCRIBERS(channel) returns the
// current count, and PUBSUB_CHANNELS() (no args) returns a comma-separated list
// of active channels. There is NO SQL poll for messages — real subscription is
// LISTEN/NOTIFY on a live connection, which this query UI cannot hold open.
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

  const conn = activeConnection.value!

  async function refreshInfo() {
    const channel = channelName.value.trim()
    try {
      if (channel) {
        const subRes = await api.query(
          `SELECT PUBSUB_SUBSCRIBERS('${channel.replace(/'/g, "''")}')`,
          conn.id
        )
        if (subRes.error) {
          if (isRlsDenied(subRes.error)) rlsDenied.value = subRes.error
        } else if (subRes.rows.length > 0) {
          subscriberCount.value = Number(subRes.rows[0][0])
        }
      }
      const chanRes = await api.query(`SELECT PUBSUB_CHANNELS()`, conn.id)
      if (chanRes.error) {
        if (isRlsDenied(chanRes.error)) rlsDenied.value = chanRes.error
        return
      }
      if (chanRes.rows.length > 0) channels.value = parseChannels(chanRes.rows[0][0])
    } catch { /* non-critical */ }
  }

  useEffect(() => {
    refreshInfo()
  }, [])

  // Auto-scroll when pinned and new messages arrive
  useEffect(() => {
    if (pinToBottom.value && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight
    }
  }, [messages.value.length, pinToBottom.value])

  async function publish() {
    const channel = channelName.value.trim()
    const msg = payload.value.trim()
    if (!channel) return
    if (!msg) return
    publishing.value = true
    try {
      const r = await api.query(
        `SELECT PUBSUB_PUBLISH('${channel.replace(/'/g, "''")}', '${msg.replace(/'/g, "''")}')`,
        conn.id
      )
      if (r.error) throw new Error(r.error)
      const reached = r.rows.length > 0 ? Number(r.rows[0][0]) : 0
      // Record locally so the user can see what they sent
      messages.value = [
        ...messages.value,
        { id: crypto.randomUUID(), payload: msg, receivedAt: new Date().toISOString() },
      ]
      payload.value = ''
      toast('success', `Published to ${channel} (${reached} subscriber${reached !== 1 ? 's' : ''})`)
      refreshInfo()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      publishing.value = false
    }
  }

  function handleKey(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      publish()
    }
  }

  function clearLog() {
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
          onInput={e => { channelName.value = (e.target as HTMLInputElement).value }}
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
          onInput={e => { payload.value = (e.target as HTMLTextAreaElement).value }}
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
