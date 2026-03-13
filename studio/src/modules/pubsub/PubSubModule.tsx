import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import s from './PubSubModule.module.css'

interface PubSubMessage {
  id: string
  payload: string
  receivedAt: string
}

interface PubSubModuleProps {
  name: string
}

export function PubSubModule({ name }: PubSubModuleProps) {
  const messages = useSignal<PubSubMessage[]>([])
  const payload = useSignal('')
  const publishing = useSignal(false)
  const subscriberCount = useSignal<number | null>(null)
  const subscribed = useSignal(false)
  const pinToBottom = useSignal(true)
  const listRef = useRef<HTMLDivElement>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadInfo() {
      try {
        const r = await api.query(
          `SELECT subscriber_count FROM pubsub_info('${name}')`,
          conn.id
        )
        if (!r.error && r.rows.length > 0) subscriberCount.value = Number(r.rows[0][0])
      } catch { /* non-critical */ }
    }
    loadInfo()
  }, [name])

  // Auto-scroll when pinned and new messages arrive
  useEffect(() => {
    if (pinToBottom.value && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight
    }
  }, [messages.value.length, pinToBottom.value])

  // Subscribe polling: poll every 2 seconds for new messages
  useEffect(() => {
    if (!subscribed.value) {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
      return
    }

    async function poll() {
      try {
        const r = await api.query(
          `SELECT id, payload, received_at FROM pubsub_poll('${name}', 50)`,
          conn.id
        )
        if (!r.error && r.rows.length > 0) {
          const newMsgs = r.rows.map(row => ({
            id: String(row[0]),
            payload: String(row[1]),
            receivedAt: String(row[2]),
          }))
          // Deduplicate by id
          const existingIds = new Set(messages.value.map(m => m.id))
          const fresh = newMsgs.filter(m => !existingIds.has(m.id))
          if (fresh.length > 0) {
            messages.value = [...messages.value, ...fresh]
          }
        }
      } catch {
        // Poll failures are non-critical; keep trying
      }
    }

    poll()
    pollRef.current = setInterval(poll, 2000)
    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [subscribed.value, name])

  // Clean up on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [])

  async function publish() {
    const msg = payload.value.trim()
    if (!msg) return
    publishing.value = true
    try {
      await api.query(
        `SELECT pubsub_publish('${name}', '${msg.replace(/'/g, "''")}')`,
        conn.id
      )
      // Record locally so the user can see what they sent
      messages.value = [
        ...messages.value,
        { id: crypto.randomUUID(), payload: msg, receivedAt: new Date().toISOString() },
      ]
      payload.value = ''
      toast('success', `Published to ${name}`)
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

  function toggleSubscribe() {
    subscribed.value = !subscribed.value
    if (subscribed.value) {
      toast('info', `Subscribed to ${name}`)
    }
  }

  function handleExportCSV() {
    const data = messages.value.map(m => ({
      id: m.id,
      payload: m.payload,
      receivedAt: m.receivedAt,
    }))
    exportCSV(data, `pubsub-${name}.csv`)
  }

  function handleExportJSON() {
    exportJSON(messages.value, `pubsub-${name}.json`)
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.channelName}>{name}</span>
        {subscriberCount.value != null && (
          <span class={s.subCount}>{subscriberCount.value} subscriber{subscriberCount.value !== 1 ? 's' : ''}</span>
        )}
        {messages.value.length > 0 && (
          <span class={s.msgBadge}>{messages.value.length}</span>
        )}
        {subscribed.value && <span class={s.liveTag}>LIVE</span>}
      </div>

      {/* Toolbar: subscribe, pin, export, clear */}
      <div class={s.toolbar}>
        <button
          class={subscribed.value ? s.subscribeBtnActive : s.subscribeBtn}
          onClick={toggleSubscribe}
        >
          {subscribed.value ? '■ Unsubscribe' : '▶ Subscribe'}
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

      <div class={s.messageList} ref={listRef}>
        {messages.value.length === 0 && (
          <div class={s.empty}>
            {subscribed.value
              ? 'Listening for messages...'
              : 'Click Subscribe to start receiving messages, or publish one below.'}
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
