import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './StreamsModule.module.css'

interface StreamsModuleProps {
  name: string
}

interface StreamEntry {
  id: string
  fields: Record<string, string>
}

// Streams are a GLOBAL Nucleus store. Reads (STREAM_XRANGE / STREAM_XREADGROUP)
// return a JSON array of { id, fields } as a single scalar cell, or an EMPTY
// STRING when the stream does not exist. Parse defensively.
export function parseStreamEntries(cell: unknown): StreamEntry[] {
  if (cell == null) return []
  const text = String(cell).trim()
  if (text === '') return []
  try {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? (parsed as StreamEntry[]) : []
  } catch {
    return []
  }
}

export function entriesToResult(entries: StreamEntry[]): QueryResult {
  return {
    columns: ['id', 'fields'],
    rows: entries.map(e => [e.id, JSON.stringify(e.fields ?? {})]),
    rowCount: entries.length,
    duration: 0,
  }
}

const sqlStr = (v: string) => `'${v.replace(/'/g, "''")}'`
// STREAM_XRANGE takes numeric epoch-ms bounds; use a far-future upper bound
// so a bare "from" cursor returns everything after it.
const MAX_MS = 9999999999999

export function StreamsModule({ name }: StreamsModuleProps) {
  const streamLen = useSignal<number | null>(null)
  const entriesResult = useSignal<QueryResult | null>(null)
  const loadingEntries = useSignal(false)
  const fromMs = useSignal(0)
  const entryLimit = useSignal(100)

  // Append (STREAM_XADD)
  const addField = useSignal('')
  const addValue = useSignal('')
  const appending = useSignal(false)

  // Consumer group create (STREAM_XGROUP_CREATE)
  const showCreateGroup = useSignal(false)
  const newGroupName = useSignal('')
  const newGroupStartMs = useSignal(0)
  const creatingGroup = useSignal(false)

  // Consume as group (STREAM_XREADGROUP + STREAM_XACK)
  const consumeGroup = useSignal('')
  const consumeConsumer = useSignal('')
  const consumeCount = useSignal(100)
  const groupEntries = useSignal<StreamEntry[]>([])
  const lastReadGroup = useSignal<string | null>(null)
  const reading = useSignal(false)

  const conn = activeConnection.value!

  useEffect(() => {
    loadMeta()
    loadEntries()
  }, [name])

  async function loadMeta() {
    try {
      const lenR = await api.query(`SELECT STREAM_XLEN(${sqlStr(name)})`, conn.id)
      if (!lenR.error && lenR.rows.length > 0) streamLen.value = Number(lenR.rows[0][0])
    } catch { /* non-critical */ }
  }

  async function loadEntries() {
    loadingEntries.value = true
    try {
      const r = await api.query(
        `SELECT STREAM_XRANGE(${sqlStr(name)}, ${fromMs.value}, ${MAX_MS}, ${entryLimit.value})`,
        conn.id
      )
      if (r.error) {
        entriesResult.value = r
      } else {
        const cell = r.rows.length > 0 ? r.rows[0][0] : null
        entriesResult.value = entriesToResult(parseStreamEntries(cell))
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loadingEntries.value = false
    }
  }

  async function appendEntry() {
    const field = addField.value.trim()
    if (!field) {
      toast('error', 'Field name is required')
      return
    }
    appending.value = true
    try {
      await api.query(
        `SELECT STREAM_XADD(${sqlStr(name)}, ${sqlStr(field)}, ${sqlStr(addValue.value)})`,
        conn.id
      )
      toast('success', `Appended to ${name}`)
      addField.value = ''
      addValue.value = ''
      await loadMeta()
      await loadEntries()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      appending.value = false
    }
  }

  async function createConsumerGroup() {
    const groupName = newGroupName.value.trim()
    if (!groupName) {
      toast('error', 'Group name is required')
      return
    }
    creatingGroup.value = true
    try {
      await api.query(
        `SELECT STREAM_XGROUP_CREATE(${sqlStr(name)}, ${sqlStr(groupName)}, ${newGroupStartMs.value})`,
        conn.id
      )
      toast('success', `Consumer group "${groupName}" created`)
      newGroupName.value = ''
      newGroupStartMs.value = 0
      showCreateGroup.value = false
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      creatingGroup.value = false
    }
  }

  async function readGroup() {
    const group = consumeGroup.value.trim()
    const consumer = consumeConsumer.value.trim()
    if (!group || !consumer) {
      toast('error', 'Group and consumer are required')
      return
    }
    reading.value = true
    try {
      const r = await api.query(
        `SELECT STREAM_XREADGROUP(${sqlStr(name)}, ${sqlStr(group)}, ${sqlStr(consumer)}, ${consumeCount.value})`,
        conn.id
      )
      if (r.error) {
        toast('error', r.error)
      } else {
        const cell = r.rows.length > 0 ? r.rows[0][0] : null
        groupEntries.value = parseStreamEntries(cell)
        lastReadGroup.value = group
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      reading.value = false
    }
  }

  async function ackEntry(entryId: string) {
    const group = lastReadGroup.value
    if (!group) return
    // Entry IDs are "ms-seq"; STREAM_XACK takes the two parts numerically.
    const [idMs, idSeq] = entryId.split('-')
    try {
      await api.query(
        `SELECT STREAM_XACK(${sqlStr(name)}, ${sqlStr(group)}, ${Number(idMs)}, ${Number(idSeq)})`,
        conn.id
      )
      toast('success', `ACK ${entryId}`)
      groupEntries.value = groupEntries.value.filter(e => e.id !== entryId)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.streamName}>{name}</span>
        {streamLen.value != null && (
          <span class={s.pill}>{streamLen.value.toLocaleString()} entries</span>
        )}
      </div>

      {/* Append entry (STREAM_XADD) */}
      <div class={s.rangeBar}>
        <span class={s.rangeLabel}>Field</span>
        <input
          class={s.rangeInput}
          placeholder="field"
          value={addField.value}
          onInput={e => { addField.value = (e.target as HTMLInputElement).value }}
        />
        <span class={s.rangeLabel}>Value</span>
        <input
          class={s.rangeInput}
          placeholder="value"
          value={addValue.value}
          onInput={e => { addValue.value = (e.target as HTMLInputElement).value }}
        />
        <button class={s.readBtn} onClick={appendEntry} disabled={appending.value || !addField.value.trim()}>
          {appending.value ? 'Adding...' : 'Append'}
        </button>
      </div>

      {/* Consumer groups (create + consume). Nucleus has no list/pending SQL
          surface, so groups are created and consumed by name. */}
      <div class={s.groupsPanel}>
        <div class={s.groupsPanelHeader}>
          <div class={s.groupsTitle}>Consumer Groups</div>
          <button
            class={s.createGroupBtn}
            onClick={() => { showCreateGroup.value = !showCreateGroup.value }}
          >
            {showCreateGroup.value ? 'Cancel' : '+ Create Group'}
          </button>
        </div>

        {showCreateGroup.value && (
          <div class={s.createGroupForm}>
            <div class={s.formRow}>
              <label class={s.formLabel}>Group name</label>
              <input
                class={s.formInput}
                placeholder="my-consumer-group"
                value={newGroupName.value}
                onInput={e => { newGroupName.value = (e.target as HTMLInputElement).value }}
              />
            </div>
            <div class={s.formRow}>
              <label class={s.formLabel}>Start (ms)</label>
              <input
                class={s.formInput}
                type="number"
                placeholder="0"
                value={newGroupStartMs.value}
                onInput={e => { newGroupStartMs.value = parseInt((e.target as HTMLInputElement).value) || 0 }}
              />
            </div>
            <button
              class={s.formSubmitBtn}
              onClick={createConsumerGroup}
              disabled={creatingGroup.value || !newGroupName.value.trim()}
            >
              {creatingGroup.value ? 'Creating...' : 'Create'}
            </button>
          </div>
        )}

        {/* Consume as group */}
        <div class={s.createGroupForm}>
          <div class={s.formRow}>
            <label class={s.formLabel}>Group</label>
            <input
              class={s.formInput}
              placeholder="group"
              value={consumeGroup.value}
              onInput={e => { consumeGroup.value = (e.target as HTMLInputElement).value }}
            />
          </div>
          <div class={s.formRow}>
            <label class={s.formLabel}>Consumer</label>
            <input
              class={s.formInput}
              placeholder="consumer"
              value={consumeConsumer.value}
              onInput={e => { consumeConsumer.value = (e.target as HTMLInputElement).value }}
            />
          </div>
          <button
            class={s.formSubmitBtn}
            onClick={readGroup}
            disabled={reading.value || !consumeGroup.value.trim() || !consumeConsumer.value.trim()}
          >
            {reading.value ? 'Reading...' : 'Read as group'}
          </button>
        </div>

        {lastReadGroup.value != null && (
          <div class={s.pendingPanel}>
            {groupEntries.value.length === 0 && (
              <div class={s.pendingMsg}>No new entries for "{lastReadGroup.value}"</div>
            )}
            {groupEntries.value.length > 0 && (
              <div class={s.pendingTable}>
                <div class={s.pendingHeader}>
                  <span class={s.pc}>Entry ID</span>
                  <span class={s.pc}>Fields</span>
                  <span class={s.pcAction} />
                </div>
                {groupEntries.value.map(pe => (
                  <div key={pe.id} class={s.pendingRow}>
                    <span class={s.pc}><span class={s.mono}>{pe.id}</span></span>
                    <span class={s.pc}><span class={s.mono}>{JSON.stringify(pe.fields ?? {})}</span></span>
                    <span class={s.pcAction}>
                      <button
                        class={s.ackBtn}
                        onClick={() => ackEntry(pe.id)}
                        title="Acknowledge this entry"
                      >
                        ACK
                      </button>
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Entry range query (STREAM_XRANGE) */}
      <div class={s.rangeBar}>
        <span class={s.rangeLabel}>From (ms)</span>
        <input
          class={s.rangeInput}
          type="number"
          value={fromMs.value}
          onInput={e => { fromMs.value = parseInt((e.target as HTMLInputElement).value) || 0 }}
        />
        <span class={s.rangeLabel}>Limit</span>
        <select class={s.limitSelect} value={entryLimit.value}
          onChange={e => { entryLimit.value = parseInt((e.target as HTMLSelectElement).value) }}>
          <option value={50}>50</option>
          <option value={100}>100</option>
          <option value={500}>500</option>
        </select>
        <button class={s.readBtn} onClick={loadEntries} disabled={loadingEntries.value}>
          {loadingEntries.value ? 'Reading...' : 'Read'}
        </button>
      </div>

      <div class={s.grid}>
        {entriesResult.value
          ? <DataGrid result={entriesResult.value} />
          : <div class={s.hint}>Loading entries...</div>
        }
      </div>
    </div>
  )
}
