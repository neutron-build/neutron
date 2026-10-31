import { useSignal } from '@preact/signals'
import { useEffect, useId, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { useRequestOwner } from '../../lib/requestOwner'
import { queryMutationOrThrow as runMutation, api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import { isRlsDenied } from '../../lib/rls'
import { RlsNotice } from '../../components/RlsNotice'
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
    if (!Array.isArray(parsed) || parsed.some(e => !e || typeof e.id !== 'string' || !e.fields || typeof e.fields !== 'object' || Array.isArray(e.fields))) throw new Error('Invalid stream entries')
    return parsed as StreamEntry[]
  } catch {
    throw new Error('Stream entries unavailable: invalid response')
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
  const controlsId = useId()
  // Stream names are user-supplied (the engine has no stream listing), so the
  // tab label is only the starting value.
  const streamName = useSignal(name)
  const streamLen = useSignal<number | null>(null)
  const entriesResult = useSignal<QueryResult | null>(null)
  const loadingEntries = useSignal(false)
  const fromMs = useSignal(0)
  const entryLimit = useSignal(100)
  const rlsDenied = useSignal<string | null>(null)

  // Append (STREAM_XADD)
  const addField = useSignal('')
  const addValue = useSignal('')
  const appending = useSignal(false)
  const appendVersion = useRef(0)
  const groupVersion = useRef(0)
  const ackInFlight = useRef(new Set<string>())

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
  const pendingBinding = useSignal<{ connectionId: string; stream: string; group: string } | null>(null)
  const readGeneration = useRef(0)
  const lastReadGroup = useSignal<string | null>(null)
  const reading = useSignal(false)

  const priorName = useRef(name)
  if (priorName.current !== name) { priorName.current = name; streamName.value = name }
  const conn = activeConnection.value
  const metaGeneration = useRef(0)
  const entriesGeneration = useRef(0)
  const pendingRequests = useRequestOwner(JSON.stringify([conn?.id, name, 'pending']))
  const requests = useRequestOwner(JSON.stringify([conn?.id, name, streamName.value]))
  const unavailable = useSignal<string | null>(null)
  const owner = useRef({ epoch: 0, alive: true, connection: conn?.id, name: name })
  function invalidate(preservePending = false) {
    requests.invalidate()
    owner.current.epoch++
    unavailable.value = null; rlsDenied.value = null
    metaGeneration.current++; entriesGeneration.current++; appendVersion.current++; groupVersion.current++; readGeneration.current++; streamLen.value = null; entriesResult.value = null; loadingEntries.value = false; reading.value = false; appending.value = false; creatingGroup.value = false; if (!preservePending) { pendingRequests.invalidate(); groupEntries.value = []; pendingBinding.value = null; lastReadGroup.value = null; }
  }
  if (owner.current.connection !== conn?.id || owner.current.name !== name) {
    invalidate(); owner.current.connection = conn?.id; owner.current.name = name
  }
  function capture(channel: string) {
    const ticket = requests.begin(channel)
    const epoch = owner.current.epoch
    const connectionId = conn?.id
    const target = streamName.value
    return { connectionId, owns: () => ticket() && owner.current.alive && epoch === owner.current.epoch && connectionId === activeConnection.value?.id && target === streamName.value }
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

  useEffect(() => { reload() }, [conn?.id, name])

  function reload() { void loadMeta(); void loadEntries() }

  async function loadMeta() {
    const binding = capture('meta')
    const stream = streamName.value.trim()
    if (!stream || !binding.connectionId || !binding.owns()) return
    const generation = ++metaGeneration.current
    const owns = () => binding.owns() && generation === metaGeneration.current
    streamLen.value = null
    try {
      const r = await api.query(`SELECT STREAM_XLEN(${sqlStr(stream)})`, binding.connectionId)
      if (!owns()) return
      if (r.error || r.canceled) throw new Error(r.error || 'Stream metadata canceled')
      if (r.rows[0]?.[0] == null || !Number.isFinite(Number(r.rows[0][0]))) throw new Error('Stream length unavailable')
      streamLen.value = Number(r.rows[0][0])
    } catch (err) { if (owns()) fail(err) }
  }

  async function loadEntries() {
    const binding = capture('entries')
    const stream = streamName.value.trim()
    if (!stream || !binding.connectionId || !binding.owns()) return
    const generation = ++entriesGeneration.current
    const from = fromMs.value, count = entryLimit.value
    const owns = () => binding.owns() && generation === entriesGeneration.current && from === fromMs.value && count === entryLimit.value
    loadingEntries.value = true; entriesResult.value = null; unavailable.value = null
    try {
      const r = await api.query(`SELECT STREAM_XRANGE(${sqlStr(stream)}, ${from}, ${MAX_MS}, ${count})`, binding.connectionId)
      if (!owns()) return
      if (r.error || r.canceled) throw new Error(r.error || 'Stream read canceled')
      if (!r.rows.length) throw new Error('Stream entries unavailable')
      entriesResult.value = entriesToResult(parseStreamEntries(r.rows[0][0]))
    } catch (err) { if (owns()) fail(err) }
    finally { if (owns()) loadingEntries.value = false }
  }

  async function appendEntry() {
    if (appending.value) return
    const binding = capture('append')
    if (!binding.connectionId || !binding.owns()) return
    const version = appendVersion.current
    const metaRevision = metaGeneration.current
    const entriesRevision = entriesGeneration.current
    const stream = streamName.value.trim()
    if (!stream) {
      toast('error', 'Stream name is required')
      return
    }
    const field = addField.value.trim()
    if (!field) {
      toast('error', 'Field name is required')
      return
    }
    appending.value = true
    try {
      await queryMutationOrThrow(
        `SELECT STREAM_XADD(${sqlStr(stream)}, ${sqlStr(field)}, ${sqlStr(addValue.value)})`,
        binding.connectionId
      )
      if (!binding.owns()) return
      toast('success', `Appended to ${stream}`)
      if (appendVersion.current === version && binding.owns()) { addField.value = ''; addValue.value = '' }
      if (metaRevision === metaGeneration.current) await loadMeta()
      if (binding.owns() && entriesRevision === entriesGeneration.current) await loadEntries()
    } catch (err: unknown) {
      if (binding.owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) }
    } finally {
      if (binding.owns()) appending.value = false
    }
  }

  async function createConsumerGroup() {
    if (creatingGroup.value) return
    const binding = capture('create-group')
    if (!binding.connectionId || !binding.owns()) return
    const version = groupVersion.current
    const stream = streamName.value.trim()
    const groupName = newGroupName.value.trim()
    if (!stream) {
      toast('error', 'Stream name is required')
      return
    }
    if (!groupName) {
      toast('error', 'Group name is required')
      return
    }
    creatingGroup.value = true
    try {
      await queryMutationOrThrow(
        `SELECT STREAM_XGROUP_CREATE(${sqlStr(stream)}, ${sqlStr(groupName)}, ${newGroupStartMs.value})`,
        binding.connectionId
      )
      if (!binding.owns()) return
      toast('success', `Consumer group "${groupName}" created`)
      if (groupVersion.current === version && binding.owns()) {
        newGroupName.value = ''; newGroupStartMs.value = 0; showCreateGroup.value = false
      }
    } catch (err: unknown) {
      if (binding.owns()) { fail(err); toast('error', err instanceof Error ? err.message : String(err)) }
    } finally {
      if (binding.owns()) creatingGroup.value = false
    }
  }

  async function readGroup() {
    const stream = streamName.value.trim()
    const group = consumeGroup.value.trim()
    const consumer = consumeConsumer.value.trim()
    if (!stream) {
      toast('error', 'Stream name is required')
      return
    }
    if (!group || !consumer) {
      toast('error', 'Group and consumer are required')
      return
    }
    const binding = capture('group')
    if (!binding.connectionId || !binding.owns() || reading.value) return
    const generation = ++readGeneration.current
    const connectionId = binding.connectionId
    const owns = () => binding.owns() && generation === readGeneration.current && group === consumeGroup.value.trim() && consumer === consumeConsumer.value.trim()
    reading.value = true
    try {
      const r = await api.query(
        `SELECT STREAM_XREADGROUP(${sqlStr(stream)}, ${sqlStr(group)}, ${sqlStr(consumer)}, ${consumeCount.value})`,
        binding.connectionId
      )
      if (!owns()) return
      if (r.error || r.canceled) {
        groupEntries.value = []; pendingBinding.value = null; lastReadGroup.value = null
        fail(new Error(r.error || 'Query canceled'))
      } else {
        if (!r.rows.length) throw new Error('Consumer entries unavailable')
        const cell = r.rows[0][0]
        groupEntries.value = parseStreamEntries(cell)
        lastReadGroup.value = group
        pendingBinding.value = { connectionId, stream, group }
      }
    } catch (err: unknown) {
      if (owns()) { groupEntries.value = []; pendingBinding.value = null; lastReadGroup.value = null; fail(err) }
    } finally {
      if (owns()) reading.value = false
    }
  }

  async function ackEntry(entryId: string) {
    const binding = pendingBinding.value
    if (!binding) return
    if (binding.connectionId !== activeConnection.value?.id || !owner.current.alive) return
    const { group, stream, connectionId } = binding
    const key = JSON.stringify([connectionId, stream, group, entryId])
    if (ackInFlight.current.has(key)) return
    const ackOwns = pendingRequests.begin(JSON.stringify(['ack', connectionId, stream, group, entryId]))
    if (!ackOwns()) return
    ackInFlight.current.add(key)
    // Entry IDs are "ms-seq"; STREAM_XACK takes the two parts numerically.
    const [idMs, idSeq] = entryId.split('-')
    try {
      await queryMutationOrThrow(
        `SELECT STREAM_XACK(${sqlStr(stream)}, ${sqlStr(group)}, ${Number(idMs)}, ${Number(idSeq)})`,
        connectionId
      )
      if (!ackOwns()) return
      toast('success', `ACK ${entryId}`)
      if (pendingBinding.value === binding) groupEntries.value = groupEntries.value.filter(e => e.id !== entryId)
    } catch (err: unknown) {
      if (ackOwns()) toast('error', err instanceof Error ? err.message : String(err))
    } finally { ackInFlight.current.delete(key) }
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <input
          class={s.rangeInput}
          style={{ width: 180 }}
          value={streamName.value}
          placeholder="stream name"
          title="Stream name (user-supplied — the engine has no stream listing)"
          onInput={e => { invalidate(true); streamName.value = (e.target as HTMLInputElement).value }}
          onKeyDown={e => { if (e.key === 'Enter') reload() }}
          onBlur={reload}
        />
        {streamLen.value != null && (
          <span class={s.pill}>{streamLen.value.toLocaleString()} entries</span>
        )}
      </div>

      {rlsDenied.value && <RlsNotice detail={rlsDenied.value} />}
      {unavailable.value && <div role="alert">Stream data unavailable: {unavailable.value}</div>}

      {/* Append entry (STREAM_XADD) */}
      <div class={s.rangeBar}>
        <span class={s.rangeLabel}>Field</span>
        <input
          class={s.rangeInput}
          placeholder="field"
          value={addField.value}
          onInput={e => { appendVersion.current++; addField.value = (e.target as HTMLInputElement).value }}
        />
        <span class={s.rangeLabel}>Value</span>
        <input
          class={s.rangeInput}
          placeholder="value"
          value={addValue.value}
          onInput={e => { appendVersion.current++; addValue.value = (e.target as HTMLInputElement).value }}
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
            onClick={() => { groupVersion.current++; showCreateGroup.value = !showCreateGroup.value }}
          >
            {showCreateGroup.value ? 'Cancel' : '+ Create Group'}
          </button>
        </div>

        {showCreateGroup.value && (
          <div class={s.createGroupForm}>
            <div class={s.formRow}>
              <label class={s.formLabel} htmlFor={`${controlsId}-new-group`}>Group name</label>
              <input
                id={`${controlsId}-new-group`}
                class={s.formInput}
                placeholder="my-consumer-group"
                value={newGroupName.value}
                onInput={e => { groupVersion.current++; newGroupName.value = (e.target as HTMLInputElement).value }}
              />
            </div>
            <div class={s.formRow}>
              <label class={s.formLabel} htmlFor={`${controlsId}-group-start`}>Start (ms)</label>
              <input
                id={`${controlsId}-group-start`}
                class={s.formInput}
                type="number"
                placeholder="0"
                value={newGroupStartMs.value}
                onInput={e => { groupVersion.current++; newGroupStartMs.value = parseInt((e.target as HTMLInputElement).value) || 0 }}
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
            <label class={s.formLabel} htmlFor={`${controlsId}-group`}>Group</label>
            <input
              id={`${controlsId}-group`}
              class={s.formInput}
              placeholder="group"
              value={consumeGroup.value}
              onInput={e => { readGeneration.current++; reading.value = false; consumeGroup.value = (e.target as HTMLInputElement).value }}
            />
          </div>
          <div class={s.formRow}>
            <label class={s.formLabel} htmlFor={`${controlsId}-consumer`}>Consumer</label>
            <input
              id={`${controlsId}-consumer`}
              class={s.formInput}
              placeholder="consumer"
              value={consumeConsumer.value}
              onInput={e => { readGeneration.current++; reading.value = false; consumeConsumer.value = (e.target as HTMLInputElement).value }}
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
              <div class={s.pendingMsg}>No new entries for "{pendingBinding.value?.stream}" / "{lastReadGroup.value}"</div>
            )}
            {groupEntries.value.length > 0 && (
              <div class={s.pendingTable}>
                <div role="note">Pending from {pendingBinding.value?.stream} / {lastReadGroup.value}</div>
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
          onInput={e => { entriesGeneration.current++; loadingEntries.value = false; entriesResult.value = null; fromMs.value = parseInt((e.target as HTMLInputElement).value) || 0 }}
        />
        <span class={s.rangeLabel}>Limit</span>
        <select class={s.limitSelect} value={entryLimit.value}
          onChange={e => { entriesGeneration.current++; loadingEntries.value = false; entriesResult.value = null; entryLimit.value = parseInt((e.target as HTMLSelectElement).value) }}>
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

const queryMutationOrThrow = (sql: string, connectionId: string, params?: unknown[]) => runMutation(sql, connectionId, params, api.query)
