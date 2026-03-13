import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './StreamsModule.module.css'

interface ConsumerGroup {
  name: string
  consumers: number
  pending: number
  lastId: string
}

interface PendingEntry {
  consumer: string
  entryId: string
  idleMs: number
}

interface StreamsModuleProps {
  name: string
}

export function StreamsModule({ name }: StreamsModuleProps) {
  const streamLen = useSignal<number | null>(null)
  const groups = useSignal<ConsumerGroup[]>([])
  const entriesResult = useSignal<QueryResult | null>(null)
  const loadingEntries = useSignal(false)
  const fromId = useSignal('0-0')
  const entryLimit = useSignal(100)

  // Create consumer group form
  const showCreateGroup = useSignal(false)
  const newGroupName = useSignal('')
  const newGroupStartId = useSignal('0-0')
  const creatingGroup = useSignal(false)

  // Pending entries for a selected group
  const selectedGroup = useSignal<string | null>(null)
  const pendingEntries = useSignal<PendingEntry[]>([])
  const pendingLoading = useSignal(false)

  // Claim form
  const claimTargetConsumer = useSignal('')
  const claimingId = useSignal<string | null>(null)

  const conn = activeConnection.value!

  useEffect(() => {
    loadMeta()
    loadEntries()
  }, [name])

  async function loadMeta() {
    try {
      const lenR = await api.query(`SELECT stream_len('${name}')`, conn.id)
      if (!lenR.error && lenR.rows.length > 0) streamLen.value = Number(lenR.rows[0][0])

      await loadGroups()
    } catch { /* non-critical */ }
  }

  async function loadGroups() {
    try {
      const grpR = await api.query(
        `SELECT group_name, consumer_count, pending_count, last_delivered_id
         FROM stream_groups('${name}')`,
        conn.id
      )
      if (!grpR.error) {
        groups.value = grpR.rows.map(r => ({
          name: String(r[0]),
          consumers: Number(r[1]),
          pending: Number(r[2]),
          lastId: String(r[3]),
        }))
      }
    } catch { /* non-critical */ }
  }

  async function loadEntries() {
    loadingEntries.value = true
    try {
      const r = await api.query(
        `SELECT id, data FROM stream_range('${name}', '${fromId.value}', '+', ${entryLimit.value})`,
        conn.id
      )
      entriesResult.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      loadingEntries.value = false
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
      const startId = newGroupStartId.value.trim() || '0-0'
      await api.query(
        `SELECT stream_create_group('${name}', '${groupName.replace(/'/g, "''")}', '${startId}')`,
        conn.id
      )
      toast('success', `Consumer group "${groupName}" created`)
      newGroupName.value = ''
      newGroupStartId.value = '0-0'
      showCreateGroup.value = false
      await loadGroups()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      creatingGroup.value = false
    }
  }

  async function loadPending(groupName: string) {
    if (selectedGroup.value === groupName) {
      // Toggle off
      selectedGroup.value = null
      pendingEntries.value = []
      return
    }
    selectedGroup.value = groupName
    pendingLoading.value = true
    try {
      const r = await api.query(
        `SELECT consumer, entry_id, idle_ms FROM stream_pending('${name}', '${groupName}', 100)`,
        conn.id
      )
      if (!r.error) {
        pendingEntries.value = r.rows.map(row => ({
          consumer: String(row[0]),
          entryId: String(row[1]),
          idleMs: Number(row[2]),
        }))
      }
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      pendingLoading.value = false
    }
  }

  async function ackEntry(groupName: string, entryId: string) {
    try {
      await api.query(
        `SELECT stream_ack('${name}', '${groupName}', '${entryId}')`,
        conn.id
      )
      toast('success', `ACK ${entryId}`)
      // Remove from local pending list
      pendingEntries.value = pendingEntries.value.filter(e => e.entryId !== entryId)
      await loadGroups()
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  async function claimEntry(groupName: string, entryId: string, targetConsumer: string) {
    if (!targetConsumer.trim()) {
      toast('error', 'Target consumer name is required')
      return
    }
    try {
      await api.query(
        `SELECT stream_claim('${name}', '${groupName}', '${targetConsumer.replace(/'/g, "''")}', '${entryId}')`,
        conn.id
      )
      toast('success', `Claimed ${entryId} for ${targetConsumer}`)
      claimingId.value = null
      claimTargetConsumer.value = ''
      // Reload pending
      await loadPending(groupName)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  function formatIdleMs(ms: number): string {
    if (ms < 1000) return `${ms}ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
    return `${(ms / 60_000).toFixed(1)}m`
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.streamName}>{name}</span>
        {streamLen.value != null && (
          <span class={s.pill}>{streamLen.value.toLocaleString()} entries</span>
        )}
      </div>

      {/* Consumer groups */}
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

        {/* Create group form */}
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
              <label class={s.formLabel}>Start ID</label>
              <input
                class={s.formInput}
                placeholder="0-0"
                value={newGroupStartId.value}
                onInput={e => { newGroupStartId.value = (e.target as HTMLInputElement).value }}
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

        {groups.value.length === 0 && !showCreateGroup.value && (
          <div class={s.noGroups}>No consumer groups</div>
        )}

        {groups.value.length > 0 && (
          <div class={s.groupsTable}>
            <div class={s.groupHeader}>
              <span class={s.gc}>Group</span>
              <span class={s.gc}>Consumers</span>
              <span class={s.gc}>Pending</span>
              <span class={s.gc}>Last ID</span>
              <span class={s.gcAction} />
            </div>
            {groups.value.map(g => (
              <div key={g.name}>
                <div class={`${s.groupRow} ${selectedGroup.value === g.name ? s.groupRowActive : ''}`}>
                  <span class={s.gc}><b>{g.name}</b></span>
                  <span class={s.gc}>{g.consumers}</span>
                  <span class={s.gc}>
                    {g.pending > 0
                      ? <span class={s.pendingBadge}>{g.pending}</span>
                      : <span class={s.ok}>0</span>
                    }
                  </span>
                  <span class={s.gc}><span class={s.mono}>{g.lastId}</span></span>
                  <span class={s.gcAction}>
                    <button
                      class={s.viewPendingBtn}
                      onClick={() => loadPending(g.name)}
                      title="View pending entries"
                    >
                      {selectedGroup.value === g.name ? 'Hide' : 'Pending'}
                    </button>
                  </span>
                </div>

                {/* Pending entries for this group */}
                {selectedGroup.value === g.name && (
                  <div class={s.pendingPanel}>
                    {pendingLoading.value && <div class={s.pendingMsg}>Loading...</div>}
                    {!pendingLoading.value && pendingEntries.value.length === 0 && (
                      <div class={s.pendingMsg}>No pending entries</div>
                    )}
                    {!pendingLoading.value && pendingEntries.value.length > 0 && (
                      <div class={s.pendingTable}>
                        <div class={s.pendingHeader}>
                          <span class={s.pc}>Consumer</span>
                          <span class={s.pc}>Entry ID</span>
                          <span class={s.pc}>Idle</span>
                          <span class={s.pcAction} />
                        </div>
                        {pendingEntries.value.map(pe => (
                          <div key={pe.entryId} class={s.pendingRow}>
                            <span class={s.pc}><span class={s.mono}>{pe.consumer}</span></span>
                            <span class={s.pc}><span class={s.mono}>{pe.entryId}</span></span>
                            <span class={s.pc}>{formatIdleMs(pe.idleMs)}</span>
                            <span class={s.pcAction}>
                              <button
                                class={s.ackBtn}
                                onClick={() => ackEntry(g.name, pe.entryId)}
                                title="Acknowledge this entry"
                              >
                                ACK
                              </button>
                              {claimingId.value === pe.entryId ? (
                                <span class={s.claimForm}>
                                  <input
                                    class={s.claimInput}
                                    placeholder="consumer..."
                                    value={claimTargetConsumer.value}
                                    onInput={e => { claimTargetConsumer.value = (e.target as HTMLInputElement).value }}
                                    onKeyDown={e => {
                                      if (e.key === 'Enter') claimEntry(g.name, pe.entryId, claimTargetConsumer.value)
                                      if (e.key === 'Escape') { claimingId.value = null }
                                    }}
                                  />
                                  <button
                                    class={s.claimSubmitBtn}
                                    onClick={() => claimEntry(g.name, pe.entryId, claimTargetConsumer.value)}
                                  >
                                    OK
                                  </button>
                                </span>
                              ) : (
                                <button
                                  class={s.claimBtn}
                                  onClick={() => { claimingId.value = pe.entryId; claimTargetConsumer.value = '' }}
                                  title="Claim this entry for another consumer"
                                >
                                  Claim
                                </button>
                              )}
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Entry range query */}
      <div class={s.rangeBar}>
        <span class={s.rangeLabel}>From ID</span>
        <input
          class={s.rangeInput}
          value={fromId.value}
          onInput={e => { fromId.value = (e.target as HTMLInputElement).value }}
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
