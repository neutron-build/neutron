import { useEffect } from 'preact/hooks'
import {
  pendingChanges, pendingCount, clearPending, revertLast,
  activeConnection, toast,
} from '../lib/store'
import { api } from '../lib/api'
import s from './CommitBar.module.css'

export function CommitBar() {
  const count = pendingCount.value

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (!count) return
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault()
        commitAll()
      }
      if ((e.metaKey || e.ctrlKey) && e.key === 'z') {
        e.preventDefault()
        revertLast()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [count])

  if (count === 0) return null

  async function commitAll() {
    const conn = activeConnection.value
    if (!conn) return
    const changes = pendingChanges.value
    try {
      for (const change of changes) {
        await api.query(change.sql, conn.id)
      }
      clearPending()
      toast('success', `${changes.length} change${changes.length === 1 ? '' : 's'} committed`)
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err)
      toast('error', `Commit failed: ${msg}`)
    }
  }

  return (
    <div class={s.bar}>
      <div class={s.changes}>
        {pendingChanges.value.map(c => (
          <span class={s.change} key={c.id} title={c.sql}>
            <span class={s.changeModel}>{c.model}</span>
            <span class={s.changeLabel}>{c.label}</span>
          </span>
        ))}
      </div>
      <div class={s.actions}>
        <button class={s.revert} onClick={revertLast} title="Undo last change (⌘Z)">
          Revert
        </button>
        <button class={s.commit} onClick={commitAll} title="Commit all changes (⌘S)">
          Commit {count} change{count === 1 ? '' : 's'}
        </button>
      </div>
    </div>
  )
}
