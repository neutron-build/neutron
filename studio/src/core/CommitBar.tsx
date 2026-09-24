import { useEffect } from 'preact/hooks'
import {
  stagedEdits, stagedCount, removeStagedEdit, discardLastStaged, clearStaged,
  commitStaged, previewStaged, revertLastCommit,
  commitPhase, commitError, lastCommit, lastPreview,
  activeConnection, toast, bindingActive,
} from '../lib/store'
import { ApiError } from '../lib/api'
import s from './CommitBar.module.css'

/** The atomic commit bar (S02): staged row operations commit as ONE
 *  transaction under one operation ID, with a dry-run preview, per-edit
 *  discard, and server-side revert of the last committed batch. Failed or
 *  outcome-unknown commits keep the staged draft (reconcilable); a dropped
 *  response is resolved through the recorded outcome, never a blind
 *  re-commit. */
export function CommitBar() {
  const count = stagedCount.value

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (!count) return
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault()
        void commit()
      }
      if ((e.metaKey || e.ctrlKey) && e.key === 'z') {
        e.preventDefault()
        discardLastStaged()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [count])

  if (count === 0 && !lastCommit.value && commitPhase.value !== 'failed') return null

  async function commit() {
    const conn = activeConnection.value
    if (!conn) return
    for (const edit of stagedEdits.value) {
      if (!bindingActive({ connectionId: edit.connectionId, schema: edit.operation.schema, table: edit.operation.table })) {
        toast('error', 'Staged edits belong to another connection — switch back or discard them; connection switching never carries edits')
        return
      }
    }
    try {
      const res = await commitStaged(conn.id)
      const reverted = res.replayed ? ' (replayed recorded outcome)' : ''
      toast('success', `Committed ${res.rowsAffected} change${res.rowsAffected === 1 ? '' : 's'} atomically${reverted}`)
    } catch (err: unknown) {
      if (err instanceof Error && err.message) {
        toast('error', `Commit failed: ${err.message}`)
      }
    }
  }

  async function preview() {
    const conn = activeConnection.value
    if (!conn) return
    try {
      const res = await previewStaged(conn.id)
      const { insert = 0, update = 0, delete: del = 0 } = res.counts
      toast('info', `Preview (nothing applied): ${insert} insert, ${update} update, ${del} delete`)
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        toast('error', `Preview refused: ${err.message}`)
        return
      }
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  async function revert() {
    const conn = activeConnection.value
    if (!conn) return
    try {
      const res = await revertLastCommit(conn.id)
      toast('success', `Reverted ${res.reverted}`)
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        toast('error', `Revert refused: ${err.message}`)
        return
      }
      toast('error', err instanceof Error ? err.message : String(err))
    }
  }

  const busy = commitPhase.value === 'committing'
  const last = lastCommit.value
  const previewReport = lastPreview.value

  return (
    <div class={s.bar}>
      <div class={s.changes}>
        {stagedEdits.value.map(e => (
          <span class={s.change} key={e.id} title={e.label}>
            <span class={s.changeModel}>{e.operation.op}</span>
            <span class={s.changeLabel}>{e.label}</span>
            <button
              class={s.revert}
              title="Discard this staged edit"
              onClick={() => removeStagedEdit(e.id)}
            >×</button>
          </span>
        ))}
        {count === 0 && commitPhase.value === 'failed' && (
          <span class={s.change}>{commitError.value ?? 'commit failed'}</span>
        )}
      </div>
      <div class={s.actions}>
        {previewReport && count > 0 && (
          <span class={s.change} title="Last dry-run preview (nothing was applied)">
            preview: {previewReport.counts.insert ?? 0}+ {previewReport.counts.update ?? 0}~ {previewReport.counts.delete ?? 0}−
          </span>
        )}
        {last && (
          <button
            class={s.revert}
            onClick={revert}
            disabled={busy || !last.response.reversible}
            title={last.response.reversible
              ? `Undo committed batch ${last.operationId} through the recorded inverse`
              : (last.response.reversibleReason ?? 'this commit cannot be reverted')}
          >
            Revert commit
          </button>
        )}
        {count > 0 && (
          <>
            <button class={s.revert} onClick={discardLastStaged} title="Discard last staged edit (⌘Z)">
              Discard
            </button>
            <button class={s.revert} onClick={() => clearStaged()} title="Discard every staged edit">
              Discard all
            </button>
            <button class={s.revert} onClick={preview} disabled={busy} title="Dry-run this batch in a rolled-back transaction">
              Preview
            </button>
            <button class={s.commit} onClick={commit} disabled={busy} title="Commit all staged edits as one atomic batch (⌘S)">
              Commit {count} change{count === 1 ? '' : 's'}
            </button>
          </>
        )}
      </div>
    </div>
  )
}
