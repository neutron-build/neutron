import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, isNucleus, schema, toast } from '../../lib/store'
import { api, ApiError } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import { friendlyError } from '../../lib/rls'
import type { ExplainOutcome, QueryResult, QueryHistoryEntry, SavedQuery, SqlTable } from '../../lib/types'
import { ExplainView } from './ExplainView'
import {
  buildCompletionNamespace, completionSchemas, defaultCompletionSchema,
  isWriteStatement, loadHistory, newRequestId, parameterCount, pushHistory,
} from './sqlTools'
import s from './SQLEditor.module.css'

// CodeMirror lazy-loaded to avoid bloating initial bundle
let cmLoaded = false
let EditorView: typeof import('@codemirror/view').EditorView
let basicSetup: typeof import('codemirror').basicSetup
let sql: typeof import('@codemirror/lang-sql').sql
let PostgreSQL: typeof import('@codemirror/lang-sql').PostgreSQL
let oneDark: typeof import('@codemirror/theme-one-dark').oneDark
let EditorState: typeof import('@codemirror/state').EditorState
let Compartment: typeof import('@codemirror/state').Compartment
let keymap: typeof import('@codemirror/view').keymap
let defaultKeymap: typeof import('@codemirror/commands').defaultKeymap

async function loadCM() {
  if (cmLoaded) return
  const [viewMod, coreMod, sqlMod, themeMod, stateMod, cmdMod] = await Promise.all([
    import('@codemirror/view'),
    import('codemirror'),
    import('@codemirror/lang-sql'),
    import('@codemirror/theme-one-dark'),
    import('@codemirror/state'),
    import('@codemirror/commands'),
  ])
  EditorView = viewMod.EditorView
  basicSetup = coreMod.basicSetup
  sql = sqlMod.sql
  PostgreSQL = sqlMod.PostgreSQL
  oneDark = themeMod.oneDark
  EditorState = stateMod.EditorState
  Compartment = stateMod.Compartment
  keymap = viewMod.keymap
  defaultKeymap = cmdMod.defaultKeymap
  cmLoaded = true
}

const INITIAL_DOC = '-- Write your SQL query here\nSELECT 1;'

interface SQLEditorProps {
  tabId: string
}

type SidePanel = 'history' | 'saved' | null

/** One bound parameter input: text value, or SQL NULL. */
interface ParamInput {
  value: string
  isNull: boolean
}

type Output =
  | { kind: 'result'; result: QueryResult }
  | { kind: 'explain'; outcome: ExplainOutcome }

interface Running {
  kind: 'query' | 'explain'
  requestId: string
}

/** Completion extension config for the current catalog + schema choice. */
function completionConfig(tables: readonly SqlTable[], selectedSchema: string) {
  return {
    dialect: PostgreSQL,
    schema: buildCompletionNamespace(tables),
    defaultSchema: selectedSchema,
    upperCaseKeywords: true,
  }
}

export function SQLEditor({ tabId }: SQLEditorProps) {
  const cmContainer = useRef<HTMLDivElement>(null)
  const cmView = useRef<import('@codemirror/view').EditorView | null>(null)
  const sqlCompartment = useRef<import('@codemirror/state').Compartment | null>(null)
  const runBtn = useRef<HTMLButtonElement>(null)
  const output = useSignal<Output | null>(null)
  const running = useSignal<Running | null>(null)
  const cancelRequested = useSignal(false)
  const cmReady = useSignal(false)
  const sqlText = useSignal(INITIAL_DOC)

  // Bound parameters ($1..$n), sized from the statement text.
  const params = useSignal<ParamInput[]>([])

  // EXPLAIN options. Analyze executes the statement; writes need a second,
  // explicit opt-in and are rolled back even then.
  const analyze = useSignal(false)
  const allowWrites = useSignal(false)

  // Side panel
  const sidePanel = useSignal<SidePanel>(null)
  const history = useSignal<QueryHistoryEntry[]>([])
  const savedQueries = useSignal<SavedQuery[]>([])
  const savedLoading = useSignal(false)

  // Save dialog
  const saveDialogOpen = useSignal(false)
  const saveName = useSignal('')

  const conn = activeConnection.value!
  const tables = schema.value?.sql ?? []
  const schemas = completionSchemas(tables)
  const completionSchema = useSignal<string>(defaultCompletionSchema(tables))
  const explainUnavailable = conn.isNucleus || isNucleus.value

  // Keep the schema choice valid when the catalog changes.
  useEffect(() => {
    const next = defaultCompletionSchema(tables, completionSchema.value)
    if (next !== completionSchema.value) completionSchema.value = next
  }, [schema.value])

  // Load history on mount / conn change
  useEffect(() => {
    history.value = loadHistory(conn.id)
  }, [conn.id])

  // Load saved queries when panel opens
  useEffect(() => {
    if (sidePanel.value !== 'saved') return
    savedLoading.value = true
    api.savedQueries.list()
      .then(qs => { savedQueries.value = qs })
      .catch(e => toast('error', String(e)))
      .finally(() => { savedLoading.value = false })
  }, [sidePanel.value])

  useEffect(() => {
    let destroyed = false
    loadCM().then(() => {
      if (destroyed || !cmContainer.current) return
      const compartment = new Compartment()
      sqlCompartment.current = compartment
      const view = new EditorView({
        state: EditorState.create({
          doc: INITIAL_DOC,
          extensions: [
            basicSetup,
            compartment.of(sql(completionConfig(schema.value?.sql ?? [], completionSchema.value))),
            oneDark,
            EditorView.theme({
              '&': { height: '100%', fontSize: '12px' },
              '.cm-scroller': { overflow: 'auto', fontFamily: 'var(--font-mono)' },
              '.cm-content': { padding: '8px 0' },
            }),
            keymap.of(defaultKeymap),
            EditorView.updateListener.of(update => {
              if (update.docChanged) sqlText.value = update.state.doc.toString()
            }),
            EditorView.contentAttributes.of({ 'aria-label': 'SQL editor' }),
          ],
        }),
        parent: cmContainer.current,
      })
      cmView.current = view
      sqlText.value = view.state.doc.toString()
      cmReady.value = true
    })
    return () => {
      destroyed = true
      cmView.current?.destroy()
      cmView.current = null
      sqlCompartment.current = null
    }
  }, [tabId])

  // Catalog-aware completion follows the live catalog and the schema choice.
  useEffect(() => {
    const view = cmView.current
    const compartment = sqlCompartment.current
    if (!cmReady.value || !view || !compartment) return
    view.dispatch({
      effects: compartment.reconfigure(sql(completionConfig(schema.value?.sql ?? [], completionSchema.value))),
    })
  }, [cmReady.value, schema.value, completionSchema.value])

  // Size the parameter inputs to the statement, keeping entered values.
  const paramCount = parameterCount(sqlText.value)
  useEffect(() => {
    const current = params.value
    if (current.length === paramCount) return
    const next = current.slice(0, paramCount)
    while (next.length < paramCount) next.push({ value: '', isNull: false })
    params.value = next
  }, [paramCount])

  function getSql() {
    return cmView.current?.state.sliceDoc() ?? ''
  }

  function setSql(text: string) {
    const view = cmView.current
    if (!view) return
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: text },
    })
  }

  /** Parameter values to bind, or undefined when the statement has none. */
  function boundParams(): (string | null)[] | undefined {
    if (paramCount === 0) return undefined
    return params.value.slice(0, paramCount).map(p => (p.isNull ? null : p.value))
  }

  function loadEntry(sqlSource: string, entryParams?: (string | null)[]) {
    setSql(sqlSource)
    if (entryParams && entryParams.length) {
      params.value = entryParams.map(v => (v === null ? { value: '', isNull: true } : { value: v, isNull: false }))
    }
  }

  // After a run the Cancel button disables; keep keyboard focus in the
  // toolbar instead of letting it fall back to the document body. The Run
  // button is focusable again only after the re-render that enables it.
  const refocusRun = useRef(false)
  function restoreFocusAfterRun() {
    const active = document.activeElement
    refocusRun.current = !active || active === document.body || (active as HTMLElement).dataset?.role === 'cancel'
  }
  useEffect(() => {
    if (running.value === null && refocusRun.current) {
      refocusRun.current = false
      runBtn.current?.focus()
    }
  }, [running.value])

  async function runQuery() {
    if (running.value) return
    const sqlSource = getSql()
    if (!sqlSource.trim()) return
    const requestId = newRequestId()
    const bound = boundParams()
    running.value = { kind: 'query', requestId }
    cancelRequested.value = false
    output.value = null
    const start = Date.now()
    try {
      const res = await api.query(sqlSource, conn.id, bound, requestId)
      output.value = { kind: 'result', result: res.error ? { ...res, error: friendlyError(res.error) } : res }
      pushHistory(conn.id, {
        sql: sqlSource.trim(),
        executedAt: new Date().toISOString(),
        duration: res.duration ?? (Date.now() - start),
        rowCount: res.rowCount ?? 0,
        params: bound,
        status: res.canceled ? 'canceled' : res.error ? 'error' : 'ok',
      })
      history.value = loadHistory(conn.id)
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      output.value = { kind: 'result', result: { columns: [], rows: [], rowCount: 0, duration: 0, error: message } }
      toast('error', message)
    } finally {
      running.value = null
      cancelRequested.value = false
      restoreFocusAfterRun()
    }
  }

  async function runExplain() {
    if (running.value || explainUnavailable) return
    const sqlSource = getSql()
    if (!sqlSource.trim()) return
    const requestId = newRequestId()
    running.value = { kind: 'explain', requestId }
    cancelRequested.value = false
    output.value = null
    try {
      const outcome = await api.explain({
        connectionId: conn.id,
        sql: sqlSource,
        params: boundParams(),
        requestId,
        analyze: analyze.value,
        allowWrites: analyze.value && allowWrites.value,
      })
      output.value = { kind: 'explain', outcome }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      output.value = { kind: 'explain', outcome: { ok: false, state: 'sql-error', error: message } }
    } finally {
      running.value = null
      cancelRequested.value = false
      restoreFocusAfterRun()
    }
  }

  /** Server-side cancel of the running statement (pg_cancel_backend). */
  async function cancelRunning() {
    const current = running.value
    if (!current || cancelRequested.value) return
    cancelRequested.value = true
    try {
      await api.cancelQuery(conn.id, current.requestId)
    } catch (err: unknown) {
      // 404: the statement finished before the cancel arrived — nothing to do.
      if (err instanceof ApiError && err.status === 404) return
      if (running.value?.requestId === current.requestId) cancelRequested.value = false
      toast('error', `Cancel failed: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  function handleKeyDown(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'Enter') {
      e.preventDefault()
      runExplain()
    } else if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      runQuery()
    }
  }

  function togglePanel(panel: SidePanel) {
    sidePanel.value = sidePanel.value === panel ? null : panel
  }

  async function saveQuery() {
    const name = saveName.value.trim()
    const sqlSource = getSql().trim()
    if (!name || !sqlSource) return
    try {
      const q = await api.savedQueries.save(name, sqlSource)
      savedQueries.value = [q, ...savedQueries.value]
      toast('success', `Saved "${name}"`)
    } catch (e) {
      toast('error', String(e))
    }
    saveDialogOpen.value = false
    saveName.value = ''
  }

  async function deleteSaved(id: string) {
    try {
      await api.savedQueries.remove(id)
      savedQueries.value = savedQueries.value.filter(q => q.id !== id)
    } catch (e) {
      toast('error', String(e))
    }
  }

  function formatTs(iso: string) {
    try {
      return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    } catch {
      return iso
    }
  }

  function updateParam(index: number, patch: Partial<ParamInput>) {
    params.value = params.value.map((p, i) => (i === index ? { ...p, ...patch } : p))
  }

  const busy = running.value !== null
  const writes = isWriteStatement(sqlText.value)
  const out = output.value

  return (
    <div class={s.editor} onKeyDown={handleKeyDown}>
      <div class={s.editorToolbar}>
        <div class={s.toolbarLeft}>
          <label class={s.schemaPicker} title="Unqualified table names complete from this schema; other schemas complete schema-qualified. Queries still run with the connection's own search_path.">
            <span>Schema</span>
            <select
              aria-label="Completion schema"
              value={completionSchema.value}
              onChange={e => { completionSchema.value = (e.target as HTMLSelectElement).value }}
              disabled={schemas.length === 0}
            >
              {schemas.length === 0 && <option value={completionSchema.value}>{completionSchema.value}</option>}
              {schemas.map(name => <option key={name} value={name}>{name}</option>)}
            </select>
          </label>
          <span class={s.hint}>⌘↵ run · ⌘⇧↵ explain</span>
        </div>
        <div class={s.toolbarRight}>
          <button
            class={`${s.panelBtn} ${sidePanel.value === 'history' ? s.panelBtnActive : ''}`}
            onClick={() => togglePanel('history')}
            title="Query history"
            aria-pressed={sidePanel.value === 'history'}
          >
            History
          </button>
          <button
            class={`${s.panelBtn} ${sidePanel.value === 'saved' ? s.panelBtnActive : ''}`}
            onClick={() => togglePanel('saved')}
            title="Saved queries"
            aria-pressed={sidePanel.value === 'saved'}
          >
            Saved
          </button>
          <button
            class={s.saveQueryBtn}
            onClick={() => { saveDialogOpen.value = true; saveName.value = '' }}
            title="Save current query"
          >
            + Save
          </button>
          <label class={s.optionToggle} title={explainUnavailable ? 'EXPLAIN is not available for Nucleus connections' : 'ANALYZE executes the statement'}>
            <input
              type="checkbox"
              checked={analyze.value}
              disabled={explainUnavailable}
              onChange={e => {
                analyze.value = (e.target as HTMLInputElement).checked
                if (!analyze.value) allowWrites.value = false
              }}
            />
            Analyze
          </label>
          {analyze.value && (
            <label class={s.optionToggle} title="Let EXPLAIN ANALYZE execute writes; they are rolled back afterwards">
              <input
                type="checkbox"
                checked={allowWrites.value}
                onChange={e => { allowWrites.value = (e.target as HTMLInputElement).checked }}
              />
              Allow writes (rolled back)
            </label>
          )}
          <button
            class={s.explainBtn}
            onClick={runExplain}
            disabled={busy || explainUnavailable}
            title={explainUnavailable
              ? 'EXPLAIN is not available for Nucleus connections: its plan format and read-only guarantees are not verified'
              : analyze.value ? 'Execute the statement and show the measured plan' : 'Show the estimated plan without executing the statement'}
          >
            {analyze.value ? 'Explain Analyze' : 'Explain'}
          </button>
          <button ref={runBtn} class={s.runBtn} onClick={runQuery} disabled={busy}>
            {running.value?.kind === 'query' ? 'Running…' : '▶ Run'}
          </button>
          <button
            class={s.cancelBtn}
            data-role="cancel"
            onClick={cancelRunning}
            disabled={!busy || cancelRequested.value}
            title="Cancel the running statement on the server"
          >
            {cancelRequested.value ? 'Canceling…' : 'Cancel'}
          </button>
        </div>
      </div>

      {analyze.value && (
        <div class={`${s.analyzeNote} ${allowWrites.value ? s.analyzeNoteWrites : ''}`} role="note">
          {allowWrites.value
            ? 'EXPLAIN ANALYZE will execute this statement including its writes, inside a transaction that Studio then rolls back. Sequence increments and effects outside the database are not undone.'
            : writes
              ? 'This statement writes. EXPLAIN ANALYZE executes statements, so Studio will refuse it unless you allow writes.'
              : 'EXPLAIN ANALYZE executes the statement (read-only transaction, rolled back). Writes are refused.'}
        </div>
      )}

      <div class={s.srStatus} role="status" aria-live="polite">
        {running.value
          ? cancelRequested.value
            ? 'Cancel requested…'
            : running.value.kind === 'query' ? 'Query running' : 'Explain running'
          : ''}
      </div>

      {/* Save dialog */}
      {saveDialogOpen.value && (
        <div class={s.saveDialog}>
          <input
            class={s.saveNameInput}
            placeholder="Query name…"
            aria-label="Saved query name"
            value={saveName.value}
            onInput={e => { saveName.value = (e.target as HTMLInputElement).value }}
            onKeyDown={e => {
              if (e.key === 'Enter') saveQuery()
              if (e.key === 'Escape') { saveDialogOpen.value = false }
            }}
            // eslint-disable-next-line jsx-a11y/no-autofocus
            autoFocus
          />
          <button class={s.saveConfirmBtn} onClick={saveQuery} disabled={!saveName.value.trim()}>
            Save
          </button>
          <button class={s.saveCancelBtn} onClick={() => { saveDialogOpen.value = false }}>
            Cancel
          </button>
        </div>
      )}

      {paramCount > 0 && (
        <fieldset class={s.params}>
          <legend>Parameters</legend>
          {params.value.slice(0, paramCount).map((p, i) => (
            <div key={i} class={s.paramRow}>
              <label class={s.paramLabel} for={`${tabId}-param-${i + 1}`}>${i + 1}</label>
              <input
                id={`${tabId}-param-${i + 1}`}
                class={s.paramInput}
                value={p.isNull ? '' : p.value}
                disabled={p.isNull}
                placeholder={p.isNull ? 'NULL' : 'value (text; PostgreSQL infers the type)'}
                onInput={e => updateParam(i, { value: (e.target as HTMLInputElement).value })}
              />
              <label class={s.paramNull}>
                <input
                  type="checkbox"
                  checked={p.isNull}
                  aria-label={`$${i + 1} is NULL`}
                  onChange={e => updateParam(i, { isNull: (e.target as HTMLInputElement).checked })}
                />
                NULL
              </label>
            </div>
          ))}
        </fieldset>
      )}

      <div class={s.editorBody}>
        <div class={s.editorMain}>
          <div class={s.cmWrap} ref={cmContainer} />
          {out?.kind === 'result' && (
            <div class={s.results}>
              {out.result.canceled && (
                <div class={s.canceledNote} role="alert">
                  Query canceled on the server.
                  {out.result.connectionReused === true && ' Its connection was verified and reused.'}
                  {out.result.connectionReused === false && ' Its connection was discarded.'}
                </div>
              )}
              {out.result.error && !out.result.canceled
                ? <div role="alert"><DataGrid result={out.result} /></div>
                : !out.result.canceled && <DataGrid result={out.result} />}
            </div>
          )}
          {out?.kind === 'explain' && (
            <div class={s.results}>
              <ExplainView outcome={out.outcome} />
            </div>
          )}
        </div>

        {/* Side panel */}
        {sidePanel.value && (
          <div class={s.sidePanel}>
            <div class={s.sidePanelTabs}>
              <button
                class={`${s.sidePanelTab} ${sidePanel.value === 'history' ? s.sidePanelTabActive : ''}`}
                onClick={() => { sidePanel.value = 'history' }}
              >
                History
              </button>
              <button
                class={`${s.sidePanelTab} ${sidePanel.value === 'saved' ? s.sidePanelTabActive : ''}`}
                onClick={() => { sidePanel.value = 'saved' }}
              >
                Saved
              </button>
            </div>

            {sidePanel.value === 'history' && (
              <div class={s.sidePanelList}>
                {history.value.length === 0 && (
                  <div class={s.sidePanelEmpty}>No history yet</div>
                )}
                {history.value.map((entry, i) => (
                  <button
                    key={i}
                    class={s.historyItem}
                    onClick={() => loadEntry(entry.sql, entry.params)}
                    title={entry.sql}
                  >
                    <span class={s.historySQL}>{entry.sql}</span>
                    <span class={s.historyMeta}>
                      {formatTs(entry.executedAt)} · {entry.rowCount} rows · {entry.duration}ms
                      {entry.params && entry.params.length > 0 && ` · ${entry.params.length} params`}
                      {entry.status === 'canceled' && ' · canceled'}
                      {entry.status === 'error' && ' · error'}
                    </span>
                  </button>
                ))}
              </div>
            )}

            {sidePanel.value === 'saved' && (
              <div class={s.sidePanelList}>
                {savedLoading.value && <div class={s.sidePanelEmpty}>Loading…</div>}
                {!savedLoading.value && savedQueries.value.length === 0 && (
                  <div class={s.sidePanelEmpty}>No saved queries</div>
                )}
                {savedQueries.value.map(q => (
                  <div key={q.id} class={s.savedItem}>
                    <button class={s.savedLoad} onClick={() => loadEntry(q.sql)} title={q.sql}>
                      <span class={s.savedName}>{q.name}</span>
                      <span class={s.savedSQL}>{q.sql}</span>
                    </button>
                    <button class={s.savedDelete} onClick={() => deleteSaved(q.id)} title="Delete" aria-label={`Delete saved query ${q.name}`}>✕</button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
