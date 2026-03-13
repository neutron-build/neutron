import { useSignal } from '@preact/signals'
import { useEffect, useRef } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult, QueryHistoryEntry, SavedQuery } from '../../lib/types'
import s from './SQLEditor.module.css'

// CodeMirror lazy-loaded to avoid bloating initial bundle
let cmLoaded = false
let EditorView: typeof import('@codemirror/view').EditorView
let basicSetup: typeof import('codemirror').basicSetup
let sql: typeof import('@codemirror/lang-sql').sql
let oneDark: typeof import('@codemirror/theme-one-dark').oneDark
let EditorState: typeof import('@codemirror/state').EditorState
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
  oneDark = themeMod.oneDark
  EditorState = stateMod.EditorState
  keymap = viewMod.keymap
  defaultKeymap = cmdMod.defaultKeymap
  cmLoaded = true
}

// --- History helpers (localStorage) ---

const HISTORY_MAX = 50

function historyKey(connId: string) {
  return `neutron:query-history:${connId}`
}

function loadHistory(connId: string): QueryHistoryEntry[] {
  try {
    return JSON.parse(localStorage.getItem(historyKey(connId)) ?? '[]')
  } catch {
    return []
  }
}

function saveHistory(connId: string, entries: QueryHistoryEntry[]) {
  localStorage.setItem(historyKey(connId), JSON.stringify(entries.slice(0, HISTORY_MAX)))
}

function pushHistory(connId: string, entry: QueryHistoryEntry) {
  const existing = loadHistory(connId)
  // Deduplicate: remove identical SQL if already at top
  const filtered = existing.filter(e => e.sql !== entry.sql)
  saveHistory(connId, [entry, ...filtered])
}

interface SQLEditorProps {
  tabId: string
}

type SidePanel = 'history' | 'saved' | null

export function SQLEditor({ tabId }: SQLEditorProps) {
  const cmContainer = useRef<HTMLDivElement>(null)
  const cmView = useRef<import('@codemirror/view').EditorView | null>(null)
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const cmReady = useSignal(false)

  // Side panel
  const sidePanel = useSignal<SidePanel>(null)
  const history = useSignal<QueryHistoryEntry[]>([])
  const savedQueries = useSignal<SavedQuery[]>([])
  const savedLoading = useSignal(false)

  // Save dialog
  const saveDialogOpen = useSignal(false)
  const saveName = useSignal('')

  const conn = activeConnection.value!

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
      const view = new EditorView({
        state: EditorState.create({
          doc: '-- Write your SQL query here\nSELECT 1;',
          extensions: [
            basicSetup,
            sql(),
            oneDark,
            EditorView.theme({
              '&': { height: '100%', fontSize: '12px' },
              '.cm-scroller': { overflow: 'auto', fontFamily: 'var(--font-mono)' },
              '.cm-content': { padding: '8px 0' },
            }),
            keymap.of(defaultKeymap),
          ],
        }),
        parent: cmContainer.current,
      })
      cmView.current = view
      cmReady.value = true
    })
    return () => {
      destroyed = true
      cmView.current?.destroy()
      cmView.current = null
    }
  }, [tabId])

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

  async function runQuery() {
    const view = cmView.current
    if (!view) return
    const sqlText = view.state.sliceDoc()
    if (!sqlText.trim()) return
    running.value = true
    result.value = null
    const start = Date.now()
    try {
      const res = await api.query(sqlText, conn.id)
      result.value = res
      // Push to history
      pushHistory(conn.id, {
        sql: sqlText.trim(),
        executedAt: new Date().toISOString(),
        duration: res.duration ?? (Date.now() - start),
        rowCount: res.rowCount ?? 0,
      })
      history.value = loadHistory(conn.id)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  function handleKeyDown(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      runQuery()
    }
  }

  function togglePanel(panel: SidePanel) {
    sidePanel.value = sidePanel.value === panel ? null : panel
  }

  async function saveQuery() {
    const name = saveName.value.trim()
    const sqlText = getSql().trim()
    if (!name || !sqlText) return
    try {
      const q = await api.savedQueries.save(name, sqlText)
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

  return (
    <div class={s.editor} onKeyDown={handleKeyDown}>
      <div class={s.editorToolbar}>
        <span class={s.hint}>⌘↵ to run</span>
        <div class={s.toolbarRight}>
          <button
            class={`${s.panelBtn} ${sidePanel.value === 'history' ? s.panelBtnActive : ''}`}
            onClick={() => togglePanel('history')}
            title="Query history"
          >
            History
          </button>
          <button
            class={`${s.panelBtn} ${sidePanel.value === 'saved' ? s.panelBtnActive : ''}`}
            onClick={() => togglePanel('saved')}
            title="Saved queries"
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
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Running…' : '▶ Run'}
          </button>
        </div>
      </div>

      {/* Save dialog */}
      {saveDialogOpen.value && (
        <div class={s.saveDialog}>
          <input
            class={s.saveNameInput}
            placeholder="Query name…"
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

      <div class={s.editorBody}>
        <div class={s.editorMain}>
          <div class={s.cmWrap} ref={cmContainer} />
          {result.value && (
            <div class={s.results}>
              <DataGrid result={result.value} />
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
                    onClick={() => setSql(entry.sql)}
                    title={entry.sql}
                  >
                    <span class={s.historySQL}>{entry.sql}</span>
                    <span class={s.historyMeta}>
                      {formatTs(entry.executedAt)} · {entry.rowCount} rows · {entry.duration}ms
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
                    <button class={s.savedLoad} onClick={() => setSql(q.sql)} title={q.sql}>
                      <span class={s.savedName}>{q.name}</span>
                      <span class={s.savedSQL}>{q.sql}</span>
                    </button>
                    <button class={s.savedDelete} onClick={() => deleteSaved(q.id)} title="Delete">✕</button>
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
