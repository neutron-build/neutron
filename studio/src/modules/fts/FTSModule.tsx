import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import s from './FTSModule.module.css'

interface SearchHit {
  id: string
  snippet: string
  score: number
}

interface FTSModuleProps {
  name: string
}

export function FTSModule({ name }: FTSModuleProps) {
  const query = useSignal('')
  const hits = useSignal<SearchHit[]>([])
  const running = useSignal(false)
  const totalDocs = useSignal<number | null>(null)
  const fuzzy = useSignal(false)
  const limit = useSignal(25)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadCount() {
      try {
        const r = await api.query(`SELECT fts_count('${name}')`, conn.id)
        if (!r.error && r.rows.length > 0) totalDocs.value = Number(r.rows[0][0])
      } catch { /* non-critical */ }
    }
    loadCount()
  }, [name])

  async function search() {
    const q = query.value.trim()
    if (!q) return
    running.value = true
    hits.value = []
    try {
      const fn = fuzzy.value ? 'fts_search_fuzzy' : 'fts_search'
      const r = await api.query(
        `SELECT id, snippet, score FROM ${fn}('${name}', '${q.replace(/'/g, "''")}', ${limit.value})
         ORDER BY score DESC`,
        conn.id
      )
      if (r.error) throw new Error(r.error)
      hits.value = r.rows.map(row => ({
        id: String(row[0]),
        snippet: String(row[1]),
        score: Number(row[2]),
      }))
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  function handleKey(e: KeyboardEvent) {
    if (e.key === 'Enter') search()
  }

  // Highlight query terms in snippet
  function highlight(text: string, q: string) {
    if (!q.trim()) return text
    const terms = q.trim().split(/\s+/).filter(Boolean)
    const pattern = new RegExp(`(${terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'gi')
    return text.replace(pattern, '<mark>$1</mark>')
  }

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.indexName}>{name}</span>
        {totalDocs.value != null && (
          <span class={s.docCount}>{totalDocs.value.toLocaleString()} documents</span>
        )}
      </div>

      <div class={s.searchBar}>
        <input
          class={s.searchInput}
          placeholder="Search documents…"
          value={query.value}
          onInput={e => { query.value = (e.target as HTMLInputElement).value }}
          onKeyDown={handleKey}
          autoFocus
        />
        <label class={s.fuzzyLabel}>
          <input
            type="checkbox"
            checked={fuzzy.value}
            onChange={() => { fuzzy.value = !fuzzy.value }}
          />
          Fuzzy
        </label>
        <select
          class={s.limitSelect}
          value={limit.value}
          onChange={e => { limit.value = parseInt((e.target as HTMLSelectElement).value) }}
        >
          <option value={10}>10</option>
          <option value={25}>25</option>
          <option value={100}>100</option>
        </select>
        <button class={s.searchBtn} onClick={search} disabled={running.value}>
          {running.value ? '...' : 'Search'}
        </button>
        <button
          class={s.exportBtn}
          onClick={() => {
            const data = hits.value.map(h => ({ id: h.id, snippet: h.snippet, score: h.score as unknown }))
            exportCSV(data, `fts-${name}.csv`)
          }}
          disabled={hits.value.length === 0}
          title="Export CSV"
        >CSV</button>
        <button
          class={s.exportBtn}
          onClick={() => exportJSON(hits.value, `fts-${name}.json`)}
          disabled={hits.value.length === 0}
          title="Export JSON"
        >JSON</button>
      </div>

      <div class={s.results}>
        {!running.value && hits.value.length === 0 && query.value && (
          <div class={s.noResults}>No results for "{query.value}"</div>
        )}
        {!running.value && hits.value.length === 0 && !query.value && (
          <div class={s.empty}>Type a query and press Enter or click Search</div>
        )}
        {hits.value.map((hit, i) => (
          <div key={hit.id} class={s.hit}>
            <div class={s.hitHeader}>
              <span class={s.hitRank}>#{i + 1}</span>
              <span class={s.hitId}>{hit.id}</span>
              <span class={s.hitScore}>{hit.score.toFixed(4)}</span>
            </div>
            <div
              class={s.hitSnippet}
              dangerouslySetInnerHTML={{ __html: highlight(hit.snippet, query.value) }}
            />
          </div>
        ))}
      </div>
    </div>
  )
}
