import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { exportCSV, exportJSON } from '../../lib/export'
import s from './FTSModule.module.css'

// Nucleus FTS is a single GLOBAL index — there is no index-name argument and
// no snippet column. FTS_SEARCH / FTS_FUZZY_SEARCH return a JSON array of
// { doc_id, score } which we parse from the single returned cell.
interface SearchHit {
  docId: string
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
  const maxDistance = useSignal(2)
  const limit = useSignal(25)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadCount() {
      try {
        const r = await api.query(`SELECT FTS_DOC_COUNT()`, conn.id)
        if (!r.error && r.rows.length > 0) totalDocs.value = Number(r.rows[0][0])
      } catch { /* non-critical */ }
    }
    loadCount()
  }, [])

  async function search() {
    const q = query.value.trim()
    if (!q) return
    running.value = true
    hits.value = []
    try {
      const esc = q.replace(/'/g, "''")
      const sql = fuzzy.value
        ? `SELECT FTS_FUZZY_SEARCH('${esc}', ${maxDistance.value}, ${limit.value})`
        : `SELECT FTS_SEARCH('${esc}', ${limit.value})`
      const r = await api.query(sql, conn.id)
      if (r.error) throw new Error(r.error)
      const cell = r.rows.length > 0 ? r.rows[0][0] : null
      hits.value = parseHits(cell)
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  function handleKey(e: KeyboardEvent) {
    if (e.key === 'Enter') search()
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
        {fuzzy.value && (
          <select
            class={s.limitSelect}
            value={maxDistance.value}
            onChange={e => { maxDistance.value = parseInt((e.target as HTMLSelectElement).value) }}
            title="Max edit distance"
          >
            <option value={1}>dist 1</option>
            <option value={2}>dist 2</option>
            <option value={3}>dist 3</option>
          </select>
        )}
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
            const data = hits.value.map(h => ({ doc_id: h.docId, score: h.score as unknown }))
            exportCSV(data, `fts-results.csv`)
          }}
          disabled={hits.value.length === 0}
          title="Export CSV"
        >CSV</button>
        <button
          class={s.exportBtn}
          onClick={() => exportJSON(hits.value, `fts-results.json`)}
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
          <div key={hit.docId} class={s.hit}>
            <div class={s.hitHeader}>
              <span class={s.hitRank}>#{i + 1}</span>
              <span class={s.hitId}>doc {hit.docId}</span>
              <span class={s.hitScore}>{hit.score.toFixed(4)}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// Parse the JSON array cell returned by FTS_SEARCH / FTS_FUZZY_SEARCH:
// [{ "doc_id": N, "score": S }]
export function parseHits(cell: unknown): SearchHit[] {
  if (cell == null) return []
  let arr: unknown
  if (typeof cell === 'string') {
    try { arr = JSON.parse(cell) } catch { return [] }
  } else {
    arr = cell
  }
  if (!Array.isArray(arr)) return []
  return arr.map((row: Record<string, unknown>) => ({
    docId: String(row.doc_id),
    score: Number(row.score),
  }))
}
