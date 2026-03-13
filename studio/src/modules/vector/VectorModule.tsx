import { useSignal } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './VectorModule.module.css'

interface VectorModuleProps {
  name: string
}

export function VectorModule({ name }: VectorModuleProps) {
  const queryVec = useSignal('')
  const k = useSignal(10)
  const metric = useSignal<'l2' | 'cosine' | 'dot'>('cosine')
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const sampleResult = useSignal<QueryResult | null>(null)
  const sampleLoading = useSignal(false)

  const conn = activeConnection.value!

  // Pull index metadata from schema
  const indexInfo = schema.value?.vector.find(v => v.name === name)

  // Load a sample of stored vectors on mount
  useEffect(() => {
    async function loadSample() {
      sampleLoading.value = true
      try {
        const r = await api.query(
          `SELECT id, embedding FROM vector_scan('${name}', 20)`,
          conn.id
        )
        sampleResult.value = r
      } catch {
        // not critical
      } finally {
        sampleLoading.value = false
      }
    }
    loadSample()
  }, [name])

  async function runSearch() {
    const vec = queryVec.value.trim()
    if (!vec) { toast('error', 'Enter a query vector'); return }

    // Validate it looks like [n,n,n]
    if (!vec.startsWith('[') || !vec.endsWith(']')) {
      toast('error', 'Vector must be in [1.0, 0.5, ...] format')
      return
    }

    running.value = true
    result.value = null
    try {
      const r = await api.query(
        `SELECT id, embedding, VECTOR_DISTANCE(embedding, VECTOR('${vec}'), '${metric.value}') AS score
         FROM vector_search('${name}', VECTOR('${vec}'), ${k.value}, '${metric.value}')
         ORDER BY score ASC`,
        conn.id
      )
      result.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  return (
    <div class={s.layout}>
      {/* Header / index info */}
      <div class={s.header}>
        <div class={s.indexInfo}>
          <span class={s.indexName}>{name}</span>
          {indexInfo && (
            <>
              <span class={s.pill}>{indexInfo.dimensions}d</span>
              <span class={s.pill}>{indexInfo.metric}</span>
              <span class={s.pill}>{indexInfo.count.toLocaleString()} vectors</span>
            </>
          )}
        </div>
      </div>

      {/* Search form */}
      <div class={s.searchPanel}>
        <div class={s.searchLabel}>Query vector</div>
        <textarea
          class={s.vecInput}
          placeholder={`[0.1, 0.2, 0.3, ...]  (${indexInfo?.dimensions ?? 'N'} dimensions)`}
          value={queryVec.value}
          onInput={e => { queryVec.value = (e.target as HTMLTextAreaElement).value }}
          rows={3}
        />
        <div class={s.searchControls}>
          <div class={s.controlGroup}>
            <label class={s.controlLabel}>k</label>
            <input
              class={s.kInput}
              type="number"
              min={1}
              max={1000}
              value={k.value}
              onInput={e => { k.value = parseInt((e.target as HTMLInputElement).value) || 10 }}
            />
          </div>
          <div class={s.controlGroup}>
            <label class={s.controlLabel}>Metric</label>
            <select
              class={s.metricSelect}
              value={metric.value}
              onChange={e => { metric.value = (e.target as HTMLSelectElement).value as any }}
            >
              <option value="cosine">Cosine</option>
              <option value="l2">L2</option>
              <option value="dot">Dot product</option>
            </select>
          </div>
          <button class={s.searchBtn} onClick={runSearch} disabled={running.value}>
            {running.value ? 'Searching…' : '⌕ Search'}
          </button>
        </div>
      </div>

      {/* Results */}
      <div class={s.results}>
        {result.value ? (
          result.value.error ? (
            <div class={s.error}>{result.value.error}</div>
          ) : (
            <>
              <div class={s.resultHeader}>
                {result.value.rowCount} nearest neighbors · {result.value.duration}ms
              </div>
              <div class={s.grid}>
                <DataGrid result={result.value} />
              </div>
            </>
          )
        ) : !running.value && (
          <div class={s.sampleSection}>
            <div class={s.sampleTitle}>Stored vectors (sample)</div>
            {sampleLoading.value && <div class={s.msg}>Loading sample…</div>}
            {sampleResult.value && !sampleResult.value.error && (
              <div class={s.grid}>
                <DataGrid result={sampleResult.value} />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
