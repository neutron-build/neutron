import { useSignal, useComputed } from '@preact/signals'
import { useEffect } from 'preact/hooks'
import { activeConnection, schema, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './VectorModule.module.css'

interface VectorModuleProps {
  name: string
}

type ViewMode = 'grid' | 'scatter'

// --- Simple 2-component PCA via power iteration ---
// Takes an array of high-dimensional vectors and returns 2D projections.
// This is a lightweight approach that avoids heavy deps like UMAP.js.

interface Point2D {
  x: number
  y: number
  id: string
  score?: number
}

function pcaProject(vectors: number[][], ids: string[], scores?: number[]): Point2D[] {
  const n = vectors.length
  if (n === 0) return []
  const dim = vectors[0].length

  // 1. Compute mean
  const mean = new Float64Array(dim)
  for (let i = 0; i < n; i++) {
    for (let d = 0; d < dim; d++) {
      mean[d] += vectors[i][d]
    }
  }
  for (let d = 0; d < dim; d++) mean[d] /= n

  // 2. Center data
  const centered: Float64Array[] = []
  for (let i = 0; i < n; i++) {
    const row = new Float64Array(dim)
    for (let d = 0; d < dim; d++) {
      row[d] = vectors[i][d] - mean[d]
    }
    centered.push(row)
  }

  // 3. Power iteration to find first principal component
  function powerIteration(data: Float64Array[], deflated: boolean, prevPC?: Float64Array): Float64Array {
    let pc = new Float64Array(dim)
    // Random init (seeded-ish for stability)
    for (let d = 0; d < dim; d++) pc[d] = Math.sin(d * 1.3 + 0.7) + Math.cos(d * 0.9)

    for (let iter = 0; iter < 100; iter++) {
      // Multiply by X^T * X * pc (covariance direction)
      const proj = new Float64Array(n)
      for (let i = 0; i < n; i++) {
        let dot = 0
        for (let d = 0; d < dim; d++) dot += data[i][d] * pc[d]
        proj[i] = dot
      }

      const newPC = new Float64Array(dim)
      for (let i = 0; i < n; i++) {
        for (let d = 0; d < dim; d++) {
          newPC[d] += data[i][d] * proj[i]
        }
      }

      // Deflate: remove component of previous PC
      if (deflated && prevPC) {
        let overlap = 0
        for (let d = 0; d < dim; d++) overlap += newPC[d] * prevPC[d]
        for (let d = 0; d < dim; d++) newPC[d] -= overlap * prevPC[d]
      }

      // Normalize
      let norm = 0
      for (let d = 0; d < dim; d++) norm += newPC[d] * newPC[d]
      norm = Math.sqrt(norm) || 1
      for (let d = 0; d < dim; d++) pc[d] = newPC[d] / norm
    }
    return pc
  }

  const pc1 = powerIteration(centered, false)
  const pc2 = powerIteration(centered, true, pc1)

  // 4. Project each point
  const points: Point2D[] = []
  for (let i = 0; i < n; i++) {
    let x = 0, y = 0
    for (let d = 0; d < dim; d++) {
      x += centered[i][d] * pc1[d]
      y += centered[i][d] * pc2[d]
    }
    points.push({
      x,
      y,
      id: ids[i],
      score: scores ? scores[i] : undefined,
    })
  }
  return points
}

// --- Parse embedding vectors from query results ---
function parseVectors(result: QueryResult): { ids: string[]; vectors: number[][]; scores: number[] } {
  const cols = result.columns.map(c => c.toLowerCase())
  const idIdx = cols.findIndex(c => c === 'id' || c === 'point_id')
  const embIdx = cols.findIndex(c => c === 'embedding' || c === 'vector' || c === 'vec')
  const scoreIdx = cols.findIndex(c => c === 'score' || c === 'distance')

  const ids: string[] = []
  const vectors: number[][] = []
  const scores: number[] = []

  for (const row of result.rows) {
    const r = row as unknown[]
    const id = idIdx >= 0 ? String(r[idIdx]) : String(ids.length)

    // embedding could be a string "[0.1, 0.2, ...]" or already an array
    let vec: number[] | null = null
    if (embIdx >= 0) {
      const raw = r[embIdx]
      if (Array.isArray(raw)) {
        vec = raw.map(Number)
      } else if (typeof raw === 'string') {
        try {
          vec = JSON.parse(raw) as number[]
        } catch {
          // Try stripping brackets
          const inner = String(raw).replace(/^\[|\]$/g, '')
          vec = inner.split(',').map(s => parseFloat(s.trim())).filter(v => !isNaN(v))
        }
      }
    }

    if (vec && vec.length > 0) {
      ids.push(id)
      vectors.push(vec)
      scores.push(scoreIdx >= 0 ? Number(r[scoreIdx]) : NaN)
    }
  }
  return { ids, vectors, scores }
}

// --- SVG Scatter Plot ---
const SVG_W = 660
const SVG_H = 500
const PAD = 55

interface ScatterPlotProps {
  points: Point2D[]
  selectedId: string | null
  onSelect: (id: string) => void
  hasScores: boolean
}

function ScatterPlot({ points, selectedId, onSelect, hasScores }: ScatterPlotProps) {
  if (points.length === 0) {
    return <div class={s.noData}>No vectors to plot. Load a sample or run a search.</div>
  }

  const plotW = SVG_W - PAD * 2
  const plotH = SVG_H - PAD * 2

  // Compute bounds
  let xMin = Infinity, xMax = -Infinity
  let yMin = Infinity, yMax = -Infinity
  for (const p of points) {
    if (p.x < xMin) xMin = p.x
    if (p.x > xMax) xMax = p.x
    if (p.y < yMin) yMin = p.y
    if (p.y > yMax) yMax = p.y
  }
  const xPad = Math.max((xMax - xMin) * 0.08, 0.001)
  const yPad = Math.max((yMax - yMin) * 0.08, 0.001)
  xMin -= xPad; xMax += xPad
  yMin -= yPad; yMax += yPad

  const xRange = xMax - xMin || 1
  const yRange = yMax - yMin || 1

  const toX = (v: number) => PAD + ((v - xMin) / xRange) * plotW
  const toY = (v: number) => PAD + plotH - ((v - yMin) / yRange) * plotH

  // Score-based coloring: lower score (closer) = brighter
  const scoreMin = hasScores ? Math.min(...points.filter(p => p.score != null && !isNaN(p.score!)).map(p => p.score!)) : 0
  const scoreMax = hasScores ? Math.max(...points.filter(p => p.score != null && !isNaN(p.score!)).map(p => p.score!)) : 1
  const scoreRange = scoreMax - scoreMin || 1

  function pointColor(p: Point2D): string {
    if (!hasScores || p.score == null || isNaN(p.score)) return 'var(--model-vector)'
    const t = 1 - (p.score - scoreMin) / scoreRange // 1 = closest, 0 = farthest
    // Interpolate from dim purple to bright cyan
    const r = Math.round(120 * (1 - t) + 0 * t)
    const g = Math.round(80 * (1 - t) + 220 * t)
    const b = Math.round(200 * (1 - t) + 255 * t)
    return `rgb(${r},${g},${b})`
  }

  // Grid lines
  const DIVS = 5
  const xStep = xRange / DIVS
  const yStep = yRange / DIVS
  const gridXs: number[] = []
  const gridYs: number[] = []
  for (let i = 0; i <= DIVS; i++) {
    gridXs.push(xMin + i * xStep)
    gridYs.push(yMin + i * yStep)
  }

  const selPt = selectedId ? points.find(p => p.id === selectedId) : null

  return (
    <svg class={s.scatterSvg} viewBox={`0 0 ${SVG_W} ${SVG_H}`} preserveAspectRatio="xMidYMid meet">
      {/* Plot background */}
      <rect x={PAD} y={PAD} width={plotW} height={plotH} class={s.plotBg} />

      {/* Horizontal grid + labels */}
      {gridYs.map(v => {
        const y = toY(v)
        return (
          <g key={`gy-${v}`}>
            <line x1={PAD} y1={y} x2={PAD + plotW} y2={y} class={s.gridLine} />
            <text x={PAD - 6} y={y + 3} class={s.axisLabel} text-anchor="end">{v.toFixed(2)}</text>
          </g>
        )
      })}

      {/* Vertical grid + labels */}
      {gridXs.map(v => {
        const x = toX(v)
        return (
          <g key={`gx-${v}`}>
            <line x1={x} y1={PAD} x2={x} y2={PAD + plotH} class={s.gridLine} />
            <text x={x} y={PAD + plotH + 14} class={s.axisLabel} text-anchor="middle">{v.toFixed(2)}</text>
          </g>
        )
      })}

      {/* Axis titles */}
      <text x={SVG_W / 2} y={SVG_H - 4} class={s.axisTitle} text-anchor="middle">PC1</text>
      <text x={12} y={SVG_H / 2} class={s.axisTitle} text-anchor="middle" transform={`rotate(-90, 12, ${SVG_H / 2})`}>PC2</text>

      {/* Border */}
      <rect x={PAD} y={PAD} width={plotW} height={plotH} fill="none" class={s.plotBorder} />

      {/* Data points */}
      {points.map(pt => {
        const cx = toX(pt.x)
        const cy = toY(pt.y)
        const isSel = selectedId === pt.id
        return (
          <g key={pt.id} onClick={() => onSelect(pt.id)} class={s.pointGroup}>
            <circle cx={cx} cy={cy} r={12} fill="transparent" />
            <circle
              cx={cx} cy={cy}
              r={isSel ? 7 : 5}
              fill={pointColor(pt)}
              class={`${s.point} ${isSel ? s.pointSelected : ''}`}
            />
          </g>
        )
      })}

      {/* Tooltip */}
      {selPt && (() => {
        const tx = toX(selPt.x)
        const ty = toY(selPt.y)
        // Flip tooltip to left if near right edge
        const flipX = tx > SVG_W - 200
        const ox = flipX ? -190 : 14
        const lines = [
          `ID: ${selPt.id}`,
          `PC1: ${selPt.x.toFixed(4)}`,
          `PC2: ${selPt.y.toFixed(4)}`,
        ]
        if (selPt.score != null && !isNaN(selPt.score)) {
          lines.push(`Score: ${selPt.score.toFixed(6)}`)
        }
        return (
          <g>
            <rect
              x={tx + ox}
              y={ty - 10 - lines.length * 16}
              width={175}
              height={8 + lines.length * 16}
              rx={4}
              class={s.tooltip}
            />
            {lines.map((line, i) => (
              <text
                key={i}
                x={tx + ox + 8}
                y={ty - lines.length * 16 + i * 16 + 4}
                class={i === 0 ? s.tooltipTitle : s.tooltipText}
              >
                {line}
              </text>
            ))}
          </g>
        )
      })()}

      {/* Legend for score coloring */}
      {hasScores && (
        <g>
          <defs>
            <linearGradient id="score-grad" x1="0" x2="1" y1="0" y2="0">
              <stop offset="0%" stop-color="rgb(0,220,255)" />
              <stop offset="100%" stop-color="rgb(120,80,200)" />
            </linearGradient>
          </defs>
          <rect x={PAD} y={6} width={100} height={8} rx={3} fill="url(#score-grad)" />
          <text x={PAD} y={24} class={s.legendLabel}>Closest</text>
          <text x={PAD + 100} y={24} class={s.legendLabel} text-anchor="end">Farthest</text>
        </g>
      )}
    </svg>
  )
}

export function VectorModule({ name }: VectorModuleProps) {
  const queryVec = useSignal('')
  const k = useSignal(10)
  const metric = useSignal<'l2' | 'cosine' | 'dot'>('cosine')
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const sampleResult = useSignal<QueryResult | null>(null)
  const sampleLoading = useSignal(false)
  const viewMode = useSignal<ViewMode>('grid')
  const selectedPointId = useSignal<string | null>(null)

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

  // Compute PCA projection from whichever result set is active
  const scatterData = useComputed<{ points: Point2D[]; hasScores: boolean }>(() => {
    const activeResult = result.value ?? sampleResult.value
    if (!activeResult || activeResult.error) return { points: [], hasScores: false }

    const { ids, vectors, scores } = parseVectors(activeResult)
    if (vectors.length < 2) return { points: [], hasScores: false }

    const hasScores = scores.some(s => !isNaN(s))
    const projected = pcaProject(vectors, ids, hasScores ? scores : undefined)
    return { points: projected, hasScores }
  })

  const hasResults = result.value && !result.value.error
  const hasSample = sampleResult.value && !sampleResult.value.error
  const canPlot = scatterData.value.points.length >= 2

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
            {running.value ? 'Searching...' : 'Search'}
          </button>
        </div>
      </div>

      {/* Results */}
      <div class={s.results}>
        {hasResults ? (
          result.value!.error ? (
            <div class={s.error}>{result.value!.error}</div>
          ) : (
            <>
              <div class={s.resultToolbar}>
                <div class={s.resultMeta}>
                  {result.value!.rowCount} nearest neighbors &middot; {result.value!.duration}ms
                </div>
                {canPlot && (
                  <div class={s.viewToggle}>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'grid' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'grid' }}
                    >
                      Results
                    </button>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'scatter' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'scatter' }}
                    >
                      Scatter
                    </button>
                  </div>
                )}
              </div>

              {viewMode.value === 'grid' ? (
                <div class={s.grid}>
                  <DataGrid result={result.value!} />
                </div>
              ) : (
                <div class={s.scatterWrap}>
                  <ScatterPlot
                    points={scatterData.value.points}
                    selectedId={selectedPointId.value}
                    onSelect={(id) => { selectedPointId.value = selectedPointId.value === id ? null : id }}
                    hasScores={scatterData.value.hasScores}
                  />
                </div>
              )}
            </>
          )
        ) : !running.value && (
          <div class={s.sampleSection}>
            <div class={s.sampleToolbar}>
              <div class={s.sampleTitle}>Stored vectors (sample)</div>
              {canPlot && (
                <div class={s.viewToggle}>
                  <button
                    class={`${s.toggleBtn} ${viewMode.value === 'grid' ? s.toggleActive : ''}`}
                    onClick={() => { viewMode.value = 'grid' }}
                  >
                    Table
                  </button>
                  <button
                    class={`${s.toggleBtn} ${viewMode.value === 'scatter' ? s.toggleActive : ''}`}
                    onClick={() => { viewMode.value = 'scatter' }}
                  >
                    PCA Plot
                  </button>
                </div>
              )}
            </div>
            {sampleLoading.value && <div class={s.msg}>Loading sample...</div>}
            {hasSample && viewMode.value === 'grid' && (
              <div class={s.grid}>
                <DataGrid result={sampleResult.value!} />
              </div>
            )}
            {hasSample && viewMode.value === 'scatter' && (
              <div class={s.scatterWrap}>
                <ScatterPlot
                  points={scatterData.value.points}
                  selectedId={selectedPointId.value}
                  onSelect={(id) => { selectedPointId.value = selectedPointId.value === id ? null : id }}
                  hasScores={scatterData.value.hasScores}
                />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
