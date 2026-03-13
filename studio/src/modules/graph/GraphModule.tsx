import { useSignal, useComputed } from '@preact/signals'
import { useEffect, useMemo } from 'preact/hooks'
import { activeConnection, toast } from '../../lib/store'
import { api } from '../../lib/api'
import { DataGrid } from '../../components/DataGrid'
import type { QueryResult } from '../../lib/types'
import s from './GraphModule.module.css'

interface GraphModuleProps {
  name: string
}

type ViewMode = 'table' | 'graph'

// --- Graph data types ---
interface GraphNode {
  id: string
  label: string
  x: number
  y: number
}

interface GraphEdge {
  source: string
  target: string
  type: string
}

// --- Color palette for node labels ---
const LABEL_COLORS = [
  '#6366f1', '#f59e0b', '#10b981', '#ef4444', '#8b5cf6',
  '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#a855f7',
]

function colorForLabel(label: string, labelMap: Map<string, number>): string {
  if (!labelMap.has(label)) {
    labelMap.set(label, labelMap.size)
  }
  return LABEL_COLORS[labelMap.get(label)! % LABEL_COLORS.length]
}

// --- Force-directed layout ---
function forceLayout(nodes: GraphNode[], edges: GraphEdge[], width: number, height: number): void {
  const ITERATIONS = 50
  const REPULSION = 5000
  const ATTRACTION = 0.005
  const DAMPING = 0.9
  const MAX_DISPLACEMENT = 30

  // Initialize random positions within the viewport
  for (const node of nodes) {
    node.x = width * 0.15 + Math.random() * width * 0.7
    node.y = height * 0.15 + Math.random() * height * 0.7
  }

  if (nodes.length <= 1) return

  const nodeById = new Map(nodes.map(n => [n.id, n]))

  for (let iter = 0; iter < ITERATIONS; iter++) {
    const dx = new Map<string, number>()
    const dy = new Map<string, number>()
    for (const n of nodes) {
      dx.set(n.id, 0)
      dy.set(n.id, 0)
    }

    // Repulsion between all node pairs
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i]
        const b = nodes[j]
        let diffX = a.x - b.x
        let diffY = a.y - b.y
        let dist = Math.sqrt(diffX * diffX + diffY * diffY)
        if (dist < 1) dist = 1

        const force = REPULSION / (dist * dist)
        const fx = (diffX / dist) * force
        const fy = (diffY / dist) * force

        dx.set(a.id, dx.get(a.id)! + fx)
        dy.set(a.id, dy.get(a.id)! + fy)
        dx.set(b.id, dx.get(b.id)! - fx)
        dy.set(b.id, dy.get(b.id)! - fy)
      }
    }

    // Attraction along edges
    for (const edge of edges) {
      const a = nodeById.get(edge.source)
      const b = nodeById.get(edge.target)
      if (!a || !b) continue

      const diffX = a.x - b.x
      const diffY = a.y - b.y
      const dist = Math.sqrt(diffX * diffX + diffY * diffY)
      if (dist < 1) continue

      const force = dist * ATTRACTION
      const fx = (diffX / dist) * force
      const fy = (diffY / dist) * force

      dx.set(a.id, dx.get(a.id)! - fx)
      dy.set(a.id, dy.get(a.id)! - fy)
      dx.set(b.id, dx.get(b.id)! + fx)
      dy.set(b.id, dy.get(b.id)! + fy)
    }

    // Apply displacements with damping
    const decay = 1 - iter / ITERATIONS
    for (const node of nodes) {
      let mx = dx.get(node.id)! * DAMPING * decay
      let my = dy.get(node.id)! * DAMPING * decay
      const mag = Math.sqrt(mx * mx + my * my)
      if (mag > MAX_DISPLACEMENT) {
        mx = (mx / mag) * MAX_DISPLACEMENT
        my = (my / mag) * MAX_DISPLACEMENT
      }
      node.x = Math.max(40, Math.min(width - 40, node.x + mx))
      node.y = Math.max(40, Math.min(height - 40, node.y + my))
    }
  }
}

// --- Parse query results into graph nodes and edges ---
function parseGraphData(result: QueryResult): { nodes: GraphNode[]; edges: GraphEdge[] } | null {
  const cols = result.columns.map(c => c.toLowerCase())
  const nodeMap = new Map<string, GraphNode>()
  const edges: GraphEdge[] = []

  // Strategy 1: look for explicit node/edge columns
  // Cypher results often come back as columns: id, label, source, target, type, etc.
  const idIdx = cols.findIndex(c => c === 'id' || c === 'node_id')
  const labelIdx = cols.findIndex(c => c === 'label' || c === 'type' || c === 'node_label')
  const srcIdx = cols.findIndex(c => c === 'source' || c === 'src' || c === 'from' || c === 'source_id' || c === 'from_id')
  const tgtIdx = cols.findIndex(c => c === 'target' || c === 'tgt' || c === 'to' || c === 'target_id' || c === 'to_id')
  const relIdx = cols.findIndex(c => c === 'rel_type' || c === 'edge_type' || c === 'relationship' || c === 'r_type')

  // If we have source + target columns, we can build edges
  if (srcIdx >= 0 && tgtIdx >= 0) {
    for (const row of result.rows) {
      const r = row as unknown[]
      const src = String(r[srcIdx] ?? '')
      const tgt = String(r[tgtIdx] ?? '')
      if (!src || !tgt) continue

      const relType = relIdx >= 0 ? String(r[relIdx] ?? '') : ''
      edges.push({ source: src, target: tgt, type: relType })

      if (!nodeMap.has(src)) {
        nodeMap.set(src, { id: src, label: src, x: 0, y: 0 })
      }
      if (!nodeMap.has(tgt)) {
        nodeMap.set(tgt, { id: tgt, label: tgt, x: 0, y: 0 })
      }
    }
  }

  // Also extract nodes from id+label columns (for MATCH (n) style)
  if (idIdx >= 0) {
    for (const row of result.rows) {
      const r = row as unknown[]
      const id = String(r[idIdx] ?? '')
      if (!id) continue
      const lbl = labelIdx >= 0 ? String(r[labelIdx] ?? '') : id
      if (!nodeMap.has(id)) {
        nodeMap.set(id, { id, label: lbl, x: 0, y: 0 })
      } else if (labelIdx >= 0) {
        nodeMap.get(id)!.label = lbl
      }
    }
  }

  // Strategy 2: try to parse JSON-like objects from cells
  if (nodeMap.size === 0) {
    for (const row of result.rows) {
      for (const cell of row as unknown[]) {
        if (typeof cell === 'string' && cell.startsWith('{')) {
          try {
            const obj = JSON.parse(cell)
            if (obj.id != null) {
              const id = String(obj.id)
              const lbl = String(obj.label ?? obj.type ?? id)
              if (!nodeMap.has(id)) {
                nodeMap.set(id, { id, label: lbl, x: 0, y: 0 })
              }
              if (obj.source != null && obj.target != null) {
                edges.push({
                  source: String(obj.source),
                  target: String(obj.target),
                  type: String(obj.type ?? obj.rel_type ?? ''),
                })
              }
            }
          } catch { /* not JSON, skip */ }
        }
      }
    }
  }

  if (nodeMap.size === 0) return null
  return { nodes: Array.from(nodeMap.values()), edges }
}

const EXAMPLE_QUERIES = [
  'MATCH (n) RETURN n LIMIT 25',
  'MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25',
  'MATCH (n {label: "Person"}) RETURN n LIMIT 50',
  'MATCH p=(a)-[*1..3]->(b) RETURN p LIMIT 20',
]

export function GraphModule({ name }: GraphModuleProps) {
  const cypher = useSignal(EXAMPLE_QUERIES[0])
  const result = useSignal<QueryResult | null>(null)
  const running = useSignal(false)
  const statsResult = useSignal<{ nodes: number; edges: number } | null>(null)
  const viewMode = useSignal<ViewMode>('table')
  const hoveredNode = useSignal<string | null>(null)

  const conn = activeConnection.value!

  useEffect(() => {
    async function loadStats() {
      try {
        const r = await api.query(
          `SELECT node_count, edge_count FROM graph_info('${name}')`,
          conn.id
        )
        if (!r.error && r.rows.length > 0) {
          statsResult.value = { nodes: Number(r.rows[0][0]), edges: Number(r.rows[0][1]) }
        }
      } catch { /* non-critical */ }
    }
    loadStats()
  }, [name])

  async function runQuery() {
    const q = cypher.value.trim()
    if (!q) return
    running.value = true
    result.value = null
    try {
      const safeQ = q.replace(/'/g, "''")
      const r = await api.query(
        `SELECT * FROM cypher_query('${name}', '${safeQ}')`,
        conn.id
      )
      result.value = r
    } catch (err: unknown) {
      toast('error', err instanceof Error ? err.message : String(err))
    } finally {
      running.value = false
    }
  }

  // Parse graph data from results
  const graphData = useComputed(() => {
    const r = result.value
    if (!r || r.error) return null
    return parseGraphData(r)
  })

  // Run force layout when graph data changes
  const layoutData = useMemo(() => {
    const data = graphData.value
    if (!data) return null

    const WIDTH = 800
    const HEIGHT = 600
    // Clone nodes so layout mutations don't affect signal cache
    const nodes = data.nodes.map(n => ({ ...n }))
    const edges = [...data.edges]
    forceLayout(nodes, edges, WIDTH, HEIGHT)
    return { nodes, edges, width: WIDTH, height: HEIGHT }
  }, [graphData.value])

  const hasGraphView = graphData.value !== null && graphData.value.nodes.length > 0

  return (
    <div class={s.layout}>
      <div class={s.header}>
        <span class={s.graphName}>{name}</span>
        {statsResult.value && (
          <div class={s.statPills}>
            <span class={s.pill}>{statsResult.value.nodes.toLocaleString()} nodes</span>
            <span class={s.pill}>{statsResult.value.edges.toLocaleString()} edges</span>
          </div>
        )}
      </div>

      <div class={s.editorPanel}>
        <div class={s.editorLabel}>
          <span>Cypher Query</span>
          <div class={s.examples}>
            {EXAMPLE_QUERIES.map((q, i) => (
              <button key={i} class={s.exampleBtn} onClick={() => { cypher.value = q }} title={q}>
                eg{i + 1}
              </button>
            ))}
          </div>
        </div>
        <textarea
          class={s.cypherInput}
          value={cypher.value}
          onInput={e => { cypher.value = (e.target as HTMLTextAreaElement).value }}
          rows={4}
          spellcheck={false}
          onKeyDown={e => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
              e.preventDefault()
              runQuery()
            }
          }}
        />
        <div class={s.editorFooter}>
          <span class={s.hint}>Cmd+Enter to run</span>
          <button class={s.runBtn} onClick={runQuery} disabled={running.value}>
            {running.value ? 'Running...' : 'Run'}
          </button>
        </div>
      </div>

      <div class={s.results}>
        {result.value ? (
          result.value.error ? (
            <div class={s.error}>{result.value.error}</div>
          ) : (
            <>
              <div class={s.resultToolbar}>
                <div class={s.resultMeta}>
                  {result.value.rowCount} rows &middot; {result.value.duration}ms
                </div>
                {hasGraphView && (
                  <div class={s.viewToggle}>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'table' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'table' }}
                    >
                      Table
                    </button>
                    <button
                      class={`${s.toggleBtn} ${viewMode.value === 'graph' ? s.toggleActive : ''}`}
                      onClick={() => { viewMode.value = 'graph' }}
                    >
                      Graph
                    </button>
                  </div>
                )}
              </div>

              {viewMode.value === 'table' || !layoutData ? (
                <div class={s.grid}>
                  <DataGrid result={result.value} />
                </div>
              ) : (
                <div class={s.svgWrap}>
                  <GraphVisualization
                    data={layoutData}
                    hoveredNode={hoveredNode.value}
                    onHoverNode={(id) => { hoveredNode.value = id }}
                  />
                </div>
              )}
            </>
          )
        ) : !running.value && (
          <div class={s.hint2}>Run a Cypher query to explore the graph</div>
        )}
      </div>
    </div>
  )
}

// --- SVG Graph Visualization ---
interface GraphVisualizationProps {
  data: {
    nodes: GraphNode[]
    edges: GraphEdge[]
    width: number
    height: number
  }
  hoveredNode: string | null
  onHoverNode: (id: string | null) => void
}

function GraphVisualization({ data, hoveredNode, onHoverNode }: GraphVisualizationProps) {
  const { nodes, edges, width, height } = data
  const nodeById = new Map(nodes.map(n => [n.id, n]))
  const labelMap = new Map<string, number>()

  // Pre-assign colors
  const nodeColors = new Map<string, string>()
  for (const node of nodes) {
    nodeColors.set(node.id, colorForLabel(node.label, labelMap))
  }

  // Collect unique labels for the legend
  const uniqueLabels = Array.from(new Set(nodes.map(n => n.label)))

  return (
    <svg
      class={s.graphSvg}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="xMidYMid meet"
    >
      <defs>
        <marker
          id="arrowhead"
          markerWidth="10"
          markerHeight="7"
          refX="24"
          refY="3.5"
          orient="auto"
        >
          <polygon points="0 0, 10 3.5, 0 7" fill="var(--text-tertiary)" />
        </marker>
      </defs>

      {/* Edges */}
      {edges.map((edge, i) => {
        const src = nodeById.get(edge.source)
        const tgt = nodeById.get(edge.target)
        if (!src || !tgt) return null

        const isHighlighted = hoveredNode === edge.source || hoveredNode === edge.target
        const midX = (src.x + tgt.x) / 2
        const midY = (src.y + tgt.y) / 2

        return (
          <g key={`edge-${i}`}>
            <line
              x1={src.x}
              y1={src.y}
              x2={tgt.x}
              y2={tgt.y}
              class={`${s.edgeLine} ${isHighlighted ? s.edgeHighlight : ''}`}
              marker-end="url(#arrowhead)"
            />
            {edge.type && (
              <text
                x={midX}
                y={midY - 6}
                class={s.edgeLabel}
                text-anchor="middle"
              >
                {edge.type}
              </text>
            )}
          </g>
        )
      })}

      {/* Nodes */}
      {nodes.map(node => {
        const color = nodeColors.get(node.id)!
        const isHovered = hoveredNode === node.id
        const r = isHovered ? 18 : 14

        return (
          <g
            key={`node-${node.id}`}
            class={s.nodeGroup}
            onMouseEnter={() => onHoverNode(node.id)}
            onMouseLeave={() => onHoverNode(null)}
          >
            <circle
              cx={node.x}
              cy={node.y}
              r={r}
              fill={color}
              class={`${s.nodeCircle} ${isHovered ? s.nodeHovered : ''}`}
            />
            <text
              x={node.x}
              y={node.y + r + 14}
              class={s.nodeLabel}
              text-anchor="middle"
            >
              {node.label.length > 16 ? node.label.slice(0, 14) + '..' : node.label}
            </text>
            {isHovered && (
              <text
                x={node.x}
                y={node.y - r - 6}
                class={s.nodeTooltip}
                text-anchor="middle"
              >
                id: {node.id}
              </text>
            )}
          </g>
        )
      })}

      {/* Legend */}
      {uniqueLabels.length > 1 && (
        <g transform={`translate(12, ${height - uniqueLabels.length * 20 - 10})`}>
          {uniqueLabels.map((lbl, i) => (
            <g key={lbl} transform={`translate(0, ${i * 20})`}>
              <rect
                width={10}
                height={10}
                rx={2}
                fill={LABEL_COLORS[labelMap.get(lbl)! % LABEL_COLORS.length]}
              />
              <text x={16} y={9} class={s.legendText}>{lbl}</text>
            </g>
          ))}
        </g>
      )}
    </svg>
  )
}
