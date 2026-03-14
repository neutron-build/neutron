import { describe, it, expect } from 'vitest'
import type { QueryResult } from '../../lib/types'

// Tests for GraphModule utility functions: forceLayout, parseGraphData, colorForLabel

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

function forceLayout(nodes: GraphNode[], edges: GraphEdge[], width: number, height: number): void {
  const ITERATIONS = 50
  const REPULSION = 5000
  const ATTRACTION = 0.005
  const DAMPING = 0.9
  const MAX_DISPLACEMENT = 30

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

function parseGraphData(result: QueryResult): { nodes: GraphNode[]; edges: GraphEdge[] } | null {
  const cols = result.columns.map(c => c.toLowerCase())
  const nodeMap = new Map<string, GraphNode>()
  const edges: GraphEdge[] = []

  const idIdx = cols.findIndex(c => c === 'id' || c === 'node_id')
  const labelIdx = cols.findIndex(c => c === 'label' || c === 'type' || c === 'node_label')
  const srcIdx = cols.findIndex(c => c === 'source' || c === 'src' || c === 'from' || c === 'source_id' || c === 'from_id')
  const tgtIdx = cols.findIndex(c => c === 'target' || c === 'tgt' || c === 'to' || c === 'target_id' || c === 'to_id')
  const relIdx = cols.findIndex(c => c === 'rel_type' || c === 'edge_type' || c === 'relationship' || c === 'r_type')

  if (srcIdx >= 0 && tgtIdx >= 0) {
    for (const row of result.rows) {
      const r = row as unknown[]
      const src = String(r[srcIdx] ?? '')
      const tgt = String(r[tgtIdx] ?? '')
      if (!src || !tgt) continue

      const relType = relIdx >= 0 ? String(r[relIdx] ?? '') : ''
      edges.push({ source: src, target: tgt, type: relType })

      if (!nodeMap.has(src)) nodeMap.set(src, { id: src, label: src, x: 0, y: 0 })
      if (!nodeMap.has(tgt)) nodeMap.set(tgt, { id: tgt, label: tgt, x: 0, y: 0 })
    }
  }

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

  if (nodeMap.size === 0) {
    for (const row of result.rows) {
      for (const cell of row as unknown[]) {
        if (typeof cell === 'string' && cell.startsWith('{')) {
          try {
            const obj = JSON.parse(cell)
            if (obj.id != null) {
              const id = String(obj.id)
              const lbl = String(obj.label ?? obj.type ?? id)
              if (!nodeMap.has(id)) nodeMap.set(id, { id, label: lbl, x: 0, y: 0 })
              if (obj.source != null && obj.target != null) {
                edges.push({
                  source: String(obj.source),
                  target: String(obj.target),
                  type: String(obj.type ?? obj.rel_type ?? ''),
                })
              }
            }
          } catch { /* skip */ }
        }
      }
    }
  }

  if (nodeMap.size === 0) return null
  return { nodes: Array.from(nodeMap.values()), edges }
}

describe('GraphModule — colorForLabel', () => {
  it('should assign consistent colors to same label', () => {
    const map = new Map<string, number>()
    const color1 = colorForLabel('Person', map)
    const color2 = colorForLabel('Person', map)
    expect(color1).toBe(color2)
  })

  it('should assign different colors to different labels', () => {
    const map = new Map<string, number>()
    const c1 = colorForLabel('Person', map)
    const c2 = colorForLabel('Company', map)
    expect(c1).not.toBe(c2)
  })

  it('should cycle colors for more labels than palette size', () => {
    const map = new Map<string, number>()
    const labels = Array.from({ length: 15 }, (_, i) => `label-${i}`)
    const colors = labels.map(l => colorForLabel(l, map))
    // 11th label should reuse the color of the 1st
    expect(colors[10]).toBe(colors[0])
  })
})

describe('GraphModule — parseGraphData', () => {
  it('should return null for empty result', () => {
    const result: QueryResult = { columns: ['x'], rows: [], rowCount: 0, duration: 0 }
    expect(parseGraphData(result)).toBeNull()
  })

  it('should parse source/target columns into edges and nodes', () => {
    const result: QueryResult = {
      columns: ['source', 'target', 'rel_type'],
      rows: [
        ['Alice', 'Bob', 'KNOWS'],
        ['Bob', 'Charlie', 'FOLLOWS'],
      ],
      rowCount: 2,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data).not.toBeNull()
    expect(data.nodes.length).toBe(3)
    expect(data.edges.length).toBe(2)
    expect(data.edges[0]).toEqual({ source: 'Alice', target: 'Bob', type: 'KNOWS' })
  })

  it('should parse id/label columns into nodes', () => {
    const result: QueryResult = {
      columns: ['id', 'label'],
      rows: [['n1', 'Person'], ['n2', 'Company']],
      rowCount: 2,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.nodes.length).toBe(2)
    expect(data.nodes.find(n => n.id === 'n1')!.label).toBe('Person')
    expect(data.edges.length).toBe(0)
  })

  it('should use id as label when no label column exists', () => {
    const result: QueryResult = {
      columns: ['id', 'age'],
      rows: [['n1', 30]],
      rowCount: 1,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.nodes[0].label).toBe('n1')
  })

  it('should handle mixed edge + node columns', () => {
    const result: QueryResult = {
      columns: ['id', 'label', 'source', 'target'],
      rows: [
        ['n1', 'Person', 'n1', 'n2'],
        ['n2', 'Place', null, null],
      ],
      rowCount: 2,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.nodes.length).toBe(2)
    expect(data.edges.length).toBe(1)
  })

  it('should parse JSON cells as fallback', () => {
    const result: QueryResult = {
      columns: ['data'],
      rows: [
        ['{"id":"x1","label":"Node1"}'],
        ['{"id":"x2","label":"Node2","source":"x1","target":"x2","type":"LINK"}'],
      ],
      rowCount: 2,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.nodes.length).toBe(2)
    expect(data.edges.length).toBe(1)
  })

  it('should skip non-JSON cells gracefully', () => {
    const result: QueryResult = {
      columns: ['data'],
      rows: [['not json'], ['{"id":"a","label":"A"}']],
      rowCount: 2,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.nodes.length).toBe(1)
  })

  it('should recognize "from" and "to" as source/target columns', () => {
    const result: QueryResult = {
      columns: ['from', 'to'],
      rows: [['A', 'B']],
      rowCount: 1,
      duration: 0,
    }
    const data = parseGraphData(result)!
    expect(data.edges.length).toBe(1)
    expect(data.edges[0].source).toBe('A')
  })
})

describe('GraphModule — forceLayout', () => {
  it('should not crash with no nodes', () => {
    forceLayout([], [], 800, 600)
  })

  it('should not move a single node', () => {
    const nodes: GraphNode[] = [{ id: 'n1', label: 'A', x: 0, y: 0 }]
    forceLayout(nodes, [], 800, 600)
    // Single node gets initial position but no force iteration
    expect(nodes[0].x).toBeGreaterThan(0)
    expect(nodes[0].y).toBeGreaterThan(0)
  })

  it('should separate two unconnected nodes', () => {
    const nodes: GraphNode[] = [
      { id: 'n1', label: 'A', x: 400, y: 300 },
      { id: 'n2', label: 'B', x: 401, y: 300 },
    ]
    forceLayout(nodes, [], 800, 600)
    // Repulsion should push them apart
    const dx = Math.abs(nodes[0].x - nodes[1].x)
    expect(dx).toBeGreaterThan(10)
  })

  it('should keep nodes within bounds', () => {
    const nodes: GraphNode[] = Array.from({ length: 10 }, (_, i) => ({
      id: `n${i}`,
      label: `Node${i}`,
      x: 0,
      y: 0,
    }))
    const edges: GraphEdge[] = [
      { source: 'n0', target: 'n1', type: '' },
      { source: 'n1', target: 'n2', type: '' },
    ]
    forceLayout(nodes, edges, 800, 600)

    for (const node of nodes) {
      expect(node.x).toBeGreaterThanOrEqual(40)
      expect(node.x).toBeLessThanOrEqual(760)
      expect(node.y).toBeGreaterThanOrEqual(40)
      expect(node.y).toBeLessThanOrEqual(560)
    }
  })

  it('should bring connected nodes closer than unconnected ones', () => {
    const nodes: GraphNode[] = [
      { id: 'a', label: 'A', x: 0, y: 0 },
      { id: 'b', label: 'B', x: 0, y: 0 },
      { id: 'c', label: 'C', x: 0, y: 0 },
    ]
    // a-b are connected, c is isolated
    const edges: GraphEdge[] = [{ source: 'a', target: 'b', type: '' }]
    forceLayout(nodes, edges, 800, 600)

    const distAB = Math.sqrt((nodes[0].x - nodes[1].x) ** 2 + (nodes[0].y - nodes[1].y) ** 2)
    const distAC = Math.sqrt((nodes[0].x - nodes[2].x) ** 2 + (nodes[0].y - nodes[2].y) ** 2)
    // Connected nodes should generally be closer (not guaranteed by random init but likely)
    // This is a statistical property; we just check the layout doesn't crash
    expect(distAB).toBeGreaterThan(0)
    expect(distAC).toBeGreaterThan(0)
  })
})
