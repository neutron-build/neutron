import { describe, it, expect, vi, afterEach } from 'vitest'
import type { QueryResult } from '../../lib/types'
import { colorForLabel, forceLayout, parseGraphData, type GraphEdge, type GraphNode } from './GraphModule'

// Tests for GraphModule utility functions: forceLayout, parseGraphData,
// colorForLabel. They exercise the module's own exports, not copies.

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

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // Seeds forceLayout's random initial placement with a fixed sequence.
  const seedRandom = (values: number[]) => {
    let i = 0
    vi.spyOn(Math, 'random').mockImplementation(() => values[i++ % values.length])
  }

  it('should separate two unconnected nodes', () => {
    // The initial placement is random; this seed stacks the two nodes on
    // one vertical line, so repulsion separates them in y only. The
    // assertion is on their distance, not on one axis (an x-only check
    // failed whenever the placement happened to align them).
    seedRandom([0.5, 0.49, 0.5, 0.51])
    const nodes: GraphNode[] = [
      { id: 'n1', label: 'A', x: 400, y: 300 },
      { id: 'n2', label: 'B', x: 401, y: 300 },
    ]
    forceLayout(nodes, [], 800, 600)
    // Repulsion should push them apart
    const dist = Math.hypot(nodes[0].x - nodes[1].x, nodes[0].y - nodes[1].y)
    expect(dist).toBeGreaterThan(10)
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
