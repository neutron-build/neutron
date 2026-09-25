// ---------------------------------------------------------------------------
// @neutron-build/nucleus/graph — Graph model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';
import { NucleusError, NucleusNotFoundError } from '../errors.js';
import type { SqlTableRef } from '../identity.js';
import {
  NucleusCapabilityError,
  SpecialtyCapabilityGate,
  SPECIALTY_CAPABILITIES,
  type SpecialtyCapabilityEvidence,
} from '../capabilities.js';
import {
  traverseGraph,
  shortestPathBounded,
  createSqlBoundGraph,
  type TraversalOptions,
  type TraversalResult,
  type SqlBoundGraph,
  type SqlBoundGraphOptions,
} from './traverse.js';

export {
  MAX_TRAVERSAL_DEPTH,
  DEFAULT_TRAVERSAL_NODE_BUDGET,
  MAX_TRAVERSAL_NODE_BUDGET,
  SQL_ID_SCHEMA_KEY,
  SQL_ID_TABLE_KEY,
  SQL_ID_ROW_KEY,
} from './traverse.js';
export type {
  TraversalOptions,
  TraversalNode,
  TraversalResult,
  SqlBoundGraph,
  SqlBoundGraphOptions,
  HydratedNode,
} from './traverse.js';
export type { SqlTableRef, SqlRowRef } from '../identity.js';
export { NucleusCapabilityError } from '../capabilities.js';
export type { SpecialtyCapability, SpecialtyCapabilityEvidence, SpecialtyCapabilityStatus } from '../capabilities.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type Direction = 'out' | 'in' | 'both';

export interface GraphNode {
  id: number;
  labels?: string[];
  properties?: Record<string, unknown>;
}

export interface GraphEdge {
  id: number;
  type: string;
  fromId: number;
  toId: number;
  properties?: Record<string, unknown>;
}

export interface GraphResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

/** An adjacency entry as returned by GRAPH_NEIGHBORS. */
export interface GraphNeighbor {
  neighborId: number;
  edgeId: number;
  edgeType: string;
}

// ---------------------------------------------------------------------------
// GraphModel interface
// ---------------------------------------------------------------------------

export interface GraphModel {
  /** Create a new node with labels and optional properties. Returns the node ID. */
  addNode(labels: string[], props?: Record<string, unknown>): Promise<number>;

  /** Create an edge between two nodes. Returns the edge ID. */
  addEdge(fromId: number, toId: number, edgeType: string, props?: Record<string, unknown>): Promise<number>;

  /** Delete a node by ID. Returns `true` if it existed. */
  deleteNode(nodeId: number): Promise<boolean>;

  /** Delete an edge by ID. Returns `true` if it existed. */
  deleteEdge(edgeId: number): Promise<boolean>;

  /** Execute a Cypher query with optional parameters. */
  query(cypher: string, params?: Record<string, unknown>): Promise<GraphResult>;

  /** Find adjacent nodes, optionally filtered by edge type and direction. */
  neighbors(nodeId: number, edgeType?: string, direction?: Direction): Promise<GraphNeighbor[]>;

  /** Find the shortest path between two nodes. Returns node IDs along the path. */
  shortestPath(fromId: number, toId: number, maxDepth?: number): Promise<number[]>;

  /** Return the total number of nodes. */
  nodeCount(): Promise<number>;

  /** Return the total number of edges. */
  edgeCount(): Promise<number>;

  /**
   * Bounded, cycle-safe traversal (X02). `maxDepth` is REQUIRED (1..32) —
   * there is deliberately no unbounded client traversal; the engine's
   * one-hop GRAPH_NEIGHBORS primitive is expanded client-side with a visited
   * set, and the statement cost (one per expanded node) is reported in the
   * result. See `TraversalResult` for the missing-start and truncation
   * contracts.
   */
  traverse(startNodeId: number, opts: TraversalOptions): Promise<TraversalResult>;

  /**
   * Bind graph nodes to the rows of one SQL table (X02 identity tying).
   * IDENTITY REFERENCE ONLY: creates no table, no column, no migration —
   * zero statements at bind time. Nodes created through the returned surface
   * carry the row identity in reserved properties and can be hydrated back
   * to SQL rows (missing/deleted rows reported as null, never stale data).
   */
  sqlNodes(ref: SqlTableRef, opts?: SqlBoundGraphOptions): SqlBoundGraph;

  /**
   * Capability evidence for the graph surfaces (X02): probe-resolved for
   * what the relationship API needs, measured-absent for what it does not
   * offer (tenant isolation, query parameters, multi-label nodes, session
   * isolation, atomic SQL+graph writes). Probes are read-only and run once
   * per client.
   */
  capabilities(): Promise<SpecialtyCapabilityEvidence[]>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class GraphModelImpl implements GraphModel {
  private readonly gate: SpecialtyCapabilityGate;

  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {
    this.gate = new SpecialtyCapabilityGate(transport, features);
  }

  private require(): void {
    requireNucleus(this.features, 'Graph');
  }

  async addNode(labels: string[], props?: Record<string, unknown>): Promise<number> {
    this.require();
    // X02 defect D3 (reproduced live): joining multiple labels with ':' does
    // not create a multi-label node — the engine's GRAPH_ADD_NODE takes
    // exactly one label, and the join was silently stored as a single label
    // like "Person:Admin" that no (n:Person) match ever selects. Fail closed
    // instead of munging.
    if (labels.length !== 1) {
      throw new NucleusCapabilityError(
        'graph-multi-label',
        'unsupported',
        "the engine's GRAPH_ADD_NODE surface is single-label",
        `addNode takes exactly one label, got ${labels.length}: [${labels.map((l) => JSON.stringify(l)).join(', ')}]`,
      );
    }
    const label = labels[0];
    const id = props
      ? await this.transport.fetchval<number>('SELECT GRAPH_ADD_NODE($1, $2)', [label, JSON.stringify(props)])
      : await this.transport.fetchval<number>('SELECT GRAPH_ADD_NODE($1)', [label]);
    // A null answer is a broken contract, not node 0 (same class as D5).
    if (id === null || id === undefined) {
      throw new NucleusError('GRAPH_ADD_NODE_CONTRACT', `GRAPH_ADD_NODE returned no id for label ${JSON.stringify(label)}`);
    }
    return id;
  }

  async addEdge(
    fromId: number,
    toId: number,
    edgeType: string,
    props?: Record<string, unknown>,
  ): Promise<number> {
    this.require();
    if (props) {
      const propsJson = JSON.stringify(props);
      const id = await this.transport.fetchval<number>('SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)', [
        fromId,
        toId,
        edgeType,
        propsJson,
      ]);
      return this.assertEdgeCreated(id, fromId, toId, edgeType);
    }
    const id = await this.transport.fetchval<number>('SELECT GRAPH_ADD_EDGE($1, $2, $3)', [fromId, toId, edgeType]);
    return this.assertEdgeCreated(id, fromId, toId, edgeType);
  }

  /**
   * X02 defect D2 (reproduced live): GRAPH_ADD_EDGE answers NULL when either
   * endpoint does not exist; the old `?? 0` masked that as a fake edge id 0.
   * Now the NULL is turned into a precise error naming the missing
   * endpoint(s) (one GRAPH_NODE diagnostic per endpoint, error path only).
   */
  private async assertEdgeCreated(
    id: number | null,
    fromId: number,
    toId: number,
    edgeType: string,
  ): Promise<number> {
    if (id !== null && id !== undefined) return id;
    const missing: string[] = [];
    for (const [nodeId, side] of [
      [fromId, 'from'],
      [toId, 'to'],
    ] as const) {
      const node = await this.transport.fetchval<string>('SELECT GRAPH_NODE($1)', [String(nodeId)]);
      if (node === null || node === undefined) missing.push(`${side} node ${nodeId} does not exist`);
    }
    throw new NucleusNotFoundError(
      `GRAPH_ADD_EDGE(${JSON.stringify(edgeType)}) refused: ${missing.join('; ') || 'engine returned NULL for an unknown reason'}`,
    );
  }

  async deleteNode(nodeId: number): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT GRAPH_DELETE_NODE($1)', [nodeId])) ?? false;
  }

  async deleteEdge(edgeId: number): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT GRAPH_DELETE_EDGE($1)', [edgeId])) ?? false;
  }

  async query(cypher: string, params?: Record<string, unknown>): Promise<GraphResult> {
    this.require();
    // X02 defect D4 (reproduced live): the engine's GRAPH_QUERY scalar takes
    // the Cypher text ONLY — a params argument was accepted by this client
    // and silently dropped, so `$param` references died in the parser
    // ("unexpected character: '$'"). Fail closed with the boundary named
    // instead of pretending substitution exists.
    if (params && Object.keys(params).length > 0) {
      throw new NucleusCapabilityError(
        'graph-query-parameters',
        'unsupported',
        'the engine GRAPH_QUERY has no parameter substitution',
        'graph.query parameters are not supported — inline values in the Cypher text',
      );
    }
    const raw = await this.transport.fetchval<string>('SELECT GRAPH_QUERY($1)', [cypher]);
    if (!raw) return { columns: [], rows: [] };
    return JSON.parse(raw) as GraphResult;
  }

  async neighbors(nodeId: number, edgeType?: string, direction: Direction = 'out'): Promise<GraphNeighbor[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT GRAPH_NEIGHBORS($1, $2)', [nodeId, direction]);
    if (!raw) return [];
    // Engine emits [{"neighbor_id":N,"edge_id":E,"edge_type":"T"}].
    const entries = JSON.parse(raw) as Array<{ neighbor_id: number; edge_id: number; edge_type: string }>;
    const neighbors = entries.map((e) => ({
      neighborId: e.neighbor_id,
      edgeId: e.edge_id,
      edgeType: e.edge_type,
    }));

    if (edgeType) {
      return neighbors.filter((n) => n.edgeType === edgeType);
    }
    return neighbors;
  }

  async shortestPath(fromId: number, toId: number, maxDepth?: number): Promise<number[]> {
    this.require();
    // X02 defect D1 (reproduced live): the engine's GRAPH_SHORTEST_PATH scalar
    // reads only (from, to) and IGNORES a third argument — this client used
    // to send maxDepth down and hand back paths far longer than the bound.
    // With a bound, the path is computed client-side by a bounded BFS that
    // cannot exceed it; without one, the engine's unbounded bidirectional
    // BFS answers (documented as unbounded).
    if (maxDepth !== undefined) {
      if (!Number.isInteger(maxDepth) || maxDepth < 1) {
        throw new Error(`shortestPath maxDepth must be a positive integer, got ${JSON.stringify(maxDepth)}`);
      }
      return shortestPathBounded(this.raw(), fromId, toId, maxDepth);
    }
    const raw = await this.transport.fetchval<string>('SELECT GRAPH_SHORTEST_PATH($1, $2)', [fromId, toId]);
    if (!raw) return [];
    return JSON.parse(raw) as number[];
  }

  async nodeCount(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT GRAPH_NODE_COUNT()')) ?? 0;
  }

  async edgeCount(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT GRAPH_EDGE_COUNT()')) ?? 0;
  }

  private raw() {
    return { transport: this.transport, features: this.features, gate: this.gate };
  }

  traverse(startNodeId: number, opts: TraversalOptions): Promise<TraversalResult> {
    return traverseGraph(this.raw(), startNodeId, opts);
  }

  sqlNodes(ref: SqlTableRef, opts?: SqlBoundGraphOptions): SqlBoundGraph {
    return createSqlBoundGraph({ ...this.raw(), graph: this }, ref, opts);
  }

  async capabilities(): Promise<SpecialtyCapabilityEvidence[]> {
    this.require();
    const out: SpecialtyCapabilityEvidence[] = [];
    for (const c of SPECIALTY_CAPABILITIES) {
      if (c === 'document-collections') continue;
      out.push(await this.gate.status(c));
    }
    return out;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.graph` to the client. */
export const withGraph: NucleusPlugin<{ graph: GraphModel }> = {
  name: 'graph',
  init(transport: Transport, features: NucleusFeatures) {
    return { graph: new GraphModelImpl(transport, features) };
  },
};
