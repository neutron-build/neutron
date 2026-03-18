// ---------------------------------------------------------------------------
// @neutron/nucleus/graph — Graph model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

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

  /** Find neighboring nodes, optionally filtered by edge type and direction. */
  neighbors(nodeId: number, edgeType?: string, direction?: Direction): Promise<GraphNode[]>;

  /** Find the shortest path between two nodes. Returns node IDs along the path. */
  shortestPath(fromId: number, toId: number, maxDepth?: number): Promise<number[]>;

  /** Return the total number of nodes. */
  nodeCount(): Promise<number>;

  /** Return the total number of edges. */
  edgeCount(): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class GraphModelImpl implements GraphModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Graph');
  }

  async addNode(labels: string[], props?: Record<string, unknown>): Promise<number> {
    this.require();
    const label = labels.join(':');
    if (props) {
      const propsJson = JSON.stringify(props);
      return (await this.transport.fetchval<number>('SELECT GRAPH_ADD_NODE($1, $2)', [label, propsJson])) ?? 0;
    }
    return (await this.transport.fetchval<number>('SELECT GRAPH_ADD_NODE($1)', [label])) ?? 0;
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
      return (
        (await this.transport.fetchval<number>('SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)', [
          fromId,
          toId,
          edgeType,
          propsJson,
        ])) ?? 0
      );
    }
    return (
      (await this.transport.fetchval<number>('SELECT GRAPH_ADD_EDGE($1, $2, $3)', [fromId, toId, edgeType])) ?? 0
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
    let raw: string | null;
    if (params && Object.keys(params).length > 0) {
      const paramsJson = JSON.stringify(params);
      raw = await this.transport.fetchval<string>('SELECT GRAPH_QUERY($1, $2)', [cypher, paramsJson]);
    } else {
      raw = await this.transport.fetchval<string>('SELECT GRAPH_QUERY($1)', [cypher]);
    }
    if (!raw) return { columns: [], rows: [] };
    return JSON.parse(raw) as GraphResult;
  }

  async neighbors(nodeId: number, edgeType?: string, direction: Direction = 'out'): Promise<GraphNode[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT GRAPH_NEIGHBORS($1, $2)', [nodeId, direction]);
    if (!raw) return [];
    const nodes = JSON.parse(raw) as GraphNode[];

    if (edgeType) {
      return nodes.filter((n) => n.properties?._edge_type === edgeType);
    }
    return nodes;
  }

  async shortestPath(fromId: number, toId: number, maxDepth?: number): Promise<number[]> {
    this.require();
    let raw: string | null;
    if (maxDepth && maxDepth > 0) {
      raw = await this.transport.fetchval<string>('SELECT GRAPH_SHORTEST_PATH($1, $2, $3)', [
        fromId,
        toId,
        maxDepth,
      ]);
    } else {
      raw = await this.transport.fetchval<string>('SELECT GRAPH_SHORTEST_PATH($1, $2)', [fromId, toId]);
    }
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
