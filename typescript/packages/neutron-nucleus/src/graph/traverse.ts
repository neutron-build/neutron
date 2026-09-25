// ---------------------------------------------------------------------------
// @neutron-build/nucleus/graph/traverse — bounded, cycle-safe traversal and
// SQL-row-identity tying (X02)
//
// The engine's graph surface is a SINGLE GLOBAL STORE with one-hop
// primitives: GRAPH_NEIGHBORS(node, direction) and an UNBOUNDED
// GRAPH_SHORTEST_PATH. It has no multi-hop, no bounded path, no per-tenant
// scoping. Traversal is therefore CLIENT-ORCHESTRATED and says so:
//
// - every traversal REQUIRES an explicit maxDepth (1..MAX_TRAVERSAL_DEPTH);
//   there is no unbounded client traversal (the engine's own shortest path
//   remains available through GraphModel.shortestPath without a bound);
// - cycles are safe: a visited set stops re-expansion (A->B->A, self-loops);
// - the statement cost is honest: one GRAPH_NEIGHBORS per expanded node,
//   reported in the result (`statements`);
// - a node/visit budget bounds work; hitting it sets `truncated` instead of
//   silently continuing.
//
// SQL IDENTITY TYING: nodes created through SqlBoundGraph carry the SQL row
// identity in flat reserved properties (sqlref_schema/sqlref_table/
// sqlref_row) so a traversal can be projected back to SQL rows. One node per
// row: addRowNode refuses a second node for the same row, and a lookup that
// finds several (written around this API) fails instead of picking one. The
// stamp is metadata: hydrate reports the SQL row honestly, INCLUDING THE
// DELETED-RECORD CASE — a row deleted after its node was created reads as
// null, never as stale data, and the graph node is left for the caller to
// decide about.
//
// Every surface here is gated on probe-resolved capabilities
// (graph-adjacency, graph-property-match); an engine that does not prove them
// fails closed with NucleusCapabilityError before the operation's first
// statement.
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';
import { NucleusConflictError, NucleusNotFoundError } from '../errors.js';
import { quoteSqlTableRef, type SqlTableRef } from '../identity.js';
import type { SpecialtyCapabilityGate } from '../capabilities.js';
import type { Direction, GraphModel, GraphNeighbor } from './index.js';

/** Hard ceiling for any traversal depth (a bound above this is a bug, not a query). */
export const MAX_TRAVERSAL_DEPTH = 32;
/** Default visit budget (distinct nodes expanded or reported). */
export const DEFAULT_TRAVERSAL_NODE_BUDGET = 1000;
/** Hard ceiling for the visit budget. */
export const MAX_TRAVERSAL_NODE_BUDGET = 10000;
/** Maximum node ids accepted by one hydrate() call (chunked internally). */
const MAX_HYDRATE_CHUNK = 256;

/** Reserved property keys carrying the SQL row identity on graph nodes.
 *  (No leading underscores: the engine's Cypher parser reserves `__`-prefixed
 *  identifiers — live-discovered in the X02 conformance leg.) */
export const SQL_ID_SCHEMA_KEY = 'sqlref_schema';
export const SQL_ID_TABLE_KEY = 'sqlref_table';
export const SQL_ID_ROW_KEY = 'sqlref_row';

/** Bounds for a traversal. `maxDepth` is required — no unbounded traversal. */
export interface TraversalOptions {
  /** Edge direction followed at every hop (default: 'out'). */
  direction?: Direction;
  /** Restrict traversal to these edge types (client-side filter — the engine's GRAPH_NEIGHBORS has no type parameter). */
  edgeTypes?: readonly string[];
  /** Maximum hops from the start node. Required, 1..MAX_TRAVERSAL_DEPTH. */
  maxDepth: number;
  /** Visit budget (default 1000, ceiling 10000). Hitting it truncates. */
  maxNodes?: number;
}

/** One node reached by a traversal. */
export interface TraversalNode {
  id: number;
  depth: number;
  viaEdgeId: number;
  viaEdgeType: string;
}

/** The result of a traversal. */
export interface TraversalResult {
  start: number;
  /** False when the start node does not exist (no traversal happened). */
  startPresent: boolean;
  /** Reached nodes (excluding the start), ordered by depth then id. */
  nodes: TraversalNode[];
  /**
   * Statements this traversal issued: one GRAPH_NODE for the start plus one
   * GRAPH_NEIGHBORS per expanded node. The read-only capability probes (once
   * per client, on first use) are not included.
   */
  statements: number;
  /** True when the visit budget stopped the traversal before depth/nodes were exhausted. */
  truncated: boolean;
}

/** Validate shared bound rules. Throws with the exact rule in the message. */
function assertBounds(maxDepth: number, maxNodes: number): void {
  if (!Number.isInteger(maxDepth) || maxDepth < 1 || maxDepth > MAX_TRAVERSAL_DEPTH) {
    throw new Error(`traversal maxDepth must be an integer between 1 and ${MAX_TRAVERSAL_DEPTH}, got ${JSON.stringify(maxDepth)}`);
  }
  if (!Number.isInteger(maxNodes) || maxNodes < 1 || maxNodes > MAX_TRAVERSAL_NODE_BUDGET) {
    throw new Error(`traversal maxNodes must be an integer between 1 and ${MAX_TRAVERSAL_NODE_BUDGET}, got ${JSON.stringify(maxNodes)}`);
  }
}

/** Fetch one hop of neighbors through the engine primitive. */
async function fetchNeighbors(
  transport: Transport,
  nodeId: number,
  direction: Direction,
): Promise<GraphNeighbor[]> {
  const raw = await transport.fetchval<string>('SELECT GRAPH_NEIGHBORS($1, $2)', [nodeId, direction]);
  if (!raw) return [];
  const entries = JSON.parse(raw) as Array<{ neighbor_id: number; edge_id: number; edge_type: string }>;
  return entries.map((e) => ({ neighborId: e.neighbor_id, edgeId: e.edge_id, edgeType: e.edge_type }));
}

/** Adjacency expansion shared by traverse() and the bounded shortest path. */
interface RawGraphModel {
  transport: Transport;
  features: NucleusFeatures;
  gate: SpecialtyCapabilityGate;
}

/**
 * Bounded, cycle-safe BFS from `start`. See the module docs for the exact
 * guarantees. Throws before any statement when the bounds are invalid.
 */
export async function traverseGraph(
  model: RawGraphModel,
  start: number,
  opts: TraversalOptions,
): Promise<TraversalResult> {
  requireNucleus(model.features, 'Graph');
  const direction = opts.direction ?? 'out';
  const maxNodes = opts.maxNodes ?? DEFAULT_TRAVERSAL_NODE_BUDGET;
  assertBounds(opts.maxDepth, maxNodes);
  const types = opts.edgeTypes ? new Set(opts.edgeTypes) : undefined;
  await model.gate.require('graph-adjacency', 'graph.traverse');

  const result: TraversalResult = {
    start,
    startPresent: true,
    nodes: [],
    statements: 0,
    truncated: false,
  };

  // Missing-record behavior for the START: distinguish "no neighbors" from
  // "node does not exist" with one GRAPH_NODE read.
  const startNode = await model.transport.fetchval<string>('SELECT GRAPH_NODE($1)', [String(start)]);
  result.statements += 1;
  if (startNode === null || startNode === undefined) {
    result.startPresent = false;
    return result;
  }

  const visited = new Set<number>([start]);
  let frontier: number[] = [start];

  for (let depth = 1; depth <= opts.maxDepth; depth++) {
    if (frontier.length === 0) break;
    const next: number[] = [];
    for (const node of frontier) {
      const neighbors = await fetchNeighbors(model.transport, node, direction);
      result.statements += 1;
      for (const n of neighbors) {
        if (types && !types.has(n.edgeType)) continue;
        if (visited.has(n.neighborId)) continue; // cycle safety
        if (result.nodes.length >= maxNodes) {
          result.truncated = true;
          result.nodes.sort((a, b) => a.depth - b.depth || a.id - b.id);
          return result;
        }
        visited.add(n.neighborId);
        result.nodes.push({ id: n.neighborId, depth, viaEdgeId: n.edgeId, viaEdgeType: n.edgeType });
        next.push(n.neighborId);
      }
    }
    frontier = next;
  }

  result.nodes.sort((a, b) => a.depth - b.depth || a.id - b.id);
  return result;
}

/**
 * Bounded shortest path (client BFS with early exit). Used by
 * GraphModel.shortestPath whenever a maxDepth is supplied — the engine's
 * GRAPH_SHORTEST_PATH scalar IGNORES a third argument (X02 defect D1,
 * reproduced live), so the bound is honored client-side or not at all.
 * Returns [] when either endpoint is missing or no path exists within the
 * bound (same contract as the unbounded engine path).
 */
export async function shortestPathBounded(
  model: RawGraphModel,
  from: number,
  to: number,
  maxDepth: number,
  direction: Direction = 'out',
): Promise<number[]> {
  requireNucleus(model.features, 'Graph');
  assertBounds(maxDepth, MAX_TRAVERSAL_NODE_BUDGET);
  await model.gate.require('graph-adjacency', 'graph.shortestPath with maxDepth');
  if (from === to) {
    const node = await model.transport.fetchval<string>('SELECT GRAPH_NODE($1)', [String(from)]);
    return node === null || node === undefined ? [] : [from];
  }

  const parent = new Map<number, { node: number; edge: number }>();
  const visited = new Set<number>([from]);
  let frontier: number[] = [from];

  for (let depth = 1; depth <= maxDepth; depth++) {
    if (frontier.length === 0) break;
    const next: number[] = [];
    for (const node of frontier) {
      const neighbors = await fetchNeighbors(model.transport, node, direction);
      for (const n of neighbors) {
        if (visited.has(n.neighborId)) continue;
        visited.add(n.neighborId);
        parent.set(n.neighborId, { node, edge: n.edgeId });
        if (n.neighborId === to) {
          // Reconstruct.
          const path: number[] = [to];
          let cur = to;
          while (cur !== from) {
            cur = parent.get(cur)!.node;
            path.push(cur);
          }
          return path.reverse();
        }
        next.push(n.neighborId);
      }
    }
    frontier = next;
  }
  return [];
}

// ---------------------------------------------------------------------------
// SqlBoundGraph — graph nodes tied explicitly to SQL row identities
// ---------------------------------------------------------------------------

/** Options for the SQL binding. */
export interface SqlBoundGraphOptions {
  /** Column holding the row identity (default: `id`). Validated as an identifier. */
  idColumn?: string;
}

/** A hydrated entry: the SQL row for a node, or null when the row is gone. */
export interface HydratedNode {
  nodeId: number;
  /** SQL row reference stamped on the node; null when the node carries no stamp. */
  sqlRef: { schema?: string; table: string; id: number; idColumn: string } | null;
  /** The SQL row, or null when the referenced row does not exist (deleted or never created). */
  row: Record<string, unknown> | null;
}

/**
 * A graph surface whose nodes represent SQL rows (X02 identity tying).
 * Reference-only binding: creating this emits NO statements and NO DDL —
 * it stamps identity metadata into node properties and reads it back.
 */
export interface SqlBoundGraph {
  /** The bound SQL table (identity metadata only). */
  readonly boundTo: SqlTableRef;
  /** The identity column (default `id`). */
  readonly idColumn: string;

  /**
   * Create THE node for one SQL row. The row need not exist yet (the stamp is
   * metadata); use `hydrate` to observe the difference. Throws
   * `NucleusConflictError` when the row already has a node. The check and
   * the insert are two statements with no engine uniqueness behind them, so
   * concurrent callers can still race; `findNodeByRow` then reports the
   * duplicate instead of choosing one.
   */
  addRowNode(rowId: number, label: string, props?: Record<string, unknown>): Promise<number>;

  /**
   * Find the node stamped for a SQL row (null when no node was created for
   * it). Throws `NucleusConflictError` when more than one node carries the
   * row's stamp. Cost: one GRAPH_QUERY that scans the graph's nodes (the
   * engine has no property index).
   */
  findNodeByRow(rowId: number): Promise<number | null>;

  /** Connect two rows of this binding. Throws `NucleusNotFoundError` when either row has no node. */
  addRowEdge(fromRowId: number, toRowId: number, edgeType: string, props?: Record<string, unknown>): Promise<number>;

  /**
   * Hydrate SQL rows for graph nodes: one GRAPH_NODE per node plus chunked,
   * fully parameterized `SELECT ... WHERE id IN (...)`. Unstamped nodes
   * report `sqlRef: null, row: null`; stamped nodes whose row is deleted
   * report `row: null` with the `sqlRef` intact — the deleted-record case
   * is data, not an error.
   */
  hydrate(nodeIds: readonly number[]): Promise<HydratedNode[]>;
}

interface StampedNode {
  id: number;
  labels: string[];
  properties: Record<string, unknown>;
}

export function createSqlBoundGraph(
  model: RawGraphModel & { graph: GraphModel },
  ref: SqlTableRef,
  opts: SqlBoundGraphOptions = {},
): SqlBoundGraph {
  requireNucleus(model.features, 'Graph');
  const gate = model.gate;
  const idColumn = opts.idColumn ?? 'id';
  // Identity references are validated up front and never become injection
  // surfaces: schema/table/idColumn are identifier-checked, row ids are
  // integers bound as parameters.
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(idColumn)) {
    throw new Error(`SQL identity column must be a plain identifier, got ${JSON.stringify(idColumn)}`);
  }
  const tableQuoted = quoteSqlTableRef(ref);

  function stamp(rowId: number, props?: Record<string, unknown>): Record<string, unknown> {
    return {
      ...(props ?? {}),
      ...(ref.schema !== undefined ? { [SQL_ID_SCHEMA_KEY]: ref.schema } : {}),
      [SQL_ID_TABLE_KEY]: ref.table,
      [SQL_ID_ROW_KEY]: rowId,
    };
  }

  async function readNode(nodeId: number): Promise<StampedNode | null> {
    const raw = await model.transport.fetchval<string>('SELECT GRAPH_NODE($1)', [String(nodeId)]);
    if (raw === null || raw === undefined) return null;
    const parsed = JSON.parse(raw) as { id: number; labels: string[]; properties: Record<string, unknown> };
    return { id: parsed.id, labels: parsed.labels, properties: parsed.properties };
  }

  async function findNodeByRow(rowId: number): Promise<number | null> {
    requireNucleus(model.features, 'Graph');
    assertRowId(rowId);
    await gate.require('graph-property-match', 'sqlNodes row lookup');
    // schema/table are identifier-validated (no quotes to escape) and the
    // row id is an integer — the literals cannot break out of the Cypher
    // string. The engine's Cypher has property equality but no IS NULL, so
    // the schema stamp is projected and matched client-side: an unqualified
    // binding must not claim a node stamped for the same table name in an
    // explicit schema, and vice versa.
    const cypher = `MATCH (n) WHERE n.${SQL_ID_TABLE_KEY} = '${ref.table}' AND n.${SQL_ID_ROW_KEY} = ${rowId} RETURN n, n.${SQL_ID_SCHEMA_KEY}`;
    const result = await model.graph.query(cypher);
    // The engine answers {"columns":["n","n.sqlref_schema"],"rows":[[id, schema|null], ...]}.
    const wantSchema = ref.schema ?? null;
    const ids = (result.rows as unknown as unknown[][])
      .filter((r) => Array.isArray(r) && typeof r[0] === 'number' && (r[1] ?? null) === wantSchema)
      .map((r) => r[0] as number);
    if (ids.length === 0) return null;
    if (ids.length > 1) {
      throw new NucleusConflictError(
        `row ${rowId} of ${tableQuoted} is stamped on ${ids.length} nodes (${ids.join(', ')}); one node per row is the binding's contract`,
        { meta: { rowId, nodeIds: ids } },
      );
    }
    return ids[0];
  }

  return {
    boundTo: ref,
    idColumn,

    async addRowNode(rowId: number, label: string, props?: Record<string, unknown>): Promise<number> {
      requireNucleus(model.features, 'Graph');
      assertRowId(rowId);
      const existing = await findNodeByRow(rowId);
      if (existing !== null) {
        throw new NucleusConflictError(`row ${rowId} of ${tableQuoted} already has node ${existing}`, {
          meta: { rowId, nodeId: existing },
        });
      }
      return model.graph.addNode([label], stamp(rowId, props));
    },

    findNodeByRow,

    async addRowEdge(fromRowId: number, toRowId: number, edgeType: string, props?: Record<string, unknown>): Promise<number> {
      requireNucleus(model.features, 'Graph');
      const fromNode = await findNodeByRow(fromRowId);
      const toNode = await findNodeByRow(toRowId);
      const missing: string[] = [];
      if (fromNode === null) missing.push(`row ${fromRowId} of ${tableQuoted} has no node`);
      if (toNode === null) missing.push(`row ${toRowId} of ${tableQuoted} has no node`);
      if (missing.length > 0) {
        throw new NucleusNotFoundError(`cannot create ${JSON.stringify(edgeType)} edge: ${missing.join('; ')}`);
      }
      return model.graph.addEdge(fromNode!, toNode!, edgeType, props);
    },

    async hydrate(nodeIds: readonly number[]): Promise<HydratedNode[]> {
      requireNucleus(model.features, 'Graph');
      await gate.require('graph-adjacency', 'sqlNodes hydrate');
      const out: HydratedNode[] = [];
      // Pass 1: read stamps (one GRAPH_NODE per node — reported cost, no
      // bulk primitive exists).
      const stamped: Array<{ nodeId: number; rowId: number | null; schema: string | undefined; table: string | null }> = [];
      for (const nodeId of nodeIds) {
        const node = await readNode(nodeId);
        const props = node?.properties ?? {};
        const table = typeof props[SQL_ID_TABLE_KEY] === 'string' ? (props[SQL_ID_TABLE_KEY] as string) : null;
        const rowRaw = props[SQL_ID_ROW_KEY];
        const rowId = typeof rowRaw === 'number' && Number.isSafeInteger(rowRaw) ? rowRaw : null;
        const schema = typeof props[SQL_ID_SCHEMA_KEY] === 'string' ? (props[SQL_ID_SCHEMA_KEY] as string) : undefined;
        stamped.push({ nodeId, rowId: table !== null && rowId !== null ? rowId : null, schema, table });
      }
      // Pass 2: chunked parameterized SELECT for the stamped rows of THIS
      // table only — same table AND same schema (a foreign table's stamp, or
      // the same table name in another schema, is not our identity).
      const ours = (s: { table: string | null; schema: string | undefined; rowId: number | null }) =>
        s.table === ref.table && s.schema === ref.schema && s.rowId !== null;
      const mine = stamped.filter(ours);
      const rowsById = new Map<number, Record<string, unknown>>();
      for (let i = 0; i < mine.length; i += MAX_HYDRATE_CHUNK) {
        const chunk = mine.slice(i, i + MAX_HYDRATE_CHUNK);
        const params = chunk.map((s) => s.rowId);
        const placeholders = params.map((_, idx) => `$${idx + 1}`).join(', ');
        const result = await model.transport.query<Record<string, unknown>>(
          `SELECT * FROM ${tableQuoted} WHERE ${quoteIdent(idColumn)} IN (${placeholders})`,
          params,
        );
        for (const row of result.rows) {
          // int8 keys arrive as numbers through this client's int8 parser;
          // a driver that hands back digit strings is accepted too.
          const raw = row[idColumn];
          const id = typeof raw === 'string' && /^[0-9]+$/.test(raw) ? Number(raw) : raw;
          if (typeof id === 'number' && Number.isSafeInteger(id)) rowsById.set(id, row);
        }
      }
      for (const s of stamped) {
        if (!ours(s) || s.rowId === null) {
          out.push({ nodeId: s.nodeId, sqlRef: null, row: null });
          continue;
        }
        out.push({
          nodeId: s.nodeId,
          sqlRef: { schema: s.schema, table: ref.table, id: s.rowId, idColumn },
          row: rowsById.get(s.rowId) ?? null, // deleted/never-created row: null, not stale data
        });
      }
      return out;
    },
  };
}

function assertRowId(rowId: number): void {
  if (!Number.isInteger(rowId) || rowId <= 0 || !Number.isSafeInteger(rowId)) {
    throw new Error(`SQL row id must be a positive safe integer, got ${JSON.stringify(rowId)}`);
  }
}

function quoteIdent(name: string): string {
  return `"${name}"`;
}
