// ---------------------------------------------------------------------------
// @neutron-build/sql — relational query API (db.query.<table>)
// ---------------------------------------------------------------------------
// Execution model (README §3.4 default): every requested relation edge is an
// INDEPENDENT correlated scalar subquery in the select list, at any nesting
// depth (Q05). To-many children aggregate with jsonb_agg(... order by <keys>)
// inside their own subquery; to-one parents build a single jsonb object that
// is NULL when the FK misses. Sibling relations never join each other, so two
// to-many children can never multiply each other — including two relations
// to the same target table (author + reviewer), which get separate
// path-derived aliases. Parent where/order/limit/offset apply to parent rows
// before child expansion (the subqueries run per output row); per-child
// where/order/limit/offset apply INSIDE the child's own subquery, so a child
// limit bounds each parent's children, never the global set. A per-child
// limit needs the aggregation to run over a limited derived table (an
// aggregate's own output is always one row — a LIMIT on the aggregate's
// SELECT would limit nothing), so such edges compile as
//   (select coalesce(jsonb_agg(obj order by K) , '[]'::jsonb)
//      from (select … from "child" as a where <correlation+filters>
//            order by K limit N) as a_l).
// The derived table projects EVERY child column: its output must feed the
// aggregate's order keys and nested-edge FK correlations, which may
// reference columns outside the requested subset (the JSON object still
// emits only the selected columns; unlimited edges have no derived table).
// Nesting depth is structurally bounded (MAX_RELATION_DEPTH), which is also
// the cycle guarantee: a cyclic graph terminates because expansion stops at
// the depth bound — no WITH RECURSIVE term exists that could loop.

import { type Condition, type OrderExpression } from "./expr.js";
import { getTableColumns, getTableName } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable, Relation, RelationOne, TableRelations } from "./schema.js";
import type { ExecContext } from "./builder.js";
import { run, whereItems } from "./builder.js";
import type { QueryExecutionOptions } from "./transactions.js";
import {
  applyProjectionDecoders,
  decodeJsonLeaf,
  projectionDecoder,
  wireReadNode,
  type ColumnCodec,
  type ColumnContext,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";
import {
  aggregate as aggregateNode,
  expr as exprNode,
  fragment,
  ident,
  isLegacySqlFragment,
  legacyFragmentError,
  projection as projectionNode,
  qual,
  selectStatement,
  subquery as subqueryNode,
  validLimit,
  type OrderSpec,
  type ProjectionNode,
  type StatementNode,
  type ValueNode,
} from "./ast.js";
import { compileStatement, quoteIdent, quoteStringLiteral } from "./compile.js";

/** Per-relation options and nested selections. `with` values are `true` (the
 *  whole target row, no nested edges) or a nested RQBArgs applying to that
 *  relation's rows: where/orderBy reference the TARGET table's columns,
 *  limit/offset bound each parent's children (to-many only), columns selects
 *  a property-key subset, and with recurses up to MAX_RELATION_DEPTH. */
export interface RQBArgs {
  where?: Condition;
  orderBy?: OrderExpression[];
  limit?: number;
  offset?: number;
  columns?: string[];
  with?: Record<string, true | RQBArgs>;
}

/** Maximum relation nesting depth (with-inside-with levels). Structurally
 *  enforced before any SQL renders; also bounds cyclic graphs. */
export const MAX_RELATION_DEPTH = 5;

/** PostgreSQL truncates identifiers past 63 bytes silently — two long
 *  path-derived aliases could collide after truncation. Generated aliases
 *  longer than this fall back to a compact deterministic name. */
const MAX_ALIAS_BYTES = 63;

export interface ResolvedRelations {
  /** table name -> relation entries */
  byTable: Map<string, Record<string, Relation>>;
}

/** Wire many() relations to their one() counterparts (FKs live on the one side).
 *  Reverse inference is allowed only when exactly ONE candidate one() exists;
 *  multiple candidates without an explicit relationName pair are an error. */
export function resolveRelations(all: TableRelations[]): ResolvedRelations {
  const byTable = new Map<string, TableRelations>();
  for (const r of all) {
    const existing = byTable.get(getTableName(r.table));
    if (existing) {
      throw new Error(
        `relations for table ${getTableName(r.table)} are declared more than once (duplicate declarations cannot be merged)`,
      );
    }
    byTable.set(getTableName(r.table), r);
  }

  // relationName pairing keys must be unambiguous per table per KIND: two
  // one()s sharing a name (which pairs with a many?) or two many()s sharing
  // a name are errors. A one() and a many() on the SAME table MAY share a
  // name — that is the self-relation pair idiom (manager/reports).
  for (const r of all) {
    const seenOne = new Map<string, string>();
    const seenMany = new Map<string, string>();
    for (const [key, rel] of Object.entries(r.entries)) {
      if (!rel.relationName) continue;
      const seen = rel.kind === "one" ? seenOne : seenMany;
      const prior = seen.get(rel.relationName);
      if (prior !== undefined) {
        throw new Error(
          `duplicate relationName "${rel.relationName}" on ${getTableName(r.table)}: ${rel.kind === "one" ? "one()" : "many()"} relations "${prior}" and "${key}" declare it`,
        );
      }
      seen.set(rel.relationName, key);
    }
  }

  for (const r of all) {
    for (const [key, rel] of Object.entries(r.entries)) {
      if (rel.kind !== "many") continue;
      const target = rel.targetTable;
      if ((Object.values(getTableColumns(target)) as AnyColumnBuilder[]).every((c) => !c.isPrimaryKey)) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)}: target table ${getTableName(target)} has no primary key; relation ordering undefined`,
        );
      }
      const targetSet = byTable.get(getTableName(target));
      if (!targetSet) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} targets ${getTableName(target)} but that table declares no relations`,
        );
      }
      const candidates: Array<[string, RelationOne]> = [];
      for (const [k, candidate] of Object.entries(targetSet.entries)) {
        if (candidate.kind === "one" && getTableName(candidate.targetTable) === getTableName(r.table)) {
          candidates.push([k, candidate]);
        }
      }
      let source: RelationOne | undefined;
      if (rel.relationName) {
        source = candidates.find(([, candidate]) => candidate.relationName === rel.relationName)?.[1];
        if (!source) {
          throw new Error(
            `relation "${key}" on ${getTableName(r.table)} declares relationName "${rel.relationName}" but no one() on ${getTableName(target)} targeting ${getTableName(r.table)} declares the same relationName — name both sides of the pair`,
          );
        }
      } else if (candidates.length === 1) {
        source = candidates[0][1];
      } else if (candidates.length === 0) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} has no matching one() on ${getTableName(target)} — declare fields/references there`,
        );
      } else {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} is ambiguous: ${getTableName(target)} declares multiple one() relations to ${getTableName(r.table)} (` +
            candidates.map(([k]) => `"${k}"`).join(", ") +
            `) — give the pair an explicit relationName on both sides`,
        );
      }
      rel.source = source;
    }
  }

  const byTableEntries = new Map<string, Record<string, Relation>>();
  for (const r of all) byTableEntries.set(getTableName(r.table), r.entries);
  return { byTable: byTableEntries };
}

function pkColumnsOf(table: AnyPgTable): AnyColumnBuilder[] {
  const pks = (Object.values(getTableColumns(table)) as AnyColumnBuilder[]).filter((c) => c.isPrimaryKey);
  if (pks.length === 0) {
    throw new Error(`target table ${getTableName(table)} has no primary key; relation ordering undefined`);
  }
  return pks;
}

/** Nested JSON objects are labeled with declared property keys; values come
 *  from the physical columns via schema metadata (never name spelling).
 *  int8/numeric leaves render ::text: as jsonb numbers both drivers would
 *  JSON.parse them into doubles (silently corrupting values beyond 2^53 and
 *  dropping numeric scale). timestamptz leaves render their UTC wall clock
 *  (session-timezone independent); other temporals and bytea leaves keep
 *  to_jsonb's exact string forms (microseconds, \x hex). Correlation
 *  predicates and order keys stay raw column references. */
function jsonLeaf(alias: string, column: AnyColumnBuilder): ValueNode {
  const ref = qual(alias, column.columnName);
  if (column.dataType === "bigint" || column.dataType === "numeric") return fragment(ref, "::text");
  if (column.dataType === "timestamptz") return fragment("to_jsonb(", ref, " at time zone 'UTC')");
  return ref;
}

/** The jsonb object for one child row: selected columns plus one key per
 *  nested edge (each a correlated scalar subquery value — NULL for a missing
 *  to-one, an array for a to-many). */
function jsonObjectForEntries(
  entries: ReadonlyArray<{ propertyKey: string; column: AnyColumnBuilder }>,
  alias: string,
  nestedPairs: ReadonlyArray<readonly [string, ValueNode]>,
): ValueNode {
  const args: ValueNode[] = [];
  for (const { propertyKey, column } of entries) {
    args.push(fragment(quoteStringLiteral(propertyKey)));
    args.push(jsonLeaf(alias, column));
  }
  for (const [key, node] of nestedPairs) {
    args.push(fragment(quoteStringLiteral(key)));
    args.push(node);
  }
  return exprNode("call", "jsonb_build_object", args);
}

/** Normalize driver output: some pgwire servers hand json/jsonb back as
 *  strings. A string is JSON-parsed when it parses — ANY JSON value, since
 *  only the caller knows the expected shape; the relation-shape checks in
 *  decodeRelationValue then accept or reject it. An unparseable string is
 *  returned as-is and fails those checks loudly instead of silently
 *  degrading to []/null. */
function normalizeNested(value: unknown): unknown {
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }
  return value;
}

// ---------------------------------------------------------------------------
// Alias generation — deterministic, path-derived, collision-free.
// ---------------------------------------------------------------------------
// Depth-1 aliases keep the historical `__rel_<key>` shape (snapshot-locked);
// deeper edges append their key (`__rel_posts__author`). Every generated
// alias is checked against the 63-byte PostgreSQL identifier limit: longer
// paths fall back to a compact counter name (`__r<n>`), advanced in traversal
// order so the same args always produce the same SQL.

interface AliasCtx {
  used: Set<string>;
  compactCounter: number;
}

function allocateAlias(ctx: AliasCtx, preferred: string): string {
  let alias = preferred;
  while (alias.length > MAX_ALIAS_BYTES || ctx.used.has(alias)) {
    ctx.compactCounter += 1;
    alias = `__r${ctx.compactCounter}`;
  }
  ctx.used.add(alias);
  return alias;
}

function edgeAlias(ctx: AliasCtx, path: readonly string[]): string {
  return allocateAlias(ctx, `__rel_${path.join("__")}`);
}

/** Derived-table alias for a limited many edge (`<alias>_lim`): distinct
 *  namespace from nested edge keys, which join with `__`. */
function limitedSourceAlias(ctx: AliasCtx, edgeAliasName: string): string {
  return allocateAlias(ctx, `${edgeAliasName}_lim`);
}

// ---------------------------------------------------------------------------
// Validation — recursive over the args tree, before any SQL renders.
// ---------------------------------------------------------------------------

/** Human-readable relation path for errors: `users.posts.comments`. */
function pathLabel(table: string, path: readonly string[]): string {
  return [table, ...path].join(".");
}

function validateRQBArgs(
  table: AnyPgTable,
  relations: Record<string, Relation>,
  relationsByTable: Map<string, Record<string, Relation>>,
  args: RQBArgs,
  depth: number,
  path: readonly string[],
  rootName: string,
): void {
  const tableName = getTableName(table);
  const label = pathLabel(rootName, path);

  if (args.columns !== undefined) {
    if (!Array.isArray(args.columns)) throw new Error(`args.columns on ${label} must be an array of property keys`);
    if (args.columns.length === 0) {
      throw new Error(`args.columns on ${label} is empty — omit columns to select every column`);
    }
    const known = new Set(Object.keys(getTableColumns(table)));
    for (const key of args.columns) {
      if (!known.has(key)) {
        throw new Error(
          `unknown column "${key}" in args.columns on ${label} (known property keys: ${[...known].join(", ")})`,
        );
      }
    }
  }

  if (args.limit !== undefined) validLimit(args.limit, `limit on ${label}`);
  if (args.offset !== undefined) validLimit(args.offset, `offset on ${label}`);

  if (args.where !== undefined && isLegacySqlFragment(args.where)) throw legacyFragmentError("where");
  for (const o of args.orderBy ?? []) {
    if (isLegacySqlFragment(o)) throw legacyFragmentError("orderBy");
    if (isLegacySqlFragment((o as OrderSpec).expr)) throw legacyFragmentError("orderBy");
  }

  for (const [key, value] of Object.entries(args.with ?? {})) {
    if (value === undefined) continue;
    const rel = relations[key];
    if (!rel) {
      const known = Object.keys(relations);
      throw new Error(
        `unknown relation "${key}" on ${tableName} (known relations: ${known.length > 0 ? known.join(", ") : "none"})`,
      );
    }
    if (depth + 1 > MAX_RELATION_DEPTH) {
      throw new Error(
        `relation nesting depth ${depth + 1} exceeds the maximum of ${MAX_RELATION_DEPTH}: ${pathLabel(rootName, [...path, key])} — reduce with-depth or split the query`,
      );
    }
    if (value === true) continue;
    if (typeof value !== "object" || Array.isArray(value)) {
      throw new Error(
        `relation "${key}" in with on ${label}: expected true or a per-relation options object, got ${describeValueKind(value)}`,
      );
    }
    if (rel.kind === "one" && (value.limit !== undefined || value.offset !== undefined)) {
      throw new Error(
        `relation "${key}" in with on ${label}: limit/offset apply to to-many relations only — a to-one matches at most one row`,
      );
    }
    if (rel.kind === "one" && value.orderBy !== undefined) {
      throw new Error(
        `relation "${key}" in with on ${label}: orderBy applies to to-many relations only — a to-one matches at most one row, so ordering cannot change the result`,
      );
    }
    const targetName = getTableName(rel.targetTable);
    const targetEntries = relationsByTable.get(targetName);
    if (!targetEntries) {
      throw new Error(
        `relation "${key}" on ${tableName} targets ${targetName} but that table declares no relations (nested with needs the target's relation set)`,
      );
    }
    validateRQBArgs(rel.targetTable, targetEntries, relationsByTable, value as RQBArgs, depth + 1, [...path, key], rootName);
  }
}

function describeValueKind(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  return `a ${typeof value}`;
}

// ---------------------------------------------------------------------------
// Child-expression remapping — per-relation where/orderBy expressions.
// ---------------------------------------------------------------------------
// The child subquery reads `from "child" as "<alias>"`, so a user expression
// referencing the child table by name (`eq(posts.views, 3)` renders
// `"posts"."views"`) must be rewritten to the alias. References to any other
// table are rejected BEFORE SQL: they would be missing FROM-clause entries
// (or silently bind to an outer same-name scope in self-relation shapes).
// Subqueries and trusted segments inside per-relation expressions are
// rejected as well — fragment TEXT is never scanned, so they cannot be
// remapped soundly (fail closed instead of binding to the wrong scope).

function remapError(label: string, reason: string): Error {
  return new Error(`${label}: ${reason}`);
}

function remapChildRefs(node: ValueNode, childTable: string, alias: string, label: string): ValueNode {
  switch (node.kind) {
    case "qualified": {
      if (node.parts.length === 2 && node.parts[0] === childTable) return qual(alias, node.parts[1]);
      throw remapError(
        label,
        `where/orderBy may reference only the relation's own table "${childTable}" — found ${node.parts.map(quoteIdent).join(".")} ` +
          `(filter the parent instead, or filter on the relation's own columns)`,
      );
    }
    case "identifier":
      return node;
    case "expr":
      return exprNode(node.form, node.op, node.args.map((a) => remapChildRefs(a, childTable, alias, label)));
    case "aggregate":
      return aggregateNode(node.op, node.args.map((a) => remapChildRefs(a, childTable, alias, label)), {
        distinct: node.distinct,
        argColumns: node.argColumns,
      });
    case "fragment":
      return fragment(
        ...node.parts.map((p) => (typeof p === "string" ? p : remapChildRefs(p, childTable, alias, label))),
      );
    case "subquery":
      throw remapError(
        label,
        "subqueries inside per-relation where/orderBy are not supported — the relation's rows are already scoped; express the filter over the relation's own columns",
      );
    case "trusted":
      throw remapError(
        label,
        "trusted SQL segments cannot be rewritten to the relation's alias (their text is never scanned) — rebuild the expression with the sql template over the relation's columns",
      );
    default:
      return node;
  }
}

function remapOrderSpec(o: OrderExpression, childTable: string, alias: string, label: string): OrderSpec {
  if (typeof (o as OrderSpec).direction === "string") {
    const spec = o as OrderSpec;
    return Object.freeze({ expr: remapChildRefs(spec.expr, childTable, alias, label), direction: spec.direction, nulls: spec.nulls });
  }
  return Object.freeze({ expr: remapChildRefs(o as ValueNode, childTable, alias, label), direction: "asc" as const });
}

/** args.columns are property keys; map them to physical columns through
 *  metadata. Unselected columns default to the full declaration order. */
function requestedEntries(table: AnyPgTable, args: RQBArgs): Array<{ propertyKey: string; column: AnyColumnBuilder }> {
  const allEntries = Object.entries(getTableColumns(table) as Record<string, AnyColumnBuilder>).map(([propertyKey, column]) => ({
    propertyKey,
    column,
  }));
  if (args.columns !== undefined && args.columns.length > 0) {
    return args.columns.map((key) => {
      const entry = allEntries.find(({ propertyKey }) => propertyKey === key);
      if (!entry) {
        throw new Error(
          `unknown column "${key}" in args.columns — validateRQBArgs must run before plan construction`,
        );
      }
      return { propertyKey: key, column: entry.column };
    });
  }
  return allEntries;
}

// ---------------------------------------------------------------------------
// Plan construction
// ---------------------------------------------------------------------------

/** Serializable description of one relation edge in a compiled plan (the
 *  shape tree behind explainQuery). */
export interface RelationEdgePlan {
  readonly key: string;
  readonly target: string;
  readonly cardinality: "one" | "many";
  readonly alias: string;
  readonly columns: readonly string[];
  readonly filtered: boolean;
  readonly ordered: boolean;
  readonly limit?: number;
  readonly offset?: number;
  readonly nested: readonly RelationEdgePlan[];
}

/** One statement of a relational plan: the compiled SQL with its parameters,
 *  capability requirements and the serializable decode-plan description. */
export interface RelationalStatementPlan {
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly capabilities: readonly StatementCapability[];
  readonly decoders: ReadonlyArray<{ key: string; wire: "text" | "native"; codec: ColumnCodec; context: ColumnContext }>;
}

/** Structured, PURE compile plan for a relational query (no driver, no
 *  connection): the statements that would run (one today — the correlated
 *  single-statement strategy), their parameters, decoders and capability
 *  requirements, plus the relation edge tree. */
export interface RelationalExplainPlan extends RelationalStatementPlan {
  readonly table: string;
  readonly statementCount: number;
  /** Deepest relation nesting level used (0 = parent columns only). */
  readonly depth: number;
  readonly edges: readonly RelationEdgePlan[];
}

interface EdgeBuild {
  readonly plan: RelationEdgePlan;
  /** The edge's scalar subquery as a bare value: aliased projection at the
   *  top level, embedded as a jsonb pair value when nested. */
  readonly value: ValueNode;
}

interface BuildCtx {
  relationsByTable: Map<string, Record<string, Relation>>;
  aliasCtx: AliasCtx;
}

/** Build one relation edge's projection (the correlated scalar subquery),
 *  recursively including its own nested edges. `outerAlias` is the enclosing
 *  scope's alias (the parent table's name at depth 1, the enclosing edge's
 *  object alias deeper). */
function buildEdge(
  ctx: BuildCtx,
  outerTable: AnyPgTable,
  outerAlias: string,
  key: string,
  rel: Relation,
  childArgs: true | RQBArgs,
  path: readonly string[],
): EdgeBuild {
  const args: RQBArgs = childArgs === true ? {} : childArgs;
  const target = rel.targetTable;
  const targetName = getTableName(target);
  const alias = edgeAlias(ctx.aliasCtx, path);
  const label = pathLabel(getTableName(outerTable), path);
  const selected = requestedEntries(target, args);
  const targetEntries = ctx.relationsByTable.get(targetName) ?? {};

  const userWhere = args.where === undefined ? [] : whereItems([remapChildRefs(args.where, targetName, alias, label)]);
  const userOrder = (args.orderBy ?? []).map((o) => remapOrderSpec(o, targetName, alias, label));

  const nestedPlans: RelationEdgePlan[] = [];

  if (rel.kind === "many") {
    const source = rel.source!;
    // source.fields live on the target (child); source.references live on the
    // outer table. Zip positionally — composite keys keep declaration order.
    const correlation = andChain(
      source.fields.map((f, i) => [qual(alias, f.columnName), qual(outerAlias, source.references[i].columnName)] as const),
    );
    // Deterministic ordering: the user's keys first (when given), then the
    // target PK as a tie-breaker so equal-valued distinct children keep a
    // stable order and LIMIT/OFFSET pick a deterministic subset.
    const pkOrderAsc: OrderSpec[] = pkColumnsOf(target).map((c) => ({
      expr: qual(alias, c.columnName) as ValueNode,
      direction: "asc" as const,
    }));
    const effectiveOrder: OrderSpec[] = [...userOrder, ...pkOrderAsc];

    if (args.limit !== undefined || args.offset !== undefined) {
      // Limited shape: the aggregate must run over a limited derived table.
      // The derived table projects EVERY target column, not just the selected
      // subset ∪ PK: the aggregate's re-remapped order keys and every nested
      // edge's FK correlation reference the DERIVED alias, and order
      // expressions may name columns through raw fragment text that no
      // structural walk can extract — correctness over projection
      // minimality. The emitted JSON still carries only the selected
      // columns, and edges without limit/offset compile with no derived
      // table at all (their projection stays exactly the selected set).
      const sourceAlias = limitedSourceAlias(ctx.aliasCtx, alias);
      const derivedColumns = Object.values(getTableColumns(target)) as AnyColumnBuilder[];
      const derived = selectStatement({
        projections: derivedColumns.map((column) => projectionNode(qual(alias, column.columnName))),
        from: ident(targetName),
        fromAlias: alias,
        where: [correlation, ...userWhere],
        orderBy: effectiveOrder,
        limit: args.limit,
        offset: args.offset,
      });
      // Object/aggregate/nested correlations read the DERIVED output columns.
      // The aggregate's order keys must reference the derived alias too —
      // re-remap the RAW user order expressions to it (a plain re-qualification
      // would miss fragment/complex expressions whose inner references only a
      // structural remap can rewrite).
      const nestedPairs = buildNestedPairs(ctx, target, sourceAlias, targetEntries, args, path, nestedPlans);
      const obj = jsonObjectForEntries(selected, sourceAlias, nestedPairs);
      const orderOnSource: OrderSpec[] = [
        ...(args.orderBy ?? []).map((o) => remapOrderSpec(o, targetName, sourceAlias, label)),
        ...pkColumnsOf(target).map((c) => ({ expr: qual(sourceAlias, c.columnName) as ValueNode, direction: "asc" as const })),
      ];
      const agg = aggWithOrder(obj, orderOnSource);
      const outer = selectStatement({
        projections: [projectionNode(agg)],
        from: subqueryNode(derived),
        fromAlias: sourceAlias,
      });
      return {
        plan: {
          key,
          target: targetName,
          cardinality: "many",
          alias,
          columns: selected.map((e) => e.propertyKey),
          filtered: userWhere.length > 0,
          ordered: (args.orderBy ?? []).length > 0,
          limit: args.limit,
          offset: args.offset,
          nested: [...nestedPlans],
        },
        value: subqueryNode(outer),
      };
    }

    const nestedPairs = buildNestedPairs(ctx, target, alias, targetEntries, args, path, nestedPlans);
    const obj = jsonObjectForEntries(selected, alias, nestedPairs);
    const agg = aggWithOrder(obj, effectiveOrder);
    const inner = selectStatement({
      projections: [projectionNode(agg)],
      from: ident(targetName),
      fromAlias: alias,
      where: [correlation, ...userWhere],
    });
    return {
      plan: {
        key,
        target: targetName,
        cardinality: "many",
        alias,
        columns: selected.map((e) => e.propertyKey),
        filtered: userWhere.length > 0,
        ordered: (args.orderBy ?? []).length > 0,
        nested: [...nestedPlans],
      },
      value: subqueryNode(inner),
    };
  }

  // to-one: fields live on the outer table, references on the target.
  const correlation = andChain(
    rel.fields.map((f, i) => [qual(alias, rel.references[i].columnName), qual(outerAlias, f.columnName)] as const),
  );
  const nestedPairs = buildNestedPairs(ctx, target, alias, targetEntries, args, path, nestedPlans);
  const obj = jsonObjectForEntries(selected, alias, nestedPairs);
  const inner = selectStatement({
    projections: [projectionNode(obj)],
    from: ident(targetName),
    fromAlias: alias,
    where: [correlation, ...userWhere],
    orderBy: userOrder,
  });
  return {
    plan: {
      key,
      target: targetName,
      cardinality: "one",
      alias,
      columns: selected.map((e) => e.propertyKey),
      filtered: userWhere.length > 0,
      ordered: userOrder.length > 0,
      nested: [...nestedPlans],
    },
    value: subqueryNode(inner),
  };
}

/** Build the (key, subquery) pairs for one edge's nested `with` entries. */
function buildNestedPairs(
  ctx: BuildCtx,
  target: AnyPgTable,
  objectAlias: string,
  targetEntries: Record<string, Relation>,
  args: RQBArgs,
  path: readonly string[],
  outPlans: RelationEdgePlan[],
): Array<[string, ValueNode]> {
  const pairs: Array<[string, ValueNode]> = [];
  for (const [nestedKey, nestedValue] of Object.entries(args.with ?? {})) {
    if (nestedValue === undefined) continue;
    const nestedRel = targetEntries[nestedKey];
    if (!nestedRel) {
      const known = Object.keys(targetEntries);
      throw new Error(
        `unknown relation "${nestedKey}" on ${getTableName(target)} (known relations: ${known.length > 0 ? known.join(", ") : "none"})`,
      );
    }
    const built = buildEdge(ctx, target, objectAlias, nestedKey, nestedRel, nestedValue as true | RQBArgs, [...path, nestedKey]);
    outPlans.push(built.plan);
    pairs.push([nestedKey, built.value]);
  }
  return pairs;
}

function maxDepthOfEdges(edges: readonly RelationEdgePlan[]): number {
  let max = 0;
  for (const e of edges) {
    max = Math.max(max, 1 + maxDepthOfEdges(e.nested));
  }
  return max;
}

export function buildRelationalPlan(
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
  relationsByTable?: Map<string, Record<string, Relation>>,
): RelationalExplainPlan {
  const byTable = relationsByTable ?? new Map<string, Record<string, Relation>>([[getTableName(table), relations]]);
  validateRQBArgs(table, relations, byTable, args, 0, [], getTableName(table));
  if (args.where !== undefined && isLegacySqlFragment(args.where)) throw legacyFragmentError("where");
  for (const o of args.orderBy ?? []) {
    if (isLegacySqlFragment(o)) throw legacyFragmentError("orderBy");
    if (isLegacySqlFragment((o as OrderSpec).expr)) throw legacyFragmentError("orderBy");
  }

  const tableName = getTableName(table);
  const requested = requestedEntries(table, args);

  const projections: ProjectionNode[] = [];
  const decoders: ProjectionDecoder[] = [];
  let usesJsonb = false;

  // Top-level rows are keyed by property keys (alias when the names differ).
  // Lossy-native columns (temporals) project their lossless text wire form,
  // exactly like the flat select path.
  for (const { propertyKey, column } of requested) {
    const ref = qual(tableName, column.columnName);
    const wire = wireReadNode(column.dataType, ref);
    if (wire !== null) {
      projections.push(projectionNode(wire, propertyKey));
      usesJsonb = true;
    } else {
      projections.push(projectionNode(ref, propertyKey === column.columnName ? undefined : propertyKey));
    }
    const decoder = projectionDecoder(tableName, column, propertyKey);
    if (decoder) decoders.push(decoder);
  }

  const ctx: BuildCtx = { relationsByTable: byTable, aliasCtx: { used: new Set<string>(), compactCounter: 0 } };
  const edgePlans: RelationEdgePlan[] = [];
  for (const [key, value] of Object.entries(args.with ?? {})) {
    if (value === undefined) continue;
    const rel = relations[key];
    if (!rel) {
      const known = Object.keys(relations);
      throw new Error(
        `unknown relation "${key}" on ${tableName} (known relations: ${known.length > 0 ? known.join(", ") : "none"})`,
      );
    }
    const built = buildEdge(ctx, table, tableName, key, rel, value as true | RQBArgs, [key]);
    edgePlans.push(built.plan);
    projections.push(projectionNode(built.value, key));
    usesJsonb = true;
  }

  const stmt = selectStatement({
    projections,
    from: ident(tableName),
    where: args.where ? whereItems([args.where]) : [],
    orderBy: (args.orderBy ?? []).map((o) =>
      typeof (o as OrderSpec).direction === "string" ? (o as OrderSpec) : { expr: o as ValueNode, direction: "asc" as const },
    ),
    limit: args.limit,
    offset: args.offset,
  });
  const compiled = compileStatement(stmt);
  const capabilities: StatementCapability[] = usesJsonb ? ["jsonb-functions"] : [];
  return {
    table: tableName,
    statementCount: 1,
    depth: maxDepthOfEdges(edgePlans),
    edges: edgePlans,
    sql: compiled.sql,
    params: compiled.params as unknown[],
    capabilities,
    decoders: decoders.map((d) => ({ key: d.key, wire: d.wire, codec: d.codec, context: d.context })),
  };
}

/** Backward-compatible entry: the compiled statement alone (SQL, params,
 *  decode plan, capability requirements). Pure. */
export function buildRelationalSQL(
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
  relationsByTable?: Map<string, Record<string, Relation>>,
): { sql: string; params: unknown[]; decoders: readonly ProjectionDecoder[]; capabilities: readonly StatementCapability[] } {
  const plan = buildRelationalPlan(table, relations, args, relationsByTable);
  const decoders: ProjectionDecoder[] = [];
  // Rebuild the executable decoder closures from the same schema metadata the
  // plan was built from (the plan's decoder list is the serializable form).
  const requested = requestedEntries(table, args);
  for (const { propertyKey, column } of requested) {
    const decoder = projectionDecoder(getTableName(table), column, propertyKey);
    if (decoder) decoders.push(decoder);
  }
  return { sql: plan.sql, params: plan.params as unknown[], decoders, capabilities: plan.capabilities };
}

/** Fold correlation pairs with `=` and `and` (raw column comparisons — the
 *  JSON-only casts never apply to predicates). */
function andChain(pairs: ReadonlyArray<readonly [ValueNode, ValueNode]>): ValueNode {
  const eqs = pairs.map(([a, b]) => exprNode("binary", "=", [a, b]));
  return eqs.reduce((acc, c) => exprNode("binary", "and", [acc, c]));
}

/** `coalesce(jsonb_agg(obj order by keys), '[]'::jsonb)` as one fragment —
 *  aggregate ORDER BY lives inside the call, so it interleaves trusted text
 *  with the object expression and the raw order keys. `asc` is omitted
 *  (PostgreSQL's default — keeps the historical PK-only shape byte-stable);
 *  `desc` and explicit nulls ordering render. */
function aggWithOrder(obj: ValueNode, orderSpecs: readonly OrderSpec[]): ValueNode {
  const parts: Array<string | ValueNode> = ["coalesce(jsonb_agg(", obj];
  if (orderSpecs.length > 0) {
    parts.push(" order by ");
    orderSpecs.forEach((spec, i) => {
      if (i > 0) parts.push(", ");
      parts.push(spec.expr);
      if (spec.direction === "desc") parts.push(" desc");
      if (spec.nulls !== undefined) parts.push(` nulls ${spec.nulls}`);
    });
  }
  parts.push("), '[]'::jsonb)");
  return fragment(...parts);
}

// ---------------------------------------------------------------------------
// Execution + recursive decode
// ---------------------------------------------------------------------------

function decodeChildRow(
  child: Record<string, unknown>,
  target: AnyPgTable,
  childArgs: true | RQBArgs,
  relationsByTable: Map<string, Record<string, Relation>>,
): void {
  const targetName = getTableName(target);
  for (const [propertyKey, column] of Object.entries(getTableColumns(target) as Record<string, AnyColumnBuilder>)) {
    const raw = child[propertyKey];
    if (raw === null || raw === undefined) continue;
    child[propertyKey] = decodeJsonLeaf(column, { propertyKey, columnName: column.columnName, tableName: targetName }, raw);
  }
  if (childArgs === true) return;
  const entries = relationsByTable.get(targetName) ?? {};
  for (const [key, value] of Object.entries(childArgs.with ?? {})) {
    if (value === undefined) continue;
    const rel = entries[key];
    if (!rel) continue;
    decodeRelationValue(child, key, rel, value as true | RQBArgs, relationsByTable);
  }
}

/** Normalize + decode one relation value in place (array of child objects
 *  for to-many, object-or-null for to-one), recursing into nested edges.
 *  The compiled SQL guarantees the wire shapes (jsonb array / jsonb object
 *  or JSON null); anything else after normalization is a structurally
 *  impossible value in that key and fails loudly rather than silently
 *  degrading to [] (which would drop data). */
function decodeRelationValue(
  row: Record<string, unknown>,
  key: string,
  rel: Relation,
  childArgs: true | RQBArgs,
  relationsByTable: Map<string, Record<string, Relation>>,
): void {
  const value = normalizeNested(row[key]);
  if (rel.kind === "one") {
    if (value === null) {
      row[key] = null;
      return;
    }
    if (value === undefined) {
      return;
    }
    if (typeof value === "object" && !Array.isArray(value)) {
      decodeChildRow(value as Record<string, unknown>, rel.targetTable, childArgs, relationsByTable);
      row[key] = value;
      return;
    }
    throw new Error(
      `relation "${key}" decode: expected a jsonb object or null for a to-one relation, got ${describeValueKind(value)}`,
    );
  }
  if (value !== undefined && value !== null && !Array.isArray(value)) {
    throw new Error(
      `relation "${key}" decode: expected a jsonb array for a to-many relation, got ${describeValueKind(value)}`,
    );
  }
  const children = Array.isArray(value) ? value : [];
  row[key] = children.map((child) => {
    if (typeof child === "object" && child !== null && !Array.isArray(child)) {
      decodeChildRow(child as Record<string, unknown>, rel.targetTable, childArgs, relationsByTable);
    }
    return child;
  });
}

export async function findMany(
  ctx: ExecContext,
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
  relationsByTable?: Map<string, Record<string, Relation>>,
  options?: QueryExecutionOptions,
): Promise<Array<Record<string, unknown>>> {
  const byTable = relationsByTable ?? new Map<string, Record<string, Relation>>([[getTableName(table), relations]]);
  const built = buildRelationalSQL(table, relations, args, byTable);
  const rows = (await run(ctx, built.sql, built.params, "query", built.capabilities, options)) as Array<Record<string, unknown>>;
  // Parent columns decode through the compiled statement's decode plan,
  // exactly like the flat select path.
  applyProjectionDecoders(rows, built.decoders);
  // Children decode recursively per the args tree: each edge's target columns
  // decode through decodeJsonLeaf (mode-aware, same codecs as the flat path),
  // then nested edges recurse.
  for (const [key, value] of Object.entries(args.with ?? {})) {
    if (value === undefined) continue;
    const rel = relations[key];
    if (!rel) continue;
    for (const row of rows) {
      if (!(key in row)) continue;
      decodeRelationValue(row, key, rel, value as true | RQBArgs, byTable);
    }
  }
  return rows;
}

export async function findFirst(
  ctx: ExecContext,
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
  relationsByTable?: Map<string, Record<string, Relation>>,
  options?: QueryExecutionOptions,
): Promise<Record<string, unknown> | undefined> {
  const rows = await findMany(ctx, table, relations, { ...args, limit: 1 }, relationsByTable, options);
  return rows[0];
}
