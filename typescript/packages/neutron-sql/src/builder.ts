// ---------------------------------------------------------------------------
// @neutron-build/sql — query builders (select / insert / update / delete)
// ---------------------------------------------------------------------------
// Every builder is a PromiseLike: awaiting executes. `.toSQL()` shows the
// exact statement without running it. One visible SQL statement per call.
//
// Since F04 all CRUD paths build AST statements and compile them with the
// one-traversal compiler (compile.ts) — the old regex-renumbering assembly
// (inline/mergeFragments) is gone. Builders are IMMUTABLE: fluent calls
// return new frozen instances (copy-on-write), so reuse cannot leak filters
// between requests. Compiled statements carry their projection decode plan
// and engine capability requirements alongside sql/params.

import type { Driver } from "./drivers.js";
import { errorSummary, paramsLoggingEnabled, statementIdOf, type Logger } from "./logger.js";
import { QueryCanceledError } from "./errors.js";
import type { QueryExecutionOptions } from "./transactions.js";
import { type Condition, type OrderExpression } from "./expr.js";
import type {
  AliasedTable,
  AnyColumnBuilder,
  AnyPgTable,
  InferInsertModelOf,
  PgTable,
  SelectTypeOf,
  UpdateTypeOf,
} from "./schema.js";
import { ALIAS_MARKER, getDerivedRecord, getTableColumns, getTableName, isAliasHandle, isPgTable, rejectAliasHandle, rejectDerivedTable, rejectViewHandle, tableRefParts } from "./schema.js";
import {
  aggregateResultColumn,
  applyProjectionDecoders,
  aggregateProjection,
  assertColumnWritable,
  columnWireReadNode,
  encodeWriteValue,
  projectionDecoder,
  richCodecSource,
  type BigintMode,
  type ColumnContext,
  type EncodedValue,
  type ProjectionDecoder,
  type StatementCapability,
  type TemporalMode,
} from "./codecs.js";
import type { ColumnDataType } from "./schema.js";
import {
  assertCteRefsResolve,
  assertDistinctPhysicalColumns,
  assertNoExcludedRefs,
  collectExcludedRefs,
  cte,
  defaultCell,
  ident,
  insertStatement,
  isLegacySqlFragment,
  isValueNode,
  join as joinNode,
  legacyFragmentError,
  onConflictClause,
  param as paramNode,
  paramCast,
  projection as projectionNode,
  qual,
  selectStatement,
  statementReferencesName,
  subquery as subqueryNode,
  updateStatement,
  validAlias,
  validNulls,
  deleteStatement,
  type AggregateNode,
  type AnyStatementNode,
  type CteNode,
  type ConflictTarget,
  type FromTarget,
  type IdentifierNode,
  type InsertCell,
  type JoinType,
  type OnConflictNode,
  type OrderSpec,
  type ParamNode,
  type ProjectionNode,
  type QualifiedNode,
  type SetOpKind,
  type StatementNode,
  type SubqueryNode,
  type UpdateAssignment,
  type ValueNode,
} from "./ast.js";
import { compileStatement, type CompiledQuery } from "./compile.js";
import type { CapabilityGate } from "./engine.js";

/** Fail-closed check for one builder-slot value: legacy {sql, params}
 *  fragments are direct-execution shapes and never splice into compiled
 *  statements (shared rejection across every slot). */
function rejectLegacyFragment(value: unknown, slot: string): void {
  if (isLegacySqlFragment(value)) throw legacyFragmentError(slot);
}

export interface ExecContext {
  driver: Driver;
  logger: Logger | null;
  /** Capability gate of the owning database (I01). When present, statements
   *  carrying requirements are checked against the connected engine before
   *  execution; unknown status fails closed. */
  capabilities?: CapabilityGate;
}

/** Fail closed when a compiled statement carries requirements the engine
 *  does not prove supported (unknown is not all-enabled). */
async function assertCapabilities(ctx: ExecContext, required: readonly StatementCapability[]): Promise<void> {
  if (required.length === 0 || !ctx.capabilities) return;
  await ctx.capabilities.assert(required);
}
export async function run(
  ctx: ExecContext,
  sqlText: string,
  params: unknown[],
  kind: "query" | "execute",
  required: readonly StatementCapability[] = [],
  options?: QueryExecutionOptions,
): Promise<unknown> {
  await assertCapabilities(ctx, required);
  const statementId = statementIdOf(sqlText);
  const started = performance.now();
  ctx.logger?.({
    kind: "query-begin",
    statementId,
    sql: sqlText,
    ...(paramsLoggingEnabled() ? { params } : {}),
  });
  try {
    const result = kind === "query" ? await ctx.driver.query(sqlText, params, options) : await ctx.driver.execute(sqlText, params, options);
    ctx.logger?.({ kind: "query-end", statementId, sql: sqlText, durationMs: performance.now() - started });
    return result;
  } catch (err) {
    ctx.logger?.({ kind: "query-error", statementId, sql: sqlText, durationMs: performance.now() - started, error: errorSummary(err) });
    if (err instanceof QueryCanceledError) {
      ctx.logger?.({ kind: "cancel", statementId, cancelReason: err.reason });
    }
    throw err;
  }
}

/** A compiled CRUD statement: sql + params from the one-traversal compiler,
 *  plus the projection decode plan and the engine capabilities the statement
 *  requires (empty when plain SQL suffices). */
export interface CompiledStatement extends CompiledQuery {
  readonly decoders: readonly ProjectionDecoder[];
  readonly capabilities: readonly StatementCapability[];
}

// ---------------------------------------------------------------------------
// Property <-> physical name mapping (metadata lookup, never spelling)
// ---------------------------------------------------------------------------

type ColumnEntries = Array<{ propertyKey: string; column: AnyColumnBuilder }>;

function columnEntries(table: AnyPgTable): ColumnEntries {
  return Object.entries(getTableColumns(table)).map(([propertyKey, column]) => ({
    propertyKey,
    column,
  }));
}

interface SelectPlan {
  readonly nodes: ProjectionNode[];
  readonly decoders: ProjectionDecoder[];
  readonly capabilities: StatementCapability[];
}

/** Q02 additions carried through forks: group-by expressions, having
 *  conditions, distinct flag, explicitly registered CTEs. */
interface SelectExtras {
  readonly group: readonly ValueNode[];
  readonly having: readonly Condition[];
  readonly distinct: boolean;
  readonly ctes: readonly StatementCte[];
}

const NO_EXTRAS: SelectExtras = Object.freeze({ group: [], having: [], distinct: false, ctes: [] });

/** One registered CTE: name + source statement + the capabilities the source
 *  acquired (merged into the consuming statement). */
interface StatementCte {
  readonly name: string;
  readonly stmt: StatementNode;
  readonly capabilities: readonly StatementCapability[];
}

/** Decode metadata for one output column of a plan — the shape
 *  derivedTable()/cteTable() pseudo-columns are built from (Q02). */
export interface PlanColumnSpec {
  readonly key: string;
  readonly dataType: ColumnDataType;
  readonly readMode?: BigintMode | TemporalMode;
  readonly valueDecoder?: (raw: string) => unknown;
  readonly canonicalText?: boolean;
  /** Q07: source column whose codec-shaping metadata (array shape, enum
   *  definition, custom codec) pseudo-columns copy. Set only when present. */
  readonly source?: AnyColumnBuilder;
}

/** A full select plan: the statement node plus its decode plan, engine
 *  capability requirements (CTE/derived sources merged in), and the output
 *  column specs (for derived-table/CTE pseudo-columns). */
export interface FullSelectPlan {
  readonly stmt: StatementNode;
  readonly decoders: readonly ProjectionDecoder[];
  readonly capabilities: readonly StatementCapability[];
  readonly columns: readonly PlanColumnSpec[];
}

/** Anything that can supply a select statement for a CTE, a derived table,
 *  or a set-operation branch. SubqueryNode is the structural form. */
export type SubquerySource =
  | SubqueryNode
  | { toPlan(): FullSelectPlan }
  | { toAST(): StatementNode };

/** Extract the statement (+ capabilities when known) from a subquery source.
 *  AST builders carry no decode/capability plan — that is the typed layer's
 *  contract — so they contribute none. */
export function sourceStatement(source: SubquerySource): { stmt: StatementNode; capabilities: readonly StatementCapability[] } {
  if (typeof source === "object" && source !== null && (source as { kind?: unknown }).kind === "subquery") {
    return { stmt: (source as SubqueryNode).select, capabilities: [] };
  }
  if (typeof (source as { toPlan?: unknown }).toPlan === "function") {
    const plan = (source as { toPlan(): FullSelectPlan }).toPlan();
    return { stmt: plan.stmt, capabilities: plan.capabilities };
  }
  if (typeof (source as { toAST?: unknown }).toAST === "function") {
    return { stmt: (source as { toAST(): StatementNode }).toAST(), capabilities: [] };
  }
  throw new Error("subquery source: requires a builder (toPlan/toAST) or a subquery node");
}

/** One typed join: the base table, its in-statement alias and the ON
 *  condition (undefined for cross joins). Frozen at construction. */
interface JoinSpec {
  readonly type: JoinType;
  readonly table: AnyPgTable;
  readonly alias: string;
  readonly on: ValueNode | undefined;
}

/** SQL target node for a table: `"name"` or `"schema"."name"`. */
export function tableTargetNode(table: AnyPgTable): IdentifierNode | QualifiedNode {
  const parts = tableRefParts(table);
  return parts.length === 1 ? ident(parts[0]) : qual(...parts);
}

/** Unwrap an alias() handle or a derived/CTE handle for a join slot. Fails
 *  closed on raw tables and non-handles: joins are keyed by alias, which is
 *  what keeps self joins and same-name tables in different schemas
 *  unambiguous. Derived/CTE handles join under their own name (the subquery
 *  inlines for derived, the CTE auto-registers for CTE handles). */
function resolveAliasHandle(handle: unknown, who: string): { table: AnyPgTable; alias: string } {
  const derived = typeof handle === "object" && handle !== null ? getDerivedRecord(handle as AnyPgTable) : undefined;
  if (derived !== undefined) {
    return { table: handle as AnyPgTable, alias: derived.name };
  }
  if (!isAliasHandle(handle)) {
    throw new Error(
      `${who}: joins take alias() handles (or derivedTable/cteTable handles, which join under their own name) — wrap the table with alias(table, "name") so every reference and the result mapping are unambiguous`,
    );
  }
  const rec = handle[ALIAS_MARKER];
  return { table: rec.table, alias: rec.alias };
}

/** Reject alias names that collide inside one statement: with the from
 *  table's own name, or with another join's alias. PostgreSQL rejects these
 *  at execution; this fails before any SQL runs, naming the collision. */
function assertDistinctJoinAliases(fromTable: AnyPgTable, joins: readonly JoinSpec[]): void {
  const seen = new Map<string, string>([[getTableName(fromTable), "the from table"]]);
  for (const j of joins) {
    const prior = seen.get(j.alias);
    if (prior !== undefined) {
      throw new Error(`join alias "${j.alias}" collides with ${prior} — give every table occurrence its own alias() name`);
    }
    seen.set(j.alias, `a previous join's alias`);
  }
}

/** Projection list for a set of `{ propertyKey, column }` entries: physical
 *  qualified references labeled with property keys, lossless text-acquisition
 *  fragments for lossy-native types (always labeled), plus the matching
 *  decode plan. `tableRef` carries the reference parts (schema-qualified when
 *  the table declares a schema). */
function selectPlanFor(tableRef: string[], tableName: string, entries: ColumnEntries): SelectPlan {
  const nodes: ProjectionNode[] = [];
  const decoders: ProjectionDecoder[] = [];
  let usesJsonb = false;
  for (const { propertyKey: key, column } of entries) {
    const ref = qual(...tableRef, column.columnName);
    const wire = columnWireReadNode(column, ref);
    if (wire !== null) {
      nodes.push(projectionNode(wire, key));
      usesJsonb = true;
    } else {
      nodes.push(projectionNode(ref, key === column.columnName ? undefined : key));
    }
    const decoder = projectionDecoder(tableName, column, key);
    if (decoder) decoders.push(decoder);
  }
  return { nodes, decoders, capabilities: usesJsonb ? ["jsonb-functions"] : [] };
}

/** Flatten top-level `and` expressions into separate where items — pure
 *  associativity, so `.where(and(a, b))` and `.where(a).where(b)` compile to
 *  the same uniformly parenthesized predicate list. */
export function whereItems(items: readonly ValueNode[]): ValueNode[] {
  const out: ValueNode[] = [];
  const visit = (node: ValueNode): void => {
    if (node.kind === "expr" && node.form === "binary" && node.op.toLowerCase() === "and") {
      for (const arg of node.args) visit(arg);
      return;
    }
    out.push(node);
  };
  for (const item of items) visit(item);
  return out;
}

/** Normalize one order/group term: order specs pass through, columns become
 *  qualified references, value nodes render as authored. */
function exprTerm(e: AnyColumnBuilder | ValueNode): ValueNode {
  if (typeof e === "object" && e !== null && typeof (e as { columnName?: unknown }).columnName === "string" && (e as { kind?: unknown }).kind === undefined) {
    const col = e as unknown as AnyColumnBuilder;
    if (col.ownerTable) return qual(...tableRefParts(col.ownerTable), col.columnName);
    return ident(col.columnName);
  }
  return e as ValueNode;
}

function orderSpecs(order: readonly OrderExpression[]): OrderSpec[] {
  return order.map((o) => (typeof (o as OrderSpec).direction === "string" ? (o as OrderSpec) : { expr: exprTerm(o as ValueNode), direction: "asc" as const }));
}

// ---------------------------------------------------------------------------
// Mutation value encoding (before execution, with column context)
// ---------------------------------------------------------------------------

function encodeForColumn(table: string, column: AnyColumnBuilder, propertyKey: string, value: unknown): EncodedValue {
  return encodeWriteValue(column, columnContext(table, column, propertyKey), value);
}

function columnContext(table: string, column: AnyColumnBuilder, propertyKey: string): ColumnContext {
  return { propertyKey, columnName: column.columnName, tableName: table };
}

function cellNode(encoded: EncodedValue): ParamNode {
  return encoded.cast === undefined ? paramNode(encoded.bind) : paramCast(encoded.bind, encoded.cast);
}

function effectiveNotNull(column: AnyColumnBuilder): boolean {
  return column.isNotNull || column.isPrimaryKey;
}

/** Required insert keys: NOT NULL (PK implies NOT NULL) without default;
 *  serial columns always have a server default. */
function requiredInsertKeys(table: AnyPgTable): Array<{ propertyKey: string; column: AnyColumnBuilder }> {
  return columnEntries(table).filter(({ column }) => effectiveNotNull(column) && !column.hasDefault && column.dataType !== "serial");
}

// ---------------------------------------------------------------------------
// ON CONFLICT (Q03) — builder-side resolution of the frozen AST clause
// ---------------------------------------------------------------------------

/** Conflict target for the typed insert builder: one column, a composite
 *  list, either plus an index predicate (partial unique indexes), or a named
 *  constraint (`on conflict on constraint …`). Omitted target = PostgreSQL
 *  arbitrates (DO NOTHING only; DO UPDATE requires a target). */
export type ConflictTargetSpec =
  | AnyColumnBuilder
  | readonly AnyColumnBuilder[]
  | { readonly columns: readonly AnyColumnBuilder[]; readonly where?: Condition }
  | { readonly constraint: string };

/** Builder-side conflict plan, frozen at the onConflict* call and resolved
 *  against the table at compile time (single path for toSQL/execute). */
interface ConflictPlan {
  readonly action: "nothing" | "update";
  readonly target?: ConflictTargetSpec;
  /** Index predicate from the `where`/`targetWhere` option. */
  readonly targetWhere?: Condition;
  /** Raw set input (update only), resolved at compile time. */
  readonly set?: Record<string, unknown>;
  /** DO UPDATE ... WHERE. */
  readonly setWhere?: Condition;
}

function isColumnBuilderLike(v: unknown): v is AnyColumnBuilder {
  return typeof v === "object" && v !== null && typeof (v as { columnName?: unknown }).columnName === "string";
}

/** Every excluded."col" reference in an on-conflict expression must name a
 *  physical column of the inserted table — typo'd references fail before SQL
 *  instead of as a database error. */
function assertExcludedColumnsResolve(tableName: string, columns: Record<string, AnyColumnBuilder>, node: ValueNode, who: string): void {
  const refs: QualifiedNode[] = [];
  collectExcludedRefs(node, refs);
  const physicalNames = new Set(Object.values(columns).map((c) => c.columnName));
  for (const ref of refs) {
    if (ref.parts.length !== 2 || !physicalNames.has(ref.parts[1])) {
      throw new Error(`${who}: excluded reference "${ref.parts.join(".")}" is not a column of ${tableName}`);
    }
  }
}

function resolveConflictTarget(
  table: AnyPgTable,
  spec: ConflictTargetSpec,
  targetWhere: Condition | undefined,
): { columns?: readonly string[]; constraint?: string; predicate?: Condition } {
  const tableName = getTableName(table);
  let cols: readonly AnyColumnBuilder[] | undefined;
  let inlineWhere: Condition | undefined;
  let constraint: string | undefined;
  if (isColumnBuilderLike(spec)) {
    cols = [spec];
  } else if (Array.isArray(spec)) {
    cols = spec;
  } else if (typeof (spec as { constraint?: unknown }).constraint === "string") {
    constraint = (spec as { constraint: string }).constraint;
  } else if (Array.isArray((spec as { columns?: unknown }).columns)) {
    cols = (spec as { columns: readonly AnyColumnBuilder[]; where?: Condition }).columns;
    inlineWhere = (spec as { columns: readonly AnyColumnBuilder[]; where?: Condition }).where;
  } else {
    throw new Error(`on conflict target on ${tableName}: pass a column, a column array, { columns, where } or { constraint }`);
  }
  if (constraint !== undefined) {
    if (constraint.length === 0) throw new Error(`on conflict target on ${tableName}: constraint name must be a non-empty string`);
    if (targetWhere !== undefined) {
      throw new Error(`on conflict target on ${tableName}: an index predicate is only valid with a column-list target — ON CONSTRAINT takes no predicate`);
    }
    return { constraint };
  }
  if (cols === undefined || cols.length === 0) {
    throw new Error(`on conflict target on ${tableName}: a column-list target needs at least one column`);
  }
  if (inlineWhere !== undefined && targetWhere !== undefined) {
    throw new Error(`on conflict target on ${tableName}: index predicate supplied twice — pass it as the target's where or the option, not both`);
  }
  const predicate = inlineWhere ?? targetWhere;
  if (predicate !== undefined) rejectLegacyFragment(predicate, "on conflict target where");
  const physicalNames = new Set(Object.values(getTableColumns(table) as Record<string, AnyColumnBuilder>).map((c) => c.columnName));
  const physical = cols.map((c) => {
    if (!isColumnBuilderLike(c)) throw new Error(`on conflict target on ${tableName}: target entries must be column builders of ${tableName}`);
    if (!physicalNames.has(c.columnName)) {
      throw new Error(`on conflict target: "${c.columnName}" is not a column of ${tableName}`);
    }
    return c.columnName;
  });
  return { columns: physical, predicate };
}

/** Encode an on-conflict SET map exactly like update .set(): literals run
 *  through the column codec, ValueNodes (expressions, excluded() refs, sql
 *  fragments) splice structurally on non-json columns, null checks NOT NULL.
 *  Duplicate physical assignments error deterministically (key-order
 *  independent). */
function conflictAssignments(table: AnyPgTable, values: Record<string, unknown>): UpdateSet[] {
  const tableName = getTableName(table);
  const columns = getTableColumns(table) as Record<string, AnyColumnBuilder>;
  const added: UpdateSet[] = [];
  const inputEntries: Array<readonly [string, string]> = [];
  for (const [key, value] of Object.entries(values)) {
    const column = columns[key];
    if (!column) throw new Error(`unknown column "${key}" on ${tableName}`);
    if (value === undefined) continue; // omitted/undefined set keys are ignored
    assertColumnWritable(column, columnContext(tableName, column, key));
    const physical = column.columnName;
    inputEntries.push([key, physical]);
    if (value === null) {
      if (effectiveNotNull(column)) {
        throw new Error(`on conflict do update set on ${tableName}: null is not allowed for NOT NULL column "${key}" ("${physical}")`);
      }
      added.push({ propertyKey: key, column: physical, value: paramNode(null) });
      continue;
    }
    const isJson = column.dataType === "json" || column.dataType === "jsonb";
    if (!isJson && isValueNode(value)) {
      assertExcludedColumnsResolve(tableName, columns, value, `on conflict do update set "${key}" on ${tableName}`);
      added.push({ propertyKey: key, column: physical, value });
      continue;
    }
    rejectLegacyFragment(value, "on conflict set");
    added.push({ propertyKey: key, column: physical, value: cellNode(encodeForColumn(tableName, column, key, value)) });
  }
  if (added.length === 0) {
    throw new Error(`on conflict do update set on ${tableName}: no assignments — undefined values are ignored`);
  }
  assertDistinctPhysicalColumns(tableName, inputEntries, "on conflict do update set");
  return added;
}

/** Resolve the builder-side conflict plan into the frozen AST clause. */
function buildOnConflictNode(table: AnyPgTable, plan: ConflictPlan): OnConflictNode {
  const tableName = getTableName(table);
  const columns = getTableColumns(table) as Record<string, AnyColumnBuilder>;
  const input: {
    action: "nothing" | "update";
    targetColumns?: readonly string[];
    targetWhere?: readonly ValueNode[];
    constraint?: string;
    sets?: readonly UpdateAssignment[];
    where?: readonly ValueNode[];
  } = { action: plan.action };
  if (plan.target !== undefined) {
    const resolved = resolveConflictTarget(table, plan.target, plan.targetWhere);
    if (resolved.constraint !== undefined) {
      input.constraint = resolved.constraint;
    } else if (resolved.columns !== undefined) {
      input.targetColumns = resolved.columns;
      if (resolved.predicate !== undefined) {
        // The index predicate cannot see excluded (live-verified PG 17:
        // "invalid reference to FROM-clause entry for table excluded") —
        // reject it here instead of as a database error.
        assertNoExcludedRefs(resolved.predicate, `on conflict target where on ${tableName}`);
        input.targetWhere = whereItems([resolved.predicate]);
      }
    }
  }
  if (plan.action === "update") {
    if (plan.set === undefined) throw new Error("onConflictUpdate: set is required");
    input.sets = conflictAssignments(table, plan.set);
    if (plan.setWhere !== undefined) {
      rejectLegacyFragment(plan.setWhere, "on conflict set where");
      assertExcludedColumnsResolve(tableName, columns, plan.setWhere, `on conflict set where on ${tableName}`);
      input.where = whereItems([plan.setWhere]);
    }
  }
  return onConflictClause(input);
}

// ---------------------------------------------------------------------------
// Select
// ---------------------------------------------------------------------------

export type Projection = Record<string, AnyColumnBuilder | ValueNode>;

export type ProjectionResult<P extends Projection> = {
  [K in keyof P]: P[K] extends AnyColumnBuilder ? SelectTypeOf<P[K]> : unknown;
};

/** One projected field under join nullability. An aliased column widens to
 *  `| null` when its alias is on the nullable side of an outer join (the
 *  alias is the join identity); a plain (from-table) column widens when the
 *  from side is the nullable side (right/full join). Aggregate projections
 *  carry their own typed nullability (count is never null; the other
 *  aggregates are `| null` on empty input) and outer-join nullability of
 *  their inputs is already captured by aggregate semantics. */
export type JoinFieldType<V, N extends string, F extends boolean> = V extends AnyColumnBuilder
  ? V extends { readonly aliasTag: infer A }
    ? A extends N
      ? SelectTypeOf<V> | null
      : SelectTypeOf<V>
    : F extends true
      ? SelectTypeOf<V> | null
      : SelectTypeOf<V>
  : V extends AggregateNode<infer T>
    ? T
    : unknown;

/** Result row of a joined select: the projected fields with outer-join
 *  nullability applied (`R0` is the no-projection base row, nulled field-wise
 *  when the from side is nullable). */
export type JoinRowOf<P extends Projection | null, R0, N extends string, F extends boolean> = P extends null
  ? F extends true
    ? { [K in keyof R0]: R0[K] | null }
    : R0
  : { [K in keyof P]: JoinFieldType<P[K], N, F> };

export class SelectBuilder<P extends Projection | null, R0 = unknown, N extends string = never, F extends boolean = false>
  implements PromiseLike<JoinRowOf<P, R0, N, F>[]>
{
  private readonly ctx: ExecContext;
  private readonly table: AnyPgTable;
  private readonly projection: Projection | null;
  private readonly extras: SelectExtras;

  constructor(
    ctx: ExecContext,
    table: AnyPgTable,
    projection: P,
    private readonly joinSpecs: readonly JoinSpec[] = [],
    private readonly conditions: readonly Condition[] = [],
    private readonly order: readonly OrderExpression[] = [],
    private readonly limitCount: number | undefined = undefined,
    private readonly offsetCount: number | undefined = undefined,
    extras: SelectExtras = NO_EXTRAS,
  ) {
    // The projection is copied and frozen at construction (shallow): the
    // caller's object stays theirs, and every fork compiles the snapshot it
    // was built from — post-fork caller mutation cannot reach any sibling
    // (F01 review-2 carry-forward). Legacy fragments never enter the copy.
    this.ctx = ctx;
    this.table = table;
    rejectAliasHandle(table, "select from");
    if (projection === null) {
      this.projection = null;
    } else {
      const copy: Projection = {};
      for (const [key, value] of Object.entries(projection)) {
        rejectLegacyFragment(value, "select projection");
        copy[key] = value;
      }
      this.projection = Object.freeze(copy);
    }
    this.extras = Object.freeze({
      group: Object.freeze([...extras.group]),
      having: Object.freeze([...extras.having]),
      distinct: extras.distinct === true,
      ctes: Object.freeze([...extras.ctes]),
    });
    for (const spec of this.joinSpecs) Object.freeze(spec);
    Object.freeze(this.joinSpecs);
    Object.freeze(this.conditions);
    Object.freeze(this.order);
    Object.freeze(this);
  }

  private fork(extras: SelectExtras): SelectBuilder<P, R0, N, F> {
    return new SelectBuilder<P, R0, N, F>(this.ctx, this.table, this.projection as P, this.joinSpecs, this.conditions, this.order, this.limitCount, this.offsetCount, extras);
  }

  // -------------------------------------------------------------------------
  // Joins (Q01): every join takes an alias() handle. Left joins make the
  // joined alias nullable; right joins make the from side nullable; full
  // joins make both nullable; inner/cross add no nullability.
  // -------------------------------------------------------------------------

  innerJoin<C extends Record<string, AnyColumnBuilder>, A extends string>(
    handle: AliasedTable<C, A>,
    on: Condition,
  ): SelectBuilder<P, R0, N, F>;
  innerJoin<R2 extends Record<string, unknown>, A2 extends string>(
    handle: import("./subqueries.js").DerivedTable<A2, R2>,
    on: Condition,
  ): SelectBuilder<P, R0, N, F>;
  innerJoin(handle: unknown, on: Condition): SelectBuilder<P, R0, N, F> {
    rejectLegacyFragment(on, "innerJoin on");
    const { table, alias } = resolveAliasHandle(handle, "innerJoin");
    return this.forkJoin<N, F>({ type: "inner", table, alias, on });
  }

  leftJoin<C extends Record<string, AnyColumnBuilder>, A extends string>(
    handle: AliasedTable<C, A>,
    on: Condition,
  ): SelectBuilder<P, R0, N | A, F>;
  leftJoin<R2 extends Record<string, unknown>, A2 extends string>(
    handle: import("./subqueries.js").DerivedTable<A2, R2>,
    on: Condition,
  ): SelectBuilder<P, R0, N | A2, F>;
  leftJoin(handle: unknown, on: Condition): SelectBuilder<P, R0, N, F> {
    rejectLegacyFragment(on, "leftJoin on");
    const { table, alias } = resolveAliasHandle(handle, "leftJoin");
    return this.forkJoin<N, F>({ type: "left", table, alias, on });
  }

  rightJoin<C extends Record<string, AnyColumnBuilder>, A extends string>(
    handle: AliasedTable<C, A>,
    on: Condition,
  ): SelectBuilder<P, R0, N, true>;
  rightJoin<R2 extends Record<string, unknown>, A2 extends string>(
    handle: import("./subqueries.js").DerivedTable<A2, R2>,
    on: Condition,
  ): SelectBuilder<P, R0, N, true>;
  rightJoin(handle: unknown, on: Condition): SelectBuilder<P, R0, N, true> {
    rejectLegacyFragment(on, "rightJoin on");
    const { table, alias } = resolveAliasHandle(handle, "rightJoin");
    return this.forkJoin<N, true>({ type: "right", table, alias, on });
  }

  fullJoin<C extends Record<string, AnyColumnBuilder>, A extends string>(
    handle: AliasedTable<C, A>,
    on: Condition,
  ): SelectBuilder<P, R0, N | A, true>;
  fullJoin<R2 extends Record<string, unknown>, A2 extends string>(
    handle: import("./subqueries.js").DerivedTable<A2, R2>,
    on: Condition,
  ): SelectBuilder<P, R0, N | A2, true>;
  fullJoin(handle: unknown, on: Condition): SelectBuilder<P, R0, N, true> {
    rejectLegacyFragment(on, "fullJoin on");
    const { table, alias } = resolveAliasHandle(handle, "fullJoin");
    return this.forkJoin<N, true>({ type: "full", table, alias, on });
  }

  crossJoin<C extends Record<string, AnyColumnBuilder>, A extends string>(
    handle: AliasedTable<C, A>,
    on?: never,
  ): SelectBuilder<P, R0, N, F>;
  crossJoin<R2 extends Record<string, unknown>, A2 extends string>(
    handle: import("./subqueries.js").DerivedTable<A2, R2>,
    on?: never,
  ): SelectBuilder<P, R0, N, F>;
  crossJoin(handle: unknown, on?: never): SelectBuilder<P, R0, N, F> {
    // Q01 review MINOR-1: a runtime ON argument must never be silently
    // dropped — a loose-typed caller would get a cartesian product.
    if (arguments.length > 1 && arguments[1] !== undefined) {
      throw new Error("crossJoin: cross joins take no on condition — use innerJoin/leftJoin/rightJoin/fullJoin or drop the condition");
    }
    const { table, alias } = resolveAliasHandle(handle, "crossJoin");
    return this.forkJoin<N, F>({ type: "cross", table, alias, on: undefined });
  }

  /** Fork with one more join, instantiating the widened nullability
   *  parameters explicitly (the constructor cannot infer them — they shape
   *  only the result row type). */
  private forkJoin<N2 extends string, F2 extends boolean>(spec: JoinSpec): SelectBuilder<P, R0, N2, F2> {
    return new SelectBuilder<P, R0, N2, F2>(
      this.ctx,
      this.table,
      this.projection as P,
      [...this.joinSpecs, spec],
      this.conditions,
      this.order,
      this.limitCount,
      this.offsetCount,
      this.extras,
    );
  }

  // -------------------------------------------------------------------------
  // Set operations (Q02): union/union all/intersect/intersect all/except/
  // except all chain left-associatively in call order (branches render
  // parenthesized), and the result rows are typed by the FIRST branch —
  // PostgreSQL takes output column names and types from it.
  // -------------------------------------------------------------------------

  union<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "union", other);
  }

  unionAll<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "union all", other);
  }

  intersect<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "intersect", other);
  }

  intersectAll<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "intersect all", other);
  }

  except<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "except", other);
  }

  exceptAll<P2 extends Projection | null, R02, N2 extends string, F2 extends boolean>(other: SelectBuilder<P2, R02, N2, F2>): SetOpBuilder<JoinRowOf<P, R0, N, F>> {
    return SetOpBuilder.compound<JoinRowOf<P, R0, N, F>>(this.ctx, this.toPlan(), "except all", other);
  }

  where(condition: Condition): SelectBuilder<P, R0, N, F> {
    rejectLegacyFragment(condition, "where");
    assertNoExcludedRefs(condition, "select where");
    return new SelectBuilder<P, R0, N, F>(this.ctx, this.table, this.projection as P, this.joinSpecs, [...this.conditions, condition], this.order, this.limitCount, this.offsetCount, this.extras);
  }

  orderBy(...exprs: Array<AnyColumnBuilder | OrderExpression>): SelectBuilder<P, R0, N, F> {
    for (const e of exprs) {
      rejectLegacyFragment(e, "orderBy");
      rejectLegacyFragment((e as OrderSpec).expr, "orderBy");
    }
    const added = exprs.map((e) => (typeof (e as OrderSpec).direction === "string" ? (e as OrderSpec) : exprTerm(e as AnyColumnBuilder | ValueNode)));
    return new SelectBuilder<P, R0, N, F>(this.ctx, this.table, this.projection as P, this.joinSpecs, this.conditions, [...this.order, ...added], this.limitCount, this.offsetCount, this.extras);
  }

  limit(n: number): SelectBuilder<P, R0, N, F> {
    return new SelectBuilder<P, R0, N, F>(this.ctx, this.table, this.projection as P, this.joinSpecs, this.conditions, this.order, n, this.offsetCount, this.extras);
  }

  offset(n: number): SelectBuilder<P, R0, N, F> {
    return new SelectBuilder<P, R0, N, F>(this.ctx, this.table, this.projection as P, this.joinSpecs, this.conditions, this.order, this.limitCount, n, this.extras);
  }

  /** Keyset pagination introspection (Q04): the pager's apply()/page() guard
   *  reads this to fail closed when composing onto a builder that already
   *  carries order terms or an offset. */
  keysetBuilderState(): { ordered: boolean; offset: boolean } {
    return { ordered: this.order.length > 0, offset: this.offsetCount !== undefined };
  }

  // -------------------------------------------------------------------------
  // Q02: group by / having / distinct / CTEs / set operations
  // -------------------------------------------------------------------------

  /** Group by columns or expressions. Multiple calls accumulate (AND of the
   *  grouping terms). Columns render as qualified references; fragments and
   *  other value nodes render as authored. */
  groupBy(...exprs: Array<AnyColumnBuilder | ValueNode>): SelectBuilder<P, R0, N, F> {
    const terms: ValueNode[] = [];
    for (const e of exprs) {
      rejectLegacyFragment(e, "groupBy");
      if (typeof e === "object" && e !== null && typeof (e as { direction?: unknown }).direction === "string") {
        throw new Error("groupBy: order specs are not group terms — pass columns or expressions, not asc()/desc()");
      }
      const term = exprTerm(e as AnyColumnBuilder | ValueNode);
      rejectLegacyFragment(term, "groupBy");
      terms.push(term);
    }
    return this.fork({ ...this.extras, group: [...this.extras.group, ...terms] });
  }

  /** HAVING conditions — the same AST values `where` takes (aggregates,
   *  fragments, combinators), joined with `and`, fragments delimited (the
   *  F04 Grouping guarantee applies to HAVING). */
  having(condition: Condition): SelectBuilder<P, R0, N, F> {
    rejectLegacyFragment(condition, "having");
    return this.fork({ ...this.extras, having: [...this.extras.having, condition] });
  }

  /** Plain `select distinct` — one row per distinct projection tuple. */
  distinct(): SelectBuilder<P, R0, N, F> {
    return this.fork({ ...this.extras, distinct: true });
  }

  /** Register a CTE by name (`with "name" as (source)`). The consuming
   *  statement carries the source's capability requirements. Prefer
   *  `cteTable(name, source)` — it registers the CTE automatically when
   *  referenced in from/joins. */
  withCte(name: string, source: SubquerySource): SelectBuilder<P, R0, N, F> {
    validAlias(name, "withCte");
    const { stmt, capabilities } = sourceStatement(source);
    return this.fork({ ...this.extras, ctes: [...this.extras.ctes, { name, stmt, capabilities }] });
  }

  /** Wrap this builder's statement for interpolation into a parent query. */
  subquery(): SubqueryNode {
    return subqueryNode(this.toPlan().stmt);
  }

  /** The statement as a frozen AST — composition point for subqueries. */
  toAST(): StatementNode {
    return this.toPlan().stmt;
  }

  /** Full plan (statement + decode plan + capability requirements). Internal
   *  composition seam shared with CTEs, derived tables and set operations. */
  toPlan(): FullSelectPlan {
    return this.buildPlan();
  }

  private projectionEntries(): Array<{ key: string; column: AnyColumnBuilder | null }> {
    if (!this.projection) {
      return columnEntries(this.table).map(({ propertyKey, column }) => ({ key: propertyKey, column }));
    }
    return Object.entries(this.projection).map(([key, value]) => ({
      key,
      column: isValueNode(value) ? null : value,
    }));
  }

  /** The full plan: statement node + decode plan + capability requirements.
   *  Pure — same builder state compiles to byte-identical SQL. Derived/CTE
   *  from/join targets resolve here (derived inline as `(select …) as
   *  "name"`, CTE handles reference the bare name and auto-register their
   *  CTE), and every source's capability requirements merge in. */
  private buildPlan(): FullSelectPlan {
    const tableName = getTableName(this.table);
    const tableRef = tableRefParts(this.table);
    const nodes: ProjectionNode[] = [];
    const decoders: ProjectionDecoder[] = [];
    const columns: PlanColumnSpec[] = [];
    const caps = new Set<StatementCapability>();
    const pushColumn = (spec: PlanColumnSpec): void => {
      columns.push(spec.readMode === undefined && spec.valueDecoder === undefined && spec.canonicalText !== true && spec.source === undefined ? { key: spec.key, dataType: spec.dataType } : spec);
    };

    if (this.projection) {
      for (const [key, value] of Object.entries(this.projection)) {
        if (isValueNode(value)) {
          if (value.kind === "aggregate") {
            const plan = aggregateProjection(value, key);
            nodes.push(plan.node);
            if (plan.decoder) decoders.push(plan.decoder);
            if (plan.usesJsonb) caps.add("jsonb-functions");
            const spec = aggregateResultColumn(value);
            pushColumn({ key, dataType: spec.dataType, readMode: spec.readMode, valueDecoder: spec.valueDecoder, canonicalText: spec.dataType === "timestamp" || spec.dataType === "timestamptz" || spec.dataType === "date", source: richCodecSource(spec.source) });
            continue;
          }
          nodes.push(projectionNode(value, key));
          pushColumn({ key, dataType: "text" });
          continue;
        }
        const parts = value.ownerTable ? tableRefParts(value.ownerTable) : tableRef;
        const ref = qual(...parts, value.columnName);
        const wire = columnWireReadNode(value, ref);
        if (wire !== null) {
          nodes.push(projectionNode(wire, key));
          caps.add("jsonb-functions");
        } else {
          nodes.push(projectionNode(ref, key === value.columnName ? undefined : key));
        }
        const decoder = projectionDecoder(parts.join("."), value, key);
        if (decoder) decoders.push(decoder);
        pushColumn({
          key,
          dataType: value.dataType,
          readMode: value.readMode as BigintMode | TemporalMode | undefined,
          valueDecoder: value.valueDecoder,
          canonicalText: value.dataType === "timestamp" || value.dataType === "timestamptz" || value.dataType === "date",
          source: richCodecSource(value),
        });
      }
    } else {
      // Default projection with joins: the from table's columns only.
      // Joined tables contribute through an explicit projection — that is the
      // documented output-mapping rule (property keys cannot collide).
      const plan = selectPlanFor(tableRef, tableName, columnEntries(this.table));
      nodes.push(...plan.nodes);
      decoders.push(...plan.decoders);
      for (const c of plan.capabilities) caps.add(c);
      for (const { propertyKey: key, column } of columnEntries(this.table)) {
        pushColumn({
          key,
          dataType: column.dataType,
          readMode: column.readMode as BigintMode | TemporalMode | undefined,
          valueDecoder: column.valueDecoder,
          canonicalText: column.dataType === "timestamp" || column.dataType === "timestamptz" || column.dataType === "date",
          source: richCodecSource(column),
        });
      }
    }

    assertDistinctJoinAliases(this.table, this.joinSpecs);

    // CTE registration: explicit withCte() calls first, then CTE handles
    // referenced in from/joins. Same name + same statement is idempotent;
    // same name + different statement fails closed (ambiguous identity).
    const cteRegs = new Map<string, StatementCte>();
    const registerCte = (reg: StatementCte): void => {
      const prior = cteRegs.get(reg.name);
      if (prior === undefined) {
        cteRegs.set(reg.name, reg);
        return;
      }
      if (prior.stmt !== reg.stmt) {
        throw new Error(`cte "${reg.name}": registered twice with different statements — a CTE name has one definition per statement`);
      }
    };
    for (const reg of this.extras.ctes) registerCte(reg);

    // From-target resolution: derived handles inline their subquery, CTE
    // handles reference the bare name (and register their CTE).
    const fromRec = getDerivedRecord(this.table);
    let from: FromTarget;
    let fromAlias: string | undefined;
    if (fromRec?.kind === "derived") {
      from = subqueryNode(fromRec.select);
      fromAlias = fromRec.name;
      for (const c of fromRec.capabilities) caps.add(c);
    } else if (fromRec?.kind === "cte") {
      from = ident(fromRec.name);
      registerCte({ name: fromRec.name, stmt: fromRec.select, capabilities: fromRec.capabilities });
      for (const c of fromRec.capabilities) caps.add(c);
    } else {
      from = tableTargetNode(this.table);
    }

    const joins = this.joinSpecs.map((j): ReturnType<typeof joinNode> => {
      const rec = getDerivedRecord(j.table);
      if (rec?.kind === "derived") {
        for (const c of rec.capabilities) caps.add(c);
        return joinNode(j.type, subqueryNode(rec.select), { alias: j.alias, on: j.on });
      }
      if (rec?.kind === "cte") {
        registerCte({ name: rec.name, stmt: rec.select, capabilities: rec.capabilities });
        for (const c of rec.capabilities) caps.add(c);
        return joinNode(j.type, ident(rec.name), { alias: j.alias, on: j.on });
      }
      return joinNode(j.type, tableTargetNode(j.table), { alias: j.alias, on: j.on });
    });

    // `with recursive` is required exactly when a CTE self-references —
    // detected structurally (fragment text is never scanned; a hand-typed
    // self-reference inside fragment text fails closed at the database).
    let recursive = false;
    for (const reg of cteRegs.values()) {
      if (statementReferencesName(reg.stmt, reg.name)) recursive = true;
      for (const c of reg.capabilities) caps.add(c);
    }

    const stmt: StatementNode = selectStatement({
      ctes: [...cteRegs.values()].map((reg) => cte(reg.name, reg.stmt)),
      recursive,
      distinct: this.extras.distinct,
      projections: nodes,
      from,
      fromAlias,
      joins,
      where: whereItems(this.conditions),
      groupBy: groupExprs(this.extras.group),
      having: whereItems(this.extras.having),
      orderBy: orderSpecs(this.order),
      limit: this.limitCount,
      offset: this.offsetCount,
    });
    // Fragment-spliced CTE references must resolve to a registered CTE of the
    // same source (Q02 review MINOR-2): unregistered or shadowed same-name
    // references fail closed here instead of silently binding.
    assertCteRefsResolve(stmt);
    return { stmt, decoders, capabilities: [...caps], columns };
  }

  toCompiled(): CompiledStatement {
    const plan = this.buildPlan();
    return {
      ...compileStatement(plan.stmt),
      decoders: plan.decoders,
      capabilities: plan.capabilities,
    };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(options?: QueryExecutionOptions): Promise<JoinRowOf<P, R0, N, F>[]> {
    const compiled = this.toCompiled();
    const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities, options)) as Array<Record<string, unknown>>;
    applyProjectionDecoders(rows, compiled.decoders);
    return rows as JoinRowOf<P, R0, N, F>[];
  }

  then<R1 = JoinRowOf<P, R0, N, F>[], R2 = never>(
    onfulfilled?: ((value: JoinRowOf<P, R0, N, F>[]) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}

/** Group-by terms: columns render as qualified references, value nodes as
 *  authored (order specs are rejected — they are not group terms). */
function groupExprs(group: readonly OrderExpression[]): ValueNode[] {
  return group as readonly ValueNode[] as ValueNode[];
}

// ---------------------------------------------------------------------------
// Set operations (Q02) — union / union all / intersect / intersect all /
// except / except all over typed select builders.
// ---------------------------------------------------------------------------

interface SetOpBranchSpec {
  readonly op: SetOpKind;
  readonly plan: FullSelectPlan;
}

/** A compound select: the first branch plus trailing set operations. Chained
 *  set operations compose LEFT-ASSOCIATIVELY in call order (branches render
 *  parenthesized, so SQL's intersect-over-union precedence never reorders
 *  them). Result rows are typed by the first branch; PostgreSQL takes output
 *  column names from it, and the first branch's decode plan applies (branch
 *  projections must be structurally aligned — same output keys and wire
 *  forms). orderBy/limit/offset on the compound apply to the WHOLE compound
 *  per PostgreSQL semantics. */
export class SetOpBuilder<R> implements PromiseLike<R[]> {
  private constructor(
    private readonly ctx: ExecContext,
    private readonly first: FullSelectPlan,
    private readonly branches: readonly SetOpBranchSpec[],
    private readonly order: readonly OrderExpression[] = [],
    private readonly limitCount: number | undefined = undefined,
    private readonly offsetCount: number | undefined = undefined,
  ) {
    Object.freeze(this.branches);
    Object.freeze(this.order);
    Object.freeze(this);
  }

  /** Start a compound from a first branch + one trailing operation. The
   *  first branch's own ORDER BY/LIMIT cannot survive composition: in SQL
   *  they would silently bind to the compound — fail closed instead. */
  static compound<R>(ctx: ExecContext, first: FullSelectPlan, op: SetOpKind, other: SubquerySource): SetOpBuilder<R> {
    if (first.stmt.orderBy.length > 0 || first.stmt.limit !== undefined || first.stmt.offset !== undefined) {
      throw new Error("set operations: the first branch carries orderBy/limit/offset — in SQL these would silently bind to the compound; order/limit the compound after the set operation instead");
    }
    const { stmt, capabilities } = sourceStatement(other);
    return new SetOpBuilder<R>(ctx, first, [{ op, plan: { stmt, decoders: [], capabilities, columns: [] } }]);
  }

  private fork<R2>(branches: readonly SetOpBranchSpec[], order?: readonly OrderExpression[], limit?: number, offset?: number): SetOpBuilder<R2> {
    return new SetOpBuilder<R2>(this.ctx, this.first, branches, order ?? this.order, limit ?? this.limitCount, offset ?? this.offsetCount);
  }

  private add<R2>(op: SetOpKind, other: SubquerySource): SetOpBuilder<R2> {
    const { stmt, capabilities } = sourceStatement(other);
    return this.fork<R2>([...this.branches, { op, plan: { stmt, decoders: [], capabilities, columns: [] } }]);
  }

  union<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("union", other);
  }

  unionAll<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("union all", other);
  }

  intersect<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("intersect", other);
  }

  intersectAll<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("intersect all", other);
  }

  except<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("except", other);
  }

  exceptAll<P extends Projection | null, R0, N extends string, F extends boolean>(other: SelectBuilder<P, R0, N, F>): SetOpBuilder<R> {
    return this.add<R>("except all", other);
  }

  orderBy(...exprs: Array<AnyColumnBuilder | OrderExpression>): SetOpBuilder<R> {
    for (const e of exprs) {
      rejectLegacyFragment(e, "orderBy");
      rejectLegacyFragment((e as OrderSpec).expr, "orderBy");
    }
    const added = exprs.map((e) => (typeof (e as OrderSpec).direction === "string" ? (e as OrderSpec) : exprTerm(e as AnyColumnBuilder | ValueNode)));
    return this.fork<R>(this.branches, [...this.order, ...added]);
  }

  limit(n: number): SetOpBuilder<R> {
    return this.fork<R>(this.branches, undefined, n, this.offsetCount);
  }

  offset(n: number): SetOpBuilder<R> {
    return this.fork<R>(this.branches, undefined, this.limitCount, n);
  }

  /** Keyset pagination introspection (Q04): same guard contract as
   *  SelectBuilder's — compounds carrying order terms or an offset are
   *  rejected by the pager's apply()/page(). */
  keysetBuilderState(): { ordered: boolean; offset: boolean } {
    return { ordered: this.order.length > 0, offset: this.offsetCount !== undefined };
  }

  /** Composition point: the compound as a subquery node. */
  subquery(): SubqueryNode {
    return subqueryNode(this.toAST());
  }

  /** The compound as a frozen AST. */
  toAST(): StatementNode {
    const s = this.first.stmt;
    return selectStatement({
      ctes: s.ctes,
      recursive: s.recursive,
      distinct: s.distinct,
      projections: s.projections,
      from: s.from,
      fromAlias: s.fromAlias,
      joins: s.joins,
      where: s.where,
      groupBy: s.groupBy,
      having: s.having,
      setOps: this.branches.map((b) => ({ op: b.op, select: b.plan.stmt })),
      orderBy: orderSpecs(this.order),
      limit: this.limitCount,
      offset: this.offsetCount,
    });
  }

  /** Full plan: the first branch's decode plan + the merged capability
   *  requirements of every branch. */
  toPlan(): FullSelectPlan {
    const caps = new Set<StatementCapability>(this.first.capabilities);
    for (const b of this.branches) for (const c of b.plan.capabilities) caps.add(c);
    return { stmt: this.toAST(), decoders: this.first.decoders, capabilities: [...caps], columns: this.first.columns };
  }

  toCompiled(): CompiledStatement {
    const plan = this.toPlan();
    return { ...compileStatement(plan.stmt), decoders: plan.decoders, capabilities: plan.capabilities };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(options?: QueryExecutionOptions): Promise<R[]> {
    const compiled = this.toCompiled();
    const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities, options)) as Array<Record<string, unknown>>;
    applyProjectionDecoders(rows, compiled.decoders);
    return rows as R[];
  }

  then<R1 = R[], R2 = never>(
    onfulfilled?: ((value: R[]) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

/** Result type of a selected returning() subset: exactly the requested
 *  property keys with their column read types. */
export type ReturningSubsetOf<TCols extends Record<string, AnyColumnBuilder>, K extends keyof TCols> = {
  [P in K]: SelectTypeOf<TCols[P]>;
};

/** Returning selection state: null = no returning, "all" = every column,
 *  otherwise the requested property keys in selection order. */
type ReturningKeys = null | "all" | readonly string[];

function returningPlanFor(
  table: AnyPgTable,
  keys: ReturningKeys,
): { nodes: ProjectionNode[]; decoders: ProjectionDecoder[]; capabilities: StatementCapability[] } {
  if (keys === null) {
    return { nodes: [], decoders: [] as ProjectionDecoder[], capabilities: [] as StatementCapability[] };
  }
  const tableName = getTableName(table);
  if (keys === "all") {
    return selectPlanFor(tableRefParts(table), tableName, columnEntries(table));
  }
  const columns = getTableColumns(table) as Record<string, AnyColumnBuilder>;
  const seen = new Set<string>();
  const entries: ColumnEntries = [];
  for (const key of keys) {
    const column = columns[key];
    if (!column) throw new Error(`returning: unknown column "${key}" on ${tableName}`);
    if (seen.has(key)) throw new Error(`returning: column "${key}" is selected twice — select each column once`);
    seen.add(key);
    entries.push({ propertyKey: key, column });
  }
  return selectPlanFor(tableRefParts(table), tableName, entries);
}

/** Validate a returning selection eagerly (at the returning() call): keys
 *  must be known property keys, each selected once. The compile-time plan
 *  re-checks as a backstop. */
function assertReturningKeys(table: AnyPgTable, keys: readonly string[]): void {
  const tableName = getTableName(table);
  const columns = getTableColumns(table) as Record<string, AnyColumnBuilder>;
  const seen = new Set<string>();
  for (const key of keys) {
    if (!Object.hasOwn(columns, key)) throw new Error(`returning: unknown column "${key}" on ${tableName}`);
    if (seen.has(key)) throw new Error(`returning: column "${key}" is selected twice — select each column once`);
    seen.add(key);
  }
}

export class InsertBuilder<TCols extends Record<string, AnyColumnBuilder>, R = number> implements PromiseLike<R> {
  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
    private readonly rows: Array<Record<string, unknown>> = [],
    private readonly hasValues = false,
    private readonly returningKeys: ReturningKeys = null,
    private readonly conflict: ConflictPlan | null = null,
  ) {
    rejectAliasHandle(table, "insert");
    rejectDerivedTable(table, "insert");
    rejectViewHandle(table, "insert");
    Object.freeze(this.rows);
    Object.freeze(this);
  }

  values(values: InferInsertModelOfRecord<TCols> | Array<InferInsertModelOfRecord<TCols>>): InsertBuilder<TCols, R> {
    const rows = Array.isArray(values) ? (values as unknown as Array<Record<string, unknown>>) : [values as unknown as Record<string, unknown>];
    return new InsertBuilder<TCols, R>(this.ctx, this.table, rows, true, this.returningKeys, this.conflict);
  }

  /** `on conflict do nothing` — a unique violation is ignored instead of
   *  failing the statement. `where` is the index predicate for partial unique
   *  index targets (only valid with a column-list target). Conflicted rows
   *  are simply not inserted: with .returning() they are omitted from the
   *  result (0..n rows — the actual inserted set, per PostgreSQL). */
  onConflictDoNothing(opts: { target?: ConflictTargetSpec; where?: Condition } = {}): InsertBuilder<TCols, R> {
    this.assertNoConflict("onConflictDoNothing");
    if (opts.where !== undefined) rejectLegacyFragment(opts.where, "on conflict target where");
    return new InsertBuilder<TCols, R>(this.ctx, this.table, this.rows, this.hasValues, this.returningKeys, {
      action: "nothing",
      target: opts.target,
      targetWhere: opts.where,
    });
  }

  /** `on conflict … do update set … [where …]` — the upsert. `target` is
   *  required (PostgreSQL rejects targetless DO UPDATE); `set` assignments
   *  may reference `excluded(col)` (the proposed row), expressions and
   *  literals; `targetWhere` is the partial-index predicate, `setWhere` the
   *  conditional-update predicate (conflicted rows it rejects are neither
   *  updated nor returned). Returning yields one row per input row that was
   *  inserted or updated. */
  onConflictUpdate(opts: {
    target: ConflictTargetSpec;
    set: UpdateSetInput<TCols>;
    targetWhere?: Condition;
    setWhere?: Condition;
  }): InsertBuilder<TCols, R> {
    this.assertNoConflict("onConflictUpdate");
    if (opts.target === undefined) throw new Error("onConflictUpdate: target is required — PostgreSQL rejects targetless DO UPDATE");
    if (opts.set === undefined || typeof opts.set !== "object") throw new Error("onConflictUpdate: set is required");
    if (opts.targetWhere !== undefined) rejectLegacyFragment(opts.targetWhere, "on conflict target where");
    if (opts.setWhere !== undefined) rejectLegacyFragment(opts.setWhere, "on conflict set where");
    return new InsertBuilder<TCols, R>(this.ctx, this.table, this.rows, this.hasValues, this.returningKeys, {
      action: "update",
      target: opts.target,
      targetWhere: opts.targetWhere,
      set: opts.set as Record<string, unknown>,
      setWhere: opts.setWhere,
    });
  }

  private assertNoConflict(who: string): void {
    if (this.conflict !== null) {
      throw new Error(
        `${who}: this insert already carries an on-conflict clause (${this.conflict.action === "nothing" ? "do nothing" : "do update"}) — build a new insert per conflict action`,
      );
    }
  }

  returning(): InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  returning<K extends keyof TCols & string>(selection: K | readonly K[]): InsertBuilder<TCols, Array<ReturningSubsetOf<TCols, K>>>;
  returning(selection?: keyof TCols & string | readonly (keyof TCols & string)[]): InsertBuilder<TCols, unknown> {
    const keys: ReturningKeys = selection === undefined ? "all" : Array.isArray(selection) ? [...selection] : [selection];
    if (keys !== "all" && keys !== null) {
      if (keys.length === 0) throw new Error("returning: select at least one column — no-argument returning() returns every column");
      assertReturningKeys(this.table, keys);
    }
    return new InsertBuilder<TCols, unknown>(this.ctx, this.table, this.rows, this.hasValues, keys, this.conflict);
  }

  toCompiled(): CompiledStatement {
    if (!this.hasValues) throw new Error("insert requires .values()");
    if (this.rows.length === 0) throw new Error("insert .values() received an empty array");

    const tableName = getTableName(this.table);
    const columns = getTableColumns(this.table) as Record<string, AnyColumnBuilder>;
    const propertyOrder = Object.keys(columns);
    if (propertyOrder.length === 0) throw new Error(`table ${tableName} has no columns`);
    const knownKeys = new Set(propertyOrder);
    const required = requiredInsertKeys(this.table);

    // Validate every row and collect the union of supplied keys. A key is
    // supplied when it is present with a value other than undefined in at
    // least one row; explicit null is a supplied value (NULL, never DEFAULT).
    // Non-null values are codec-encoded now (validation + canonical text) so
    // the values section binds exactly what was validated. Duplicate physical
    // assignments inside one row (two property keys mapping to one physical
    // column) are rejected order-independently — the emitted column list
    // would assign the column twice.
    const supplied = new Set<string>();
    const encodedRows: Array<Map<string, EncodedValue>> = [];
    for (let rowIdx = 0; rowIdx < this.rows.length; rowIdx++) {
      const row = this.rows[rowIdx];
      if (typeof row !== "object" || row === null || Array.isArray(row)) {
        throw new Error(`insert .values() rows must be objects on ${tableName}`);
      }
      const rowKeys = Object.keys(row);
      const encoded = new Map<string, EncodedValue>();
      for (const key of rowKeys) {
        if (!knownKeys.has(key)) throw new Error(`unknown column "${key}" on ${tableName}`);
        const column = columns[key];
        const value = row[key];
        if (value === undefined) continue;
        assertColumnWritable(column, columnContext(tableName, column, key));
        rejectLegacyFragment(value, "insert values");
        if (effectiveNotNull(column) && value === null) {
          throw new Error(
            `insert on ${tableName}: null is not allowed for NOT NULL column "${key}" ("${column.columnName}")`,
          );
        }
        if (value !== null) {
          encoded.set(key, encodeForColumn(tableName, column, key, value));
        }
        supplied.add(key);
      }
      // Duplicate physical assignments inside one row (two property keys
      // mapping to one physical column) are rejected order-independently —
      // the emitted column list would assign the column twice. Keys valued
      // undefined are OMITTED by the undefined-is-omitted convention, so
      // they cannot collide (Q04 entry condition).
      assertDistinctPhysicalColumns(
        tableName,
        rowKeys
          .filter((k) => row[k] !== undefined)
          .map((k) => [k, columns[k].columnName] as const),
        "insert values",
      );
      encodedRows.push(encoded);
      const missing = required.filter(({ propertyKey }) => !Object.hasOwn(row, propertyKey) || row[propertyKey] === undefined);
      if (missing.length > 0) {
        throw new Error(
          `insert on ${tableName} row ${rowIdx} is missing required column(s) ` +
          missing.map(({ propertyKey, column }) => `"${propertyKey}" ("${column.columnName}")`).join(", ") +
          " — NOT NULL without a default",
        );
      }
    }

    // One stable schema-ordered column list shared by every row. Cell reads
    // use own properties only: an inherited value is not a supplied value.
    const orderedKeys = propertyOrder.filter((k) => supplied.has(k));

    const returningPlan = returningPlanFor(this.table, this.returningKeys);
    const onConflict = this.conflict === null ? undefined : buildOnConflictNode(this.table, this.conflict);

    const stmt: AnyStatementNode = (() => {
      if (orderedKeys.length === 0) {
        // Every row is default-only. Postgres has no multi-row DEFAULT VALUES
        // form, so batch by explicitly requesting DEFAULT for one column.
        if (this.rows.length === 1) {
          return insertStatement({ table: tableTargetNode(this.table), defaultValues: true, onConflict, returning: returningPlan.nodes });
        }
        return insertStatement({
          table: tableTargetNode(this.table),
          columns: [columns[propertyOrder[0]].columnName],
          rows: this.rows.map(() => [defaultCell()] as ReadonlyArray<InsertCell>),
          onConflict,
          returning: returningPlan.nodes,
        });
      }
      return insertStatement({
        table: tableTargetNode(this.table),
        columns: orderedKeys.map((k) => columns[k].columnName),
        rows: this.rows.map((row, rowIdx) => {
          const encoded = encodedRows[rowIdx];
          return orderedKeys.map<InsertCell>((k) => {
            if (!Object.hasOwn(row, k) || row[k] === undefined) return defaultCell();
            if (row[k] === null) return paramNode(null);
            return cellNode(encoded.get(k)!);
          });
        }),
        onConflict,
        returning: returningPlan.nodes,
      });
    })();

    assertCteRefsResolve(stmt);
    return {
      ...compileStatement(stmt),
      decoders: returningPlan.decoders,
      capabilities: returningPlan.capabilities,
    };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(options?: QueryExecutionOptions): Promise<R> {
    const compiled = this.toCompiled();
    if (this.returningKeys !== null) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities, options)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities, options)) as R;
  }

  then<R1 = R, R2 = never>(
    onfulfilled?: ((value: R) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return Promise.resolve(this.execute()).then(onfulfilled, onrejected);
  }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

interface UpdateSet {
  /** Declared property key (for duplicate-assignment diagnostics). */
  readonly propertyKey: string;
  readonly column: string;
  readonly value: ValueNode;
}

export class UpdateBuilder<TCols extends Record<string, AnyColumnBuilder>, R = number> implements PromiseLike<R> {
  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
    private readonly sets: ReadonlyArray<UpdateSet> = [],
    private readonly hasSet = false,
    private readonly conditions: readonly Condition[] = [],
    private readonly returningKeys: ReturningKeys = null,
  ) {
    rejectAliasHandle(table, "update");
    rejectDerivedTable(table, "update");
    rejectViewHandle(table, "update");
    Object.freeze(this.sets);
    Object.freeze(this.conditions);
    Object.freeze(this);
  }

  set(values: UpdateSetInput<TCols>): UpdateBuilder<TCols, R> {
    const tableName = getTableName(this.table);
    const columns = getTableColumns(this.table) as Record<string, AnyColumnBuilder>;
    const added: UpdateSet[] = [];
    for (const [key, value] of Object.entries(values)) {
      const column = columns[key];
      if (!column) throw new Error(`unknown column "${key}" on ${tableName}`);
      if (value === undefined) continue; // omitted/undefined update keys are ignored
      assertColumnWritable(column, columnContext(tableName, column, key));
      const physical = column.columnName;
      // Null is checked BEFORE fragment detection: null is a bindable value
      // for nullable columns, never an object to interrogate.
      if (value === null) {
        if (effectiveNotNull(column)) {
          throw new Error(`update on ${tableName}: null is not allowed for NOT NULL column "${key}" ("${physical}")`);
        }
        added.push({ propertyKey: key, column: physical, value: paramNode(null) });
        continue;
      }
      const isJson = column.dataType === "json" || column.dataType === "jsonb";
      // AST fragments (sql`...`) stay supported assignments on every column
      // type except json/jsonb, where plain-object values always bind as
      // values. excluded() references are on-conflict-only and fail closed
      // here instead of as a database "missing FROM-clause entry".
      if (!isJson && isValueNode(value)) {
        assertNoExcludedRefs(value, `update set on ${tableName} column "${key}"`);
        added.push({ propertyKey: key, column: physical, value });
        continue;
      }
      rejectLegacyFragment(value, "update set");
      added.push({ propertyKey: key, column: physical, value: cellNode(encodeForColumn(tableName, column, key, value)) });
    }
    return new UpdateBuilder<TCols, R>(this.ctx, this.table, [...this.sets, ...added], true, this.conditions, this.returningKeys);
  }

  where(condition: Condition): UpdateBuilder<TCols, R> {
    rejectLegacyFragment(condition, "where");
    assertNoExcludedRefs(condition, "update where");
    return new UpdateBuilder<TCols, R>(this.ctx, this.table, this.sets, this.hasSet, [...this.conditions, condition], this.returningKeys);
  }

  returning(): UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  returning<K extends keyof TCols & string>(selection: K | readonly K[]): UpdateBuilder<TCols, Array<ReturningSubsetOf<TCols, K>>>;
  returning(selection?: keyof TCols & string | readonly (keyof TCols & string)[]): UpdateBuilder<TCols, unknown> {
    const keys: ReturningKeys = selection === undefined ? "all" : Array.isArray(selection) ? [...selection] : [selection];
    if (keys !== "all" && keys !== null) {
      if (keys.length === 0) throw new Error("returning: select at least one column — no-argument returning() returns every column");
      assertReturningKeys(this.table, keys);
    }
    return new UpdateBuilder<TCols, unknown>(this.ctx, this.table, this.sets, this.hasSet, this.conditions, keys);
  }

  toCompiled(): CompiledStatement {
    if (!this.hasSet) throw new Error("update requires .set()");
    if (this.sets.length === 0) throw new Error("update .set() had no assignments — undefined values are ignored");
    if (this.conditions.length === 0) throw new Error("update without .where() is not allowed");
    const tableName = getTableName(this.table);
    // A column assigned by two .set() calls (or two property keys mapping to
    // one physical column) is a deterministic pre-SQL error — never a
    // traversal-order-dependent "multiple assignments" database error.
    assertDistinctPhysicalColumns(
      tableName,
      this.sets.map((s) => [s.propertyKey, s.column] as const),
      "update set",
    );
    const returningPlan = returningPlanFor(this.table, this.returningKeys);
    const stmt = updateStatement({
      table: tableTargetNode(this.table),
      sets: this.sets,
      where: whereItems(this.conditions),
      returning: returningPlan.nodes,
    });
    assertCteRefsResolve(stmt);
    return {
      ...compileStatement(stmt),
      decoders: returningPlan.decoders,
      capabilities: returningPlan.capabilities,
    };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(options?: QueryExecutionOptions): Promise<R> {
    const compiled = this.toCompiled();
    if (this.returningKeys !== null) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities, options)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities, options)) as R;
  }

  then<R1 = R, R2 = never>(
    onfulfilled?: ((value: R) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return Promise.resolve(this.execute()).then(onfulfilled, onrejected);
  }
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

export class DeleteBuilder<TCols extends Record<string, AnyColumnBuilder>, R = number> implements PromiseLike<R> {
  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
    private readonly conditions: readonly Condition[] = [],
    private readonly wantsReturning = false,
  ) {
    rejectAliasHandle(table, "delete");
    rejectDerivedTable(table, "delete");
    rejectViewHandle(table, "delete");
    Object.freeze(this.conditions);
    Object.freeze(this);
  }

  where(condition: Condition): DeleteBuilder<TCols, R> {
    rejectLegacyFragment(condition, "where");
    assertNoExcludedRefs(condition, "delete where");
    return new DeleteBuilder<TCols, R>(this.ctx, this.table, [...this.conditions, condition], this.wantsReturning);
  }

  returning(): DeleteBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    return new DeleteBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>(
      this.ctx,
      this.table,
      this.conditions,
      true,
    ) as unknown as DeleteBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  }

  toCompiled(): CompiledStatement {
    if (this.conditions.length === 0) throw new Error("delete without .where() is not allowed");
    const tableName = getTableName(this.table);
    const returningPlan = this.wantsReturning
      ? selectPlanFor(tableRefParts(this.table), tableName, columnEntries(this.table))
      : { nodes: [] as ProjectionNode[], decoders: [] as ProjectionDecoder[], capabilities: [] as StatementCapability[] };
    const stmt = deleteStatement({
      table: tableTargetNode(this.table),
      where: whereItems(this.conditions),
      returning: returningPlan.nodes,
    });
    assertCteRefsResolve(stmt);
    return {
      ...compileStatement(stmt),
      decoders: returningPlan.decoders,
      capabilities: returningPlan.capabilities,
    };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(options?: QueryExecutionOptions): Promise<R> {
    const compiled = this.toCompiled();
    if (this.wantsReturning) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities, options)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities, options)) as R;
  }

  then<R1 = R, R2 = never>(
    onfulfilled?: ((value: R) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return Promise.resolve(this.execute()).then(onfulfilled, onrejected);
  }
}

// ---------------------------------------------------------------------------
// Shared model helpers (local aliases so builder generics stay readable)
// ---------------------------------------------------------------------------

export type InferSelectModelOfRecord<TCols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof TCols]: SelectTypeOf<TCols[K]>;
};

/** Required keys are NOT NULL without default and must be present; the rest
 *  stay optional (null/undefined/default per column). */
export type InferInsertModelOfRecord<TCols extends Record<string, AnyColumnBuilder>> = InferInsertModelOf<TCols>;

export type UpdateSetInput<TCols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof TCols]?: UpdateTypeOf<TCols[K]> | ValueNode;
};

// ---------------------------------------------------------------------------
// AST select builder (F01) — immutable structural builder over the same
// compiler; the typed-condition CRUD builders above share compileStatement.
// ---------------------------------------------------------------------------

/** Projection values: a schema column (physical ref + property-key label) or
 *  any value node (always labeled with the projection key). */
export type AstProjection = Record<string, AnyColumnBuilder | ValueNode>;

/** Join/from targets: a schema table, a schema-qualified name node, a CTE
 *  name, or a subquery node. */
export type AstJoinTarget = AnyPgTable | SubqueryNode | IdentifierNode | QualifiedNode;

interface AstJoinSpec {
  readonly type: JoinType;
  readonly table: AstJoinTarget;
  readonly alias: string | undefined;
  readonly on: ValueNode | undefined;
}

interface AstFromSpec {
  readonly table: AstJoinTarget;
  readonly alias: string | undefined;
}

interface AstExtras {
  readonly group: readonly ValueNode[];
  readonly having: readonly ValueNode[];
  readonly distinct: boolean;
  readonly setOps: readonly { op: SetOpKind; select: StatementNode }[];
}

const AST_NO_EXTRAS: AstExtras = Object.freeze({ group: [], having: [], distinct: false, setOps: [] });

export class AstSelectBuilder {
  private readonly projectionSpec: AstProjection | null;
  private readonly extras: AstExtras;

  /** Internal — construct through `astSelect()`. Kept public only so the
   *  entry-point factory can build instances; the shape is not API. */
  constructor(
    private readonly fromSpec: AstFromSpec,
    projectionSpec: AstProjection | null,
    private readonly joinSpecs: readonly AstJoinSpec[],
    private readonly wheres: readonly ValueNode[],
    private readonly orders: readonly OrderSpec[],
    private readonly cteSpecs: readonly CteNode[],
    private readonly limitCount: number | undefined,
    private readonly offsetCount: number | undefined,
    extras: AstExtras = AST_NO_EXTRAS,
  ) {
    // F-R2: the freeze must be deep over builder-owned state. Forks share
    // these arrays (copy-on-write), so a mutable array would let one branch
    // corrupt another via wheres.pop()/push(). Nodes inside are already
    // frozen at construction; the specs the builder creates itself are
    // frozen here element-wise. projectionSpec is COPIED shallowly and the
    // private copy frozen: freezing the caller's object directly would
    // mutate it, but sharing it by reference would let post-fork caller
    // mutation reach every sibling — F01 review-2's carry-forward.
    if (projectionSpec === null) {
      this.projectionSpec = null;
    } else {
      const copy: AstProjection = {};
      for (const [key, value] of Object.entries(projectionSpec)) {
        rejectLegacyFragment(value, "astSelect projection");
        copy[key] = value;
      }
      this.projectionSpec = Object.freeze(copy);
    }
    this.extras = Object.freeze({
      group: Object.freeze([...extras.group]),
      having: Object.freeze([...extras.having]),
      distinct: extras.distinct === true,
      setOps: Object.freeze([...extras.setOps]),
    });
    Object.freeze(this.fromSpec);
    for (const spec of this.joinSpecs) Object.freeze(spec);
    Object.freeze(this.joinSpecs);
    Object.freeze(this.wheres);
    for (const spec of this.orders) Object.freeze(spec);
    Object.freeze(this.orders);
    Object.freeze(this.cteSpecs);
    Object.freeze(this);
  }

  private fork(extras: AstExtras): AstSelectBuilder {
    return new AstSelectBuilder(this.fromSpec, this.projectionSpec, this.joinSpecs, this.wheres, this.orders, this.cteSpecs, this.limitCount, this.offsetCount, extras);
  }

  /** Non-cross joins need a condition; cross joins take none. */
  join(type: "cross", table: AstJoinTarget, alias?: string, on?: undefined): AstSelectBuilder;
  join(type: Exclude<JoinType, "cross">, table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder;
  join(type: JoinType, table: AstJoinTarget, alias?: string, on?: ValueNode): AstSelectBuilder {
    if (type !== "cross" && on === undefined) throw new Error(`ast join: ${type} joins require an on condition`);
    if (type === "cross" && on !== undefined) throw new Error("ast join: cross joins take no on condition");
    if (on !== undefined) rejectLegacyFragment(on, "ast join on");
    if (alias !== undefined) validAlias(alias, "ast join alias");
    if (isPgTable(table)) rejectAliasHandle(table, "ast join");
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      [...this.joinSpecs, { type, table, alias, on }],
      this.wheres,
      this.orders,
      this.cteSpecs,
      this.limitCount,
      this.offsetCount,
      this.extras,
    );
  }

  innerJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("inner", table, alias, on);
  }

  leftJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("left", table, alias, on);
  }

  rightJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("right", table, alias, on);
  }

  fullJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("full", table, alias, on);
  }

  crossJoin(table: AstJoinTarget, alias: string | undefined, on?: never): AstSelectBuilder {
    // Q01 review MINOR-1: reject a runtime ON argument instead of dropping it.
    if (arguments.length > 2 && arguments[2] !== undefined) {
      throw new Error("ast crossJoin: cross joins take no on condition — use join(\"inner\"|\"left\"|…) or drop the condition");
    }
    return this.join("cross", table, alias, undefined);
  }

  where(node: ValueNode): AstSelectBuilder {
    rejectLegacyFragment(node, "where");
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      [...this.wheres, node],
      this.orders,
      this.cteSpecs,
      this.limitCount,
      this.offsetCount,
      this.extras,
    );
  }

  orderBy(expr: ValueNode, direction: "asc" | "desc" = "asc"): AstSelectBuilder {
    rejectLegacyFragment(expr, "orderBy");
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      this.wheres,
      [...this.orders, { expr, direction }],
      this.cteSpecs,
      this.limitCount,
      this.offsetCount,
      this.extras,
    );
  }

  withCte(name: string, sub: SubqueryNode | { subquery(): SubqueryNode }): AstSelectBuilder {
    const stmt = typeof (sub as { subquery?: unknown }).subquery === "function" ? (sub as { subquery(): SubqueryNode }).subquery().select : (sub as SubqueryNode).select;
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      this.wheres,
      this.orders,
      [...this.cteSpecs, cte(name, stmt)],
      this.limitCount,
      this.offsetCount,
      this.extras,
    );
  }

  limit(n: number): AstSelectBuilder {
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      this.wheres,
      this.orders,
      this.cteSpecs,
      n,
      this.offsetCount,
      this.extras,
    );
  }

  offset(n: number): AstSelectBuilder {
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      this.wheres,
      this.orders,
      this.cteSpecs,
      this.limitCount,
      n,
      this.extras,
    );
  }

  // -------------------------------------------------------------------------
  // Q02: group by / having / distinct / set operations. Set operations on
  // this structural builder compose left-associatively in call order
  // (branches render parenthesized); orderBy/limit/offset set AFTER a set
  // operation apply to the whole compound per PostgreSQL semantics.
  // -------------------------------------------------------------------------

  groupBy(...exprs: Array<AnyColumnBuilder | ValueNode>): AstSelectBuilder {
    const terms = exprs.map((e) => {
      rejectLegacyFragment(e, "astSelect groupBy");
      return exprTerm(e);
    });
    for (const t of terms) rejectLegacyFragment(t, "astSelect groupBy");
    return this.fork({ ...this.extras, group: [...this.extras.group, ...terms] });
  }

  having(node: ValueNode): AstSelectBuilder {
    rejectLegacyFragment(node, "astSelect having");
    return this.fork({ ...this.extras, having: [...this.extras.having, node] });
  }

  distinct(): AstSelectBuilder {
    return this.fork({ ...this.extras, distinct: true });
  }

  union(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("union", other);
  }

  unionAll(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("union all", other);
  }

  intersect(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("intersect", other);
  }

  intersectAll(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("intersect all", other);
  }

  except(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("except", other);
  }

  exceptAll(other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    return this.addSetOp("except all", other);
  }

  private addSetOp(op: SetOpKind, other: AstSelectBuilder | SubqueryNode): AstSelectBuilder {
    if (this.orders.length > 0 || this.limitCount !== undefined || this.offsetCount !== undefined) {
      throw new Error("astSelect set operations: this branch carries orderBy/limit/offset — in SQL these would silently bind to the compound; apply them after the set operation instead");
    }
    const stmt = typeof other === "object" && other !== null && (other as { kind?: unknown }).kind === "subquery" ? (other as SubqueryNode).select : (other as AstSelectBuilder).toAST();
    return this.fork({ ...this.extras, setOps: [...this.extras.setOps, { op, select: stmt }] });
  }

  /** The statement as a frozen AST — composition point for subqueries.
   *  `with recursive` is computed from structural CTE self-references.
   *  CTE handles in from/joins auto-register their `with` entry (same
   *  conflict rule as the typed builder), and fragment-spliced CTE
   *  references must resolve (Q02 review MINOR-2). */
  toAST(): StatementNode {
    const from = this.fromSpec.table;
    const fromRec = isPgTable(from) ? getDerivedRecord(from) : undefined;
    const regs = new Map<string, StatementNode>();
    const register = (name: string, select: StatementNode): void => {
      const prior = regs.get(name);
      if (prior === undefined) {
        regs.set(name, select);
        return;
      }
      if (prior !== select) {
        throw new Error(`cte "${name}": registered twice with different statements — a CTE name has one definition per statement`);
      }
    };
    for (const c of this.cteSpecs) register(c.name, c.select);
    let fromTarget: FromTarget;
    let fromAlias = this.fromSpec.alias;
    if (fromRec?.kind === "derived") {
      fromTarget = subqueryNode(fromRec.select);
      if (fromAlias === undefined) fromAlias = fromRec.name;
    } else if (fromRec?.kind === "cte") {
      fromTarget = ident(fromRec.name);
      register(fromRec.name, fromRec.select);
    } else {
      fromTarget = isPgTable(from) ? tableTargetNode(from) : from;
    }
    const joins = this.joinSpecs.map((j) => {
      const rec = isPgTable(j.table) ? getDerivedRecord(j.table) : undefined;
      if (rec?.kind === "derived") {
        return joinNode(j.type, subqueryNode(rec.select), { alias: j.alias ?? rec.name, on: j.on });
      }
      if (rec?.kind === "cte") {
        register(rec.name, rec.select);
        return joinNode(j.type, ident(rec.name), { alias: j.alias, on: j.on });
      }
      const target = isPgTable(j.table) ? tableTargetNode(j.table) : j.table;
      return joinNode(j.type, target, { alias: j.alias, on: j.on });
    });
    const ctes = [...regs.entries()].map(([name, select]) => cte(name, select));
    const stmt = selectStatement({
      ctes,
      recursive: ctes.some((c) => statementReferencesName(c.select, c.name)),
      distinct: this.extras.distinct,
      projections: this.buildProjections(),
      from: fromTarget,
      fromAlias,
      joins,
      where: this.wheres,
      groupBy: this.extras.group,
      having: this.extras.having,
      setOps: this.extras.setOps,
      orderBy: this.orders,
      limit: this.limitCount,
      offset: this.offsetCount,
    });
    assertCteRefsResolve(stmt);
    return stmt;
  }

  /** Wrap this builder's statement for interpolation into a parent query. */
  subquery(): SubqueryNode {
    return subqueryNode(this.toAST());
  }

  /** Pure compile: same builder state → byte-identical SQL + params. */
  toSQL(): CompiledQuery {
    return compileStatement(this.toAST());
  }

  /** Output column specs for derivedTable()/cteTable() pseudo-columns. A
   *  default projection over a subquery/CTE reference has no enumerable
   *  columns — project explicitly. */
  derivedColumns(): PlanColumnSpec[] {
    if (this.projectionSpec === null) {
      const from = this.fromSpec.table;
      if (!isPgTable(from)) {
        throw new Error("derivedTable/cteTable: the source selects star from a subquery/CTE reference — project its columns explicitly");
      }
      return columnEntries(from).map(({ propertyKey, column }) => ({
        key: propertyKey,
        dataType: column.dataType,
        readMode: column.readMode as BigintMode | TemporalMode | undefined,
        valueDecoder: column.valueDecoder,
        canonicalText: column.canonicalText,
        source: richCodecSource(column),
      }));
    }
    return Object.entries(this.projectionSpec).map(([key, value]): PlanColumnSpec => {
      if (isPgColumnRef(value)) {
        return { key, dataType: value.dataType, readMode: value.readMode as BigintMode | TemporalMode | undefined, valueDecoder: value.valueDecoder, source: richCodecSource(value) };
      }
      if (isValueNode(value) && value.kind === "aggregate") {
        const spec = aggregateResultColumn(value);
        return { key, dataType: spec.dataType, readMode: spec.readMode, valueDecoder: spec.valueDecoder, canonicalText: spec.dataType === "timestamp" || spec.dataType === "timestamptz" || spec.dataType === "date", source: richCodecSource(spec.source) };
      }
      return { key, dataType: "text" };
    });
  }

  private buildProjections(): ProjectionNode[] {
    if (this.projectionSpec === null) {
      const from = this.fromSpec.table;
      if (!isPgTable(from)) {
        throw new Error("astSelect: a projection is required when selecting from a subquery or CTE reference");
      }
      // Default projection: every column, labeled with its property key
      // whenever the property key differs from the physical name.
      const parts = tableRefParts(from);
      return columnEntries(from).map(({ propertyKey, column }) =>
        projectionNode(qual(...parts, column.columnName), propertyKey === column.columnName ? undefined : propertyKey),
      );
    }
    return Object.entries(this.projectionSpec).map(([key, value]) => {
      if (isPgColumnRef(value)) {
        if (!value.ownerTable) throw new Error(`astSelect: projected column "${key}" has no owner table`);
        return projectionNode(qual(...tableRefParts(value.ownerTable), value.columnName), key === value.columnName ? undefined : key);
      }
      return projectionNode(value, key);
    });
  }
}

function isPgColumnRef(value: unknown): value is AnyColumnBuilder {
  return typeof value === "object" && value !== null && typeof (value as { columnName?: unknown }).columnName === "string";
}

/** Entry point for the AST-backed select. `astSelect({ email: users.email })`
 *  projects mapped columns; `astSelect()` takes every column of the from
 *  table with property-key labels. */
export function astSelect(projection: AstProjection | null = null): { from(table: AstJoinTarget, alias?: string): AstSelectBuilder } {
  return {
    from: (table: AstJoinTarget, alias?: string) => {
      if (alias !== undefined) validAlias(alias, "astSelect from alias");
      if (isPgTable(table)) rejectAliasHandle(table, "astSelect from");
      return new AstSelectBuilder({ table, alias }, projection, [], [], [], [], undefined, undefined);
    },
  };
}
