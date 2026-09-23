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
import type { Logger } from "./logger.js";
import { type Condition, type OrderExpression } from "./expr.js";
import type {
  AnyColumnBuilder,
  AnyPgTable,
  InferInsertModelOf,
  PgTable,
  SelectTypeOf,
  UpdateTypeOf,
} from "./schema.js";
import { getTableColumns, getTableName, isPgTable } from "./schema.js";
import {
  applyProjectionDecoders,
  encodeWriteValue,
  projectionDecoder,
  wireReadNode,
  type ColumnContext,
  type EncodedValue,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";
import {
  cte,
  defaultCell,
  ident,
  insertStatement,
  isLegacySqlFragment,
  isValueNode,
  join as joinNode,
  legacyFragmentError,
  param as paramNode,
  paramCast,
  projection as projectionNode,
  qual,
  selectStatement,
  subquery as subqueryNode,
  updateStatement,
  validAlias,
  deleteStatement,
  type AnyStatementNode,
  type CteNode,
  type IdentifierNode,
  type InsertCell,
  type JoinType,
  type OrderSpec,
  type ParamNode,
  type ProjectionNode,
  type StatementNode,
  type SubqueryNode,
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
): Promise<unknown> {
  await assertCapabilities(ctx, required);
  const started = performance.now();
  try {
    const result = kind === "query" ? await ctx.driver.query(sqlText, params) : await ctx.driver.execute(sqlText, params);
    ctx.logger?.({ sql: sqlText, params, durationMs: performance.now() - started });
    return result;
  } catch (err) {
    ctx.logger?.({ sql: sqlText, params, durationMs: performance.now() - started, error: err as Error });
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

/** Projection list for a set of `{ propertyKey, column }` entries: physical
 *  qualified references labeled with property keys, lossless text-acquisition
 *  fragments for lossy-native types (always labeled), plus the matching
 *  decode plan. */
function selectPlanFor(tableName: string, entries: ColumnEntries): SelectPlan {
  const nodes: ProjectionNode[] = [];
  const decoders: ProjectionDecoder[] = [];
  let usesJsonb = false;
  for (const { propertyKey: key, column } of entries) {
    const ref = qual(tableName, column.columnName);
    const wire = wireReadNode(column.dataType, ref);
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

function orderSpecs(order: readonly OrderExpression[]): OrderSpec[] {
  return order.map((o) => (typeof (o as OrderSpec).direction === "string" ? (o as OrderSpec) : { expr: o as ValueNode, direction: "asc" as const }));
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
// Select
// ---------------------------------------------------------------------------

export type Projection = Record<string, AnyColumnBuilder | ValueNode>;

export type ProjectionResult<P extends Projection> = {
  [K in keyof P]: P[K] extends AnyColumnBuilder ? SelectTypeOf<P[K]> : unknown;
};

export class SelectBuilder<T> implements PromiseLike<T[]> {
  private readonly ctx: ExecContext;
  private readonly table: AnyPgTable;
  private readonly projection: Projection | null;

  constructor(
    ctx: ExecContext,
    table: AnyPgTable,
    projection: Projection | null,
    private readonly conditions: readonly Condition[] = [],
    private readonly order: readonly OrderExpression[] = [],
    private readonly limitCount: number | undefined = undefined,
    private readonly offsetCount: number | undefined = undefined,
  ) {
    // The projection is copied and frozen at construction (shallow): the
    // caller's object stays theirs, and every fork compiles the snapshot it
    // was built from — post-fork caller mutation cannot reach any sibling
    // (F01 review-2 carry-forward). Legacy fragments never enter the copy.
    this.ctx = ctx;
    this.table = table;
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
    Object.freeze(this.conditions);
    Object.freeze(this.order);
    Object.freeze(this);
  }

  where(condition: Condition): SelectBuilder<T> {
    rejectLegacyFragment(condition, "where");
    return new SelectBuilder(this.ctx, this.table, this.projection, [...this.conditions, condition], this.order, this.limitCount, this.offsetCount);
  }

  orderBy(...exprs: OrderExpression[]): SelectBuilder<T> {
    for (const e of exprs) {
      rejectLegacyFragment(e, "orderBy");
      rejectLegacyFragment((e as OrderSpec).expr, "orderBy");
    }
    return new SelectBuilder(this.ctx, this.table, this.projection, this.conditions, [...this.order, ...exprs], this.limitCount, this.offsetCount);
  }

  limit(n: number): SelectBuilder<T> {
    return new SelectBuilder(this.ctx, this.table, this.projection, this.conditions, this.order, n, this.offsetCount);
  }

  offset(n: number): SelectBuilder<T> {
    return new SelectBuilder(this.ctx, this.table, this.projection, this.conditions, this.order, this.limitCount, n);
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

  /** The statement + decode plan; pure — same builder state compiles to
   *  byte-identical SQL. */
  toCompiled(): CompiledStatement {
    const tableName = getTableName(this.table);
    const nodes: ProjectionNode[] = [];
    const decoders: ProjectionDecoder[] = [];
    let usesJsonb = false;

    if (this.projection) {
      for (const [key, value] of Object.entries(this.projection)) {
        if (isValueNode(value)) {
          nodes.push(projectionNode(value, key));
          continue;
        }
        const owner = value.ownerTable ? getTableName(value.ownerTable) : tableName;
        const ref = qual(owner, value.columnName);
        const wire = wireReadNode(value.dataType, ref);
        if (wire !== null) {
          nodes.push(projectionNode(wire, key));
          usesJsonb = true;
        } else {
          nodes.push(projectionNode(ref, key === value.columnName ? undefined : key));
        }
        const decoder = projectionDecoder(owner, value, key);
        if (decoder) decoders.push(decoder);
      }
    } else {
      const plan = selectPlanFor(tableName, columnEntries(this.table));
      nodes.push(...plan.nodes);
      decoders.push(...plan.decoders);
      usesJsonb = plan.capabilities.length > 0;
    }

    const stmt: StatementNode = selectStatement({
      projections: nodes,
      from: ident(tableName),
      where: whereItems(this.conditions),
      orderBy: orderSpecs(this.order),
      limit: this.limitCount,
      offset: this.offsetCount,
    });
    return {
      ...compileStatement(stmt),
      decoders,
      capabilities: usesJsonb ? ["jsonb-functions"] : [],
    };
  }

  toSQL(): { sql: string; params: unknown[] } {
    const compiled = this.toCompiled();
    return { sql: compiled.sql, params: compiled.params as unknown[] };
  }

  async execute(): Promise<T[]> {
    const compiled = this.toCompiled();
    const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities)) as Array<Record<string, unknown>>;
    applyProjectionDecoders(rows, compiled.decoders);
    return rows as T[];
  }

  then<R1 = T[], R2 = never>(
    onfulfilled?: ((value: T[]) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

export class InsertBuilder<TCols extends Record<string, AnyColumnBuilder>, R = number> implements PromiseLike<R> {
  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
    private readonly rows: Array<Record<string, unknown>> = [],
    private readonly hasValues = false,
    private readonly wantsReturning = false,
  ) {
    Object.freeze(this.rows);
    Object.freeze(this);
  }

  values(values: InferInsertModelOfRecord<TCols> | Array<InferInsertModelOfRecord<TCols>>): InsertBuilder<TCols, R> {
    const rows = Array.isArray(values) ? (values as unknown as Array<Record<string, unknown>>) : [values as unknown as Record<string, unknown>];
    return new InsertBuilder<TCols, R>(this.ctx, this.table, rows, true, this.wantsReturning);
  }

  returning(): InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    return new InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>(
      this.ctx,
      this.table,
      this.rows,
      this.hasValues,
      true,
    ) as unknown as InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
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
    // the values section binds exactly what was validated.
    const supplied = new Set<string>();
    const encodedRows: Array<Map<string, EncodedValue>> = [];
    for (let rowIdx = 0; rowIdx < this.rows.length; rowIdx++) {
      const row = this.rows[rowIdx];
      if (typeof row !== "object" || row === null || Array.isArray(row)) {
        throw new Error(`insert .values() rows must be objects on ${tableName}`);
      }
      const encoded = new Map<string, EncodedValue>();
      for (const key of Object.keys(row)) {
        if (!knownKeys.has(key)) throw new Error(`unknown column "${key}" on ${tableName}`);
        const column = columns[key];
        const value = row[key];
        if (value === undefined) continue;
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

    const returningPlan = this.wantsReturning
      ? selectPlanFor(tableName, columnEntries(this.table))
      : { nodes: [] as ProjectionNode[], decoders: [] as ProjectionDecoder[], capabilities: [] as StatementCapability[] };

    const stmt: AnyStatementNode = (() => {
      if (orderedKeys.length === 0) {
        // Every row is default-only. Postgres has no multi-row DEFAULT VALUES
        // form, so batch by explicitly requesting DEFAULT for one column.
        if (this.rows.length === 1) {
          return insertStatement({ table: ident(tableName), defaultValues: true, returning: returningPlan.nodes });
        }
        return insertStatement({
          table: ident(tableName),
          columns: [columns[propertyOrder[0]].columnName],
          rows: this.rows.map(() => [defaultCell()] as ReadonlyArray<InsertCell>),
          returning: returningPlan.nodes,
        });
      }
      return insertStatement({
        table: ident(tableName),
        columns: orderedKeys.map((k) => columns[k].columnName),
        rows: this.rows.map((row, rowIdx) => {
          const encoded = encodedRows[rowIdx];
          return orderedKeys.map<InsertCell>((k) => {
            if (!Object.hasOwn(row, k) || row[k] === undefined) return defaultCell();
            if (row[k] === null) return paramNode(null);
            return cellNode(encoded.get(k)!);
          });
        }),
        returning: returningPlan.nodes,
      });
    })();

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

  async execute(): Promise<R> {
    const compiled = this.toCompiled();
    if (this.wantsReturning) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities)) as R;
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
    private readonly wantsReturning = false,
  ) {
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
      const physical = column.columnName;
      // Null is checked BEFORE fragment detection: null is a bindable value
      // for nullable columns, never an object to interrogate.
      if (value === null) {
        if (effectiveNotNull(column)) {
          throw new Error(`update on ${tableName}: null is not allowed for NOT NULL column "${key}" ("${physical}")`);
        }
        added.push({ column: physical, value: paramNode(null) });
        continue;
      }
      const isJson = column.dataType === "json" || column.dataType === "jsonb";
      // AST fragments (sql`...`) stay supported assignments on every column
      // type except json/jsonb, where plain-object values always bind as
      // values.
      if (!isJson && isValueNode(value)) {
        added.push({ column: physical, value });
        continue;
      }
      rejectLegacyFragment(value, "update set");
      added.push({ column: physical, value: cellNode(encodeForColumn(tableName, column, key, value)) });
    }
    return new UpdateBuilder<TCols, R>(this.ctx, this.table, [...this.sets, ...added], true, this.conditions, this.wantsReturning);
  }

  where(condition: Condition): UpdateBuilder<TCols, R> {
    rejectLegacyFragment(condition, "where");
    return new UpdateBuilder<TCols, R>(this.ctx, this.table, this.sets, this.hasSet, [...this.conditions, condition], this.wantsReturning);
  }

  returning(): UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    return new UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>(
      this.ctx,
      this.table,
      this.sets,
      this.hasSet,
      this.conditions,
      true,
    ) as unknown as UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  }

  toCompiled(): CompiledStatement {
    if (!this.hasSet) throw new Error("update requires .set()");
    if (this.sets.length === 0) throw new Error("update .set() had no assignments — undefined values are ignored");
    if (this.conditions.length === 0) throw new Error("update without .where() is not allowed");
    const tableName = getTableName(this.table);
    const returningPlan = this.wantsReturning
      ? selectPlanFor(tableName, columnEntries(this.table))
      : { nodes: [] as ProjectionNode[], decoders: [] as ProjectionDecoder[], capabilities: [] as StatementCapability[] };
    const stmt = updateStatement({
      table: ident(tableName),
      sets: this.sets,
      where: whereItems(this.conditions),
      returning: returningPlan.nodes,
    });
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

  async execute(): Promise<R> {
    const compiled = this.toCompiled();
    if (this.wantsReturning) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities)) as R;
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
    Object.freeze(this.conditions);
    Object.freeze(this);
  }

  where(condition: Condition): DeleteBuilder<TCols, R> {
    rejectLegacyFragment(condition, "where");
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
      ? selectPlanFor(tableName, columnEntries(this.table))
      : { nodes: [] as ProjectionNode[], decoders: [] as ProjectionDecoder[], capabilities: [] as StatementCapability[] };
    const stmt = deleteStatement({
      table: ident(tableName),
      where: whereItems(this.conditions),
      returning: returningPlan.nodes,
    });
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

  async execute(): Promise<R> {
    const compiled = this.toCompiled();
    if (this.wantsReturning) {
      const rows = (await run(this.ctx, compiled.sql, compiled.params as unknown[], "query", compiled.capabilities)) as Array<Record<string, unknown>>;
      applyProjectionDecoders(rows, compiled.decoders);
      return rows as R;
    }
    return (await run(this.ctx, compiled.sql, compiled.params as unknown[], "execute", compiled.capabilities)) as R;
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

/** Join/from targets: a schema table, a CTE name, or a subquery node. */
export type AstJoinTarget = AnyPgTable | SubqueryNode | IdentifierNode;

interface AstJoinSpec {
  readonly type: JoinType;
  readonly table: AnyPgTable | SubqueryNode | IdentifierNode;
  readonly alias: string | undefined;
  readonly on: ValueNode | undefined;
}

interface AstFromSpec {
  readonly table: AnyPgTable | SubqueryNode | IdentifierNode;
  readonly alias: string | undefined;
}

export class AstSelectBuilder {
  private readonly projectionSpec: AstProjection | null;

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
    Object.freeze(this.fromSpec);
    for (const spec of this.joinSpecs) Object.freeze(spec);
    Object.freeze(this.joinSpecs);
    Object.freeze(this.wheres);
    for (const spec of this.orders) Object.freeze(spec);
    Object.freeze(this.orders);
    Object.freeze(this.cteSpecs);
    Object.freeze(this);
  }

  /** Non-cross joins need a condition; cross joins take none. */
  join(type: "cross", table: AstJoinTarget, alias?: undefined, on?: undefined): AstSelectBuilder;
  join(type: Exclude<JoinType, "cross">, table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder;
  join(type: JoinType, table: AstJoinTarget, alias?: string, on?: ValueNode): AstSelectBuilder {
    if (type !== "cross" && on === undefined) throw new Error(`ast join: ${type} joins require an on condition`);
    if (alias !== undefined) validAlias(alias, "ast join alias");
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      [...this.joinSpecs, { type, table, alias, on }],
      this.wheres,
      this.orders,
      this.cteSpecs,
      this.limitCount,
      this.offsetCount,
    );
  }

  innerJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("inner", table, alias, on);
  }

  leftJoin(table: AstJoinTarget, alias: string | undefined, on: ValueNode): AstSelectBuilder {
    return this.join("left", table, alias, on);
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
    );
  }

  withCte(name: string, sub: SubqueryNode): AstSelectBuilder {
    return new AstSelectBuilder(
      this.fromSpec,
      this.projectionSpec,
      this.joinSpecs,
      this.wheres,
      this.orders,
      [...this.cteSpecs, cte(name, sub.select)],
      this.limitCount,
      this.offsetCount,
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
    );
  }

  /** The statement as a frozen AST — composition point for subqueries. */
  toAST(): StatementNode {
    const from = this.fromSpec.table;
    const fromTarget = isPgTable(from) ? ident(getTableName(from)) : from;
    return selectStatement({
      ctes: this.cteSpecs,
      projections: this.buildProjections(),
      from: fromTarget,
      fromAlias: this.fromSpec.alias,
      joins: this.joinSpecs.map((j) => {
        const target = isPgTable(j.table) ? ident(getTableName(j.table)) : j.table;
        return joinNode(j.type, target, { alias: j.alias, on: j.on });
      }),
      where: this.wheres,
      orderBy: this.orders,
      limit: this.limitCount,
      offset: this.offsetCount,
    });
  }

  /** Wrap this builder's statement for interpolation into a parent query. */
  subquery(): SubqueryNode {
    return subqueryNode(this.toAST());
  }

  /** Pure compile: same builder state → byte-identical SQL + params. */
  toSQL(): CompiledQuery {
    return compileStatement(this.toAST());
  }

  private buildProjections(): ProjectionNode[] {
    if (this.projectionSpec === null) {
      const from = this.fromSpec.table;
      if (!isPgTable(from)) {
        throw new Error("astSelect: a projection is required when selecting from a subquery or CTE reference");
      }
      // Default projection: every column, labeled with its property key
      // whenever the property key differs from the physical name.
      return columnEntries(from).map(({ propertyKey, column }) =>
        projectionNode(qual(getTableName(from), column.columnName), propertyKey === column.columnName ? undefined : propertyKey),
      );
    }
    return Object.entries(this.projectionSpec).map(([key, value]) => {
      if (isPgColumnRef(value)) {
        const owner = value.ownerTable ? getTableName(value.ownerTable) : undefined;
        if (!owner) throw new Error(`astSelect: projected column "${key}" has no owner table`);
        return projectionNode(qual(owner, value.columnName), key === value.columnName ? undefined : key);
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
      return new AstSelectBuilder({ table, alias }, projection, [], [], [], [], undefined, undefined);
    },
  };
}
