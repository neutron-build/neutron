// ---------------------------------------------------------------------------
// @neutron-build/sql — query builders (select / insert / update / delete)
// ---------------------------------------------------------------------------
// Every builder is a PromiseLike: awaiting executes. `.toSQL()` shows the
// exact statement without running it. One visible SQL statement per call.

import type { Driver } from "./drivers.js";
import type { Logger } from "./logger.js";
import { type Condition, type OrderExpression, type SqlFragment, qident, qualify } from "./expr.js";
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
  cte,
  ident,
  join as joinNode,
  projection as projectionNode,
  qual,
  selectStatement,
  subquery as subqueryNode,
  type CteNode,
  type IdentifierNode,
  type JoinType,
  type OrderSpec,
  type ProjectionNode,
  type StatementNode,
  type SubqueryNode,
  type ValueNode,
} from "./ast.js";
import { compileStatement, type CompiledQuery } from "./compile.js";

export interface ExecContext {
  driver: Driver;
  logger: Logger | null;
}

export async function run(ctx: ExecContext, sqlText: string, params: unknown[], kind: "query" | "execute"): Promise<unknown> {
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

/** Splice a fragment's params into the shared list, renumbering its $N refs. */
function inline(fragment: SqlFragment, params: unknown[]): string {
  let n = 0;
  return fragment.sql.replace(/\$(\d+)/g, () => {
    params.push(fragment.params[n]);
    n++;
    return `$${params.length}`;
  });
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

/** Physical column reference labeled with its property key so driver rows come
 *  back keyed by declared JS property names. Identical names skip the alias. */
function aliasedColumn(table: string, column: AnyColumnBuilder, propertyKey: string): string {
  const ref = qualify(table, column.columnName);
  return propertyKey === column.columnName ? ref : `${ref} as ${qident(propertyKey)}`;
}

function returningList(table: AnyPgTable): string {
  return columnEntries(table)
    .map(({ propertyKey, column }) =>
      propertyKey === column.columnName ? qident(column.columnName) : `${qident(column.columnName)} as ${qident(propertyKey)}`,
    )
    .join(", ");
}

// ---------------------------------------------------------------------------
// Mutation value validation (before execution, with column context)
// ---------------------------------------------------------------------------

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** json/jsonb values must survive JSON serialization unchanged. */
function jsonRepresentable(value: unknown): boolean {
  if (value === null) return true;
  const t = typeof value;
  if (t === "string" || t === "boolean") return true;
  if (t === "number") return Number.isFinite(value);
  if (t === "object") {
    if (Array.isArray(value)) return value.every(jsonRepresentable);
    if (isPlainObject(value)) return Object.values(value).every(jsonRepresentable);
  }
  return false;
}

/** Duck-typed SqlFragment: own `sql` string + own `params` array. */
function isFragmentLike(value: object): value is SqlFragment {
  const v = value as { sql?: unknown; params?: unknown };
  return Object.hasOwn(value, "sql") && typeof v.sql === "string" && Object.hasOwn(value, "params") && Array.isArray(v.params);
}

/** Null if the value is a bindable shape for this column; otherwise the reason. */
function valueBindingError(column: AnyColumnBuilder, value: unknown): string | null {
  if (value === null) return null; // nullability is checked separately
  const t = typeof value;
  const isJson = column.dataType === "json" || column.dataType === "jsonb";
  if (t === "string" || t === "boolean" || t === "undefined") return null;
  if (t === "number") return isJson && !Number.isFinite(value) ? "non-finite numbers are not JSON-representable" : null;
  if (t === "bigint") return isJson ? "bigint is not JSON-representable" : null;
  if (t === "symbol" || t === "function") return `${t} values cannot be bound`;
  if (Array.isArray(value)) {
    if (column.dataType === "vector") {
      return value.every((e) => typeof e === "number" && Number.isFinite(e)) ? null : "vector columns accept arrays of finite numbers";
    }
    if (isJson) return value.every(jsonRepresentable) ? null : "array values for json/jsonb columns must contain only JSON-representable values";
    return `array values are not bindable for ${column.dataType} columns`;
  }
  if (value instanceof Date) {
    return column.dataType === "timestamp" || column.dataType === "timestamptz" || column.dataType === "date"
      ? null
      : `Date values are not bindable for ${column.dataType} columns`;
  }
  if (value instanceof Uint8Array) {
    return column.dataType === "bytea" ? null : "Uint8Array values are only bindable for bytea columns";
  }
  if (isJson) {
    return isPlainObject(value) && Object.values(value).every(jsonRepresentable)
      ? null
      : "values for json/jsonb columns must be JSON-representable";
  }
  if (isPlainObject(value)) {
    return "nested object values are not bindable (only json/jsonb columns accept objects; use a sql`…` fragment in .set() for expressions)";
  }
  return `values of this shape are not bindable for ${column.dataType} columns`;
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

export type Projection = Record<string, AnyColumnBuilder | SqlFragment>;

export type ProjectionResult<P extends Projection> = {
  [K in keyof P]: P[K] extends AnyColumnBuilder ? SelectTypeOf<P[K]> : unknown;
};

export class SelectBuilder<T> implements PromiseLike<T[]> {
  private conditions: Condition[] = [];
  private order: OrderExpression[] = [];
  private limitCount?: number;
  private offsetCount?: number;

  constructor(
    private readonly ctx: ExecContext,
    private readonly table: AnyPgTable,
    private readonly projection: Projection | null,
  ) {}

  where(condition: Condition): this {
    this.conditions.push(condition);
    return this;
  }

  orderBy(...exprs: OrderExpression[]): this {
    this.order.push(...exprs);
    return this;
  }

  limit(n: number): this {
    this.limitCount = n;
    return this;
  }

  offset(n: number): this {
    this.offsetCount = n;
    return this;
  }

  toSQL(): { sql: string; params: unknown[] } {
    const params: unknown[] = [];

    let selectList: string;
    if (this.projection) {
      const parts: string[] = [];
      for (const [key, value] of Object.entries(this.projection)) {
        if ("columnName" in value) {
          // Output label = the projection key; physical ref from metadata.
          const owner = value.ownerTable ? getTableName(value.ownerTable) : getTableName(this.table);
          const ref = qualify(owner, value.columnName);
          parts.push(key === value.columnName ? ref : `${ref} as ${qident(String(key))}`);
        } else {
          parts.push(`${inline(value, params)} as ${qident(String(key))}`);
        }
      }
      selectList = parts.join(", ");
    } else {
      selectList = columnEntries(this.table)
        .map(({ propertyKey, column }) => aliasedColumn(getTableName(this.table), column, propertyKey))
        .join(", ");
    }

    let sqlText = `select ${selectList} from ${qident(getTableName(this.table))}`;

    if (this.conditions.length > 0) {
      sqlText += ` where ${this.conditions.map((c) => inline(c, params)).join(" and ")}`;
    }
    if (this.order.length > 0) {
      sqlText += ` order by ${this.order.map((o) => o.sql).join(", ")}`;
    }
    if (this.limitCount !== undefined) sqlText += ` limit ${this.limitCount}`;
    if (this.offsetCount !== undefined) sqlText += ` offset ${this.offsetCount}`;

    return { sql: sqlText, params };
  }

  async execute(): Promise<T[]> {
    const { sql: sqlText, params } = this.toSQL();
    return (await run(this.ctx, sqlText, params, "query")) as T[];
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
  private rows: Array<Record<string, unknown>> = [];
  private hasValues = false;
  private wantsReturning = false;

  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
  ) {}

  values(values: InferInsertModelOfRecord<TCols> | Array<InferInsertModelOfRecord<TCols>>): this {
    this.rows = Array.isArray(values) ? (values as unknown as Array<Record<string, unknown>>) : [values as unknown as Record<string, unknown>];
    this.hasValues = true;
    return this;
  }

  returning(): InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    this.wantsReturning = true;
    return this as unknown as InsertBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  }

  toSQL(): { sql: string; params: unknown[] } {
    if (!this.hasValues) throw new Error("insert requires .values()");
    if (this.rows.length === 0) throw new Error("insert .values() received an empty array");

    const columns = getTableColumns(this.table) as Record<string, AnyColumnBuilder>;
    const propertyOrder = Object.keys(columns);
    if (propertyOrder.length === 0) throw new Error(`table ${getTableName(this.table)} has no columns`);
    const knownKeys = new Set(propertyOrder);
    const required = requiredInsertKeys(this.table);

    // Validate every row and collect the union of supplied keys. A key is
    // supplied when it is present with a value other than undefined in at
    // least one row; explicit null is a supplied value (NULL, never DEFAULT).
    const supplied = new Set<string>();
    for (let rowIdx = 0; rowIdx < this.rows.length; rowIdx++) {
      const row = this.rows[rowIdx];
      if (typeof row !== "object" || row === null || Array.isArray(row)) {
        throw new Error(`insert .values() rows must be objects on ${getTableName(this.table)}`);
      }
      for (const key of Object.keys(row)) {
        if (!knownKeys.has(key)) throw new Error(`unknown column "${key}" on ${getTableName(this.table)}`);
        const column = columns[key];
        const value = row[key];
        if (value === undefined) continue;
        if (effectiveNotNull(column) && value === null) {
          throw new Error(
            `insert on ${getTableName(this.table)}: null is not allowed for NOT NULL column "${key}" ("${column.columnName}")`,
          );
        }
        const reason = valueBindingError(column, value);
        if (reason) {
          throw new Error(`invalid value for column "${key}" ("${column.columnName}") on ${getTableName(this.table)}: ${reason}`);
        }
        supplied.add(key);
      }
      const missing = required.filter(({ propertyKey }) => !Object.hasOwn(row, propertyKey) || row[propertyKey] === undefined);
      if (missing.length > 0) {
        throw new Error(
          `insert on ${getTableName(this.table)} row ${rowIdx} is missing required column(s) ` +
            missing.map(({ propertyKey, column }) => `"${propertyKey}" ("${column.columnName}")`).join(", ") +
            " — NOT NULL without a default",
        );
      }
    }

    // One stable schema-ordered column list shared by every row. Cell reads
    // use own properties only: an inherited value is not a supplied value.
    const orderedKeys = propertyOrder.filter((k) => supplied.has(k));
    const params: unknown[] = [];

    let columnsSql: string;
    let valuesSql: string;
    if (orderedKeys.length === 0) {
      // Every row is default-only. Postgres has no multi-row DEFAULT VALUES
      // form, so batch by explicitly requesting DEFAULT for one column.
      if (this.rows.length === 1) {
        columnsSql = "";
        valuesSql = "default values";
      } else {
        const fallback = columns[propertyOrder[0]].columnName;
        columnsSql = ` (${qident(fallback)})`;
        valuesSql = `values ${this.rows.map(() => "(default)").join(", ")}`;
      }
    } else {
      columnsSql = ` (${orderedKeys.map((k) => qident(columns[k].columnName)).join(", ")})`;
      valuesSql = `values ${this.rows
        .map((row) => {
          const cells = orderedKeys.map((k) => {
            const value = Object.hasOwn(row, k) ? row[k] : undefined;
            if (value === undefined) return "default";
            params.push(value);
            return `$${params.length}`;
          });
          return `(${cells.join(", ")})`;
        })
        .join(", ")}`;
    }

    let sqlText = `insert into ${qident(getTableName(this.table))}${columnsSql} ${valuesSql}`;
    if (this.wantsReturning) sqlText += ` returning ${returningList(this.table)}`;
    return { sql: sqlText, params };
  }

  async execute(): Promise<R> {
    const { sql: sqlText, params } = this.toSQL();
    if (this.wantsReturning) {
      return (await run(this.ctx, sqlText, params, "query")) as R;
    }
    return (await run(this.ctx, sqlText, params, "execute")) as R;
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

export class UpdateBuilder<TCols extends Record<string, AnyColumnBuilder>, R = number> implements PromiseLike<R> {
  private conditions: Condition[] = [];
  private sets: Array<{ col: string; value?: unknown; frag?: SqlFragment }> = [];
  private hasSet = false;
  private wantsReturning = false;

  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
  ) {}

  set(values: UpdateSetInput<TCols>): this {
    const columns = getTableColumns(this.table) as Record<string, AnyColumnBuilder>;
    for (const [key, value] of Object.entries(values)) {
      const column = columns[key];
      if (!column) throw new Error(`unknown column "${key}" on ${getTableName(this.table)}`);
      this.hasSet = true;
      if (value === undefined) continue; // omitted/undefined update keys are ignored
      const physical = column.columnName;
      // Null is checked BEFORE fragment detection: null is a bindable value
      // for nullable columns, never an object to interrogate.
      if (value === null) {
        if (effectiveNotNull(column)) {
          throw new Error(`update on ${getTableName(this.table)}: null is not allowed for NOT NULL column "${key}" ("${physical}")`);
        }
        this.sets.push({ col: physical, value });
        continue;
      }
      const isJson = column.dataType === "json" || column.dataType === "jsonb";
      // sql fragments stay supported assignments on every column type except
      // json/jsonb, where plain-object values always bind as values.
      if (!isJson && typeof value === "object" && isFragmentLike(value)) {
        this.sets.push({ col: physical, frag: value });
        continue;
      }
      const reason = valueBindingError(column, value);
      if (reason) throw new Error(`invalid value for column "${key}" ("${physical}") on ${getTableName(this.table)}: ${reason}`);
      this.sets.push({ col: physical, value });
    }
    return this;
  }

  where(condition: Condition): this {
    this.conditions.push(condition);
    return this;
  }

  returning(): UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    this.wantsReturning = true;
    return this as unknown as UpdateBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  }

  toSQL(): { sql: string; params: unknown[] } {
    if (!this.hasSet) throw new Error("update requires .set()");
    if (this.sets.length === 0) throw new Error("update .set() had no assignments — undefined values are ignored");
    if (this.conditions.length === 0) throw new Error("update without .where() is not allowed");
    const params: unknown[] = [];
    const assignments = this.sets.map((s) => {
      if (s.frag) return `${qident(s.col)} = ${inline(s.frag, params)}`;
      params.push(s.value);
      return `${qident(s.col)} = $${params.length}`;
    });
    let sqlText = `update ${qident(getTableName(this.table))} set ${assignments.join(", ")}`;
    sqlText += ` where ${this.conditions.map((c) => inline(c, params)).join(" and ")}`;
    if (this.wantsReturning) sqlText += ` returning ${returningList(this.table)}`;
    return { sql: sqlText, params };
  }

  async execute(): Promise<R> {
    const { sql: sqlText, params } = this.toSQL();
    if (this.wantsReturning) {
      return (await run(this.ctx, sqlText, params, "query")) as R;
    }
    return (await run(this.ctx, sqlText, params, "execute")) as R;
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
  private conditions: Condition[] = [];
  private wantsReturning = false;

  constructor(
    private readonly ctx: ExecContext,
    private readonly table: PgTable<TCols>,
  ) {}

  where(condition: Condition): this {
    this.conditions.push(condition);
    return this;
  }

  returning(): DeleteBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>> {
    this.wantsReturning = true;
    return this as unknown as DeleteBuilder<TCols, Array<InferSelectModelOfRecord<TCols>>>;
  }

  toSQL(): { sql: string; params: unknown[] } {
    if (this.conditions.length === 0) throw new Error("delete without .where() is not allowed");
    const params: unknown[] = [];
    let sqlText = `delete from ${qident(getTableName(this.table))}`;
    sqlText += ` where ${this.conditions.map((c) => inline(c, params)).join(" and ")}`;
    if (this.wantsReturning) sqlText += ` returning ${returningList(this.table)}`;
    return { sql: sqlText, params };
  }

  async execute(): Promise<R> {
    const { sql: sqlText, params } = this.toSQL();
    if (this.wantsReturning) {
      return (await run(this.ctx, sqlText, params, "query")) as R;
    }
    return (await run(this.ctx, sqlText, params, "execute")) as R;
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
  [K in keyof TCols]?: UpdateTypeOf<TCols[K]> | SqlFragment;
};

// ---------------------------------------------------------------------------
// AST select builder (F01 spike — the minimal builder integration)
// ---------------------------------------------------------------------------
// One representative path proves ast + compile end to end: mapped columns,
// bound where values, aliased joins and composable subqueries. This builder
// is IMMUTABLE: every fluent call returns a new frozen instance sharing no
// mutable state, so forking and reuse cannot leak filters between requests.
// Execution integration for all CRUD paths is F04; here `.toSQL()` is pure.

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
  /** Internal — construct through `astSelect()`. Kept public only so the
   *  entry-point factory can build instances; the shape is not API. */
  constructor(
    private readonly fromSpec: AstFromSpec,
    private readonly projectionSpec: AstProjection | null,
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
    // frozen here element-wise. projectionSpec is caller-owned input and is
    // deliberately not frozen (freezing it would mutate the caller's object).
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
    from: (table: AstJoinTarget, alias?: string) =>
      new AstSelectBuilder({ table, alias }, projection, [], [], [], [], undefined, undefined),
  };
}
