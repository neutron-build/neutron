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
  decodeNativeValue,
  decodeTextWire,
  encodeWriteValue,
  needsFlatDecode,
  wireReadExpr,
  type ColumnContext,
  type EncodedValue,
} from "./codecs.js";
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
 *  back keyed by declared JS property names. Identical names skip the alias.
 *  Columns whose driver-native value is lossy (temporals) project a lossless
 *  text expression instead — the expression is always labeled. */
function aliasedColumn(table: string, column: AnyColumnBuilder, propertyKey: string): string {
  const ref = qualify(table, column.columnName);
  const wire = wireReadExpr(column.dataType, ref);
  if (wire) return `${wire} as ${qident(propertyKey)}`;
  return propertyKey === column.columnName ? ref : `${ref} as ${qident(propertyKey)}`;
}

function returningList(table: AnyPgTable): string {
  return columnEntries(table)
    .map(({ propertyKey, column }) => {
      const wire = wireReadExpr(column.dataType, qident(column.columnName));
      if (wire) return `${wire} as ${qident(propertyKey)}`;
      return propertyKey === column.columnName
        ? qident(column.columnName)
        : `${qident(column.columnName)} as ${qident(propertyKey)}`;
    })
    .join(", ");
}

// ---------------------------------------------------------------------------
// Row decode plans (flat select / RETURNING paths)
// ---------------------------------------------------------------------------

interface RowDecode {
  key: string;
  column: AnyColumnBuilder;
  wire: "text" | "native";
}

function rowDecodePlan<E extends { key: string; column: AnyColumnBuilder | null }>(entries: E[]): RowDecode[] {
  const plan: RowDecode[] = [];
  for (const { key, column } of entries) {
    if (!column) continue;
    if (wireReadExpr(column.dataType, "x") !== null) {
      plan.push({ key, column, wire: "text" });
    } else if (needsFlatDecode(column)) {
      plan.push({ key, column, wire: "native" });
    }
  }
  return plan;
}

function columnContext(table: string, column: AnyColumnBuilder, propertyKey: string): ColumnContext {
  return { propertyKey, columnName: column.columnName, tableName: table };
}

function applyRowDecode(rows: Array<Record<string, unknown>>, plan: RowDecode[], table: string): void {
  if (plan.length === 0) return;
  for (const row of rows) {
    for (const entry of plan) {
      if (row[entry.key] === null || row[entry.key] === undefined) continue;
      const ctx = columnContext(table, entry.column, entry.key);
      row[entry.key] =
        entry.wire === "text"
          ? decodeTextWire(entry.column, ctx, row[entry.key])
          : decodeNativeValue(entry.column, ctx, row[entry.key]);
    }
  }
}

// ---------------------------------------------------------------------------
// Mutation value encoding (before execution, with column context)
// ---------------------------------------------------------------------------

/** Duck-typed SqlFragment: own `sql` string + own `params` array. */
function isFragmentLike(value: object): value is SqlFragment {
  const v = value as { sql?: unknown; params?: unknown };
  return Object.hasOwn(value, "sql") && typeof v.sql === "string" && Object.hasOwn(value, "params") && Array.isArray(v.params);
}

function encodeForColumn(table: string, column: AnyColumnBuilder, propertyKey: string, value: unknown): EncodedValue {
  return encodeWriteValue(column, columnContext(table, column, propertyKey), value);
}

/** Bind site for an encoded value: temporal and json/jsonb values bind as
 *  canonical text at explicitly text-typed sites so both drivers pass the
 *  string through untouched (postgres.js otherwise re-encodes server-typed
 *  date/json params through Date/JSON.stringify, losing microseconds and
 *  double-encoding strings). */
function bindSite(n: number, encoded: EncodedValue): string {
  return encoded.cast === undefined ? `$${n}` : `$${n}::text::${encoded.cast}`;
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

  private projectionEntries(): Array<{ key: string; column: AnyColumnBuilder | null }> {
    if (!this.projection) {
      return columnEntries(this.table).map(({ propertyKey, column }) => ({ key: propertyKey, column }));
    }
    return Object.entries(this.projection).map(([key, value]) => ({
      key,
      column: "columnName" in value ? (value as AnyColumnBuilder) : null,
    }));
  }

  toSQL(): { sql: string; params: unknown[] } {
    const params: unknown[] = [];

    let selectList: string;
    if (this.projection) {
      const parts: string[] = [];
      for (const [key, value] of Object.entries(this.projection)) {
        if ("columnName" in value) {
          // Output label = the projection key; physical ref from metadata.
          // Lossy-native columns project a lossless text expression (always
          // labeled — the expression itself has no usable output name).
          const owner = value.ownerTable ? getTableName(value.ownerTable) : getTableName(this.table);
          const ref = qualify(owner, value.columnName);
          const wire = wireReadExpr(value.dataType, ref);
          parts.push(wire ? `${wire} as ${qident(String(key))}` : key === value.columnName ? ref : `${ref} as ${qident(String(key))}`);
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
    const rows = (await run(this.ctx, sqlText, params, "query")) as Array<Record<string, unknown>>;
    applyRowDecode(rows, rowDecodePlan(this.projectionEntries()), getTableName(this.table));
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
    // Non-null values are codec-encoded now (validation + canonical text) so
    // the values section binds exactly what was validated.
    const supplied = new Set<string>();
    const encodedRows: Array<Map<string, EncodedValue>> = [];
    for (let rowIdx = 0; rowIdx < this.rows.length; rowIdx++) {
      const row = this.rows[rowIdx];
      if (typeof row !== "object" || row === null || Array.isArray(row)) {
        throw new Error(`insert .values() rows must be objects on ${getTableName(this.table)}`);
      }
      const encoded = new Map<string, EncodedValue>();
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
        if (value !== null) {
          encoded.set(key, encodeForColumn(getTableName(this.table), column, key, value));
        }
        supplied.add(key);
      }
      encodedRows.push(encoded);
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
        .map((row, rowIdx) => {
          const encoded = encodedRows[rowIdx];
          const cells = orderedKeys.map((k) => {
            if (!Object.hasOwn(row, k) || row[k] === undefined) return "default";
            if (row[k] === null) {
              params.push(null);
              return `$${params.length}`;
            }
            const enc = encoded.get(k)!;
            params.push(enc.bind);
            return bindSite(params.length, enc);
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
      const rows = (await run(this.ctx, sqlText, params, "query")) as Array<Record<string, unknown>>;
      applyRowDecode(
        rows,
        rowDecodePlan(columnEntries(this.table).map(({ propertyKey, column }) => ({ key: propertyKey, column }))),
        getTableName(this.table),
      );
      return rows as R;
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
  private sets: Array<{ col: string; value?: unknown; frag?: SqlFragment; cast?: string }> = [];
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
      const encoded = encodeForColumn(getTableName(this.table), column, key, value);
      this.sets.push({ col: physical, value: encoded.bind, cast: encoded.cast });
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
      return `${qident(s.col)} = ${s.cast === undefined ? `$${params.length}` : `$${params.length}::text::${s.cast}`}`;
    });
    let sqlText = `update ${qident(getTableName(this.table))} set ${assignments.join(", ")}`;
    sqlText += ` where ${this.conditions.map((c) => inline(c, params)).join(" and ")}`;
    if (this.wantsReturning) sqlText += ` returning ${returningList(this.table)}`;
    return { sql: sqlText, params };
  }

  async execute(): Promise<R> {
    const { sql: sqlText, params } = this.toSQL();
    if (this.wantsReturning) {
      const rows = (await run(this.ctx, sqlText, params, "query")) as Array<Record<string, unknown>>;
      applyRowDecode(
        rows,
        rowDecodePlan(columnEntries(this.table).map(({ propertyKey, column }) => ({ key: propertyKey, column }))),
        getTableName(this.table),
      );
      return rows as R;
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
      const rows = (await run(this.ctx, sqlText, params, "query")) as Array<Record<string, unknown>>;
      applyRowDecode(
        rows,
        rowDecodePlan(columnEntries(this.table).map(({ propertyKey, column }) => ({ key: propertyKey, column }))),
        getTableName(this.table),
      );
      return rows as R;
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
