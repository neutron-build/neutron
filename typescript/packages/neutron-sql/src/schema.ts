// ---------------------------------------------------------------------------
// @neutron-build/sql — schema definition (Drizzle-shaped, no codegen)
// ---------------------------------------------------------------------------

import type { BigintMode, BigintOptions, TemporalMode, TemporalOptions, NumericOptions } from "./codecs.js";

export type ColumnDataType =
  | "serial"
  | "integer"
  | "smallint"
  | "bigint"
  | "double"
  | "real"
  | "numeric"
  | "text"
  | "varchar"
  | "boolean"
  | "timestamp"
  | "timestamptz"
  | "date"
  | "json"
  | "jsonb"
  | "uuid"
  | "bytea"
  | "vector";

/** Default read type per SQL type (the master codec table). int8 never passes
 *  through JS Number (bigint, with optional string / checked safe-number
 *  modes); numerics are exact decimal strings; temporals are canonical
 *  strings (microsecond-preserving; explicit Date mode truncates to
 *  milliseconds); dates carry no timezone interpretation. */
export type JsTypeOf<D extends ColumnDataType> =
  D extends "serial" | "integer" | "smallint" | "double" | "real"
    ? number
    : D extends "bigint"
      ? bigint
      : D extends "numeric" | "text" | "varchar" | "uuid" | "timestamp" | "timestamptz" | "date"
        ? string
        : D extends "boolean"
          ? boolean
          : D extends "bytea"
            ? Uint8Array
            : D extends "vector"
              ? number[]
              : unknown;

/** Write type: values the codec layer accepts for D. Temporal columns take
 *  canonical strings (microsecond-exact) or Dates (millisecond precision;
 *  timestamps store the UTC wall clock); dates take "YYYY-MM-DD" strings
 *  only. */
export type JsWriteTypeOf<D extends ColumnDataType> =
  D extends "bigint"
    ? string | number | bigint
    : D extends "numeric"
      ? string | number
      : D extends "serial" | "integer" | "smallint" | "double" | "real"
        ? number
        : D extends "text" | "varchar" | "uuid"
          ? string
          : D extends "boolean"
            ? boolean
            : D extends "timestamp" | "timestamptz"
              ? string | Date
              : D extends "date"
                ? string
                : D extends "bytea"
                  ? Uint8Array
                  : D extends "vector"
                    ? number[]
                    : unknown;

export type BigintRead<M extends BigintMode> = M extends "string" ? string : M extends "number" ? number : bigint;
export type TemporalRead<M extends TemporalMode> = M extends "date" ? Date : string;

export interface ForeignKeyRef {
  (): ColumnBuilder<ColumnDataType, boolean, boolean>;
  onDelete?: "cascade" | "restrict" | "set null" | "no action";
}

export class ColumnBuilder<
  D extends ColumnDataType = ColumnDataType,
  NN extends boolean = false,
  HD extends boolean = false,
  RT = JsTypeOf<D>,
> {
  declare readonly _: { dataType: D; notNull: NN; hasDefault: HD; readType: RT };

  readonly columnName: string;
  readonly dataType: D;
  /** Codec read mode (bigint: bigint|string|number; timestamp/timestamptz:
   *  string|date). Set by the column factory only. */
  readMode?: BigintMode | TemporalMode;
  /** Optional user decoder for numeric columns (exact decimal string in). */
  valueDecoder?: (raw: string) => unknown;
  isPrimaryKey = false;
  isNotNull: boolean = false;
  hasDefault: boolean = false;
  defaultValue: unknown = undefined;
  isUnique = false;
  /** Q02: the column lives in a derived table/CTE whose source already
   *  materialized the canonical text wire form (to_jsonb(...)::text) — the
   *  outer acquisition must cast back to the temporal type before
   *  re-rendering, or the JSON quotes would double. */
  canonicalText = false;
  varcharLength?: number;
  vectorDimensions?: number;
  nowDefault = false;
  foreignKey?: ForeignKeyRef;
  ownerTable?: AnyPgTable;

  constructor(columnName: string, dataType: D) {
    this.columnName = columnName;
    this.dataType = dataType;
  }

  notNull(): ColumnBuilder<D, true, HD, RT> {
    this.isNotNull = true;
    return this as unknown as ColumnBuilder<D, true, HD, RT>;
  }

  default(value: JsWriteTypeOf<D>): ColumnBuilder<D, NN, true, RT> {
    this.hasDefault = true;
    this.defaultValue = value;
    return this as unknown as ColumnBuilder<D, NN, true, RT>;
  }

  defaultNow(): ColumnBuilder<D, NN, true, RT> {
    this.hasDefault = true;
    this.nowDefault = true;
    return this as unknown as ColumnBuilder<D, NN, true, RT>;
  }

  /** Primary keys are implicitly NOT NULL (PostgreSQL semantics); the insert
   *  type reflects that. Serial primary keys still default server-side. */
  primaryKey(): ColumnBuilder<D, true, HD, RT> {
    this.isPrimaryKey = true;
    return this as unknown as ColumnBuilder<D, true, HD, RT>;
  }

  unique(): ColumnBuilder<D, NN, HD, RT> {
    this.isUnique = true;
    return this as unknown as ColumnBuilder<D, NN, HD, RT>;
  }

  references(
    ref: ForeignKeyRef,
    opts?: { onDelete?: ForeignKeyRef["onDelete"] },
  ): ColumnBuilder<D, NN, HD, RT> {
    const bound: ForeignKeyRef = Object.assign(() => ref(), { onDelete: opts?.onDelete });
    this.foreignKey = bound;
    return this as unknown as ColumnBuilder<D, NN, HD, RT>;
  }
}

export type AnyColumnBuilder = ColumnBuilder<ColumnDataType, boolean, boolean, unknown>;

export type SelectTypeOf<C> = C extends ColumnBuilder<infer _D, infer NN, infer _HD, infer RT>
  ? NN extends true
    ? RT
    : RT | null
  : never;

/** Default-mode read type of a column inside a relation child projection
 *  (JSON path) — int8/numeric render ::text, temporals render as canonical
 *  strings and bytea as \x hex text inside the aggregation, all decoded
 *  through the same codecs as the flat path. */
export type RelationLeafTypeOf<D extends ColumnDataType> =
  D extends "serial" | "integer" | "smallint" | "double" | "real"
    ? number
    : D extends "numeric" | "text" | "varchar" | "uuid" | "timestamp" | "timestamptz" | "date"
      ? string
      : D extends "bigint"
        ? bigint
        : D extends "boolean"
          ? boolean
          : D extends "bytea"
            ? Uint8Array
            : D extends "vector"
              ? number[]
              : unknown;

/** Relation child leaf read type: the column's declared read mode, exactly
 *  like the flat path (children decode through the same codecs). */
export type RelationSelectTypeOf<C> = C extends ColumnBuilder<infer _D, infer NN, infer _HD, infer RT>
  ? NN extends true
    ? RT
    : RT | null
  : never;

export type InsertTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer HD, any>
  ? NN extends true
    ? HD extends true
      ? JsWriteTypeOf<D> | undefined
      : D extends "serial"
        ? JsWriteTypeOf<D> | undefined
        : JsWriteTypeOf<D>
    : JsWriteTypeOf<D> | null | undefined
  : never;

export type UpdateTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer _HD, any>
  ? NN extends true
    ? JsWriteTypeOf<D>
    : JsWriteTypeOf<D> | null
  : never;

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------
// Authoritative table metadata (name, column map, indexes) lives in a
// symbol-keyed internal record, never in plain properties: user columns named
// `columns`, `tableName` or `indexes` are ordinary columns and cannot clobber
// it. Read metadata through the accessor helpers below.

export const TABLE_SYMBOL = Symbol.for("@neutron-build/sql.table");

export interface TableMetadata<Cols extends Record<string, AnyColumnBuilder> = Record<string, AnyColumnBuilder>> {
  readonly tableName: string;
  /** SQL schema the table lives in (undefined = the connection's default
   *  search path, effectively public). Declared through pgSchema(); the
   *  query layer (CRUD + alias joins) renders qualified references, while
   *  DDL emission, schema export and relational reads reject schema-declared
   *  tables until their scoped cards (Q05/Q07). */
  readonly schema?: string;
  readonly columns: Cols;
  readonly indexes: TableIndex[];
}

export interface PgTableCore<Cols extends Record<string, AnyColumnBuilder> = Record<string, AnyColumnBuilder>> {
  readonly [TABLE_SYMBOL]: TableMetadata<Cols>;
  readonly $inferSelect: InferSelectModelOf<Cols>;
  readonly $inferInsert: InferInsertModelOf<Cols>;
}

/** A table: symbol-keyed metadata plus its columns as top-level properties (users.email). */
export type PgTable<Cols extends Record<string, AnyColumnBuilder> = Record<string, AnyColumnBuilder>> = PgTableCore<Cols> & Cols;

function tableMetaOf(table: AnyPgTable, who: string): TableMetadata {
  if (typeof table !== "object" || table === null) {
    throw new Error(`${who}: not a neutron-sql table (missing table metadata record)`);
  }
  const meta = (table as { [TABLE_SYMBOL]?: unknown })[TABLE_SYMBOL];
  if (typeof meta !== "object" || meta === null || typeof (meta as TableMetadata).tableName !== "string") {
    throw new Error(`${who}: not a neutron-sql table (missing table metadata record)`);
  }
  return meta as TableMetadata;
}

/** Authoritative table name. Fails closed on anything that is not a table. */
export function getTableName(table: AnyPgTable): string {
  return tableMetaOf(table, "getTableName").tableName;
}

/** Authoritative SQL schema name, or undefined for the default search path. */
export function getTableSchema(table: AnyPgTable): string | undefined {
  return tableMetaOf(table, "getTableSchema").schema;
}

/** Reference parts for a table: `[name]` or `[schema, name]`. Every query
 *  layer site that builds a table-qualified reference funnels through here so
 *  same-name tables in different schemas can never address each other. */
export function tableRefParts(table: AnyPgTable): string[] {
  const meta = tableMetaOf(table, "tableRefParts");
  return meta.schema === undefined ? [meta.tableName] : [meta.schema, meta.tableName];
}

/** Authoritative column map (property key -> ColumnBuilder). */
export function getTableColumns(table: AnyPgTable): Record<string, AnyColumnBuilder> {
  return tableMetaOf(table, "getTableColumns").columns;
}

/** Authoritative index list. */
export function getTableIndexes(table: AnyPgTable): TableIndex[] {
  return tableMetaOf(table, "getTableIndexes").indexes;
}

export type InferSelectModelOf<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]: SelectTypeOf<Cols[K]>;
};

/** Insert keys that are NOT NULL (PK implies NOT NULL) without a default.
 *  Serial columns always have a server default and are never required. */
type InsertRequiredKeys<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]-?: Cols[K] extends ColumnBuilder<"serial", boolean, boolean>
    ? never
    : Cols[K] extends ColumnBuilder<ColumnDataType, true, false>
      ? K
      : never;
}[keyof Cols];

type Simplify<T> = { [K in keyof T]: T[K] } & {};

export type InferInsertModelOf<Cols extends Record<string, AnyColumnBuilder>> = Simplify<
  {
    [K in InsertRequiredKeys<Cols>]: InsertTypeOf<Cols[K]>;
  } & {
    [K in Exclude<keyof Cols, InsertRequiredKeys<Cols>>]?: InsertTypeOf<Cols[K]>;
  }
>;

export class TableIndex {
  constructor(
    readonly indexName: string,
    readonly unique: boolean,
    readonly columns: string[] = [],
    readonly method: "btree" | "hash" | "gin" | "gist" = "btree",
  ) {}

  on(...cols: Array<{ columnName: string }>): TableIndex {
    for (const c of cols) this.columns.push(c.columnName);
    return this;
  }
}

export function index(name: string): TableIndex {
  return new TableIndex(name, false);
}

export function uniqueIndex(name: string): TableIndex {
  return new TableIndex(name, true);
}

export function pgTable<Cols extends Record<string, AnyColumnBuilder>>(
  name: string,
  columns: Cols,
  extras?: (t: PgTable<Cols>) => TableIndex[],
): PgTable<Cols> {
  return makeTable(name, columns, extras, undefined);
}

function makeTable<Cols extends Record<string, AnyColumnBuilder>>(
  name: string,
  columns: Cols,
  extras: ((t: PgTable<Cols>) => TableIndex[]) | undefined,
  schema: string | undefined,
): PgTable<Cols> {
  const meta: { tableName: string; schema?: string; columns: Cols; indexes: TableIndex[] } = {
    tableName: name,
    columns,
    indexes: [],
  };
  if (schema !== undefined) meta.schema = schema;
  const table = {
    [TABLE_SYMBOL]: meta,
    // phantom — never read at runtime
    $inferSelect: undefined as never,
    $inferInsert: undefined as never,
    ...columns,
  } as PgTable<Cols>;
  for (const col of Object.values(columns)) {
    col.ownerTable = table;
  }
  if (extras) {
    // Index definitions may reference columns through the table object; they
    // read the user-facing column properties, which cannot clobber metadata.
    meta.indexes = extras(table);
  }
  const frozenMeta: TableMetadata<Cols> = meta;
  Object.freeze(frozenMeta);
  return table;
}

/** A named SQL schema: `pgSchema("alt").table("users", {...})` declares
 *  `alt.users`. Query-layer surface (CRUD select/insert/update/delete and
 *  alias joins render qualified references); DDL emission, schema export and
 *  relational reads reject schema-declared tables until Q05/Q07. */
export interface PgSchemaBuilder {
  readonly schemaName: string;
  table<Cols extends Record<string, AnyColumnBuilder>>(
    name: string,
    columns: Cols,
    extras?: (t: PgTable<Cols>) => TableIndex[],
  ): PgTable<Cols>;
}

export function pgSchema(schema: string): PgSchemaBuilder {
  if (typeof schema !== "string" || schema.length === 0) throw new Error("pgSchema: schema must be a non-empty string");
  if (schema.includes("\0")) throw new Error("pgSchema: schema must not contain NUL bytes");
  return {
    schemaName: schema,
    table: (name, columns, extras) => makeTable(name, columns, extras, schema),
  };
}

export function isPgTable(value: unknown): value is AnyPgTable {
  if (typeof value !== "object" || value === null) return false;
  const meta = (value as { [TABLE_SYMBOL]?: unknown })[TABLE_SYMBOL];
  return typeof meta === "object" && meta !== null && typeof (meta as TableMetadata).tableName === "string";
}

// ---------------------------------------------------------------------------
// Aliases — the join identity (Q01)
// ---------------------------------------------------------------------------
// `alias(table, "p")` binds a table to a name. Joins on the typed select
// builder take alias handles ONLY: the alias is the table's identity inside
// the statement, which is what makes self joins and same-SQL-name tables in
// different schemas unambiguous both in SQL text and in the result mapping.
//
// Runtime: the handle is a pseudo-table whose metadata tableName IS the alias
// and whose columns are copies whose ownerTable points back at it — so every
// existing reference-building path (eq/asc/sql interpolation, projections,
// decoders) renders alias-qualified references unchanged.
// Types: the handle's columns are the original column types intersected with
// `{ aliasTag: A }`, letting outer-join nullability key on the alias literal.

export const ALIAS_MARKER: unique symbol = Symbol.for("@neutron-build/sql.alias");

export interface AliasRecord {
  /** The base table the alias wraps. */
  readonly table: AnyPgTable;
  readonly alias: string;
}

/** Columns of an aliased table: original column types carrying the alias. */
export type AliasedCols<Cols extends Record<string, AnyColumnBuilder>, A extends string> = {
  [K in keyof Cols]: Cols[K] & { readonly aliasTag: A };
};

/** `alias(posts, "p")` — structurally a table (metadata + column properties)
 *  plus the alias record, with column types tagged by the alias. */
export type AliasedTable<Cols extends Record<string, AnyColumnBuilder>, A extends string> = PgTableCore<AliasedCols<Cols, A>> &
  AliasedCols<Cols, A> & {
    readonly [ALIAS_MARKER]: AliasRecord;
  };

export function alias<Cols extends Record<string, AnyColumnBuilder>, A extends string>(
  table: PgTable<Cols>,
  name: A,
): AliasedTable<Cols, A> {
  if (isAliasHandle(table)) {
    throw new Error(`alias: input is already an alias handle ("${table[ALIAS_MARKER].alias}") — alias the base table, not a handle`);
  }
  if (isDerivedTableHandle(table)) {
    const rec = table[DERIVED_MARKER];
    throw new Error(
      `alias: input is a ${rec.kind === "cte" ? "CTE" : "derived-table"} handle ("${rec.name}") — name it at construction (derivedTable/cteTable) instead of re-aliasing; the subquery would be lost`,
    );
  }
  tableMetaOf(table, "alias");
  if (typeof name !== "string" || name.length === 0) throw new Error("alias: name must be a non-empty string");
  if (name.includes("\0")) throw new Error("alias: name must not contain NUL bytes");
  if (name.includes(".")) throw new Error(`alias: name "${name}" must not contain "." — an alias is one identifier, not a qualification`);
  if (/^__q\d+$/.test(name)) throw new Error(`alias: name "${name}" is reserved for compiler-generated derived tables`);

  const base = getTableColumns(table);
  const clonedCols = {} as Record<string, AnyColumnBuilder>;
  for (const [key, column] of Object.entries(base)) {
    // Copy own state (class fields); the original column is never mutated or
    // shared. The clone's owner is wired to the handle below, so references
    // resolve to the alias.
    const clone = Object.create(Object.getPrototypeOf(column)) as AnyColumnBuilder;
    Object.assign(clone, column);
    (clone as { aliasTag?: string }).aliasTag = name;
    clonedCols[key] = clone;
  }
  const handle = {
    [TABLE_SYMBOL]: { tableName: name, columns: clonedCols, indexes: [] },
    $inferSelect: undefined as never,
    $inferInsert: undefined as never,
    ...clonedCols,
    [ALIAS_MARKER]: Object.freeze({ table, alias: name }) as AliasRecord,
  } as unknown as AliasedTable<Cols, A>;
  for (const clone of Object.values(clonedCols)) {
    clone.ownerTable = handle as unknown as AnyPgTable;
    Object.freeze(clone);
  }
  Object.freeze(clonedCols);
  Object.freeze(handle);
  return handle;
}

/** True for `alias()` products. Alias handles are join identities, never
 *  mutation targets or from-tables. */
export function isAliasHandle(value: unknown): value is { [ALIAS_MARKER]: AliasRecord } & AnyPgTable {
  if (typeof value !== "object" || value === null) return false;
  if (!isPgTable(value)) return false;
  const rec = (value as { [ALIAS_MARKER]?: unknown })[ALIAS_MARKER];
  return typeof rec === "object" && rec !== null && typeof (rec as AliasRecord).alias === "string";
}

/** Fail closed when an alias handle reaches a slot that takes base tables. */
export function rejectAliasHandle(value: unknown, who: string): void {
  if (isAliasHandle(value)) {
    throw new Error(`${who}: received the alias handle "${value[ALIAS_MARKER].alias}" — alias handles are join identities; pass the base table here`);
  }
}

// ---------------------------------------------------------------------------
// Derived tables and CTE references (Q02)
// ---------------------------------------------------------------------------
// `derivedTable(name, source)` / `cteTable(name, source)` build table-like
// handles over a subquery: a pseudo-table whose metadata tableName IS the
// name and whose columns are pseudo-columns synthesized from the source's
// projections (typed from the source's row type). Derived handles inline
// `(select …) as "name"` at their reference site; CTE handles render the
// bare name and auto-register the CTE on the consuming statement.

export const DERIVED_MARKER: unique symbol = Symbol.for("@neutron-build/sql.derived");

/** Runtime record carried by derived/CTE handles. `select` is the source
 *  statement; `capabilities` are the requirements the source acquired
 *  (e.g. jsonb-functions for temporal wire forms) — merged into every
 *  consuming statement. */
export interface DerivedRecord {
  readonly kind: "derived" | "cte";
  readonly name: string;
  readonly select: import("./ast.js").StatementNode;
  readonly capabilities: readonly import("./codecs.js").StatementCapability[];
}

export type AnyDerivedHandle = { readonly [DERIVED_MARKER]: DerivedRecord } & AnyPgTable;

/** True for derivedTable()/cteTable() products. */
export function isDerivedTableHandle(value: unknown): value is AnyDerivedHandle {
  if (typeof value !== "object" || value === null) return false;
  const rec = (value as { [DERIVED_MARKER]?: unknown })[DERIVED_MARKER];
  if (typeof rec !== "object" || rec === null) return false;
  const kind = (rec as DerivedRecord).kind;
  return kind === "derived" || kind === "cte";
}

/** The derived record of a table, or undefined for base tables and alias
 *  handles. */
export function getDerivedRecord(table: AnyPgTable): DerivedRecord | undefined {
  if (typeof table !== "object" || table === null) return undefined;
  const rec = (table as { [DERIVED_MARKER]?: unknown })[DERIVED_MARKER];
  if (typeof rec !== "object" || rec === null) return undefined;
  const r = rec as DerivedRecord;
  return (r.kind === "derived" || r.kind === "cte") && typeof r.name === "string" ? r : undefined;
}

/** Fail closed when a derived/CTE handle reaches a slot that takes base
 *  tables (mutations, DDL, export, relational registration). */
export function rejectDerivedTable(value: unknown, who: string): void {
  if (isDerivedTableHandle(value)) {
    const rec = value[DERIVED_MARKER];
    throw new Error(
      `${who}: received the ${rec.kind === "cte" ? "CTE" : "derived-table"} handle "${rec.name}" — these are query-surface identities (from/joins/selects only); pass a pgTable here`,
    );
  }
}

// ---------------------------------------------------------------------------
// Relations — declared per table, resolved lazily so thunk FKs and circular
// imports work the way Drizzle's do.
// ---------------------------------------------------------------------------

export const RELATIONS_SYMBOL = Symbol.for("@neutron-build/sql.relations");

export type AnyPgTable = PgTableCore<any>;

/** To-one relation: FK columns on the declaring table (ordered `fields`)
 *  pointing at ordered `references` on the target. `relationName` pairs a
 *  one() with its many() counterpart when several FKs target the same table. */
export interface RelationOne<T extends AnyPgTable = AnyPgTable> {
  kind: "one";
  targetTable: T;
  fields: AnyColumnBuilder[];
  references: AnyColumnBuilder[];
  relationName?: string;
}

/** To-many relation: the inverse of a one() on the target table. The source
 *  one() is resolved (and ambiguity rejected) by `resolveRelations`. */
export interface RelationMany<T extends AnyPgTable = AnyPgTable> {
  kind: "many";
  targetTable: T;
  relationName?: string;
  /** Resolved from the matching one() on the target table. */
  source?: RelationOne;
}

export type Relation<T extends AnyPgTable = AnyPgTable> = RelationOne<T> | RelationMany<T>;

export interface TableRelations<
  T extends AnyPgTable = AnyPgTable,
  E extends Record<string, Relation> = Record<string, Relation>,
> {
  readonly [RELATIONS_SYMBOL]: true;
  readonly table: T;
  readonly entries: E;
}

export type RelationConfig = {
  fields: AnyColumnBuilder[];
  references: AnyColumnBuilder[];
  relationName?: string;
};

export type ManyConfig = { relationName?: string };

export function relations<
  T extends AnyPgTable,
  E extends Record<string, Relation>,
>(
  table: T,
  configure: (r: {
    one: <Target extends AnyPgTable>(target: Target, config: RelationConfig) => RelationOne<Target>;
    many: <Target extends AnyPgTable>(target: Target, config?: ManyConfig) => RelationMany<Target>;
  }) => E,
): TableRelations<T, E> {
  const one = <Target extends AnyPgTable>(target: Target, config: RelationConfig): RelationOne<Target> => ({
    kind: "one",
    targetTable: target,
    fields: config.fields,
    references: config.references,
    relationName: config.relationName,
  });
  const many = <Target extends AnyPgTable>(target: Target, config?: ManyConfig): RelationMany<Target> => ({
    kind: "many",
    targetTable: target,
    relationName: config?.relationName,
  });
  return {
    [RELATIONS_SYMBOL]: true as const,
    table,
    entries: configure({ one, many }),
  };
}

export function isTableRelations(value: unknown): value is TableRelations {
  return (
    typeof value === "object" && value !== null && (value as { [RELATIONS_SYMBOL]?: unknown })[RELATIONS_SYMBOL] === true
  );
}

// ---------------------------------------------------------------------------
// Column helpers
// ---------------------------------------------------------------------------

function applyBigintMode(c: ColumnBuilder<"bigint", boolean, boolean, unknown>, mode: BigintMode): void {
  if (mode !== "bigint" && mode !== "string" && mode !== "number") {
    throw new Error(`unknown bigint codec mode "${String(mode)}" (known: bigint, string, number)`);
  }
  c.readMode = mode;
}

function applyTemporalMode(c: ColumnBuilder<"timestamp" | "timestamptz", boolean, boolean, unknown>, mode: TemporalMode): void {
  if (mode !== "string" && mode !== "date") {
    throw new Error(`unknown temporal codec mode "${String(mode)}" (known: string, date)`);
  }
  c.readMode = mode;
}

export function serial(name: string): ColumnBuilder<"serial", false, false> {
  return new ColumnBuilder(name, "serial");
}
export function integer(name: string): ColumnBuilder<"integer", false, false> {
  return new ColumnBuilder(name, "integer");
}
export function smallint(name: string): ColumnBuilder<"smallint", false, false> {
  return new ColumnBuilder(name, "smallint");
}
/** int8 column. Default read mode `bigint` never passes through JS Number;
 *  `string` keeps the exact decimal string; `number` is a checked safe-number
 *  mode that rejects values outside ±(2^53-1). */
export function bigint<M extends BigintMode = "bigint">(
  name: string,
  opts: BigintOptions<M> = {},
): ColumnBuilder<"bigint", false, false, BigintRead<M>> {
  const c = new ColumnBuilder<"bigint", false, false, BigintRead<M>>(name, "bigint");
  applyBigintMode(c, opts.mode ?? "bigint");
  return c;
}
export function double(name: string): ColumnBuilder<"double", false, false> {
  return new ColumnBuilder(name, "double");
}
export function real(name: string): ColumnBuilder<"real", false, false> {
  return new ColumnBuilder(name, "real");
}
/** Exact decimal column: reads as the exact decimal string (scale and
 *  trailing zeros preserved). An optional user decoder converts the exact
 *  string and owns any precision narrowing. */
export function numeric(name: string, opts?: { decoder?: undefined }): ColumnBuilder<"numeric", false, false>;
export function numeric<D extends (raw: string) => unknown>(
  name: string,
  opts: { decoder: D },
): ColumnBuilder<"numeric", false, false, ReturnType<D>>;
export function numeric(name: string, opts: NumericOptions = {}): ColumnBuilder<"numeric", false, false, unknown> {
  const c = new ColumnBuilder<"numeric", false, false, string>(name, "numeric");
  if (opts.decoder !== undefined) {
    if (typeof opts.decoder !== "function") throw new Error(`numeric("${name}"): decoder must be a function`);
    c.valueDecoder = opts.decoder;
  }
  return c as unknown as ColumnBuilder<"numeric", false, false, unknown>;
}
export function text(name: string): ColumnBuilder<"text", false, false> {
  return new ColumnBuilder(name, "text");
}
export function varchar(name: string, length: number): ColumnBuilder<"varchar", false, false> {
  const c = new ColumnBuilder<"varchar", false, false>(name, "varchar");
  c.varcharLength = length;
  return c;
}
export function boolean(name: string): ColumnBuilder<"boolean", false, false> {
  return new ColumnBuilder(name, "boolean");
}
/** timestamp without time zone. Reads as the canonical timezone-free string
 *  `YYYY-MM-DDTHH:MM:SS[.ffffff]` (microseconds preserved). Explicit
 *  `mode: "date"` returns Dates interpreting the wall clock as UTC and
 *  truncating to milliseconds. */
export function timestamp<M extends TemporalMode = "string">(
  name: string,
  opts: TemporalOptions<M> = {},
): ColumnBuilder<"timestamp", false, false, TemporalRead<M>> {
  const c = new ColumnBuilder<"timestamp", false, false, TemporalRead<M>>(name, "timestamp");
  applyTemporalMode(c, opts.mode ?? "string");
  return c;
}
/** timestamptz. Reads as the canonical UTC string
 *  `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (microseconds preserved, process and
 *  server timezones irrelevant). Explicit `mode: "date"` returns Dates
 *  (exact instant, millisecond truncation). */
export function timestamptz<M extends TemporalMode = "string">(
  name: string,
  opts: TemporalOptions<M> = {},
): ColumnBuilder<"timestamptz", false, false, TemporalRead<M>> {
  const c = new ColumnBuilder<"timestamptz", false, false, TemporalRead<M>>(name, "timestamptz");
  applyTemporalMode(c, opts.mode ?? "string");
  return c;
}
/** date column: `YYYY-MM-DD` strings in and out, no timezone interpretation
 *  (Date values are rejected — a Date has no timezone-free meaning). */
export function date(name: string): ColumnBuilder<"date", false, false> {
  return new ColumnBuilder(name, "date");
}
export function json(name: string): ColumnBuilder<"json", false, false> {
  return new ColumnBuilder(name, "json");
}
export function jsonb(name: string): ColumnBuilder<"jsonb", false, false> {
  return new ColumnBuilder(name, "jsonb");
}
export function uuid(name: string): ColumnBuilder<"uuid", false, false> {
  return new ColumnBuilder(name, "uuid");
}
export function bytea(name: string): ColumnBuilder<"bytea", false, false> {
  return new ColumnBuilder(name, "bytea");
}
/**
 * Nucleus vector column (experimental). On vanilla Postgres the migration
 * generator skips it with a warning and a documented comment instead of
 * emitting failing DDL.
 */
export function vector(name: string, dimensions: number): ColumnBuilder<"vector", false, false> {
  const c = new ColumnBuilder<"vector", false, false>(name, "vector");
  c.vectorDimensions = dimensions;
  return c;
}
