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
  const meta: { tableName: string; columns: Cols; indexes: TableIndex[] } = { tableName: name, columns, indexes: [] };
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

export function isPgTable(value: unknown): value is AnyPgTable {
  if (typeof value !== "object" || value === null) return false;
  const meta = (value as { [TABLE_SYMBOL]?: unknown })[TABLE_SYMBOL];
  return typeof meta === "object" && meta !== null && typeof (meta as TableMetadata).tableName === "string";
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
