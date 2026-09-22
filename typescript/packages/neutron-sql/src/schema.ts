// ---------------------------------------------------------------------------
// @neutron-build/sql — schema definition (Drizzle-shaped, no codegen)
// ---------------------------------------------------------------------------

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

/** Read type: what the drivers hand back for D today. int8/numeric arrive as
 *  strings on both drivers and are typed honestly as such (no coercion);
 *  exact codecs are a later, separately documented change. */
export type JsTypeOf<D extends ColumnDataType> =
  D extends "serial" | "integer" | "smallint" | "double" | "real"
    ? number
    : D extends "bigint" | "numeric" | "text" | "varchar" | "uuid"
      ? string
      : D extends "boolean"
        ? boolean
        : D extends "timestamp" | "timestamptz" | "date"
          ? Date
          : D extends "bytea"
            ? Uint8Array
            : D extends "vector"
              ? number[]
              : unknown;

/** Write type: values the drivers accept for D today. int8 additionally takes
 *  number/bigint and numeric takes number; sending never coerces the stored
 *  value — 9007199254740993n and "9007199254740993" store identically. */
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
            : D extends "timestamp" | "timestamptz" | "date"
              ? Date
              : D extends "bytea"
                ? Uint8Array
                : D extends "vector"
                  ? number[]
                  : unknown;

export interface ForeignKeyRef {
  (): ColumnBuilder<ColumnDataType, boolean, boolean>;
  onDelete?: "cascade" | "restrict" | "set null" | "no action";
}

export class ColumnBuilder<
  D extends ColumnDataType = ColumnDataType,
  NN extends boolean = false,
  HD extends boolean = false,
> {
  declare readonly _: { dataType: D; notNull: NN; hasDefault: HD };

  readonly columnName: string;
  readonly dataType: D;
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

  notNull(): ColumnBuilder<D, true, HD> {
    this.isNotNull = true;
    return this as unknown as ColumnBuilder<D, true, HD>;
  }

  default(value: JsWriteTypeOf<D>): ColumnBuilder<D, NN, true> {
    this.hasDefault = true;
    this.defaultValue = value;
    return this as unknown as ColumnBuilder<D, NN, true>;
  }

  defaultNow(): ColumnBuilder<D, NN, true> {
    this.hasDefault = true;
    this.nowDefault = true;
    return this as unknown as ColumnBuilder<D, NN, true>;
  }

  /** Primary keys are implicitly NOT NULL (PostgreSQL semantics); the insert
   *  type reflects that. Serial primary keys still default server-side. */
  primaryKey(): ColumnBuilder<D, true, HD> {
    this.isPrimaryKey = true;
    return this as unknown as ColumnBuilder<D, true, HD>;
  }

  unique(): ColumnBuilder<D, NN, HD> {
    this.isUnique = true;
    return this as unknown as ColumnBuilder<D, NN, HD>;
  }

  references(
    ref: ForeignKeyRef,
    opts?: { onDelete?: ForeignKeyRef["onDelete"] },
  ): ColumnBuilder<D, NN, HD> {
    const bound: ForeignKeyRef = Object.assign(() => ref(), { onDelete: opts?.onDelete });
    this.foreignKey = bound;
    return this as unknown as ColumnBuilder<D, NN, HD>;
  }
}

export type AnyColumnBuilder = ColumnBuilder<ColumnDataType, boolean, boolean>;

export type SelectTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer _HD>
  ? NN extends true
    ? JsTypeOf<D>
    : JsTypeOf<D> | null
  : never;

/** Read type of a column inside a relation child projection (JSON path).
 *  int8/numeric leaves are rendered ::text inside the aggregation and
 *  temporal/bytea leaves pass through to_jsonb's string form, so all of
 *  those arrive as strings; int4/float8 leaves are JSON numbers within JS
 *  safe precision. Explicit decode modes (BigInt/Date/Uint8Array) are a
 *  later, separately documented change. */
export type RelationLeafTypeOf<D extends ColumnDataType> =
  D extends "serial" | "integer" | "smallint" | "double" | "real"
    ? number
    : D extends "bigint" | "numeric" | "text" | "varchar" | "uuid" | "timestamp" | "timestamptz" | "date" | "bytea"
      ? string
      : D extends "boolean"
        ? boolean
        : D extends "vector"
          ? number[]
          : unknown;

export type RelationSelectTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer _HD>
  ? NN extends true
    ? RelationLeafTypeOf<D>
    : RelationLeafTypeOf<D> | null
  : never;

export type InsertTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer HD>
  ? NN extends true
    ? HD extends true
      ? JsWriteTypeOf<D> | undefined
      : D extends "serial"
        ? JsWriteTypeOf<D> | undefined
        : JsWriteTypeOf<D>
    : JsWriteTypeOf<D> | null | undefined
  : never;

export type UpdateTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer _HD>
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

export function serial(name: string): ColumnBuilder<"serial", false, false> {
  return new ColumnBuilder(name, "serial");
}
export function integer(name: string): ColumnBuilder<"integer", false, false> {
  return new ColumnBuilder(name, "integer");
}
export function smallint(name: string): ColumnBuilder<"smallint", false, false> {
  return new ColumnBuilder(name, "smallint");
}
export function bigint(name: string): ColumnBuilder<"bigint", false, false> {
  return new ColumnBuilder(name, "bigint");
}
export function double(name: string): ColumnBuilder<"double", false, false> {
  return new ColumnBuilder(name, "double");
}
export function real(name: string): ColumnBuilder<"real", false, false> {
  return new ColumnBuilder(name, "real");
}
export function numeric(name: string): ColumnBuilder<"numeric", false, false> {
  return new ColumnBuilder(name, "numeric");
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
export function timestamp(name: string): ColumnBuilder<"timestamp", false, false> {
  return new ColumnBuilder(name, "timestamp");
}
export function timestamptz(name: string): ColumnBuilder<"timestamptz", false, false> {
  return new ColumnBuilder(name, "timestamptz");
}
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
