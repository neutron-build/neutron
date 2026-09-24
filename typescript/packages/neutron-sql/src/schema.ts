// ---------------------------------------------------------------------------
// @neutron-build/sql — schema definition (Drizzle-shaped, no codegen)
// ---------------------------------------------------------------------------

import type { BigintMode, BigintOptions, TemporalMode, TemporalOptions, NumericOptions } from "./codecs.js";
import type { SchemaExpression } from "./ddl-text.js";
import type { OrderSpec } from "./ast.js";

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
  | "vector"
  | "enum";

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
      : D extends "numeric" | "text" | "varchar" | "uuid" | "timestamp" | "timestamptz" | "date" | "enum"
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
        : D extends "text" | "varchar" | "uuid" | "enum"
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

/** Referential actions (PostgreSQL ON DELETE / ON UPDATE). */
export type ReferentialAction = "cascade" | "restrict" | "set null" | "set default" | "no action";

export interface ForeignKeyRef {
  (): ColumnBuilder<ColumnDataType, boolean, boolean, unknown, unknown>;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

/** Enum type metadata carried by enum columns (Q07). `schema` is undefined
 *  for the default search path (exported as "public"). */
export interface PgEnumDefinition {
  readonly schema: string | undefined;
  readonly name: string;
  readonly values: readonly string[];
}

/** Custom codec over a column's lossless representation (Q07e): `decode`
 *  maps the value the column codec produced (flat select, JSON relation
 *  leaf, returning) to the user-facing read value; `encode` maps a
 *  user-facing value back to what the column codec accepts (writes,
 *  predicates, declared defaults). The SQL type is unchanged — the codec is
 *  query-layer mapping, invisible to the schema contract. */
export interface CustomCodec<R, W, RT2, WT2> {
  decode(value: R): RT2;
  encode(value: WT2): W;
}

/** Element types `.array()` supports: their PostgreSQL text output is exact
 *  and session-independent, so arrays are acquired as the array literal
 *  (`col::text`) and decoded per element on both drivers. Temporal, bytea,
 *  vector and serial arrays are explicitly unsupported (their text forms
 *  depend on DateStyle/TimeZone/bytea_output, or the type has no array). */
export const ARRAY_ELEMENT_TYPES: ReadonlySet<ColumnDataType> = new Set<ColumnDataType>([
  "integer", "smallint", "bigint", "double", "real", "numeric",
  "text", "varchar", "boolean", "uuid", "enum", "json", "jsonb",
]);

export class ColumnBuilder<
  D extends ColumnDataType = ColumnDataType,
  NN extends boolean = false,
  HD extends boolean = false,
  RT = JsTypeOf<D>,
  WT = JsWriteTypeOf<D>,
> {
  declare readonly _: { dataType: D; notNull: NN; hasDefault: HD; readType: RT; writeType: WT };

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
  /** Q07: set to 1 by `.array()` — a one-dimensional array whose elements
   *  have type `dataType`. */
  arrayDimensions?: 1;
  /** Q07: enum type of an enum column (dataType "enum"). */
  enumDef?: PgEnumDefinition;
  /** Q07c: identity kind of an identity column. */
  identityKind?: "always" | "by default";
  /** Q07c: stored generated-column expression (rendered SQL text is set at
   *  export; the declaration keeps the raw expression until then). */
  generatedExpr?: SchemaExpression;
  /** Q07e: custom codec mapping over the column's lossless representation. */
  customCodec?: CustomCodec<unknown, unknown, unknown, unknown>;

  constructor(columnName: string, dataType: D) {
    this.columnName = columnName;
    this.dataType = dataType;
  }

  notNull(): ColumnBuilder<D, true, HD, RT, WT> {
    this.isNotNull = true;
    return this as unknown as ColumnBuilder<D, true, HD, RT, WT>;
  }

  default(value: WT): ColumnBuilder<D, NN, true, RT, WT> {
    if (this.generatedExpr !== undefined || this.identityKind !== undefined) {
      throw new Error(`column "${this.columnName}": .default() — a column cannot combine a default with a generation expression or identity`);
    }
    this.hasDefault = true;
    this.defaultValue = value;
    return this as unknown as ColumnBuilder<D, NN, true, RT, WT>;
  }

  defaultNow(): ColumnBuilder<D, NN, true, RT, WT> {
    if (this.generatedExpr !== undefined || this.identityKind !== undefined) {
      throw new Error(`column "${this.columnName}": .defaultNow() — a column cannot combine a default with a generation expression or identity`);
    }
    this.hasDefault = true;
    this.nowDefault = true;
    return this as unknown as ColumnBuilder<D, NN, true, RT, WT>;
  }

  /** Primary keys are implicitly NOT NULL (PostgreSQL semantics); the insert
   *  type reflects that. Serial primary keys still default server-side. */
  primaryKey(): ColumnBuilder<D, true, HD, RT, WT> {
    this.isPrimaryKey = true;
    return this as unknown as ColumnBuilder<D, true, HD, RT, WT>;
  }

  unique(): ColumnBuilder<D, NN, HD, RT, WT> {
    this.isUnique = true;
    return this as unknown as ColumnBuilder<D, NN, HD, RT, WT>;
  }

  references(
    ref: ForeignKeyRef,
    opts?: { onDelete?: ReferentialAction; onUpdate?: ReferentialAction },
  ): ColumnBuilder<D, NN, HD, RT, WT> {
    const bound: ForeignKeyRef = Object.assign(() => ref(), { onDelete: opts?.onDelete, onUpdate: opts?.onUpdate });
    this.foreignKey = bound;
    return this as unknown as ColumnBuilder<D, NN, HD, RT, WT>;
  }

  /** One-dimensional PostgreSQL array of this column's type (Q07). Elements
   *  may be SQL NULL, so they read and write as `T | null`. Call it on the
   *  bare column factory, before `.default()`/`.primaryKey()`. Arrays read
   *  through the array literal (`col::text`) and decode per element with the
   *  element codec on both drivers; values with more than one dimension or
   *  non-default lower bounds fail to decode instead of being reshaped. */
  array(): ColumnBuilder<D, NN, HD, Array<RT | null>, Array<WT | null>> {
    const who = `column "${this.columnName}": .array()`;
    if (this.arrayDimensions !== undefined) throw new Error(`${who} — already an array; multi-dimensional array columns are not supported`);
    if (!ARRAY_ELEMENT_TYPES.has(this.dataType)) {
      throw new Error(
        `${who} — ${this.dataType} arrays are not supported (supported element types: ${[...ARRAY_ELEMENT_TYPES].join(", ")}); ` +
          `temporal/bytea text forms depend on session settings, so no lossless acquisition exists for them yet`,
      );
    }
    if (this.hasDefault || this.isPrimaryKey || this.isUnique || this.foreignKey !== undefined || this.ownerTable !== undefined) {
      throw new Error(`${who} must be called on the bare column factory, before .default()/.primaryKey()/.unique()/.references() and before the table is built`);
    }
    if (this.dataType === "numeric" && this.valueDecoder !== undefined) {
      throw new Error(`${who} — numeric decoders apply to scalar columns only; decode array elements after reading`);
    }
    this.arrayDimensions = 1;
    return this as unknown as ColumnBuilder<D, NN, HD, Array<RT | null>, Array<WT | null>>;
  }

  /** GENERATED ALWAYS AS IDENTITY (Q07c): the database generates values;
   *  writes are rejected at the type level (write type `never`) and at
   *  runtime. Integer columns only. Implies NOT NULL and a default. */
  generatedAlwaysAsIdentity(): ColumnBuilder<D, true, true, RT, never> {
    this.assertIdentityEligible("generatedAlwaysAsIdentity");
    this.identityKind = "always";
    this.isNotNull = true;
    this.hasDefault = true;
    return this as unknown as ColumnBuilder<D, true, true, RT, never>;
  }

  /** GENERATED BY DEFAULT AS IDENTITY (Q07c): the database generates values
   *  unless the statement supplies one. Implies NOT NULL and a default. */
  generatedByDefaultAsIdentity(): ColumnBuilder<D, true, true, RT, WT> {
    this.assertIdentityEligible("generatedByDefaultAsIdentity");
    this.identityKind = "by default";
    this.isNotNull = true;
    this.hasDefault = true;
    return this as unknown as ColumnBuilder<D, true, true, RT, WT>;
  }

  private assertIdentityEligible(who: string): void {
    if (this.dataType !== "integer" && this.dataType !== "smallint" && this.dataType !== "bigint" && this.dataType !== "serial") {
      throw new Error(`column "${this.columnName}": .${who}() — identity columns must be integer, smallint, bigint or serial`);
    }
    if (this.dataType === "serial") {
      throw new Error(`column "${this.columnName}": .${who}() — serial already implies a sequence default; use integer/smallint/bigint with identity instead`);
    }
    if (this.arrayDimensions !== undefined) throw new Error(`column "${this.columnName}": .${who}() — identity array columns are not supported`);
    if (this.hasDefault || this.nowDefault || this.generatedExpr !== undefined) {
      throw new Error(`column "${this.columnName}": .${who}() — a column cannot combine identity with a default or a generation expression`);
    }
  }

  /** GENERATED ALWAYS AS (expr) STORED (Q07c): the value is computed by the
   *  database and cannot be written (write type `never`, runtime rejection).
   *  Virtual generated columns are explicitly unsupported (PostgreSQL < 18
   *  does not have them; nothing depends on them here). */
  generatedAlwaysAs(expr: SchemaExpression, opts?: { mode?: "stored" }): ColumnBuilder<D, NN, false, RT, never> {
    const who = `column "${this.columnName}": .generatedAlwaysAs()`;
    if (opts?.mode !== undefined && opts.mode !== "stored") {
      throw new Error(`${who} — mode ${JSON.stringify(opts.mode)} is not supported; only stored generated columns exist (virtual generated columns require PostgreSQL 18 and are explicitly unsupported)`);
    }
    if (this.generatedExpr !== undefined) throw new Error(`${who} — already generated`);
    if (this.identityKind !== undefined || this.hasDefault || this.nowDefault) {
      throw new Error(`${who} — a column cannot combine a generation expression with identity or a default`);
    }
    if (this.arrayDimensions !== undefined) throw new Error(`${who} — generated array columns are not supported`);
    this.generatedExpr = expr;
    return this as unknown as ColumnBuilder<D, NN, false, RT, never>;
  }

  /** Custom codec over this column's lossless representation (Q07e). The
   *  SQL type is unchanged; decode maps codec output to the read value and
   *  encode maps user values back before the column codec runs. Call before
   *  `.default()` — declared defaults store the user-facing value. */
  codec<RT2, WT2>(codec: CustomCodec<RT, WT, RT2, WT2>): ColumnBuilder<D, NN, HD, RT2, WT2> {
    const who = `column "${this.columnName}": .codec()`;
    if (this.customCodec !== undefined) throw new Error(`${who} — already has a custom codec`);
    if (this.arrayDimensions !== undefined) {
      throw new Error(`${who} — custom codecs apply to the column's own value; decode array elements in the codec of a scalar column instead`);
    }
    if (this.generatedExpr !== undefined || this.identityKind === "always") {
      throw new Error(`${who} — database-generated columns cannot carry a custom codec (their values are never written through it)`);
    }
    if (typeof codec?.decode !== "function" || typeof codec?.encode !== "function") {
      throw new Error(`${who} — a custom codec must be { decode, encode } functions`);
    }
    this.customCodec = codec as unknown as CustomCodec<unknown, unknown, unknown, unknown>;
    return this as unknown as ColumnBuilder<D, NN, HD, RT2, WT2>;
  }
}

export type AnyColumnBuilder = ColumnBuilder<ColumnDataType, boolean, boolean, unknown, unknown>;

/** The write (insert/update value) type of a column; `never` for columns
 *  the database generates. */
export type WriteTypeOf<C> = C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown, infer WT> ? WT : never;

/** The value type predicates and cursors compare a column against: the
 *  write type, or — for database-generated columns, which cannot be written
 *  but are compared like any other — the SQL type's base write type. */
export type PredicateValueOf<C> = C extends ColumnBuilder<infer D, boolean, boolean, unknown, infer WT>
  ? [WT] extends [never]
    ? JsWriteTypeOf<D>
    : WT
  : never;

export type SelectTypeOf<C> = C extends ColumnBuilder<infer _D, infer NN, infer _HD, infer RT, unknown>
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
    : D extends "numeric" | "text" | "varchar" | "uuid" | "timestamp" | "timestamptz" | "date" | "enum"
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
export type RelationSelectTypeOf<C> = C extends ColumnBuilder<infer _D, infer NN, infer _HD, infer RT, unknown>
  ? NN extends true
    ? RT
    : RT | null
  : never;

/** Insert value type. A write type of `never` marks a column the database
 *  generates (GENERATED ALWAYS identity or generated expression columns):
 *  only `undefined` (omission) is accepted. */
export type InsertTypeOf<C> = C extends ColumnBuilder<infer D, infer NN, infer HD, unknown, infer WT>
  ? [WT] extends [never]
    ? undefined
    : NN extends true
      ? HD extends true
        ? WT | undefined
        : D extends "serial"
          ? WT | undefined
          : WT
      : WT | null | undefined
  : never;

export type UpdateTypeOf<C> = C extends ColumnBuilder<infer _D, infer NN, infer _HD, unknown, infer WT>
  ? [WT] extends [never]
    ? never
    : NN extends true
      ? WT
      : WT | null
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
   *  search path, exported as public). Declared through pgSchema(); the
   *  query layer (CRUD + alias joins) renders qualified references and
   *  schema export v2 exports the table under its schema (Q07), while the
   *  legacy TS DDL emitter and relational reads reject schema-declared
   *  tables. */
  readonly schema?: string;
  readonly columns: Cols;
  readonly indexes: TableIndex[];
  /** Q07b: table-level constraints declared through the extras callback. */
  readonly constraints: TableConstraint[];
  /** Q07d: present when this handle is a view, not a base table. */
  readonly viewDef?: ViewDefinition;
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

/** Authoritative table-level constraint list (Q07b). */
export function getTableConstraints(table: AnyPgTable): TableConstraint[] {
  return tableMetaOf(table, "getTableConstraints").constraints;
}

/** View definition when this handle is a view (Q07d), else undefined. */
export function getViewDefinition(table: AnyPgTable): ViewDefinition | undefined {
  return tableMetaOf(table, "getViewDefinition").viewDef;
}

export type InferSelectModelOf<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]: SelectTypeOf<Cols[K]>;
};

/** Insert keys that are NOT NULL (PK implies NOT NULL) without a default.
 *  Serial columns always have a server default and are never required. */
type InsertRequiredKeys<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]-?: Cols[K] extends ColumnBuilder<"serial", boolean, boolean, unknown, unknown>
    ? never
    : Cols[K] extends ColumnBuilder<ColumnDataType, true, false, unknown, unknown>
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

/** One index key part: a column reference or an SQL expression, with
 *  optional per-key ordering (Q07b). Raw until export renders expressions. */
export interface IndexKeyPartDef {
  readonly column?: string;
  readonly expression?: SchemaExpression;
  readonly order?: "asc" | "desc";
  readonly nulls?: "first" | "last";
}

export type IndexMethod = "btree" | "hash" | "gin" | "gist" | "spgist" | "brin";

export class TableIndex {
  readonly columns: string[] = [];
  readonly keyParts: IndexKeyPartDef[] = [];
  methodValue: IndexMethod = "btree";
  whereExpr?: SchemaExpression;
  readonly includeCols: string[] = [];

  constructor(
    readonly indexName: string,
    readonly unique: boolean,
    columns: string[] = [],
    method: IndexMethod = "btree",
  ) {
    this.columns = columns;
    this.methodValue = method;
    for (const c of columns) this.keyParts.push({ column: c });
  }

  /** Access method (Q07b). */
  using(method: IndexMethod): TableIndex {
    this.methodValue = method;
    return this;
  }

  /** Key parts: bare columns, `asc(col)`/`desc(col)`/`ascNullsFirst(...)`-
   *  style order specs (expr.ts), or `sql` fragments for expression keys. */
  on(...cols: Array<{ columnName: string } | OrderSpec | SchemaExpression>): TableIndex {
    for (const c of cols) {
      if (typeof c === "object" && c !== null && "columnName" in c && typeof (c as { columnName: unknown }).columnName === "string") {
        this.columns.push((c as { columnName: string }).columnName);
        this.keyParts.push({ column: (c as { columnName: string }).columnName });
        continue;
      }
      if (isOrderSpec(c)) {
        this.keyParts.push(orderSpecKeyPart(c));
        continue;
      }
      if (typeof c === "object" && c !== null && (c as { kind?: unknown }).kind === "fragment") {
        this.keyParts.push({ expression: c as SchemaExpression });
        continue;
      }
      throw new Error(`index "${this.indexName}": .on() accepts columns, order specs (asc/desc/...) or sql fragments`);
    }
    return this;
  }

  /** Partial-index predicate (Q07b). */
  where(predicate: SchemaExpression): TableIndex {
    if (this.whereExpr !== undefined) throw new Error(`index "${this.indexName}": .where() called twice`);
    this.whereExpr = predicate;
    return this;
  }

  /** Included columns for INCLUDE (btree only, Q07b). */
  include(...cols: Array<{ columnName: string }>): TableIndex {
    for (const c of cols) this.includeCols.push(c.columnName);
    return this;
  }

  get method(): IndexMethod {
    return this.methodValue;
  }
}

function isOrderSpec(v: unknown): v is OrderSpec {
  return typeof v === "object" && v !== null && "expr" in v && "direction" in v;
}

function orderSpecKeyPart(spec: OrderSpec): IndexKeyPartDef {
  const e = spec.expr as unknown;
  const opts = { ...(spec.nulls ? { nulls: spec.nulls } : {}), ...(spec.direction === "desc" ? { order: "desc" as const } : {}) };
  if (typeof e === "object" && e !== null && (e as { kind?: unknown }).kind === "qualified") {
    return { column: (e as { parts: string[] }).parts[(e as { parts: string[] }).parts.length - 1], ...opts };
  }
  throw new Error("index key order specs must wrap a column — ordered expression keys are explicitly unsupported (wrap the expression, order the column keys)");
}

// ---------------------------------------------------------------------------
// Table-level constraints (Q07b)
// ---------------------------------------------------------------------------

export interface TablePrimaryKeyDef {
  readonly kind: "primary-key";
  readonly name?: string;
  readonly columns: readonly string[];
  readonly deferrable?: boolean;
  readonly initiallyDeferred?: boolean;
}

export interface TableUniqueDef {
  readonly kind: "unique";
  readonly name?: string;
  readonly columns: readonly string[];
  readonly deferrable?: boolean;
  readonly initiallyDeferred?: boolean;
}

export interface TableCheckDef {
  readonly kind: "check";
  readonly name: string;
  readonly expression: SchemaExpression;
  readonly deferrable?: boolean;
  readonly initiallyDeferred?: boolean;
}

export interface TableForeignKeyShape {
  readonly kind: "foreign-key";
  readonly name?: string;
  readonly columns: readonly string[];
  readonly refTable?: AnyPgTable;
  readonly refColumns: readonly string[];
  readonly onDelete?: ReferentialAction;
  readonly onUpdate?: ReferentialAction;
  readonly match?: ForeignKeyMatch;
  readonly deferrable?: boolean;
  readonly initiallyDeferred?: boolean;
}

export type ForeignKeyMatch = "simple" | "full" | "partial";

/** Foreign-key builder: `foreignKey({ columns: [t.a, t.b] }).references(other, [other.x, other.y], { onDelete: "cascade" })`. */
export class TableForeignKeyDef implements TableForeignKeyShape {
  readonly kind = "foreign-key" as const;
  readonly name?: string;
  readonly columns: string[];
  refTable?: AnyPgTable;
  refColumns: string[] = [];
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
  match?: ForeignKeyMatch;
  deferrable?: boolean;
  initiallyDeferred?: boolean;

  constructor(config: { name?: string; columns: ReadonlyArray<{ columnName: string }> }) {
    this.name = config.name;
    this.columns = config.columns.map((c) => c.columnName);
  }

  /** Target tuple and referential behavior. */
  references(
    table: AnyPgTable,
    columns: ReadonlyArray<{ columnName: string }>,
    opts?: { onDelete?: ReferentialAction; onUpdate?: ReferentialAction; match?: ForeignKeyMatch; deferrable?: boolean; initiallyDeferred?: boolean },
  ): this {
    this.refTable = table;
    this.refColumns = columns.map((c) => c.columnName);
    this.onDelete = opts?.onDelete;
    this.onUpdate = opts?.onUpdate;
    this.match = opts?.match;
    this.deferrable = opts?.deferrable;
    this.initiallyDeferred = opts?.initiallyDeferred;
    return this;
  }
}

export type TableConstraint = TablePrimaryKeyDef | TableUniqueDef | TableCheckDef | TableForeignKeyDef;

export function primaryKey(config: { name?: string; columns: ReadonlyArray<{ columnName: string }> }): TablePrimaryKeyDef {
  return { kind: "primary-key", name: config.name, columns: config.columns.map((c) => c.columnName) };
}

export function unique(config: { name?: string; columns: ReadonlyArray<{ columnName: string }> }): TableUniqueDef {
  return { kind: "unique", name: config.name, columns: config.columns.map((c) => c.columnName) };
}

export function check(name: string, expression: SchemaExpression): TableCheckDef {
  return { kind: "check", name, expression };
}

export function foreignKey(config: { name?: string; columns: ReadonlyArray<{ columnName: string }> }): TableForeignKeyDef {
  return new TableForeignKeyDef(config);
}

export type TableExtra = TableIndex | TableConstraint;

function isTableConstraint(v: unknown): v is TableConstraint {
  return typeof v === "object" && v !== null && "kind" in v;
}

function splitExtras(extras: TableExtra[]): { indexes: TableIndex[]; constraints: TableConstraint[] } {
  const indexes: TableIndex[] = [];
  const constraints: TableConstraint[] = [];
  for (const e of extras) {
    if (isTableConstraint(e)) constraints.push(e);
    else indexes.push(e);
  }
  return { indexes, constraints };
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
  extras?: (t: PgTable<Cols>) => TableExtra[],
): PgTable<Cols> {
  return makeTable(name, columns, extras, undefined);
}

function makeTable<Cols extends Record<string, AnyColumnBuilder>>(
  name: string,
  columns: Cols,
  extras: ((t: PgTable<Cols>) => TableExtra[]) | undefined,
  schema: string | undefined,
): PgTable<Cols> {
  const meta: { tableName: string; schema?: string; columns: Cols; indexes: TableIndex[]; constraints: TableConstraint[] } = {
    tableName: name,
    columns,
    indexes: [],
    constraints: [],
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
    // Index/constraint definitions may reference columns through the table
    // object; they read the user-facing column properties, which cannot
    // clobber metadata.
    const extraList = extras(table);
    if (!Array.isArray(extraList)) throw new Error(`pgTable("${name}"): the extras callback must return an array of index()/constraint definitions`);
    Object.assign(meta, splitExtras(extraList));
  }
  const frozenMeta: TableMetadata<Cols> = meta;
  Object.freeze(frozenMeta);
  return table;
}

/** A named SQL schema: `pgSchema("alt").table("users", {...})` declares
 *  `alt.users`; `.enum(...)` declares `alt.<enum>`. The query layer (CRUD
 *  select/insert/update/delete and alias joins) renders qualified
 *  references and schema export v2 (Q07) exports these objects under their
 *  schema. The legacy TS DDL emitter (schemaToDDL) and relational reads
 *  (db.query) reject schema-declared tables. */
export interface PgSchemaBuilder {
  readonly schemaName: string;
  table<Cols extends Record<string, AnyColumnBuilder>>(
    name: string,
    columns: Cols,
    extras?: (t: PgTable<Cols>) => TableExtra[],
  ): PgTable<Cols>;
  enum<const V extends readonly [string, ...string[]]>(name: string, values: V): PgEnum<V>;
  view<Cols extends Record<string, AnyColumnBuilder>>(name: string, columns: Cols, opts: ViewOptions): PgTable<Cols>;
}

function checkSchemaObjectName(value: unknown, who: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${who} must be a non-empty string`);
  for (const ch of value) {
    const cp = ch.codePointAt(0)!;
    if (cp < 0x20 || cp === 0x7f) throw new Error(`${who} must not contain control characters (including NUL)`);
  }
  return value;
}

export function pgSchema(schema: string): PgSchemaBuilder {
  if (typeof schema !== "string" || schema.length === 0) throw new Error("pgSchema: schema must be a non-empty string");
  if (schema.includes("\0")) throw new Error("pgSchema: schema must not contain NUL bytes");
  checkSchemaObjectName(schema, "pgSchema: schema");
  return {
    schemaName: schema,
    table: (name, columns, extras) => makeTable(name, columns, extras, schema),
    enum: (name, values) => makeEnum(name, values, schema),
    view: (name, columns, opts) => makeView(name, columns, opts, schema) as PgTable<never>,
  };
}

// ---------------------------------------------------------------------------
// Enums (Q07)
// ---------------------------------------------------------------------------
// `pgEnum("mood", ["sad", "ok"])` declares the PostgreSQL enum type and
// returns a column factory: `mood("current_mood")` is an enum column whose
// read/write type is the literal union of the declared values. Value order
// is semantic (PostgreSQL compares enum values by declaration order) and is
// exported verbatim. Reads and writes validate membership at runtime, so a
// database enum that has drifted from the declaration fails loudly instead
// of producing values outside the declared type.

export const ENUM_SYMBOL = Symbol.for("@neutron-build/sql.enum");

export interface PgEnum<V extends readonly [string, ...string[]]> {
  (columnName: string): ColumnBuilder<"enum", false, false, V[number], V[number]>;
  readonly [ENUM_SYMBOL]: PgEnumDefinition;
  readonly enumName: string;
  readonly schema: string | undefined;
  readonly enumValues: V;
}

export type AnyPgEnum = PgEnum<readonly [string, ...string[]]>;

function makeEnum<const V extends readonly [string, ...string[]]>(name: string, values: V, schema: string | undefined): PgEnum<V> {
  checkSchemaObjectName(name, "pgEnum: name");
  if (!Array.isArray(values) || values.length === 0) throw new Error(`pgEnum("${name}"): values must be a non-empty array`);
  const seen = new Set<string>();
  for (const v of values) {
    checkSchemaObjectName(v, `pgEnum("${name}"): value`);
    if (seen.has(v)) throw new Error(`pgEnum("${name}"): value "${v}" is declared twice`);
    seen.add(v);
  }
  const frozenValues = Object.freeze([...values]) as unknown as V;
  const def: PgEnumDefinition = Object.freeze({ schema, name, values: frozenValues });
  const factory = (columnName: string): ColumnBuilder<"enum", false, false, V[number], V[number]> => {
    const c = new ColumnBuilder<"enum", false, false, V[number], V[number]>(columnName, "enum");
    c.enumDef = def;
    return c;
  };
  Object.defineProperty(factory, ENUM_SYMBOL, { value: def, enumerable: false });
  Object.defineProperty(factory, "enumName", { value: name, enumerable: true });
  Object.defineProperty(factory, "schema", { value: schema, enumerable: true });
  Object.defineProperty(factory, "enumValues", { value: frozenValues, enumerable: true });
  return Object.freeze(factory) as unknown as PgEnum<V>;
}

/** Declare an enum type in the default schema (exported as public). */
export function pgEnum<const V extends readonly [string, ...string[]]>(name: string, values: V): PgEnum<V> {
  return makeEnum(name, values, undefined);
}

export function isPgEnum(value: unknown): value is AnyPgEnum {
  return typeof value === "function" && typeof (value as { [ENUM_SYMBOL]?: unknown })[ENUM_SYMBOL] === "object";
}

/** Enum definition of an enum object. */
export function getEnumDefinition(value: AnyPgEnum): PgEnumDefinition {
  return value[ENUM_SYMBOL];
}

// ---------------------------------------------------------------------------
// Views (Q07d)
// ---------------------------------------------------------------------------
// A view handle is a read-only table: the same symbol-keyed metadata shape
// (so select/join/projection paths accept it unchanged) plus a viewDef
// record. Its columns are clones of the projection columns with ownerTable
// rebound to the view, so references render view-qualified. Mutations
// (insert/update/delete) and relational reads reject view handles.

export interface ViewDefinition {
  readonly definition: SchemaExpression;
  readonly checkOption?: "local" | "cascaded";
  readonly securityInvoker?: boolean;
}

function cloneColumnForView(column: AnyColumnBuilder, view: AnyPgTable): AnyColumnBuilder {
  const c = new ColumnBuilder(column.columnName, column.dataType);
  c.readMode = column.readMode;
  c.valueDecoder = column.valueDecoder;
  c.varcharLength = column.varcharLength;
  c.vectorDimensions = column.vectorDimensions;
  c.arrayDimensions = column.arrayDimensions;
  c.enumDef = column.enumDef;
  c.identityKind = column.identityKind;
  c.generatedExpr = column.generatedExpr;
  c.customCodec = column.customCodec;
  c.isNotNull = column.isNotNull;
  c.hasDefault = column.hasDefault;
  c.defaultValue = column.defaultValue;
  c.nowDefault = column.nowDefault;
  c.canonicalText = column.canonicalText;
  c.ownerTable = view;
  return c as AnyColumnBuilder;
}

export interface ViewOptions {
  /** View body: SQL text or a `sql` template over the source tables'
   *  columns (references keep their table qualification). */
  definition: SchemaExpression;
  checkOption?: "local" | "cascaded";
  securityInvoker?: boolean;
}

function makeView(name: string, columns: Record<string, AnyColumnBuilder>, opts: ViewOptions, schema: string | undefined): AnyPgTable {
  checkSchemaObjectName(name, "pgView: name");
  if (opts === null || typeof opts !== "object" || !("definition" in opts)) {
    throw new Error(`pgView("${name}"): options must carry the view { definition }`);
  }
  if (opts.checkOption !== undefined && opts.checkOption !== "local" && opts.checkOption !== "cascaded") {
    throw new Error(`pgView("${name}"): checkOption must be "local" or "cascaded"`);
  }
  const view: { [TABLE_SYMBOL]?: unknown; $inferSelect?: never; $inferInsert?: never } = {
    [TABLE_SYMBOL]: {
      tableName: name,
      columns: {},
      indexes: [],
      constraints: [],
      viewDef: {
        definition: opts.definition,
        ...(opts.checkOption ? { checkOption: opts.checkOption } : {}),
        ...(opts.securityInvoker === true ? { securityInvoker: true } : {}),
      },
    },
    $inferSelect: undefined as never,
    $inferInsert: undefined as never,
  };
  if (schema !== undefined) (view[TABLE_SYMBOL] as { schema?: string }).schema = schema;
  const meta = view[TABLE_SYMBOL] as TableMetadata;
  const viewHandle = view as unknown as AnyPgTable;
  const cloned: Record<string, AnyColumnBuilder> = {};
  for (const [key, col] of Object.entries(columns)) {
    cloned[key] = cloneColumnForView(col, viewHandle);
  }
  (meta as { columns: unknown }).columns = cloned;
  const handle = { ...view, ...cloned } as unknown as AnyPgTable;
  (handle as { [TABLE_SYMBOL]?: unknown })[TABLE_SYMBOL] = meta;
  for (const col of Object.values(cloned)) {
    col.ownerTable = handle;
  }
  Object.freeze(meta);
  return handle;
}

/** Declare a view in the default schema (exported as public). Columns are
 *  typically source-table handles: `pgView("active", { id: users.id },
 *  { definition: sql`select ...` })`. Views are read-only. */
export function pgView<Cols extends Record<string, AnyColumnBuilder>>(name: string, columns: Cols, opts: ViewOptions): PgTable<Cols> {
  return makeView(name, columns, opts, undefined) as PgTable<Cols>;
}

export function isPgView(value: unknown): value is AnyPgTable {
  return isPgTable(value) && getViewDefinition(value) !== undefined;
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

/** Fail closed when a view handle reaches a mutation slot (Q07d): views are
 *  read-only here — no auto-updatable-view inference. */
export function rejectViewHandle(value: unknown, who: string): void {
  if (isPgTable(value) && getViewDefinition(value) !== undefined) {
    throw new Error(
      `${who}: "${getTableName(value)}" is a view — views are read-only in neutron-sql (no auto-updatable-view inference); write to a base table instead`,
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
