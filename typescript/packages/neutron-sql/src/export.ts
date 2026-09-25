// ---------------------------------------------------------------------------
// @neutron-build/sql — schema export (bridge for `neutron migrate generate`)
// ---------------------------------------------------------------------------
// Serializes schema objects to stable JSON. The Go CLI introspects the live
// database and diffs against this file to emit migration SQL.
//
// Version 1 (exportSchema) is the legacy writer the current CLI consumes.
// Version 2 (exportSchemaV2, F04) emits the cross-language schema contract
// document (contracts/data/schema-v2.json) deterministically — the canonical
// serialization below agrees byte-for-byte with the Go implementation and the
// contracts/data reference consumer. readSchemaDocumentV1 is the explicit v1
// compatibility reader: it upgrades legacy documents to v2 under the same
// rules as the Go upgrade reader (contracts/data/CANONICAL.md §6), reporting
// every ambiguity instead of guessing.

import type { AnyColumnBuilder, AnyPgEnum, AnyPgTable, PgEnumDefinition } from "./schema.js";
import { getEnumDefinition, getTableColumns, getTableConstraints, getTableName, getTableIndexes, getTableSchema, getViewDefinition, isPgEnum, isPgTable, rejectDerivedTable } from "./schema.js";
import { renderSchemaExpression } from "./ddl-text.js";

/** The v1 export contract covers the pre-Q07 surface only (default-search-
 *  path tables with plain columns, column-level constraints and simple
 *  column indexes). Every Q07 feature fails closed here — the v2 writer
 *  (exportSchemaV2) owns the full surface. */
function assertPlainTable(table: AnyPgTable, who: string): void {
  rejectDerivedTable(table, who);
  if (getViewDefinition(table) !== undefined) {
    throw new Error(`${who}: "${getTableName(table)}" is a view — the v1 export shape cannot represent views; use exportSchemaV2 (Q07)`);
  }
  const schema = getTableSchema(table);
  if (schema !== undefined) {
    throw new Error(
      `${who}: table "${schema}"."${getTableName(table)}" declares a schema — the v1 export shape covers the default search path only; use exportSchemaV2 (Q07)`,
    );
  }
  if (getTableConstraints(table).length > 0) {
    throw new Error(`${who}: table "${getTableName(table)}" declares table-level constraints — the v1 export shape cannot represent them; use exportSchemaV2 (Q07)`);
  }
  for (const idx of getTableIndexes(table)) {
    if (idx.keyParts.some((p) => p.expression !== undefined || p.order !== undefined || p.nulls !== undefined) || idx.whereExpr !== undefined || idx.includeCols.length > 0) {
      throw new Error(`${who}: index "${idx.indexName}" uses expressions, ordering options, a predicate or INCLUDE — the v1 export shape cannot represent it; use exportSchemaV2 (Q07)`);
    }
  }
  for (const col of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
    const at = `${who}: column "${col.columnName}" of "${getTableName(table)}"`;
    if (col.dataType === "enum" || col.enumDef !== undefined) throw new Error(`${at} is an enum column — the v1 export shape cannot represent enum types; use exportSchemaV2 (Q07)`);
    if (col.arrayDimensions !== undefined) throw new Error(`${at} is an array column — the v1 export shape cannot represent arrays; use exportSchemaV2 (Q07)`);
    if (col.identityKind !== undefined) throw new Error(`${at} is an identity column — the v1 export shape cannot represent identity defaults; use exportSchemaV2 (Q07)`);
    if (col.generatedExpr !== undefined) throw new Error(`${at} is a generated column — the v1 export shape cannot represent generated columns; use exportSchemaV2 (Q07)`);
    if (col.foreignKey?.onUpdate !== undefined) throw new Error(`${at} declares a foreign key with ON UPDATE — the v1 export shape drops it; use exportSchemaV2 (Q07)`);
  }
}
import type { TablesInput } from "./db.js";
import { quoteStringLiteral } from "./compile.js";
import { encodeWriteValue, type ColumnContext } from "./codecs.js";

export interface ExportedForeignKey {
  table: string;
  column: string;
  onDelete?: string;
}

export interface ExportedColumn {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
  hasDefault: boolean;
  default?: string;
  defaultNow?: boolean;
  varcharLength?: number;
  vectorDimensions?: number;
  /** Nucleus-only extension: skipped (with a comment) on vanilla Postgres. */
  nucleusOnly?: boolean;
  foreignKey?: ExportedForeignKey;
}

export interface ExportedIndex {
  name: string;
  unique: boolean;
  columns: string[];
}

export interface ExportedTable {
  name: string;
  columns: ExportedColumn[];
  indexes: ExportedIndex[];
}

export interface ExportedSchema {
  version: 1;
  tables: ExportedTable[];
}

export function exportSchema(tables: TablesInput): ExportedSchema {
  const out: ExportedTable[] = [];
  for (const value of Object.values(tables)) {
    if (isPgEnum(value)) {
      throw new Error(`exportSchema: enum "${getEnumDefinition(value).name}" cannot be exported by the legacy v1 writer — use exportSchemaV2 (Q07)`);
    }
    if (!isPgTable(value)) continue;
    out.push(exportTable(value));
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return { version: 1, tables: out };
}

export function exportTable(table: AnyPgTable): ExportedTable {
  assertPlainTable(table, "exportTable");
  const columns: ExportedColumn[] = [];
  for (const col of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
    const exported: ExportedColumn = {
      name: col.columnName,
      type: col.dataType,
      // primary keys are implicitly NOT NULL in PostgreSQL
      notNull: col.isNotNull || col.isPrimaryKey,
      primaryKey: col.isPrimaryKey,
      unique: col.isUnique,
      hasDefault: col.hasDefault,
    };
    if (col.hasDefault) {
      if (col.nowDefault) exported.defaultNow = true;
      else if (col.defaultValue !== undefined)
        exported.default = col.dataType === "text" || col.dataType === "varchar" || col.dataType === "uuid"
          ? String(col.defaultValue)
          : String(col.defaultValue);
    }
    if (col.varcharLength) exported.varcharLength = col.varcharLength;
    if (col.dataType === "vector") {
      // X01: the v1 shape cannot carry the pgvector capability; the old
      // nucleusOnly marking (skip-on-Postgres) is withdrawn — a skipped
      // column is a silently queried-but-nonexistent column. Export v2 owns
      // vector columns.
      throw new Error(`exportTable: column "${col.columnName}" of "${getTableName(table)}" is a vector column — the legacy v1 export shape cannot represent it (the pgvector capability and its dimension contract); use exportSchemaV2 (X01)`);
    }
    if (col.foreignKey) {
      const target = col.foreignKey();
      const targetTable = target.ownerTable;
      if (targetTable) {
        const fk: ExportedForeignKey = { table: getTableName(targetTable), column: target.columnName };
        if (col.foreignKey.onDelete) fk.onDelete = col.foreignKey.onDelete;
        exported.foreignKey = fk;
      }
    }
    columns.push(exported);
  }
  return {
    name: getTableName(table),
    columns,
    indexes: getTableIndexes(table).map((idx) => ({ name: idx.indexName, unique: idx.unique, columns: idx.columns.slice() })),
  };
}

// ---------------------------------------------------------------------------
// Schema document v2 (contracts/data/schema-v2.json)
// ---------------------------------------------------------------------------

export interface V2Identity {
  readonly schema: string;
  readonly name: string;
}

export interface V2TypeRef {
  readonly name: string;
  readonly codec: string;
  readonly params?: Readonly<Record<string, number>>;
  /** True when the column is a one-dimensional array of the base type. */
  readonly array?: boolean;
  /** Enum type identity (required when name is "enum"). */
  readonly enum?: V2Identity;
}

export type V2Default =
  | { readonly kind: "literal"; readonly sql: string }
  | { readonly kind: "expression"; readonly sql: string }
  | { readonly kind: "identity"; readonly generated: "always" | "by default" }
  | { readonly kind: "sequence"; readonly sequence: V2Identity };

/** Stored generated-column expression (Q07c). Virtual generated columns
 *  are explicitly unsupported (PostgreSQL < 18). */
export interface V2Generated {
  readonly expression: string;
}

export interface V2Column {
  readonly name: string;
  readonly type: V2TypeRef;
  readonly notNull: boolean;
  readonly default?: V2Default;
  readonly generated?: V2Generated;
}

export type V2ConstraintType = "primary-key" | "unique" | "foreign-key" | "check";

export interface V2Constraint {
  readonly name: string;
  readonly type: V2ConstraintType;
  readonly columns?: readonly string[];
  readonly expression?: string;
  readonly references?: {
    readonly table: V2Identity;
    readonly columns: readonly string[];
    readonly onDelete?: string;
    readonly onUpdate?: string;
    readonly match?: string;
  };
  readonly deferrable?: boolean;
  readonly initiallyDeferred?: boolean;
}

export interface V2IndexKeyPart {
  readonly column?: string;
  readonly expression?: string;
  readonly order?: "asc" | "desc";
  readonly nulls?: "first" | "last";
  /** Explicit operator class (X01): pgvector's hnsw/ivfflat indexes select
   *  their distance semantics this way (vector_ops / vector_cosine_ops /
   *  vector_ip_ops). Omitted = the method's default class for the type. */
  readonly opclass?: string;
}

export interface V2Index {
  readonly identity: V2Identity;
  readonly unique: boolean;
  readonly method: string;
  readonly key: readonly V2IndexKeyPart[];
  readonly where?: string;
  readonly include?: readonly string[];
  /** Access-method parameters (X01): `with (m = 16, ef_construction = 64)`
   *  on HNSW, `with (lists = 100)` on IVFFlat. Integer or string values;
   *  keys are plain identifiers. Canonical form sorts the keys. */
  readonly with?: Readonly<Record<string, number | string>>;
}

export interface V2Enum {
  readonly identity: V2Identity;
  readonly managed: boolean;
  readonly values: readonly string[];
}

export interface V2View {
  readonly identity: V2Identity;
  readonly managed: boolean;
  readonly definition: string;
  readonly checkOption?: "local" | "cascaded";
  readonly securityInvoker?: boolean;
}

export interface V2Table {
  readonly identity: V2Identity;
  readonly managed: boolean;
  readonly columns: readonly V2Column[];
  readonly constraints: readonly V2Constraint[];
  readonly indexes: readonly V2Index[];
}

export interface SchemaDocumentV2 {
  readonly version: 2;
  readonly dialect: "postgresql";
  readonly capabilities: readonly string[];
  readonly schemas: ReadonlyArray<{ readonly name: string }>;
  readonly tables: readonly V2Table[];
  readonly enums: readonly V2Enum[];
  readonly views: readonly V2View[];
  readonly opaque: readonly never[];
}

/** Schema input for exportSchemaV2: tables, views and enum objects keyed by
 *  any property name (keys are presentation only). */
export type SchemaV2Input = Record<string, AnyPgTable | AnyPgEnum>;

/** pg_catalog type names for the schema-builder type vocabulary, with the
 *  canonical v2 codec per type (contracts/data TYPE_CODECS). */
const V2_TYPE_NAMES: Record<string, string> = {
  serial: "int4", integer: "int4", smallint: "int2", bigint: "int8",
  double: "float8", real: "float4", numeric: "numeric",
  text: "text", varchar: "varchar", boolean: "bool",
  timestamp: "timestamp", timestamptz: "timestamptz", date: "date",
  json: "json", jsonb: "jsonb", uuid: "uuid", bytea: "bytea", vector: "vector",
  tsvector: "tsvector",
};
const V2_TYPE_CODECS: Record<string, string> = {
  bool: "boolean", int2: "number", int4: "number", int8: "bigint",
  float4: "number", float8: "number", numeric: "decimal-string",
  text: "string", varchar: "string",
  timestamp: "timestamp-string", timestamptz: "timestamptz-string", date: "date-string",
  bytea: "binary", uuid: "uuid", json: "json", jsonb: "json", vector: "vector",
  tsvector: "tsvector",
};
const V2_INDEX_METHODS = new Set(["btree", "hash", "gin", "gist", "spgist", "brin", "hnsw", "ivfflat"]);
const V2_IDENTITY_TYPES = new Set(["serial", "integer", "smallint", "bigint"]);

/** Default-operator-class facts for the index methods and column types in
 *  the vocabulary: exactly these combinations apply on PostgreSQL without
 *  naming an operator class (built-in pg_opclass defaults; arrays uniformly
 *  via array_ops). Everything else is refused by the server with SQLSTATE
 *  42704, so export refuses it at definition time — the contract has no
 *  operator-class slot to spell an explicit one. Derived from a live probe
 *  on PostgreSQL 17 and the REL_15_STABLE pg_opclass catalog source; the
 *  two agree on every vocabulary combination. */
const V2_INDEX_METHOD_SCALARS: Record<string, ReadonlySet<string>> = {
  btree: new Set(["text", "varchar", "bool", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea", "enum", "jsonb"]),
  hash: new Set(["text", "varchar", "bool", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea", "enum", "jsonb"]),
  gin: new Set(["jsonb", "tsvector"]),
  gist: new Set(),
  spgist: new Set(["text", "varchar"]),
  brin: new Set(["text", "varchar", "int2", "int4", "int8", "float4", "float8", "numeric", "timestamp", "timestamptz", "date", "uuid", "bytea"]),
  // pgvector access methods (X01): both apply to the vector type; the
  // default operator class is vector_ops (L2). cosine/inner-product indexes
  // name vector_cosine_ops / vector_ip_ops explicitly (key-part opclass).
  hnsw: new Set(["vector"]),
  ivfflat: new Set(["vector"]),
};
const V2_INDEX_METHOD_ARRAYS = new Set(["btree", "hash", "gin"]);

const V2_LITERAL_PATTERN = /^('([^']|'')*'(::[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?(\[\])*)?|-?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?|true|false|null)$/;
const V2_NUMERIC_DEFAULT = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;

function exportError(code: string, at: string, detail: string): Error {
  return new Error(`[${code}] ${at}: ${detail}`);
}

/** Tag a declared default (a JS value from `.default(...)`) as a v2 default.
 *  The codec canonicalizes the value (validation with column context); the
 *  literal spelling is a deterministic function of the canonical value. */
function v2DefaultFor(tableId: V2Identity, column: AnyColumnBuilder): V2Default | undefined {
  const at = `tables[${tableId.schema}.${tableId.name}].columns[${column.columnName}].default`;
  if (column.dataType === "serial") {
    // The schema DDL `serial` creates exactly this named sequence; an
    // explicit default alongside it is ambiguous and rejected above.
    return { kind: "sequence", sequence: { schema: tableId.schema, name: `${tableId.name}_${column.columnName}_seq` } };
  }
  if (column.identityKind !== undefined) {
    return { kind: "identity", generated: column.identityKind };
  }
  if (column.nowDefault) return { kind: "expression", sql: "now()" };
  if (!column.hasDefault) return undefined;
  if (column.defaultValue === undefined) {
    throw exportError("invalid-default", at, "default is declared without a value — exportSchemaV2 cannot spell it");
  }
  const ctx: ColumnContext = { propertyKey: column.columnName, columnName: column.columnName, tableName: tableId.name };
  const encoded = encodeWriteValue(column, ctx, column.defaultValue);
  const dt = column.dataType;
  let sql: string;
  if (column.arrayDimensions !== undefined) {
    // The encoded bind is the array literal text; spell it as one string
    // literal cast to the element type. Enum array casts are quoted type
    // names, which the literal vocabulary cannot spell — rejected below.
    sql = `${quoteStringLiteral(String(encoded.bind))}::${(encoded.cast ?? "").replace(/"/g, "")}`;
    if ((encoded.cast ?? "").includes('"')) {
      throw exportError(
        "invalid-literal",
        at,
        `array default needs a quoted enum type cast (${encoded.cast}) — not spellable as a contract literal; enum array column defaults are explicitly unsupported`,
      );
    }
  } else if (dt === "boolean") {
    sql = encoded.bind === true ? "true" : "false";
  } else if (dt === "integer" || dt === "smallint" || dt === "bigint" || dt === "double" || dt === "real" || dt === "numeric") {
    sql = String(encoded.bind);
    if (!V2_NUMERIC_DEFAULT.test(sql)) {
      throw exportError("invalid-default", at, `numeric default ${JSON.stringify(sql)} is not a plain numeric literal`);
    }
  } else if (dt === "bytea") {
    const bytes = encoded.bind as Uint8Array;
    sql = `\\x${[...bytes].map((b) => b.toString(16).padStart(2, "0")).join("")}`;
    sql = quoteStringLiteral(sql);
  } else {
    sql = quoteStringLiteral(String(encoded.bind));
  }
  if (!V2_LITERAL_PATTERN.test(sql)) {
    throw exportError("invalid-literal", at, `default does not spell exactly one SQL literal token: ${JSON.stringify(sql)}`);
  }
  return { kind: "literal", sql };
}

function v2TypeFor(tableId: V2Identity, column: AnyColumnBuilder): V2TypeRef {
  const at = `tables[${tableId.schema}.${tableId.name}].columns[${column.columnName}].type`;
  const typeName = column.dataType === "enum" ? "enum" : V2_TYPE_NAMES[column.dataType];
  if (typeName === undefined) {
    throw exportError("unknown-type", at, `unknown column type ${JSON.stringify(column.dataType)}`);
  }
  if (column.identityKind !== undefined && !V2_IDENTITY_TYPES.has(column.dataType)) {
    throw exportError("invalid-default", `${at}.default`, "identity columns must be integer, smallint or bigint");
  }
  if (column.arrayDimensions !== undefined && column.dataType === "enum" && column.enumDef === undefined) {
    throw exportError("unknown-type", at, "enum array column carries no enum definition");
  }
  const ref: { name: string; codec: string; params?: Record<string, number>; array?: boolean; enum?: V2Identity } =
    column.arrayDimensions !== undefined
      ? { name: typeName, codec: "array", array: true }
      : { name: typeName, codec: column.dataType === "enum" ? "enum" : V2_TYPE_CODECS[typeName] };
  if (column.dataType === "vector") {
    if (typeof column.vectorDimensions !== "number") {
      throw exportError("invalid-type-params", at, "vector columns require dimensions");
    }
    ref.params = { dimensions: column.vectorDimensions };
  } else if (column.dataType === "varchar" && column.varcharLength) {
    ref.params = { length: column.varcharLength };
  }
  if (column.dataType === "enum") {
    if (column.enumDef === undefined) {
      throw exportError("unknown-type", at, "enum column carries no enum definition");
    }
    ref.enum = { schema: column.enumDef.schema ?? "public", name: column.enumDef.name };
  }
  return ref;
}

/** Normalize an index key part to the contract's minimal form: order is
 *  written only for DESC, nulls only when it is not the direction default
 *  (ASC defaults to NULLS LAST, DESC to NULLS FIRST). This is exactly what
 *  introspection derives from pg_index.indoption, so desired and
 *  introspected documents canonicalize identically. */
function v2IndexKeyPart(part: { column?: string; expression?: unknown; order?: "asc" | "desc"; nulls?: "first" | "last"; opclass?: string }): V2IndexKeyPart {
  const order = part.order === "desc" ? ("desc" as const) : undefined;
  const nulls =
    (order === undefined && part.nulls === "first") || (order === "desc" && part.nulls === "last")
      ? part.nulls
      : undefined;
  return {
    ...(part.column !== undefined ? { column: part.column } : {}),
    ...(part.expression !== undefined ? { expression: part.expression as string } : {}),
    ...(order ? { order } : {}),
    ...(nulls ? { nulls } : {}),
    ...(part.opclass !== undefined ? { opclass: part.opclass } : {}),
  };
}

/** Export the given tables, views and enums as a schema document v2.
 * Deterministic: the same objects (in any input key order) produce the same
 * document and the same canonical bytes. Columns keep declaration order
 * (attnum semantics); tables/enums/views/constraints/indexes are emitted in
 * a stable order and sorted sets normalize further in canonicalSchemaJson.
 * Every schema/enum/identity the document references is validated here the
 * same way the Go contract validator does, so an exported document always
 * validates. */
export function exportSchemaV2(input: SchemaV2Input): SchemaDocumentV2 {
  const exported: AnyPgTable[] = [];
  const viewHandles: AnyPgTable[] = [];
  const standaloneEnums = new Map<string, { def: PgEnumDefinition; at: string }>();
  for (const [key, value] of Object.entries(input)) {
    if (isPgEnum(value)) {
      const def = getEnumDefinition(value);
      const id = `${def.schema ?? "public"}.${def.name}`;
      if (standaloneEnums.has(id)) {
        throw exportError("duplicate-enum", `enums[${id}]`, `enum ${JSON.stringify(id)} is declared twice (input key ${JSON.stringify(key)})`);
      }
      standaloneEnums.set(id, { def, at: `input[${JSON.stringify(key)}]` });
      continue;
    }
    if (!isPgTable(value)) continue;
    rejectDerivedTable(value, "exportSchemaV2");
    if (getViewDefinition(value) !== undefined) {
      viewHandles.push(value);
      continue;
    }
    exported.push(value);
  }
  const byId = (t: AnyPgTable): string => `${getTableSchema(t) ?? "public"}.${getTableName(t)}`;
  exported.sort((a, b) => byteCompare(byId(a), byId(b)));
  const tableIds = new Map(exported.map((t) => [t, byId(t)]));

  // Schemas: public is always declared; every object adds its own.
  const schemas = new Set<string>(["public"]);
  for (const t of exported) schemas.add(getTableSchema(t) ?? "public");
  for (const v of viewHandles) schemas.add(getTableSchema(v) ?? "public");

  // Enums: standalone declarations plus every enum referenced by a column.
  const enums = new Map<string, { def: PgEnumDefinition; at: string }>(standaloneEnums);
  for (const table of exported) {
    for (const column of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
      if (column.enumDef === undefined) continue;
      const id = `${column.enumDef.schema ?? "public"}.${column.enumDef.name}`;
      const existing = enums.get(id);
      if (existing !== undefined && existing.def !== column.enumDef) {
        // Same identity declared through two pgEnum objects: values must agree.
        if (existing.def.values.length !== column.enumDef.values.length || existing.def.values.some((v, i) => v !== column.enumDef!.values[i])) {
          throw exportError(
            "duplicate-enum",
            `enums[${id}]`,
            `enum ${JSON.stringify(id)} is declared twice with different values (${existing.at} vs tables[${byId(table)}].columns[${column.columnName}])`,
          );
        }
      } else if (existing === undefined) {
        enums.set(id, { def: column.enumDef, at: `tables[${byId(table)}].columns[${column.columnName}]` });
      }
      schemas.add(column.enumDef.schema ?? "public");
    }
  }
  for (const id of enums.keys()) schemas.add(id.slice(0, id.indexOf(".")));

  const enumList: V2Enum[] = [...enums.entries()]
    .sort((a, b) => byteCompare(a[0], b[0]))
    .map(([id, e]) => ({ identity: { schema: id.slice(0, id.indexOf(".")), name: id.slice(id.indexOf(".") + 1) }, managed: true, values: [...e.def.values] }));

  const out: V2Table[] = [];
  const keyTuples = new Map<string, string[][]>();
  let hasVector = false;

  for (const table of exported) {
    const tableId: V2Identity = { schema: getTableSchema(table) ?? "public", name: getTableName(table) };
    const columns: V2Column[] = [];
    const constraints: V2Constraint[] = [];
    const pkColumns: string[] = [];
    const tableConstraints = getTableConstraints(table);
    const tablePk = tableConstraints.find((c) => c.kind === "primary-key");
    const seenColumnNames = new Set<string>();
    const columnTypes = new Map<string, { name: string; array: boolean }>();

    for (const column of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
      const at = `tables[${tableId.schema}.${tableId.name}].columns[${column.columnName}]`;
      if (seenColumnNames.has(column.columnName)) {
        throw exportError("duplicate-column", at, `column ${JSON.stringify(column.columnName)} is declared twice`);
      }
      seenColumnNames.add(column.columnName);
      if (column.dataType === "serial") {
        if (column.defaultValue !== undefined || column.nowDefault) {
          throw exportError(
            "invalid-default",
            `${at}.default`,
            "serial column with an explicit default is not representable — declare integer with an explicit default, or plain serial",
          );
        }
      }
      if (column.dataType === "vector") hasVector = true;
      const inTablePk = tablePk !== undefined && tablePk.columns.includes(column.columnName);
      if (column.isPrimaryKey && tablePk !== undefined) {
        throw exportError(
          "multiple-primary-key",
          at,
          "column-level .primaryKey() and a table-level primaryKey() are declared — a table has exactly one primary key",
        );
      }
      const generated: V2Generated | undefined =
        column.generatedExpr !== undefined
          ? { expression: renderSchemaExpression(column.generatedExpr, { what: `${at}.generated expression`, table }) }
          : undefined;
      if (generated !== undefined && column.hasDefault) {
        throw exportError("invalid-default", `${at}.default`, "a generated column cannot also declare a default");
      }
      const def = v2DefaultFor(tableId, column);
      const v2Type = v2TypeFor(tableId, column);
      columnTypes.set(column.columnName, { name: v2Type.name, array: v2Type.array === true });
      const v2Column: V2Column = {
        name: column.columnName,
        type: v2Type,
        notNull: column.isNotNull || column.isPrimaryKey || inTablePk,
        ...(def ? { default: def } : {}),
        ...(generated ? { generated } : {}),
      };
      columns.push(v2Column);

      if (column.isPrimaryKey) pkColumns.push(column.columnName);
      if (column.isUnique) {
        constraints.push({ name: `${tableId.name}_${column.columnName}_key`, type: "unique", columns: [column.columnName] });
      }
      if (column.foreignKey) {
        const target = column.foreignKey();
        const targetTable = target?.ownerTable;
        if (!target) {
          throw exportError("fk-target", at, "foreign key thunk resolved to nothing");
        }
        if (!targetTable) {
          throw exportError("fk-target", at, "foreign key references a column with no owning table");
        }
        if (getViewDefinition(targetTable) !== undefined) {
          throw exportError("fk-target", at, "foreign key references a view — targets must be tables");
        }
        const targetName = getTableName(targetTable);
        const targetSchema = getTableSchema(targetTable) ?? "public";
        if (!tableIds.has(targetTable)) {
          throw exportError(
            "fk-target",
            at,
            `foreign key references table "${targetSchema}"."${targetName}", which is not part of the exported schema`,
          );
        }
        const references: V2Constraint["references"] = {
          table: { schema: targetSchema, name: targetName },
          columns: [target.columnName],
          ...(column.foreignKey.onDelete ? { onDelete: column.foreignKey.onDelete } : {}),
          ...(column.foreignKey.onUpdate ? { onUpdate: column.foreignKey.onUpdate } : {}),
        };
        constraints.push({
          name: `${tableId.name}_${column.columnName}_fkey`,
          type: "foreign-key",
          columns: [column.columnName],
          references,
        });
      }
    }

    if (pkColumns.length > 0) {
      constraints.push({ name: `${tableId.name}_pkey`, type: "primary-key", columns: [...pkColumns] });
    }

    for (const extra of tableConstraints) {
      const at = `tables[${tableId.schema}.${tableId.name}].constraints`;
      const assertColumns = (cols: readonly string[], what: string): void => {
        if (!Array.isArray(cols) || cols.length === 0) throw exportError("constraint-column", at, `${what} must list at least one column`);
        for (const c of cols) {
          if (!seenColumnNames.has(c)) {
            throw exportError("constraint-column", `${at}[${c}]`, `${what} references unknown column ${JSON.stringify(c)}`);
          }
        }
      };
      if (extra.kind === "primary-key") {
        assertColumns(extra.columns, "primaryKey");
        constraints.push({
          name: extra.name ?? `${tableId.name}_pkey`,
          type: "primary-key",
          columns: [...extra.columns],
          ...(extra.deferrable !== undefined ? { deferrable: extra.deferrable } : {}),
          ...(extra.initiallyDeferred !== undefined ? { initiallyDeferred: extra.initiallyDeferred } : {}),
        });
      } else if (extra.kind === "unique") {
        assertColumns(extra.columns, "unique");
        constraints.push({
          name: extra.name ?? `${tableId.name}_${extra.columns.join("_")}_key`,
          type: "unique",
          columns: [...extra.columns],
          ...(extra.deferrable !== undefined ? { deferrable: extra.deferrable } : {}),
          ...(extra.initiallyDeferred !== undefined ? { initiallyDeferred: extra.initiallyDeferred } : {}),
        });
      } else if (extra.kind === "check") {
        constraints.push({
          name: extra.name,
          type: "check",
          expression: renderSchemaExpression(extra.expression, { what: `${at}[${extra.name}].expression`, table }),
          ...(extra.deferrable !== undefined ? { deferrable: extra.deferrable } : {}),
          ...(extra.initiallyDeferred !== undefined ? { initiallyDeferred: extra.initiallyDeferred } : {}),
        });
      } else {
        assertColumns(extra.columns, "foreignKey");
        if (extra.refTable === undefined || extra.refColumns.length !== extra.columns.length) {
          throw exportError("fk-target", at, "table-level foreign key has no resolved .references(table, columns) target");
        }
        if (getViewDefinition(extra.refTable) !== undefined) {
          throw exportError("fk-target", at, "foreign key references a view — targets must be tables");
        }
        if (!tableIds.has(extra.refTable)) {
          throw exportError(
            "fk-target",
            at,
            `foreign key references table "${getTableSchema(extra.refTable) ?? "public"}"."${getTableName(extra.refTable)}", which is not part of the exported schema`,
          );
        }
        constraints.push({
          name: extra.name ?? `${tableId.name}_${extra.columns.join("_")}_fkey`,
          type: "foreign-key",
          columns: [...extra.columns],
          references: {
            table: { schema: getTableSchema(extra.refTable) ?? "public", name: getTableName(extra.refTable) },
            columns: [...extra.refColumns],
            ...(extra.onDelete ? { onDelete: extra.onDelete } : {}),
            ...(extra.onUpdate ? { onUpdate: extra.onUpdate } : {}),
            ...(extra.match ? { match: extra.match } : {}),
          },
          ...(extra.deferrable !== undefined ? { deferrable: extra.deferrable } : {}),
          ...(extra.initiallyDeferred !== undefined ? { initiallyDeferred: extra.initiallyDeferred } : {}),
        });
      }
    }

    // Column-level FK targets must be covered by a primary-key or unique
    // constraint (the contract's cross-object rule), evaluated after all
    // constraints are known — collect tuples first, verify below.
    keyTuples.set(
      `${tableId.schema}.${tableId.name}`,
      constraints.filter((c) => c.type === "primary-key" || c.type === "unique").map((c) => [...(c.columns ?? [])]),
    );

    out.push({
      identity: tableId,
      managed: true,
      columns,
      constraints,
      indexes: getTableIndexes(table).map((idx) => {
        const idxAt = `tables[${tableId.schema}.${tableId.name}].indexes[${idx.indexName}]`;
        if (!V2_INDEX_METHODS.has(idx.methodValue)) {
          throw exportError("unknown-index-method", idxAt, `index method ${JSON.stringify(idx.methodValue)} is not in the contract vocabulary`);
        }
        if (idx.includeCols.length > 0 && idx.methodValue !== "btree") {
          throw exportError("invalid-index", idxAt, "INCLUDE is a btree-only feature");
        }
        if (idx.keyParts.length === 0) {
          throw exportError("index-key", idxAt, "index must declare at least one key part");
        }
        const key = idx.keyParts.map((p, i) => {
          if (p.column !== undefined) {
            if (!seenColumnNames.has(p.column)) {
              throw exportError("index-column", `${idxAt}[${i}]`, `index references unknown column ${JSON.stringify(p.column)}`);
            }
            const colType = columnTypes.get(p.column);
            if (
              p.opclass === undefined &&
              colType !== undefined &&
              !(colType.array ? V2_INDEX_METHOD_ARRAYS.has(idx.methodValue) : (V2_INDEX_METHOD_SCALARS[idx.methodValue] ?? new Set()).has(colType.name))
            ) {
              throw exportError(
                "invalid-index",
                `${idxAt}[${i}]`,
                `index method ${JSON.stringify(idx.methodValue)} over column ${JSON.stringify(p.column)} (${colType.name}${colType.array ? "[]" : ""}) has no default operator class on any supported server (PostgreSQL refuses it with SQLSTATE 42704) — explicitly unsupported — contract has no operator-class slot`,
              );
            }
            return v2IndexKeyPart(p);
          }
          if (p.expression === undefined) throw exportError("index-key", `${idxAt}[${i}]`, "index key part has neither column nor expression");
          if (p.order !== undefined || p.nulls !== undefined) {
            throw exportError("index-key", `${idxAt}[${i}]`, "ordered expression keys are explicitly unsupported (per-part expression deparse is engine-dependent)");
          }
          if (idx.methodValue !== "btree") {
            throw exportError(
              "invalid-index",
              `${idxAt}[${i}]`,
              'expression keys are only definable with method "btree" — the default operator class of an expression result type cannot be verified at definition time (contract has no operator-class slot)',
            );
          }
          return v2IndexKeyPart({
            expression: renderSchemaExpression(p.expression, { what: `${idxAt}[${i}].expression`, table }),
          });
        });
        for (const inc of idx.includeCols) {
          if (!seenColumnNames.has(inc)) {
            throw exportError("index-column", idxAt, `INCLUDE references unknown column ${JSON.stringify(inc)}`);
          }
        }
        let withParams: Readonly<Record<string, number | string>> | undefined;
        if (idx.withParams !== undefined) {
          for (const [k, v] of Object.entries(idx.withParams)) {
            if (!/^[a-z_][a-z0-9_]*$/.test(k)) {
              throw exportError("invalid-index", `${idxAt}.with[${k}]`, "access-method parameter names must be plain lowercase identifiers");
            }
            if (typeof v !== "number" || !Number.isInteger(v)) {
              throw exportError("invalid-index", `${idxAt}.with[${k}]`, "access-method parameters are integers (m, ef_construction, lists, fillfactor)");
            }
          }
          withParams = { ...idx.withParams };
        }
        return {
          identity: { schema: tableId.schema, name: idx.indexName },
          unique: idx.unique,
          method: idx.methodValue,
          key,
          ...(idx.whereExpr !== undefined ? { where: renderSchemaExpression(idx.whereExpr, { what: `${idxAt}.where`, table }) } : {}),
          ...(idx.includeCols.length > 0 ? { include: [...idx.includeCols] } : {}),
          ...(withParams !== undefined ? { with: withParams } : {}),
        };
      }),
    });
  }

  // FK target coverage (mirrors the Go validator's cross-reference rule).
  for (const table of out) {
    const tableKey = `${table.identity.schema}.${table.identity.name}`;
    for (const constraint of table.constraints) {
      if (constraint.type !== "foreign-key" || constraint.references === undefined) continue;
      const targetKey = `${constraint.references.table.schema}.${constraint.references.table.name}`;
      const tuples = keyTuples.get(targetKey) ?? [];
      const refCols = constraint.references.columns;
      if (!tuples.some((tuple) => tuple.length === refCols.length && tuple.every((c, i) => c === refCols[i]))) {
        throw exportError(
          "fk-not-unique",
          `tables[${tableKey}].constraints[${constraint.name}]`,
          `foreign key references (${refCols.join(", ")}) on ${targetKey}, which is not covered by a primary-key or unique constraint`,
        );
      }
    }
  }

  // Views: rendered definitions, exported under their schema.
  const views: V2View[] = [];
  const seenViews = new Set<string>();
  for (const view of viewHandles) {
    const viewDef = getViewDefinition(view)!;
    const id: V2Identity = { schema: getTableSchema(view) ?? "public", name: getTableName(view) };
    const key = `${id.schema}.${id.name}`;
    if (seenViews.has(key)) throw exportError("duplicate-view", `views[${key}]`, `view ${JSON.stringify(key)} is declared twice`);
    seenViews.add(key);
    views.push({
      identity: id,
      managed: true,
      definition: renderSchemaExpression(viewDef.definition, { what: `views[${key}].definition` }),
      ...(viewDef.checkOption ? { checkOption: viewDef.checkOption } : {}),
      ...(viewDef.securityInvoker === true ? { securityInvoker: true } : {}),
    });
  }
  views.sort((a, b) => (a.identity.schema !== b.identity.schema ? byteCompare(a.identity.schema, b.identity.schema) : byteCompare(a.identity.name, b.identity.name)));

  return {
    version: 2,
    dialect: "postgresql",
    // X01: vector columns mean the POSTGRESQL pgvector extension (detected
    // per-database via pg_extension — separate from engine capabilities and
    // from Nucleus model capabilities). Backends without it fail migrations
    // and queries; they never skip the column.
    capabilities: hasVector ? ["pgvector"] : [],
    schemas: [...schemas].sort(byteCompare).map((name) => ({ name })),
    tables: out,
    enums: enumList,
    views,
    opaque: [],
  };
}

// ---------------------------------------------------------------------------
// Canonical serialization (contracts/data/CANONICAL.md) — deterministic bytes
// for hashing; the Go implementation and the reference consumer must agree.
// ---------------------------------------------------------------------------

function byteCompare(a: string, b: string): number {
  const ac = [...a];
  const bc = [...b];
  const n = Math.min(ac.length, bc.length);
  for (let i = 0; i < n; i++) {
    const x = ac[i].codePointAt(0)!;
    const y = bc[i].codePointAt(0)!;
    if (x !== y) return x - y;
  }
  return ac.length - bc.length;
}

function canonicalString(s: string): string {
  let out = '"';
  for (const ch of s) {
    const cp = ch.codePointAt(0)!;
    if (cp === 0x22) out += '\\"';
    else if (cp === 0x5c) out += "\\\\";
    else if (cp === 8) out += "\\b";
    else if (cp === 9) out += "\\t";
    else if (cp === 10) out += "\\n";
    else if (cp === 12) out += "\\f";
    else if (cp === 13) out += "\\r";
    else if (cp < 0x20) out += "\\u00" + cp.toString(16).padStart(2, "0");
    else out += ch;
  }
  return out + '"';
}

/** Serialize a schema document v2 into its canonical form: object keys sorted
 *  bytewise, set collections (capabilities, schemas, tables, constraints,
 *  indexes, enums, views, opaque, index INCLUDE) sorted, ordered tuples
 *  (columns, constraint columns, references columns, index key parts, enum
 *  values) preserved, minimal string escaping, no whitespace. Pure: same
 *  document → identical bytes on every machine. Byte-identical to the Go
 *  canonicalizer (cli/internal/db schema_v2.go v2CanonicalBytes) and the
 *  contracts/data reference consumer. */
export function canonicalSchemaJson(doc: SchemaDocumentV2): string {
  const parts: string[] = [];
  parts.push('{"capabilities":[');
  [...doc.capabilities].sort(byteCompare).forEach((c, i) => {
    if (i > 0) parts.push(",");
    parts.push(canonicalString(c));
  });
  parts.push('],"dialect":"postgresql","enums":[');
  [...doc.enums]
    .sort((a, b) => identityCompare(a.identity, b.identity))
    .forEach((e, i) => {
      if (i > 0) parts.push(",");
      parts.push(`{"identity":${canonicalIdentity(e.identity)},"managed":${e.managed},"values":[`);
      parts.push(e.values.map(canonicalString).join(","));
      parts.push("]}");
    });
  parts.push('],"opaque":[');
  parts.push('],"schemas":[');
  [...doc.schemas].sort((a, b) => byteCompare(a.name, b.name)).forEach((s, i) => {
    if (i > 0) parts.push(",");
    parts.push(`{"name":${canonicalString(s.name)}}`);
  });
  parts.push('],"tables":[');
  const tables = [...doc.tables].sort((a, b) => identityCompare(a.identity, b.identity));
  tables.forEach((t, i) => {
    if (i > 0) parts.push(",");
    parts.push('{"columns":[');
    t.columns.forEach((c, j) => {
      if (j > 0) parts.push(",");
      parts.push("{");
      if (c.default) parts.push(`"default":${canonicalDefault(c.default)},`);
      if (c.generated) parts.push(`"generated":{"expression":${canonicalString(c.generated.expression)}},`);
      parts.push(`"name":${canonicalString(c.name)},"notNull":${c.notNull},"type":`);
      parts.push(canonicalType(c.type));
      parts.push("}");
    });
    parts.push('],"constraints":[');
    [...t.constraints].sort((a, b) => byteCompare(a.name, b.name)).forEach((c, j) => {
      if (j > 0) parts.push(",");
      parts.push("{");
      if (c.columns && c.columns.length > 0) parts.push(`"columns":[${c.columns.map(canonicalString).join(",")}],`);
      if (c.deferrable !== undefined) parts.push(`"deferrable":${c.deferrable},`);
      if (c.expression !== undefined) parts.push(`"expression":${canonicalString(c.expression)},`);
      if (c.initiallyDeferred !== undefined) parts.push(`"initiallyDeferred":${c.initiallyDeferred},`);
      parts.push(`"name":${canonicalString(c.name)}`);
      if (c.references) {
        const r = c.references;
        parts.push(',"references":{"columns":[');
        parts.push(r.columns.map(canonicalString).join(","));
        parts.push("]");
        if (r.match) parts.push(`,"match":${canonicalString(r.match)}`);
        if (r.onDelete) parts.push(`,"onDelete":${canonicalString(r.onDelete)}`);
        if (r.onUpdate) parts.push(`,"onUpdate":${canonicalString(r.onUpdate)}`);
        parts.push(`,"table":${canonicalIdentity(r.table)}}`);
      }
      parts.push(`,"type":${canonicalString(c.type)}}`);
    });
    parts.push(`],"identity":${canonicalIdentity(t.identity)},"indexes":[`);
    [...t.indexes]
      .sort((a, b) => identityCompare(a.identity, b.identity))
      .forEach((idx, j) => {
        if (j > 0) parts.push(",");
        parts.push(`{"identity":${canonicalIdentity(idx.identity)}`);
        if (idx.include && idx.include.length > 0) {
          parts.push(`,"include":[${[...idx.include].sort(byteCompare).map(canonicalString).join(",")}]`);
        }
        parts.push(',"key":[');
        idx.key.forEach((part, k) => {
          if (k > 0) parts.push(",");
          parts.push("{");
          if (part.column !== undefined) parts.push(`"column":${canonicalString(part.column)}`);
          if (part.expression !== undefined) parts.push(`${part.column !== undefined ? "," : ""}"expression":${canonicalString(part.expression)}`);
          if (part.nulls !== undefined) parts.push(`,"nulls":${canonicalString(part.nulls)}`);
          if (part.opclass !== undefined) parts.push(`,"opclass":${canonicalString(part.opclass)}`);
          if (part.order !== undefined) parts.push(`,"order":${canonicalString(part.order)}`);
          parts.push("}");
        });
        parts.push(`],"method":${canonicalString(idx.method)},"unique":${idx.unique}`);
        if (idx.where !== undefined) parts.push(`,"where":${canonicalString(idx.where)}`);
        if (idx.with !== undefined) {
          const keys = Object.keys(idx.with).sort(byteCompare);
          parts.push(`,"with":{`);
          keys.forEach((k, n) => {
            if (n > 0) parts.push(",");
            const v = idx.with![k];
            parts.push(`${canonicalString(k)}:${typeof v === "string" ? canonicalString(v) : String(v)}`);
          });
          parts.push("}");
        }
        parts.push("}");
      });
    parts.push(`],"managed":${t.managed}}`);
  });
  parts.push('],"version":2,"views":[');
  [...doc.views]
    .sort((a, b) => identityCompare(a.identity, b.identity))
    .forEach((v, i) => {
      if (i > 0) parts.push(",");
      parts.push("{");
      if (v.checkOption !== undefined) parts.push(`"checkOption":${canonicalString(v.checkOption)},`);
      parts.push(`"definition":${canonicalString(v.definition)},"identity":${canonicalIdentity(v.identity)},"managed":${v.managed}`);
      if (v.securityInvoker !== undefined) parts.push(`,"securityInvoker":${v.securityInvoker}`);
      parts.push("}");
    });
  parts.push("]}");
  return parts.join("");
}

function identityCompare(a: V2Identity, b: V2Identity): number {
  return a.schema !== b.schema ? byteCompare(a.schema, b.schema) : byteCompare(a.name, b.name);
}

function canonicalIdentity(id: V2Identity): string {
  return `{"name":${canonicalString(id.name)},"schema":${canonicalString(id.schema)}}`;
}

function canonicalType(type: V2TypeRef): string {
  const parts: string[] = ["{"];
  if (type.array === true) parts.push('"array":true,');
  parts.push(`"codec":${canonicalString(type.codec)}`);
  if (type.enum) parts.push(`,"enum":${canonicalIdentity(type.enum)}`);
  parts.push(`,"name":${canonicalString(type.name)}`);
  if (type.params) {
    const keys = Object.keys(type.params).sort(byteCompare);
    if (keys.length > 0) {
      parts.push(',"params":{');
      keys.forEach((k, i) => {
        if (i > 0) parts.push(",");
        parts.push(`${canonicalString(k)}:${type.params![k]}`);
      });
      parts.push("}");
    }
  }
  parts.push("}");
  return parts.join("");
}

function canonicalDefault(def: V2Default): string {
  if (def.kind === "sequence") {
    return `{"kind":"sequence","sequence":{"name":${canonicalString(def.sequence.name)},"schema":${canonicalString(def.sequence.schema)}}}`;
  }
  if (def.kind === "identity") {
    return `{"generated":${canonicalString(def.generated)},"kind":"identity"}`;
  }
  return `{"kind":${canonicalString(def.kind)},"sql":${canonicalString(def.sql)}}`;
}

// ---------------------------------------------------------------------------
// v1 compatibility reader — legacy exportSchema documents → v2, under the
// same rules as the Go upgrade reader (CANONICAL.md §6).
// ---------------------------------------------------------------------------

interface V1ColumnInput {
  name?: unknown;
  type?: unknown;
  notNull?: unknown;
  primaryKey?: unknown;
  unique?: unknown;
  uniqueName?: unknown;
  hasDefault?: unknown;
  default?: unknown;
  defaultNow?: unknown;
  varcharLength?: unknown;
  vectorDimensions?: unknown;
  nucleusOnly?: unknown;
  foreignKey?: unknown;
}

interface V1TableInput {
  name?: unknown;
  columns?: unknown;
  indexes?: unknown;
}

/** Read a legacy version-1 export document (the exportSchema shape) and
 *  upgrade it to a schema document v2. Upgrades only what the v1 shape
 *  determines unambiguously; every ambiguity — defaults that could be a
 *  literal or an SQL expression, serial columns with explicit defaults —
 *  fails with [ambiguous-default] naming the field, never a guess. */
export function readSchemaDocumentV1(input: unknown): SchemaDocumentV2 {
  let root: unknown = input;
  if (typeof input === "string") {
    try {
      root = JSON.parse(input);
    } catch (err) {
      throw exportError("invalid-json", "$", err instanceof RangeError ? "parser recursion depth exceeded" : String(err));
    }
  }
  if (root === null || typeof root !== "object" || Array.isArray(root)) {
    throw exportError("invalid-value", "$", "schema document must be a JSON object");
  }
  const doc = root as { version?: unknown; tables?: unknown };
  if (doc.version === 2) {
    throw exportError(
      "unknown-version",
      "$.version",
      "document is already version 2 — v2 documents are validated by the schema contract (contracts/data), not upgraded",
    );
  }
  if (doc.version !== 1) {
    throw exportError("unknown-version", "$.version", `schema document must declare version 1 (got ${JSON.stringify(doc.version)})`);
  }
  if (!Array.isArray(doc.tables)) {
    throw exportError("invalid-value", "$.tables", "tables must be an array");
  }

  const tables: V2Table[] = [];
  const seenTables = new Set<string>();
  let hasVector = false;

  for (const rawTable of doc.tables as V1TableInput[]) {
    const table = upgradeV1Table(rawTable, seenTables);
    if (tableHasVectorColumn(rawTable)) hasVector = true;
    tables.push(table);
  }

  tables.sort((a, b) => byteCompare(a.identity.name, b.identity.name));
  return {
    version: 2,
    dialect: "postgresql",
    // X01: same capability separation as exportSchemaV2 — vector in an
    // upgraded v1 document means the PostgreSQL pgvector extension.
    capabilities: hasVector ? ["pgvector"] : [],
    schemas: [{ name: "public" }],
    tables,
    enums: [],
    views: [],
    opaque: [],
  };
}

function v1String(value: unknown, at: string): string {
  if (typeof value !== "string") throw exportError("invalid-value", at, "expected a string");
  return value;
}

function v1Bool(value: unknown, at: string): boolean {
  if (typeof value !== "boolean") throw exportError("invalid-value", at, "expected a boolean");
  return value;
}

function tableHasVectorColumn(table: V1TableInput): boolean {
  return Array.isArray(table.columns) && (table.columns as V1ColumnInput[]).some((c) => c.type === "vector");
}

function upgradeV1Table(raw: V1TableInput, seenTables: Set<string>): V2Table {
  const tableName = v1String(raw.name, "$.tables[].name");
  const at = `tables[public.${tableName}]`;
  if (tableName === "") throw exportError("invalid-identity", `${at}.name`, "table name must not be empty");
  if (seenTables.has(tableName)) throw exportError("duplicate-table", at, `table ${JSON.stringify(tableName)} is declared twice`);
  seenTables.add(tableName);
  if (!Array.isArray(raw.columns) || raw.columns.length === 0) {
    throw exportError("invalid-value", `${at}.columns`, "table must declare at least one column");
  }

  const columns: V2Column[] = [];
  const constraints: V2Constraint[] = [];
  const pkColumns: string[] = [];
  const seenColumns = new Set<string>();

  for (const rawCol of raw.columns as V1ColumnInput[]) {
    const col = upgradeV1Column(rawCol, tableName);
    if (seenColumns.has(col.name)) {
      throw exportError("duplicate-column", `${at}.columns[${col.name}]`, `column ${JSON.stringify(col.name)} is declared twice`);
    }
    seenColumns.add(col.name);
    columns.push(col.column);
    if (rawCol.primaryKey === true) pkColumns.push(col.name);
    if (rawCol.unique === true) {
      const uniqueName = rawCol.uniqueName === undefined || rawCol.uniqueName === "" ? `${tableName}_${col.name}_key` : v1String(rawCol.uniqueName, `${at}.columns[${col.name}].uniqueName`);
      constraints.push({ name: uniqueName, type: "unique", columns: [col.name] });
    }
    if (rawCol.foreignKey !== undefined && rawCol.foreignKey !== null) {
      const fk = rawCol.foreignKey as { table?: unknown; column?: unknown; onDelete?: unknown };
      const references: V2Constraint["references"] = {
        table: { schema: "public", name: v1String(fk.table, `${at}.columns[${col.name}].foreignKey.table`) },
        columns: [v1String(fk.column, `${at}.columns[${col.name}].foreignKey.column`)],
        ...(fk.onDelete !== undefined && fk.onDelete !== ""
          ? { onDelete: v1String(fk.onDelete, `${at}.columns[${col.name}].foreignKey.onDelete`) }
          : {}),
      };
      constraints.push({
        name: `${tableName}_${col.name}_fkey`,
        type: "foreign-key",
        columns: [col.name],
        references,
      });
    }
  }

  if (pkColumns.length > 0) {
    constraints.push({ name: `${tableName}_pkey`, type: "primary-key", columns: [...pkColumns] });
  }

  const indexes: V2Index[] = [];
  if (raw.indexes !== undefined && raw.indexes !== null) {
    if (!Array.isArray(raw.indexes)) throw exportError("invalid-value", `${at}.indexes`, "indexes must be an array");
    for (const rawIdx of raw.indexes as Array<{ name?: unknown; unique?: unknown; columns?: unknown }>) {
      const idxAt = `${at}.indexes[${v1String(rawIdx.name, `${at}.indexes[].name`)}]`;
      if (!Array.isArray(rawIdx.columns) || rawIdx.columns.length === 0) {
        throw exportError("index-key", idxAt, "index must declare at least one key column");
      }
      indexes.push({
        identity: { schema: "public", name: v1String(rawIdx.name, `${idxAt}.name`) },
        unique: v1Bool(rawIdx.unique, `${idxAt}.unique`),
        method: "btree", // v1 could only express plain btree column indexes
        key: (rawIdx.columns as unknown[]).map((c) => ({ column: v1String(c, `${idxAt}.columns[]`) })),
      });
    }
  }

  return {
    identity: { schema: "public", name: tableName },
    managed: true,
    columns,
    constraints,
    indexes,
  };
}

function upgradeV1Column(raw: V1ColumnInput, tableName: string): { name: string; column: V2Column } {
  const name = v1String(raw.name, `tables[${tableName}].columns[].name`);
  const at = `tables[public.${tableName}].columns[${name}]`;
  if (name === "") throw exportError("invalid-identity", `${at}.name`, "column name must not be empty");
  const typeName = V2_TYPE_NAMES[v1String(raw.type, `${at}.type`)];
  if (typeName === undefined) {
    throw exportError("unknown-type", `${at}.type`, `unknown v1 column type ${JSON.stringify(raw.type)}`);
  }
  const notNull = raw.notNull === undefined ? false : v1Bool(raw.notNull, `${at}.notNull`);
  const primaryKey = raw.primaryKey === undefined ? false : v1Bool(raw.primaryKey, `${at}.primaryKey`);
  const hasDefault = raw.hasDefault === undefined ? false : v1Bool(raw.hasDefault, `${at}.hasDefault`);
  const defaultNow = raw.defaultNow === undefined ? false : v1Bool(raw.defaultNow, `${at}.defaultNow`);

  const type: { name: string; codec: string; params?: Record<string, number> } = { name: typeName, codec: V2_TYPE_CODECS[typeName] };
  if (raw.type === "vector") {
    // X02 (X01 review M2): align with Go ValidateSchemaV1ForUpgrade — the
    // v1 upgrade reader accepts ONLY the legacy nucleusOnly+dims shape.
    // A non-nucleusOnly v1 vector column never had meaning (the old
    // nucleusOnly flag WAS the pre-X01 escape hatch); both readers must
    // give the same answer for the same bytes.
    const nucleusOnly = raw.nucleusOnly === undefined ? false : v1Bool(raw.nucleusOnly, `${at}.nucleusOnly`);
    if (!nucleusOnly) {
      throw exportError(
        "invalid-legacy-vector",
        at,
        "legacy vector columns must be nucleusOnly (pre-X01 shape); a non-nucleusOnly vector column never had meaning in v1",
      );
    }
    const dims = raw.vectorDimensions;
    if (typeof dims !== "number" || !Number.isInteger(dims) || dims <= 0) {
      throw exportError("invalid-type-params", `${at}.vectorDimensions`, "vector type requires vectorDimensions > 0");
    }
    type.params = { dimensions: dims };
  } else {
    if (raw.nucleusOnly !== undefined && v1Bool(raw.nucleusOnly, `${at}.nucleusOnly`)) {
      throw exportError("invalid-value", `${at}.nucleusOnly`, "nucleusOnly is only valid for vector columns");
    }
    if (raw.vectorDimensions !== undefined && raw.vectorDimensions !== 0) {
      throw exportError("invalid-value", `${at}.vectorDimensions`, "vectorDimensions is only valid for vector columns");
    }
    if (raw.type === "varchar" && typeof raw.varcharLength === "number" && raw.varcharLength > 0) {
      type.params = { length: raw.varcharLength };
    }
  }

  const v1Type = raw.type as string;
  let def: V2Default | undefined;
  if (v1Type === "serial") {
    if (raw.default !== undefined && raw.default !== null) {
      throw exportError(
        "ambiguous-default",
        `${at}.default`,
        `serial column carries an explicit default ${JSON.stringify(raw.default)}; v1 cannot tell whether it supplements or replaces the implicit sequence default — re-export as schema document v2`,
      );
    }
    if (defaultNow) {
      throw exportError(
        "ambiguous-default",
        `${at}.default`,
        "serial column also sets defaultNow; v1 cannot tell which default applies — re-export as schema document v2",
      );
    }
    def = { kind: "sequence", sequence: { schema: "public", name: `${tableName}_${name}_seq` } };
  } else if (defaultNow) {
    def = { kind: "expression", sql: "now()" };
  } else if (hasDefault) {
    if (raw.default === undefined || raw.default === null) {
      throw exportError(
        "ambiguous-default",
        `${at}.default`,
        "column sets hasDefault: true but spells no default value; the default exists and is not representable in v1 — re-export as schema document v2",
      );
    }
    const d = v1String(raw.default, `${at}.default`);
    def = upgradeV1Default(d, v1Type, `public.${tableName}`, at, name);
  }

  const column: V2Column = {
    name,
    type,
    notNull: notNull || primaryKey, // PK implies NOT NULL (v1 normalization rule)
    ...(def ? { default: def } : {}),
  };

  return { name, column };
}

function upgradeV1Default(d: string, v1Type: string, tableKey: string, at: string, name: string): V2Default {
  switch (v1Type) {
    case "integer":
    case "smallint":
    case "bigint":
    case "double":
    case "real":
    case "numeric":
      if (V2_NUMERIC_DEFAULT.test(d)) return { kind: "literal", sql: d };
      break;
    case "boolean":
      if (d === "true" || d === "false") return { kind: "literal", sql: d };
      break;
  }
  throw exportError(
    "ambiguous-default",
    `${at}.default`,
    `v1 default ${JSON.stringify(d)} on column ${tableKey}.${name} could be a string literal or an SQL expression; the v1 format cannot distinguish them — re-export the schema as document v2 where defaults are tagged`,
  );
}
