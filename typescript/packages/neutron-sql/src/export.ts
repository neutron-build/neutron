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

import type { AnyColumnBuilder, AnyPgTable } from "./schema.js";
import { getTableColumns, getTableName, getTableIndexes, getTableSchema, isPgTable, rejectDerivedTable } from "./schema.js";

/** The v1/v2 export contract covers default-search-path tables only
 *  (identity.schema is "public"). A declared schema would export under the
 *  wrong identity, so schema-qualified tables fail closed until Q07 owns
 *  cross-schema export. */
function assertPlainTable(table: AnyPgTable, who: string): void {
  rejectDerivedTable(table, who);
  const schema = getTableSchema(table);
  if (schema !== undefined) {
    throw new Error(
      `${who}: table "${schema}"."${getTableName(table)}" declares a schema — schema export for schema-qualified tables lands with Q07 (the query layer supports them)`,
    );
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
      exported.nucleusOnly = true;
      exported.vectorDimensions = col.vectorDimensions;
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
}

export type V2Default =
  | { readonly kind: "literal"; readonly sql: string }
  | { readonly kind: "expression"; readonly sql: string }
  | { readonly kind: "sequence"; readonly sequence: V2Identity };

export interface V2Column {
  readonly name: string;
  readonly type: V2TypeRef;
  readonly notNull: boolean;
  readonly default?: V2Default;
}

export type V2ConstraintType = "primary-key" | "unique" | "foreign-key";

export interface V2Constraint {
  readonly name: string;
  readonly type: V2ConstraintType;
  readonly columns: readonly string[];
  readonly references?: {
    readonly table: V2Identity;
    readonly columns: readonly string[];
    readonly onDelete?: string;
  };
}

export interface V2Index {
  readonly identity: V2Identity;
  readonly unique: boolean;
  readonly method: string;
  readonly key: ReadonlyArray<{ readonly column?: string }>;
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
  readonly enums: readonly never[];
  readonly views: readonly never[];
  readonly opaque: readonly never[];
}

/** pg_catalog type names for the schema-builder type vocabulary, with the
 *  canonical v2 codec per type (contracts/data TYPE_CODECS). */
const V2_TYPE_NAMES: Record<string, string> = {
  serial: "int4", integer: "int4", smallint: "int2", bigint: "int8",
  double: "float8", real: "float4", numeric: "numeric",
  text: "text", varchar: "varchar", boolean: "bool",
  timestamp: "timestamp", timestamptz: "timestamptz", date: "date",
  json: "json", jsonb: "jsonb", uuid: "uuid", bytea: "bytea", vector: "vector",
};
const V2_TYPE_CODECS: Record<string, string> = {
  bool: "boolean", int2: "number", int4: "number", int8: "bigint",
  float4: "number", float8: "number", numeric: "decimal-string",
  text: "string", varchar: "string",
  timestamp: "timestamp-string", timestamptz: "timestamptz-string", date: "date-string",
  bytea: "binary", uuid: "uuid", json: "json", jsonb: "json", vector: "vector",
};

const V2_LITERAL_PATTERN = /^('([^']|'')*'(::[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?(\[\])*)?|-?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?|true|false|null)$/;
const V2_NUMERIC_DEFAULT = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;

function exportError(code: string, at: string, detail: string): Error {
  return new Error(`[${code}] ${at}: ${detail}`);
}

/** Tag a declared default (a JS value from `.default(...)`) as a v2 default.
 *  The codec canonicalizes the value (validation with column context); the
 *  literal spelling is a deterministic function of the canonical value. */
function v2DefaultFor(table: string, column: AnyColumnBuilder): V2Default | undefined {
  if (column.dataType === "serial") {
    // The schema DDL `serial` creates exactly this named sequence; an
    // explicit default alongside it is ambiguous and rejected above.
    return { kind: "sequence", sequence: { schema: "public", name: `${table}_${column.columnName}_seq` } };
  }
  if (column.nowDefault) return { kind: "expression", sql: "now()" };
  if (!column.hasDefault) return undefined;
  const at = `tables[public.${table}].columns[${column.columnName}].default`;
  if (column.defaultValue === undefined) {
    throw exportError("invalid-default", at, "default is declared without a value — exportSchemaV2 cannot spell it");
  }
  const ctx: ColumnContext = { propertyKey: column.columnName, columnName: column.columnName, tableName: table };
  const encoded = encodeWriteValue(column, ctx, column.defaultValue);
  const dt = column.dataType;
  let sql: string;
  if (dt === "boolean") {
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

function v2TypeFor(table: string, column: AnyColumnBuilder): V2TypeRef {
  const at = `tables[public.${table}].columns[${column.columnName}].type`;
  const typeName = V2_TYPE_NAMES[column.dataType];
  if (typeName === undefined) {
    throw exportError("unknown-type", at, `unknown column type ${JSON.stringify(column.dataType)}`);
  }
  const ref: { name: string; codec: string; params?: Record<string, number> } = { name: typeName, codec: V2_TYPE_CODECS[typeName] };
  if (column.dataType === "vector") {
    if (typeof column.vectorDimensions !== "number") {
      throw exportError("invalid-type-params", at, "vector columns require dimensions");
    }
    ref.params = { dimensions: column.vectorDimensions };
  } else if (column.dataType === "varchar" && column.varcharLength) {
    ref.params = { length: column.varcharLength };
  }
  return ref;
}

/** Export the given tables as a schema document v2. Deterministic: the same
 *  tables (in any input key order) produce the same document and the same
 *  canonical bytes. Columns keep declaration order (attnum semantics);
 *  tables/constraints/indexes are emitted in a stable order and sorted sets
 *  normalize further in canonicalSchemaJson. */
export function exportSchemaV2(tables: TablesInput): SchemaDocumentV2 {
  const exported: AnyPgTable[] = [];
  for (const value of Object.values(tables)) {
    if (isPgTable(value)) {
      assertPlainTable(value, "exportSchemaV2");
      exported.push(value);
    }
  }
  // Deterministic table order: schema-qualified identity (public here).
  exported.sort((a, b) => byteCompare(getTableName(a), getTableName(b)));

  const byName = new Map(exported.map((t) => [getTableName(t), t]));
  const out: V2Table[] = [];
  let hasVector = false;

  for (const table of exported) {
    const tableName = getTableName(table);
    const columns: V2Column[] = [];
    const constraints: V2Constraint[] = [];
    const pkColumns: string[] = [];

    for (const column of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
      const at = `tables[public.${tableName}].columns[${column.columnName}]`;
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
      const def = v2DefaultFor(tableName, column);
      const v2Column: V2Column = {
        name: column.columnName,
        type: v2TypeFor(tableName, column),
        notNull: column.isNotNull || column.isPrimaryKey, // PK implies NOT NULL
        ...(def ? { default: def } : {}),
      };
      columns.push(v2Column);

      if (column.isPrimaryKey) pkColumns.push(column.columnName);
      if (column.isUnique) {
        constraints.push({ name: `${tableName}_${column.columnName}_key`, type: "unique", columns: [column.columnName] });
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
        const targetName = getTableName(targetTable);
        if (!byName.has(targetName)) {
          throw exportError(
            "fk-target",
            at,
            `foreign key references table ${JSON.stringify(targetName)}, which is not part of the exported schema`,
          );
        }
        // v2 requires FK targets covered by a primary-key or unique constraint.
        const targetColumn = (Object.values(getTableColumns(targetTable)) as AnyColumnBuilder[]).find(
          (c) => c.columnName === target.columnName,
        );
        if (!targetColumn || (!targetColumn.isPrimaryKey && !targetColumn.isUnique)) {
          throw exportError(
            "fk-not-unique",
            at,
            `foreign key references ${targetName}.${target.columnName}, which is not covered by a primary-key or unique constraint`,
          );
        }
        const references: V2Constraint["references"] = {
          table: { schema: "public", name: targetName },
          columns: [target.columnName],
          ...(column.foreignKey.onDelete ? { onDelete: column.foreignKey.onDelete } : {}),
        };
        constraints.push({
          name: `${tableName}_${column.columnName}_fkey`,
          type: "foreign-key",
          columns: [column.columnName],
          references,
        });
      }
    }

    if (pkColumns.length > 0) {
      constraints.push({ name: `${tableName}_pkey`, type: "primary-key", columns: [...pkColumns] });
    }

    out.push({
      identity: { schema: "public", name: tableName },
      managed: true,
      columns,
      constraints,
      indexes: getTableIndexes(table).map((idx) => ({
        identity: { schema: "public", name: idx.indexName },
        unique: idx.unique,
        method: idx.method ?? "btree",
        key: idx.columns.map((c) => ({ column: c })),
      })),
    });
  }

  return {
    version: 2,
    dialect: "postgresql",
    capabilities: hasVector ? ["nucleus"] : [],
    schemas: [{ name: "public" }],
    tables: out,
    enums: [],
    views: [],
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
 *  indexes) sorted, ordered tuples (columns, constraint columns, references
 *  columns, index key parts) preserved, minimal string escaping, no
 *  whitespace. Pure: same document → identical bytes on every machine. */
export function canonicalSchemaJson(doc: SchemaDocumentV2): string {
  const parts: string[] = [];
  parts.push('{"capabilities":[');
  [...doc.capabilities].sort(byteCompare).forEach((c, i) => {
    if (i > 0) parts.push(",");
    parts.push(canonicalString(c));
  });
  parts.push('],"dialect":"postgresql","enums":[],"opaque":[');
  parts.push('],"schemas":[');
  [...doc.schemas].sort((a, b) => byteCompare(a.name, b.name)).forEach((s, i) => {
    if (i > 0) parts.push(",");
    parts.push(`{"name":${canonicalString(s.name)}}`);
  });
  parts.push('],"tables":[');
  const tables = [...doc.tables].sort((a, b) =>
    a.identity.schema !== b.identity.schema
      ? byteCompare(a.identity.schema, b.identity.schema)
      : byteCompare(a.identity.name, b.identity.name),
  );
  tables.forEach((t, i) => {
    if (i > 0) parts.push(",");
    parts.push('{"columns":[');
    t.columns.forEach((c, j) => {
      if (j > 0) parts.push(",");
      parts.push("{");
      if (c.default) parts.push(`"default":${canonicalDefault(c.default)},`);
      parts.push(`"name":${canonicalString(c.name)},"notNull":${c.notNull},"type":`);
      parts.push(canonicalType(c.type));
      parts.push("}");
    });
    parts.push('],"constraints":[');
    [...t.constraints].sort((a, b) => byteCompare(a.name, b.name)).forEach((c, j) => {
      if (j > 0) parts.push(",");
      parts.push("{");
      if (c.columns.length > 0) parts.push(`"columns":[${c.columns.map(canonicalString).join(",")}],`);
      parts.push(`"name":${canonicalString(c.name)}`);
      if (c.references) {
        const r = c.references;
        parts.push(',"references":{"columns":[');
        parts.push(r.columns.map(canonicalString).join(","));
        parts.push("]");
        if (r.onDelete) parts.push(`,"onDelete":${canonicalString(r.onDelete)}`);
        parts.push(`,"table":{"name":${canonicalString(r.table.name)},"schema":${canonicalString(r.table.schema)}}}`);
      }
      parts.push(`,"type":${canonicalString(c.type)}}`);
    });
    parts.push(`],"identity":{"name":${canonicalString(t.identity.name)},"schema":${canonicalString(t.identity.schema)}},"indexes":[`);
    [...t.indexes]
      .sort((a, b) =>
        a.identity.schema !== b.identity.schema
          ? byteCompare(a.identity.schema, b.identity.schema)
          : byteCompare(a.identity.name, b.identity.name),
      )
      .forEach((idx, j) => {
        if (j > 0) parts.push(",");
        parts.push(`{"identity":{"name":${canonicalString(idx.identity.name)},"schema":${canonicalString(idx.identity.schema)}},"key":[`);
        idx.key.forEach((part, k) => {
          if (k > 0) parts.push(",");
          if (part.column !== undefined) parts.push(`{"column":${canonicalString(part.column)}}`);
        });
        parts.push(`],"method":${canonicalString(idx.method)},"unique":${idx.unique}}`);
      });
    parts.push(`],"managed":${t.managed}}`);
  });
  parts.push('],"version":2,"views":[]}');
  return parts.join("");
}

function canonicalType(type: V2TypeRef): string {
  const parts: string[] = [`{"codec":${canonicalString(type.codec)},"name":${canonicalString(type.name)}`];
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
    capabilities: hasVector ? ["nucleus"] : [],
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
    const dims = raw.vectorDimensions;
    if (typeof dims !== "number" || !Number.isInteger(dims)) {
      throw exportError("invalid-type-params", `${at}.vectorDimensions`, "vector columns require integer dimensions");
    }
    type.params = { dimensions: dims };
  } else if (raw.type === "varchar" && typeof raw.varcharLength === "number" && raw.varcharLength > 0) {
    type.params = { length: raw.varcharLength };
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
