// ---------------------------------------------------------------------------
// @neutron-build/sql — DDL emission from schema objects
// ---------------------------------------------------------------------------
// Deterministic: same schema in, same SQL out. This is the source the P1
// migration generator diffs against introspected databases.

import { getTableColumns, getTableName, getTableIndexes, getTableSchema, getTableConstraints, rejectDerivedTable, getViewDefinition } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable } from "./schema.js";
import { qident } from "./expr.js";
import { quoteStringLiteral } from "./compile.js";

/** The legacy DDL emitter covers the pre-Q07 surface only (default
 *  search-path tables, plain columns, column-level constraints, simple
 *  column indexes). Every Q07 feature fails closed here instead of emitting
 *  lossy SQL — the schema export v2 document plus the CLI planner own the
 *  full surface (Q07). */
function assertLegacySurface(table: AnyPgTable, who: string): void {
  rejectDerivedTable(table, "schemaToDDL");
  if (getViewDefinition(table) !== undefined) {
    throw new Error(`${who}: "${getTableName(table)}" is a view — the legacy DDL emitter does not support views; use schema export v2`);
  }
  const schema = getTableSchema(table);
  if (schema !== undefined) {
    throw new Error(
      `${who}: table "${schema}"."${getTableName(table)}" declares a schema — the legacy DDL emitter covers the default search path only; use schema export v2 (Q07)`,
    );
  }
  if (getTableConstraints(table).length > 0) {
    throw new Error(
      `${who}: table "${getTableName(table)}" declares table-level constraints — the legacy DDL emitter cannot emit them; use schema export v2 (Q07)`,
    );
  }
  for (const idx of getTableIndexes(table)) {
    if (
      idx.keyParts.some((p) => p.expression !== undefined || p.order !== undefined || p.nulls !== undefined || p.opclass !== undefined) ||
      idx.whereExpr !== undefined ||
      idx.includeCols.length > 0 ||
      idx.withParams !== undefined
    ) {
      throw new Error(
        `${who}: index "${idx.indexName}" uses expressions, ordering options, a predicate, INCLUDE, operator classes or access-method parameters — the legacy DDL emitter cannot emit it; use schema export v2`,
      );
    }
  }
  for (const col of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
    const at = `${who}: column "${col.columnName}" of "${getTableName(table)}"`;
    if (col.dataType === "enum" || col.enumDef !== undefined) throw new Error(`${at} is an enum column — the legacy DDL emitter cannot emit enum types; use schema export v2 (Q07)`);
    if (col.arrayDimensions !== undefined) throw new Error(`${at} is an array column — the legacy DDL emitter cannot emit array types; use schema export v2 (Q07)`);
    if (col.identityKind !== undefined) throw new Error(`${at} is an identity column — the legacy DDL emitter cannot emit identity; use schema export v2 (Q07)`);
    if (col.generatedExpr !== undefined) throw new Error(`${at} is a generated column — the legacy DDL emitter cannot emit generation expressions; use schema export v2 (Q07)`);
    if (col.foreignKey?.onUpdate !== undefined) throw new Error(`${at} declares a foreign key with ON UPDATE — the legacy DDL emitter drops it; use schema export v2 (Q07)`);
    if (col.dataType === "vector" || col.dataType === "tsvector") {
      throw new Error(
        `${at} is a ${col.dataType} column — the legacy DDL emitter cannot emit it (vector requires the pgvector extension and its dimension parameter; tsvector belongs to the FTS surface); use schema export v2 with the pgvector capability or the /fts module (X01)`,
      );
    }
  }
}

function assertPlainTable(table: AnyPgTable, who: string): void {
  assertLegacySurface(table, who);
}

export function sqlTypeOf(col: AnyColumnBuilder): string {
  if (col.arrayDimensions !== undefined) {
    throw new Error(`column "${col.columnName}": array columns have no legacy SQL type spelling — use schema export v2 (Q07)`);
  }
  if (col.dataType === "enum") {
    throw new Error(`column "${col.columnName}": enum columns have no legacy SQL type spelling — use schema export v2 (Q07)`);
  }
  switch (col.dataType) {
    case "serial":
      return "serial";
    case "integer":
      return "integer";
    case "smallint":
      return "smallint";
    case "bigint":
      return "bigint";
    case "double":
      return "double precision";
    case "real":
      return "real";
    case "numeric":
      return "numeric";
    case "text":
      return "text";
    case "varchar":
      return col.varcharLength ? `varchar(${col.varcharLength})` : "varchar";
    case "boolean":
      return "boolean";
    case "timestamp":
      return "timestamp";
    case "timestamptz":
      return "timestamptz";
    case "date":
      return "date";
    case "json":
      return "json";
    case "jsonb":
      return "jsonb";
    case "uuid":
      return "uuid";
    case "bytea":
      return "bytea";
    case "vector":
      throw new Error(`column "${col.columnName}": vector columns have no legacy SQL type spelling — the dimension parameter is load-bearing; use schema export v2 (X01)`);
    case "tsvector":
      throw new Error(`column "${col.columnName}": tsvector columns have no legacy SQL type spelling; use schema export v2 (X01)`);
  }
}

function defaultLiteral(col: AnyColumnBuilder): string | undefined {
  if (!col.hasDefault) return undefined;
  if (col.nowDefault) return "now()";
  const v = col.defaultValue;
  if (v === null) return "null";
  if (v === undefined) return undefined;
  switch (col.dataType) {
    case "boolean":
      return v ? "true" : "false";
    case "integer":
    case "smallint":
    case "bigint":
    case "serial":
    case "double":
    case "real":
    case "numeric":
      return String(v);
    default:
      return quoteStringLiteral(String(v));
  }
}

function columnDefLine(table: AnyPgTable, col: AnyColumnBuilder, inlineRefs: boolean): string {
  const parts = [qident(col.columnName), sqlTypeOf(col)];
  if (col.isPrimaryKey) parts.push("primary key");
  if (col.isNotNull) parts.push("not null");
  if (col.isUnique) parts.push("unique");
  const def = defaultLiteral(col);
  if (def !== undefined) parts.push(`default ${def}`);
  if (inlineRefs && col.foreignKey) {
    parts.push(referenceClause(col));
  }
  return parts.join(" ");
}

/** Topologically sort tables so referenced tables are created first. */
export function topoSortTables(tables: AnyPgTable[]): { order: AnyPgTable[]; deferredFks: Array<{ table: AnyPgTable; column: AnyColumnBuilder }> } {
  const deferredFks: Array<{ table: AnyPgTable; column: AnyColumnBuilder }> = [];
  const byName = new Map(tables.map((t) => [getTableName(t), t]));
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const order: AnyPgTable[] = [];

  const visit = (table: AnyPgTable): void => {
    if (visited.has(getTableName(table))) return;
    if (visiting.has(getTableName(table))) return; // cycle: defer this edge via ALTER
    visiting.add(getTableName(table));
    for (const col of Object.values(getTableColumns(table)) as AnyColumnBuilder[]) {
      if (!col.foreignKey) continue;
      const target = col.foreignKey().ownerTable;
      if (!target || !byName.has(getTableName(target))) continue;
      if (visiting.has(getTableName(target))) {
        deferredFks.push({ table, column: col });
        continue;
      }
      visit(target);
    }
    visiting.delete(getTableName(table));
    visited.add(getTableName(table));
    order.push(table);
  };

  for (const t of tables) visit(t);
  return { order, deferredFks };
}

export function referenceClause(col: AnyColumnBuilder): string {
  const target = col.foreignKey!();
  const targetTable = target.ownerTable;
  if (!targetTable) {
    throw new Error(`column ${col.columnName} has a foreign key to a column with no owning table`);
  }
  let refSql = `references ${qident(getTableName(targetTable))} (${qident(target.columnName)})`;
  if (col.foreignKey!.onDelete) refSql += ` on delete ${col.foreignKey!.onDelete}`;
  return refSql;
}

export function createTableSQL(table: AnyPgTable, inlineRefs = true): string {
  assertPlainTable(table, "createTableSQL");
  const lines = (Object.values(getTableColumns(table)) as AnyColumnBuilder[]).map((col) => `  ${columnDefLine(table, col, inlineRefs)}`);
  return `create table ${qident(getTableName(table))} (\n${lines.join(",\n")}\n)`;
}

export function addForeignKeySQL(table: AnyPgTable, column: AnyColumnBuilder, constraintName?: string): string {
  assertPlainTable(table, "addForeignKeySQL");
  const name = constraintName ?? `${getTableName(table)}_${column.columnName}_fkey`;
  return `alter table ${qident(getTableName(table))} add constraint ${qident(name)} foreign key (${qident(column.columnName)}) ${referenceClause(column)}`;
}

export function createIndexSQL(table: AnyPgTable, index: { indexName: string; unique: boolean; columns: string[]; method?: string }): string {
  assertPlainTable(table, "createIndexSQL");
  const cols = index.columns.map((c) => qident(c)).join(", ");
  const method = index.method && index.method !== "btree" ? ` using ${index.method}` : "";
  return `create ${index.unique ? "unique " : ""}index ${qident(index.indexName)} on ${qident(getTableName(table))}${method} (${cols})`;
}

export function dropTableSQL(table: AnyPgTable): string {
  assertPlainTable(table, "dropTableSQL");
  return `drop table if exists ${qident(getTableName(table))}`;
}

/** Full DDL for a schema: tables (dep-ordered), FKs for cyclic edges, indexes. */
export function schemaToDDL(tables: AnyPgTable[]): string[] {
  for (const t of tables) assertPlainTable(t, "schemaToDDL");
  const { order, deferredFks } = topoSortTables(tables);
  const statements: string[] = [];
  for (const table of order) {
    statements.push(createTableSQL(table));
    for (const idx of getTableIndexes(table)) {
      statements.push(createIndexSQL(table, idx));
    }
  }
  for (const { table, column } of deferredFks) {
    statements.push(addForeignKeySQL(table, column));
  }
  return statements;
}
