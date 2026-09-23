// ---------------------------------------------------------------------------
// @neutron-build/sql — DDL emission from schema objects
// ---------------------------------------------------------------------------
// Deterministic: same schema in, same SQL out. This is the source the P1
// migration generator diffs against introspected databases.

import { getTableColumns, getTableName, getTableIndexes, getTableSchema, rejectDerivedTable } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable } from "./schema.js";
import { qident } from "./expr.js";
import { quoteStringLiteral } from "./compile.js";

/** DDL/migration emission covers default-search-path tables only. A declared
 *  schema would silently emit DDL against the wrong (search-path) location,
 *  so schema-qualified tables fail closed here until Q07 owns them. */
function assertPlainTable(table: AnyPgTable, who: string): void {
  rejectDerivedTable(table, who);
  const schema = getTableSchema(table);
  if (schema !== undefined) {
    throw new Error(
      `${who}: table "${schema}"."${getTableName(table)}" declares a schema — DDL/migration emission for schema-qualified tables lands with Q07 (the query layer supports them)`,
    );
  }
}

export function sqlTypeOf(col: AnyColumnBuilder): string {
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
      return "vector";
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
