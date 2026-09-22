// ---------------------------------------------------------------------------
// @neutron-build/sql — schema export (bridge for `neutron migrate generate`)
// ---------------------------------------------------------------------------
// Serializes schema objects to stable JSON. The Go CLI introspects the live
// database and diffs against this file to emit migration SQL.

import type { AnyColumnBuilder, AnyPgTable } from "./schema.js";
import { getTableColumns, getTableName, getTableIndexes, isPgTable } from "./schema.js";
import type { TablesInput } from "./db.js";

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
