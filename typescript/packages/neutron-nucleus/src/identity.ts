// ---------------------------------------------------------------------------
// @neutron-build/nucleus/identity — shared SQL identity metadata (X02)
//
// Documents and graph nodes can reference rows that live in ordinary SQL
// tables. These references are IDENTITY METADATA ONLY: they name a SQL
// object, they never emit DDL, create no columns, and appear in no
// migration. The models keep their own semantics (README §6: KV/graph/
// streams are resources, not pretend SQL column types); what is shared is
// the identity, not the transaction and not the storage.
// ---------------------------------------------------------------------------

import { assertIdentifier } from './helpers.js';

/** A reference to a SQL table (the identity anchor for a model resource). */
export interface SqlTableRef {
  /** Schema that owns the table. Omit for the default search path. */
  schema?: string;
  /** Table name. */
  table: string;
}

/** A reference to one SQL row (table identity plus primary-key value). */
export interface SqlRowRef extends SqlTableRef {
  /** Primary-key value of the referenced row (int8-safe integer). */
  id: number;
}

/**
 * Validate a table reference without touching the database.
 * Throws on names that are not plain SQL identifiers — a bound resource must
 * never become an injection surface when its name is later rendered into a
 * parameterized SELECT.
 */
export function assertSqlTableRef(ref: SqlTableRef): void {
  if (typeof ref.table !== 'string' || ref.table === '') {
    throw new Error(`SQL table reference requires a table name, got ${JSON.stringify(ref.table)}`);
  }
  assertIdentifier(ref.table, 'SQL table reference table');
  if (ref.schema !== undefined) {
    assertIdentifier(ref.schema, 'SQL table reference schema');
  }
}

/**
 * Validate a row reference. The id must be a positive integer inside the
 * int8-safe range: engine row ids are int64 and this client's int8 parser is
 * number-based (an unsafe integer would silently round — the exact failure
 * class this program exists to end).
 */
export function assertSqlRowRef(ref: SqlRowRef): void {
  assertSqlTableRef(ref);
  if (typeof ref.id !== 'number' || !Number.isInteger(ref.id) || ref.id <= 0 || !Number.isSafeInteger(ref.id)) {
    throw new Error(`SQL row reference id must be a positive safe integer, got ${JSON.stringify(ref.id)}`);
  }
}

/** Render the quoted, dot-qualified table name for a parameterized SELECT. */
export function quoteSqlTableRef(ref: SqlTableRef): string {
  assertSqlTableRef(ref);
  return ref.schema === undefined ? `"${ref.table}"` : `"${ref.schema}"."${ref.table}"`;
}
