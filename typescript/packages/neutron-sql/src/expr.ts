// ---------------------------------------------------------------------------
// @neutron-build/sql — expressions, operators, raw SQL fragments
// ---------------------------------------------------------------------------
// A fragment carries SQL text with $1..$n placeholders relative to its own
// params. Merging renumbers. The final query assembly is therefore trivial.

import { getTableName } from "./schema.js";
import type { AnyColumnBuilder, ColumnBuilder, JsTypeOf, ColumnDataType } from "./schema.js";

export interface SqlFragment {
  readonly sql: string;
  readonly params: readonly unknown[];
}

export function raw(sqlText: string, params: unknown[] = []): SqlFragment {
  return { sql: sqlText, params };
}

/** Raw SQL with interpolated values: sql`select * from t where id = ${1}` */
export function sql(strings: TemplateStringsArray, ...values: unknown[]): SqlFragment {
  let text = "";
  const params: unknown[] = [];
  for (let i = 0; i < strings.length; i++) {
    text += strings[i];
    if (i < values.length) {
      params.push(values[i]);
      text += `$${params.length}`;
    }
  }
  return { sql: text, params };
}

export function qident(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

export function qualify(table: string, column: string): string {
  return `${qident(table)}.${qident(column)}`;
}

// ---------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------

export type Condition = SqlFragment;

function colRef(col: AnyColumnBuilder | string, table?: string): string {
  if (typeof col === "string") return qident(col);
  if (table) return qualify(table, col.columnName);
  if (col.ownerTable) return qualify(getTableName(col.ownerTable), col.columnName);
  return qident(col.columnName);
}

function cmp(op: string, col: AnyColumnBuilder | string, value: unknown, table?: string): Condition {
  return { sql: `${colRef(col, table)} ${op} $1`, params: [value] };
}

export function eq<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp("=", col, value, table);
}

export function ne<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp("<>", col, value, table);
}

export function lt<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp("<", col, value, table);
}

export function lte<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp("<=", col, value, table);
}

export function gt<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp(">", col, value, table);
}

export function gte<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean> | string, value: JsTypeOf<D>, table?: string): Condition {
  return cmp(">=", col, value, table);
}

export function like(col: AnyColumnBuilder | string, pattern: string, table?: string): Condition {
  return cmp("like", col, pattern, table);
}

export function ilike(col: AnyColumnBuilder | string, pattern: string, table?: string): Condition {
  return cmp("ilike", col, pattern, table);
}

export function inArray<D extends ColumnDataType>(
  col: ColumnBuilder<D, boolean, boolean> | string,
  values: Array<JsTypeOf<D>>,
  table?: string,
): Condition {
  if (values.length === 0) return { sql: "1 = 0", params: [] };
  const placeholders = values.map((_, i) => `$${i + 1}`).join(", ");
  return { sql: `${colRef(col, table)} in (${placeholders})`, params: values.slice() };
}

export function isNull(col: AnyColumnBuilder | string, table?: string): Condition {
  return { sql: `${colRef(col, table)} is null`, params: [] };
}

export function isNotNull(col: AnyColumnBuilder | string, table?: string): Condition {
  return { sql: `${colRef(col, table)} is not null`, params: [] };
}

export function and(...conditions: Array<Condition | undefined>): Condition {
  const parts = conditions.filter((c): c is Condition => c !== undefined);
  if (parts.length === 0) return { sql: "1 = 1", params: [] };
  if (parts.length === 1) return parts[0];
  return { sql: `(${mergeFragments(parts, " and ", true).sql})`, params: mergeFragments(parts, " and ", true).params };
}

export function or(...conditions: Array<Condition | undefined>): Condition {
  const parts = conditions.filter((c): c is Condition => c !== undefined);
  if (parts.length === 0) return { sql: "1 = 1", params: [] };
  if (parts.length === 1) return parts[0];
  return { sql: `(${mergeFragments(parts, " or ", true).sql})`, params: mergeFragments(parts, " or ", true).params };
}

export function not(condition: Condition): Condition {
  return mergeFragments([condition], "", true, "not ");
}

/** Concatenate fragments, renumbering placeholders into one shared param list. */
export function mergeFragments(
  fragments: SqlFragment[],
  joiner: string,
  parenthesize: boolean,
  prefix = "",
): SqlFragment {
  const params: unknown[] = [];
  const pieces: string[] = [];
  for (const frag of fragments) {
    let n = 0;
    const text = frag.sql.replace(/\$(\d+)/g, () => {
      params.push(frag.params[n]);
      n++;
      return `$${params.length}`;
    });
    pieces.push(parenthesize ? `(${text})` : text);
  }
  return { sql: prefix + pieces.join(joiner), params };
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

export type OrderExpression = SqlFragment;

export function asc(col: AnyColumnBuilder | string, table?: string): OrderExpression {
  return { sql: `${colRef(col, table)} asc`, params: [] };
}

export function desc(col: AnyColumnBuilder | string, table?: string): OrderExpression {
  return { sql: `${colRef(col, table)} desc`, params: [] };
}
