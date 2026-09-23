// ---------------------------------------------------------------------------
// @neutron-build/sql — expressions, operators, raw SQL fragments
// ---------------------------------------------------------------------------
// Since F04 every predicate/order builder produces AST value nodes, compiled
// by the one-traversal compiler (compile.ts) — no fragment text with $n
// placeholders is ever renumbered here. `raw()` remains for the explicit
// direct-execution escape hatch: its {sql, params} shape is executed as-is
// through the driver and never interpolated into other statements.

import { getTableName, tableRefParts } from "./schema.js";
import type { AnyColumnBuilder, ColumnBuilder, JsWriteTypeOf, ColumnDataType } from "./schema.js";
import { encodeWriteValue } from "./codecs.js";
import {
  expr as exprNode,
  fragment,
  ident,
  isLegacySqlFragment,
  legacyFragmentError,
  param as paramNode,
  paramCast,
  qual,
  type OrderSpec,
  type ValueNode,
} from "./ast.js";

/** Parameterized raw text for DIRECT execution only (`driver.query(raw.sql,
 *  raw.params)`). Its `$n` placeholders are never spliced into compiled
 *  statements — that would require scanning raw SQL text. */
export interface SqlFragment {
  readonly sql: string;
  readonly params: readonly unknown[];
}

export function raw(sqlText: string, params: unknown[] = []): SqlFragment {
  return { sql: sqlText, params };
}

export { quoteIdent as qident } from "./compile.js";

// ---------------------------------------------------------------------------
// Conditions — AST value nodes
// ---------------------------------------------------------------------------

export type Condition = ValueNode;

function colRef(col: AnyColumnBuilder | string, table?: string): ValueNode {
  if (typeof col === "string") return ident(col);
  if (table) return qual(table, col.columnName);
  if (col.ownerTable) return qual(...tableRefParts(col.ownerTable), col.columnName);
  return ident(col.columnName);
}

/** Predicate values run through the column codec: validation with column
 *  context, canonical encoding, and a text-typed bind site for temporal and
 *  json/jsonb values (postgres.js otherwise re-encodes server-typed params
 *  through Date/JSON.stringify). */
function encodedParamNode(col: AnyColumnBuilder, table: string | undefined, value: unknown): ValueNode {
  const tableName = table ?? (col.ownerTable ? getTableName(col.ownerTable) : col.columnName);
  const enc = encodeWriteValue(col, { propertyKey: col.columnName, columnName: col.columnName, tableName }, value);
  return enc.cast === undefined ? paramNode(enc.bind) : paramCast(enc.bind, enc.cast);
}

function cmp(op: string, col: AnyColumnBuilder | string, value: unknown, table?: string): Condition {
  if (typeof col === "string") {
    return exprNode("binary", op, [ident(col), paramNode(value)]);
  }
  return exprNode("binary", op, [colRef(col, table), encodedParamNode(col, table, value)]);
}

export function eq<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp("=", col, value, table);
}

export function ne<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp("<>", col, value, table);
}

export function lt<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp("<", col, value, table);
}

export function lte<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp("<=", col, value, table);
}

export function gt<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp(">", col, value, table);
}

export function gte<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown> | string, value: JsWriteTypeOf<D>, table?: string): Condition {
  return cmp(">=", col, value, table);
}

export function like(col: AnyColumnBuilder | string, pattern: string, table?: string): Condition {
  return cmp("like", col, pattern, table);
}

export function ilike(col: AnyColumnBuilder | string, pattern: string, table?: string): Condition {
  return cmp("ilike", col, pattern, table);
}

export function inArray<D extends ColumnDataType>(
  col: ColumnBuilder<D, boolean, boolean, unknown> | string,
  values: Array<JsWriteTypeOf<D>>,
  table?: string,
): Condition {
  if (values.length === 0) return fragment("1 = 0");
  const cells = typeof col === "string" ? values.map((v) => paramNode(v)) : values.map((value) => encodedParamNode(col, table, value));
  const parts: Array<string | ValueNode> = [colRef(col, table), " in ("];
  cells.forEach((cell, i) => {
    if (i > 0) parts.push(", ");
    parts.push(cell);
  });
  parts.push(")");
  return fragment(...parts);
}

export function isNull(col: AnyColumnBuilder | string, table?: string): Condition {
  return fragment(colRef(col, table), " is null");
}

export function isNotNull(col: AnyColumnBuilder | string, table?: string): Condition {
  return fragment(colRef(col, table), " is not null");
}

function rejectLegacy(condition: Condition, connective: string): void {
  if (isLegacySqlFragment(condition)) throw legacyFragmentError(connective);
}

function combine(op: "and" | "or", conditions: Array<Condition | undefined>): Condition {
  const parts = conditions.filter((c): c is Condition => c !== undefined);
  for (const c of parts) rejectLegacy(c, op);
  if (parts.length === 0) return fragment("1 = 1");
  return parts.reduce((acc, c) => exprNode("binary", op, [acc, c]));
}

export function and(...conditions: Array<Condition | undefined>): Condition {
  return combine("and", conditions);
}

export function or(...conditions: Array<Condition | undefined>): Condition {
  return combine("or", conditions);
}

export function not(condition: Condition): Condition {
  rejectLegacy(condition, "not");
  return exprNode("unary", "not", [condition]);
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------

/** Order expression: an explicit `asc(col)`/`desc(col)` spec, or any value
 *  node (a raw expression defaults to ascending). */
export type OrderExpression = OrderSpec | ValueNode;

export function asc(col: AnyColumnBuilder | string, table?: string): OrderSpec {
  return Object.freeze({ expr: colRef(col, table), direction: "asc" } as OrderSpec);
}

export function desc(col: AnyColumnBuilder | string, table?: string): OrderSpec {
  return Object.freeze({ expr: colRef(col, table), direction: "desc" } as OrderSpec);
}
