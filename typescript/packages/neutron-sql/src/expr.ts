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
  aggregate,
  expr as exprNode,
  fragment,
  ident,
  isLegacySqlFragment,
  legacyFragmentError,
  param as paramNode,
  paramCast,
  qual,
  type AggregateNode,
  type OrderSpec,
  type SubqueryNode,
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

/** `excluded.<column>` — the row proposed for insertion, addressable inside
 *  ON CONFLICT DO UPDATE SET assignments and predicates (Q03). Renders the
 *  pseudo-relation reference `excluded."col"`. Only valid in on-conflict
 *  clauses; update .set() values reject it before SQL. */
export function excluded(col: AnyColumnBuilder): ReturnType<typeof qual> {
  return qual("excluded", col.columnName);
}

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

function isAggregateNode(v: unknown): v is AggregateNode<unknown> {
  return typeof v === "object" && v !== null && (v as { kind?: unknown }).kind === "aggregate";
}

function orderRef(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): ValueNode {
  if (isAggregateNode(col)) return col;
  return colRef(col, table);
}

export function asc(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "asc" } as OrderSpec);
}

export function desc(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "desc" } as OrderSpec);
}

/** Explicit-null-ordering order specs (Q04). Keyset pagination REQUIRES one
 *  of these on every term (the ordering must be total and unambiguous);
 *  plain orderBy may use them anywhere `nulls first/last` is wanted. */
export function ascNullsLast(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "asc", nulls: "last" } as OrderSpec);
}

export function ascNullsFirst(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "asc", nulls: "first" } as OrderSpec);
}

export function descNullsLast(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "desc", nulls: "last" } as OrderSpec);
}

export function descNullsFirst(col: AnyColumnBuilder | AggregateNode<unknown> | string, table?: string): OrderSpec {
  return Object.freeze({ expr: orderRef(col, table), direction: "desc", nulls: "first" } as OrderSpec);
}

// ---------------------------------------------------------------------------
// Aggregates (Q02) — typed helpers over the frozen AggregateNode. Result
// types follow PostgreSQL exactly (see codecs.ts aggregateProjection for the
// decode side): count/sum-over-integers return int8 (bigint via the F03
// codec, never null — count is 0 on empty input); sum-over-int8/numeric and
// avg-over-exact return exact numeric strings; sum/avg over floats return
// number; min/max return the column's read type; all but count are null on
// empty input.
// ---------------------------------------------------------------------------

type IntFamily = "serial" | "integer" | "smallint";

export type SumResult<D extends ColumnDataType> =
  D extends IntFamily ? bigint | null
  : D extends "bigint" | "numeric" ? string | null
  : D extends "double" | "real" ? number | null
  : never;

export type AvgResult<D extends ColumnDataType> =
  D extends "double" | "real" ? number | null
  : D extends IntFamily | "bigint" | "numeric" ? string | null
  : never;

export type MinMaxResult<C> = C extends ColumnBuilder<ColumnDataType, boolean, boolean, infer RT> ? RT | null : never;

function aggregateArg<T>(op: "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or", col: AnyColumnBuilder | ValueNode, distinct = false): AggregateNode<T> {
  if (typeof col === "object" && col !== null && typeof (col as AnyColumnBuilder).columnName === "string") {
    const column = col as AnyColumnBuilder;
    return aggregate<T>(op, [colRef(column)], { distinct, argColumns: [column] });
  }
  return aggregate<T>(op, [col as ValueNode], { distinct });
}

/** `count(*)` — bigint, never null (0 on empty input). */
export function count(): AggregateNode<bigint>;
export function count(col: AnyColumnBuilder | ValueNode): AggregateNode<bigint>;
export function count(col?: AnyColumnBuilder | ValueNode): AggregateNode<bigint> {
  if (col === undefined) return aggregate<bigint>("count", []);
  return aggregateArg<bigint>("count", col);
}

/** `count(distinct col)` — bigint, never null. */
export function countDistinct(col: AnyColumnBuilder | ValueNode): AggregateNode<bigint> {
  return aggregateArg<bigint>("count", col, true);
}

/** `sum(col)` — int8 columns and numerics sum to exact strings, integer
 *  columns to int8 (bigint), floats to number. Null on empty input. */
export function sum<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown>): AggregateNode<SumResult<D>> {
  return aggregateArg<SumResult<D>>("sum", col);
}

/** `avg(col)` — exact strings except over floats (number). Null on empty input. */
export function avg<D extends ColumnDataType>(col: ColumnBuilder<D, boolean, boolean, unknown>): AggregateNode<AvgResult<D>> {
  return aggregateArg<AvgResult<D>>("avg", col);
}

/** `min(col)` / `max(col)` — the column's own read type and codec (temporal
 *  modes honored). Null on empty input. */
export function min<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C): AggregateNode<MinMaxResult<C>> {
  return aggregateArg<MinMaxResult<C>>("min", col);
}

export function max<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C): AggregateNode<MinMaxResult<C>> {
  return aggregateArg<MinMaxResult<C>>("max", col);
}

/** `string_agg(col, separator)` — text, null on empty input. */
export function stringAgg(col: ColumnBuilder<"text" | "varchar", boolean, boolean, unknown>, separator: string): AggregateNode<string | null> {
  return aggregate<string | null>("string_agg", [colRef(col), paramNode(separator)], { argColumns: [col, undefined] });
}

/** `bool_and(col)` / `bool_or(col)` — boolean, null on empty input. */
export function boolAnd(col: ColumnBuilder<"boolean", boolean, boolean, unknown>): AggregateNode<boolean | null> {
  return aggregateArg<boolean | null>("bool_and", col);
}

export function boolOr(col: ColumnBuilder<"boolean", boolean, boolean, unknown>): AggregateNode<boolean | null> {
  return aggregateArg<boolean | null>("bool_or", col);
}

// ---------------------------------------------------------------------------
// Subquery predicates (Q02)
// ---------------------------------------------------------------------------

/** `exists (subquery)` as a condition. Accepts a SubqueryNode or any builder
 *  exposing `.subquery()` (typed and AST select builders, set-op builders). */
export function exists(source: SubqueryNode | { subquery(): SubqueryNode }): Condition {
  const node = typeof (source as { subquery?: unknown }).subquery === "function" ? (source as { subquery(): SubqueryNode }).subquery() : (source as SubqueryNode);
  if (typeof node !== "object" || node === null || (node as { kind?: unknown }).kind !== "subquery") {
    throw new Error("exists: requires a subquery node or a builder with .subquery()");
  }
  return fragment("exists ", node);
}
