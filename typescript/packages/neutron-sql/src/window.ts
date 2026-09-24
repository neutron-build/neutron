// ---------------------------------------------------------------------------
// @neutron-build/sql — window functions (Q08)
// ---------------------------------------------------------------------------
// `over(fn, spec)` builds `fn(...) over (partition by … order by … frame)` as
// an AST fragment assembled STRUCTURALLY: every keyword is chosen here from a
// fixed vocabulary, every expression is a value node, frame offsets are
// validated safe integers. The fragment is registered as a window expression
// (module-private WeakMap) so the compiler can enforce placement — PostgreSQL
// allows window calls only in the select list and ORDER BY — and so the
// select planner can attach a decode plan (row_number/rank/dense_rank are
// int8 and follow the int8 codec; value functions keep their argument
// column's codec, temporals acquire the lossless wire form).
//
//   const ranked = cteTable("ranked", db.select({
//     id: posts.id,
//     rn: over(rowNumber(), { partitionBy: [posts.userId], orderBy: [desc(posts.createdAt), asc(posts.id)] }),
//   }).from(posts));
//   await db.select().from(ranked).where(lte(ranked.rn, 3n));
//
// Windows are never allowed in WHERE / GROUP BY / HAVING / JOIN ON / DML
// clauses; nested window calls and DISTINCT aggregates over a window are
// rejected (both are PostgreSQL errors) before any SQL runs.

import {
  aggregate,
  fragment,
  ident,
  isLegacySqlFragment,
  isValueNode,
  legacyFragmentError,
  param,
  projection,
  qual,
  validNulls,
  type AggregateNode,
  type AnyStatementNode,
  type FragmentNode,
  type FragmentPart,
  type OrderSpec,
  type ProjectionNode,
  type StatementNode,
  type ValueNode,
} from "./ast.js";
import {
  aggregateResultColumn,
  projectionDecoder,
  type BigintMode,
  type ProjectionDecoder,
  type StatementCapability,
  type TemporalMode,
} from "./codecs.js";
import { ColumnBuilder, tableRefParts, type AnyColumnBuilder, type ColumnDataType } from "./schema.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

declare const windowReadType: unique symbol;

/** A window expression: a structural fragment whose phantom carries the
 *  PostgreSQL result type (never present at runtime). */
export type WindowExpr<T> = FragmentNode & { readonly [windowReadType]: { readType: T } };

export type WindowFunctionOp =
  | "row_number"
  | "rank"
  | "dense_rank"
  | "percent_rank"
  | "cume_dist"
  | "ntile"
  | "lag"
  | "lead"
  | "first_value"
  | "last_value"
  | "nth_value";

/** A window-only function call. Not a value node: it must be wrapped by
 *  `over()` (PostgreSQL rejects these functions without OVER). */
export interface WindowFunction<T> {
  readonly kind: "window-function";
  readonly op: WindowFunctionOp;
  readonly args: readonly ValueNode[];
  /** Source column of the value argument (lag/lead/first/last/nth_value) —
   *  drives the decode plan. */
  readonly argColumn?: AnyColumnBuilder;
  /** Phantom (type-level only). */
  readonly _: { readType: T };
}

/** Value read type of a column (nullable: a window value function may read
 *  outside the partition/frame). */
export type WindowValueResult<C> = C extends ColumnBuilder<ColumnDataType, boolean, boolean, infer RT> ? RT | null : never;

/** Frame bound: `unbounded preceding`, `n preceding`, `current row`,
 *  `n following`, `unbounded following`. */
export type FrameBound =
  | "unbounded preceding"
  | "current row"
  | "unbounded following"
  | { readonly preceding: number }
  | { readonly following: number };

export interface WindowFrame {
  /** `rows` and `groups` take integer offsets; `range` supports only
   *  unbounded/current-row bounds here (value offsets are type-dependent). */
  readonly mode: "rows" | "range" | "groups";
  readonly start: FrameBound;
  /** Omitted: `<mode> <start>` (end defaults to current row in PostgreSQL). */
  readonly end?: FrameBound;
  /** Frame exclusion (PostgreSQL 11): `exclude current row | group | ties |
   *  no others`. Omitted: NO OTHERS (the whole frame). */
  readonly exclude?: FrameExclude;
}

/** Frame exclusion policy of a WindowFrame. */
export type FrameExclude = "current row" | "group" | "ties" | "no others";

export type WindowOrderTerm = AnyColumnBuilder | OrderSpec | ValueNode;

export interface WindowSpec {
  readonly partitionBy?: ReadonlyArray<AnyColumnBuilder | ValueNode>;
  readonly orderBy?: ReadonlyArray<WindowOrderTerm>;
  readonly frame?: WindowFrame;
}

/** Decode metadata of a window result — the shape derived-table/CTE
 *  pseudo-columns are built from (mirrors aggregateResultColumn). */
export interface WindowResultSpec {
  readonly dataType: ColumnDataType;
  readonly readMode?: BigintMode | TemporalMode;
  readonly valueDecoder?: (raw: string) => unknown;
}

interface WindowMeta {
  readonly fn: WindowFunction<unknown> | AggregateNode;
  readonly partitionBy: readonly ValueNode[];
  readonly orderBy: readonly OrderSpec[];
  readonly frame?: WindowFrame;
}

// Every fragment built by this module is registered here: the compiler's
// placement/locking checks and the select planner identify windows by
// identity, never by scanning SQL text.
const WINDOW_META = new WeakMap<object, WindowMeta>();

/** True when `v` is a window expression built by `over()`. */
export function isWindowExpr(v: unknown): v is WindowExpr<unknown> {
  return typeof v === "object" && v !== null && WINDOW_META.has(v);
}

// ---------------------------------------------------------------------------
// Window function constructors
// ---------------------------------------------------------------------------

function windowFn<T>(op: WindowFunctionOp, args: readonly ValueNode[], argColumn?: AnyColumnBuilder): WindowFunction<T> {
  return Object.freeze({ kind: "window-function", op, args: Object.freeze([...args]), argColumn } as unknown as WindowFunction<T>);
}

function isColumn(v: unknown): v is AnyColumnBuilder {
  return typeof v === "object" && v !== null && typeof (v as { columnName?: unknown }).columnName === "string" && (v as { kind?: unknown }).kind === undefined;
}

function columnRef(col: AnyColumnBuilder): ValueNode {
  if (col.ownerTable) return qual(...tableRefParts(col.ownerTable), col.columnName);
  return ident(col.columnName);
}

function requireColumn(col: unknown, who: string): AnyColumnBuilder {
  if (!isColumn(col)) throw new Error(`${who}: takes a schema column (its codec drives the result decode)`);
  return col;
}

function positiveInt(n: unknown, who: string, allowZero = false): number {
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < (allowZero ? 0 : 1)) {
    throw new Error(`${who}: must be a ${allowZero ? "non-negative" : "positive"} safe integer (got ${String(n)})`);
  }
  return n;
}

/** `row_number()` — int8 (bigint by default), never null. */
export function rowNumber(): WindowFunction<bigint> {
  return windowFn("row_number", []);
}

/** `rank()` — int8, never null. */
export function rank(): WindowFunction<bigint> {
  return windowFn("rank", []);
}

/** `dense_rank()` — int8, never null. */
export function denseRank(): WindowFunction<bigint> {
  return windowFn("dense_rank", []);
}

/** `percent_rank()` — double precision in [0, 1]. */
export function percentRank(): WindowFunction<number> {
  return windowFn("percent_rank", []);
}

/** `cume_dist()` — double precision in (0, 1]. */
export function cumeDist(): WindowFunction<number> {
  return windowFn("cume_dist", []);
}

/** `ntile(buckets)` — integer bucket number 1..buckets. */
export function ntile(buckets: number): WindowFunction<number> {
  return windowFn("ntile", [param(positiveInt(buckets, "ntile buckets"))]);
}

/** `lag(col, offset)` — the value `offset` rows before, null outside the
 *  partition. Offset is a non-negative integer (default 1). */
export function lag<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C, offset = 1): WindowFunction<WindowValueResult<C>> {
  const column = requireColumn(col, "lag");
  return windowFn("lag", [columnRef(column), param(positiveInt(offset, "lag offset", true))], column);
}

/** `lead(col, offset)` — the value `offset` rows after, null outside the
 *  partition. */
export function lead<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C, offset = 1): WindowFunction<WindowValueResult<C>> {
  const column = requireColumn(col, "lead");
  return windowFn("lead", [columnRef(column), param(positiveInt(offset, "lead offset", true))], column);
}

/** `first_value(col)` — null when the frame is empty. */
export function firstValue<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C): WindowFunction<WindowValueResult<C>> {
  const column = requireColumn(col, "firstValue");
  return windowFn("first_value", [columnRef(column)], column);
}

/** `last_value(col)` — note PostgreSQL's default frame ends at the current
 *  row; pass an explicit frame for the partition's last value. */
export function lastValue<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C): WindowFunction<WindowValueResult<C>> {
  const column = requireColumn(col, "lastValue");
  return windowFn("last_value", [columnRef(column)], column);
}

/** `nth_value(col, n)` — null when the frame has fewer than n rows. */
export function nthValue<C extends ColumnBuilder<ColumnDataType, boolean, boolean, unknown>>(col: C, n: number): WindowFunction<WindowValueResult<C>> {
  const column = requireColumn(col, "nthValue");
  return windowFn("nth_value", [columnRef(column), param(positiveInt(n, "nthValue n"))], column);
}

// ---------------------------------------------------------------------------
// over()
// ---------------------------------------------------------------------------

const WINDOW_OPS: ReadonlySet<string> = new Set([
  "row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead", "first_value", "last_value", "nth_value",
]);

function isWindowFunction(v: unknown): v is WindowFunction<unknown> {
  return typeof v === "object" && v !== null && (v as { kind?: unknown }).kind === "window-function" && WINDOW_OPS.has((v as { op?: unknown }).op as string);
}

function isAggregate(v: unknown): v is AggregateNode {
  return typeof v === "object" && v !== null && (v as { kind?: unknown }).kind === "aggregate" && isValueNode(v);
}

function normalizePartition(term: AnyColumnBuilder | ValueNode): ValueNode {
  if (isLegacySqlFragment(term)) throw legacyFragmentError("over partitionBy");
  if (isColumn(term)) return columnRef(term);
  if (!isValueNode(term)) throw new Error("over partitionBy: entries must be columns or value nodes");
  return term;
}

function normalizeOrder(term: WindowOrderTerm): OrderSpec {
  if (isLegacySqlFragment(term)) throw legacyFragmentError("over orderBy");
  if (typeof term === "object" && term !== null && typeof (term as OrderSpec).direction === "string" && (term as { kind?: unknown }).kind === undefined) {
    const spec = term as OrderSpec;
    if (spec.direction !== "asc" && spec.direction !== "desc") throw new Error(`over orderBy: direction must be "asc" or "desc"`);
    if (!isValueNode(spec.expr)) throw new Error("over orderBy: order expression must be a value node");
    return Object.freeze({ expr: spec.expr, direction: spec.direction, nulls: validNulls(spec.nulls, "over orderBy") }) as OrderSpec;
  }
  if (isColumn(term)) return Object.freeze({ expr: columnRef(term), direction: "asc" }) as OrderSpec;
  if (!isValueNode(term)) throw new Error("over orderBy: entries must be columns, asc()/desc() specs or value nodes");
  return Object.freeze({ expr: term, direction: "asc" }) as OrderSpec;
}

const BOUND_RANK = { "unbounded preceding": 0, preceding: 1, "current row": 2, following: 3, "unbounded following": 4 } as const;

function boundRank(b: FrameBound, who: string): number {
  if (b === "unbounded preceding" || b === "current row" || b === "unbounded following") return BOUND_RANK[b];
  if (typeof b === "object" && b !== null) {
    const keys = Object.keys(b);
    if (keys.length === 1 && keys[0] === "preceding") {
      positiveInt((b as { preceding: number }).preceding, `${who} preceding offset`, true);
      return BOUND_RANK.preceding;
    }
    if (keys.length === 1 && keys[0] === "following") {
      positiveInt((b as { following: number }).following, `${who} following offset`, true);
      return BOUND_RANK.following;
    }
  }
  throw new Error(`${who}: unknown frame bound ${JSON.stringify(b)} (use "unbounded preceding", { preceding: n }, "current row", { following: n }, "unbounded following")`);
}

function renderBound(b: FrameBound): string {
  if (typeof b === "string") return b;
  if ("preceding" in b) return `${b.preceding} preceding`;
  return `${b.following} following`;
}

function hasOffset(b: FrameBound | undefined): boolean {
  return typeof b === "object" && b !== null;
}

function validateFrame(frame: WindowFrame, orderBy: readonly OrderSpec[]): WindowFrame {
  if (typeof frame !== "object" || frame === null) throw new Error("over frame: must be { mode, start, end?, exclude? }");
  if (frame.mode !== "rows" && frame.mode !== "range" && frame.mode !== "groups") {
    throw new Error(`over frame: mode must be "rows", "range" or "groups" (got ${JSON.stringify(frame.mode)})`);
  }
  if (frame.exclude !== undefined && !FRAME_EXCLUDES.has(frame.exclude)) {
    throw new Error(`over frame: exclude must be "current row", "group", "ties" or "no others" (got ${JSON.stringify(frame.exclude)})`);
  }
  const start = boundRank(frame.start, "over frame start");
  const end = frame.end === undefined ? BOUND_RANK["current row"] : boundRank(frame.end, "over frame end");
  if (frame.start === "unbounded following") throw new Error("over frame: a frame cannot start at unbounded following");
  if (frame.end === "unbounded preceding") throw new Error("over frame: a frame cannot end at unbounded preceding");
  if (start > end) {
    throw new Error(`over frame: start "${renderBound(frame.start)}" comes after end "${renderBound(frame.end ?? "current row")}" — PostgreSQL rejects frames that end before they start`);
  }
  if (frame.mode === "range" && (hasOffset(frame.start) || hasOffset(frame.end))) {
    throw new Error("over frame: range mode with value offsets is not supported (the offset type depends on the ordering column) — use rows/groups offsets or unbounded/current-row bounds");
  }
  if (frame.mode === "groups" && orderBy.length === 0) {
    throw new Error("over frame: groups mode requires an orderBy (PostgreSQL: GROUPS mode requires an ORDER BY clause)");
  }
  return Object.freeze({ mode: frame.mode, start: frame.start, end: frame.end, exclude: frame.exclude });
}

const FRAME_EXCLUDES: ReadonlySet<string> = new Set(["current row", "group", "ties", "no others"]);

function containsWindowDirect(node: ValueNode): boolean {
  if (WINDOW_META.has(node)) return true;
  switch (node.kind) {
    case "expr":
    case "aggregate":
      return node.args.some(containsWindowDirect);
    case "fragment":
      return node.parts.some((p) => typeof p !== "string" && containsWindowDirect(p));
    default:
      return false;
  }
}

/** True when a value node contains a window expression outside any nested
 *  subquery (a subquery is its own scope). */
export function containsWindow(node: ValueNode): boolean {
  return containsWindowDirect(node);
}

/** Wrap a function (window-only or aggregate) with an OVER clause. The
 *  result is a value node usable in select projections and ORDER BY. */
export function over<T>(fn: WindowFunction<T> | AggregateNode<T>, spec: WindowSpec = {}): WindowExpr<T> {
  if (!isWindowFunction(fn) && !isAggregate(fn)) {
    throw new Error("over: takes a window function (rowNumber(), rank(), lag(col), …) or an aggregate (count(), sum(col), …)");
  }
  if (isAggregate(fn) && fn.distinct === true) {
    throw new Error("over: DISTINCT aggregates cannot be window functions (PostgreSQL: DISTINCT is not implemented for window functions)");
  }
  if (typeof spec !== "object" || spec === null) throw new Error("over: spec must be an object { partitionBy?, orderBy?, frame? }");
  for (const k of Object.keys(spec)) {
    if (k !== "partitionBy" && k !== "orderBy" && k !== "frame") throw new Error(`over: unknown spec key "${k}" (known: partitionBy, orderBy, frame)`);
  }
  const partitionBy = (spec.partitionBy ?? []).map(normalizePartition);
  const orderBy = (spec.orderBy ?? []).map(normalizeOrder);
  const frame = spec.frame === undefined ? undefined : validateFrame(spec.frame, orderBy);
  const nested = [...fn.args, ...partitionBy, ...orderBy.map((o) => o.expr)].some(containsWindowDirect);
  if (nested) throw new Error("over: window function calls cannot be nested (PostgreSQL rejects a window call inside another window's arguments or spec)");
  const meta: WindowMeta = Object.freeze({ fn, partitionBy: Object.freeze(partitionBy), orderBy: Object.freeze(orderBy), frame });
  return buildWindowFragment(meta, false) as WindowExpr<T>;
}

/** Cast canonical-text pseudo-column arguments (derived/CTE columns holding
 *  the canonical temporal wire text) back to their temporal type — the same
 *  rule aggregateProjection applies to min/max. */
function castCanonicalArg(arg: ValueNode, column: AnyColumnBuilder | undefined): ValueNode {
  if (column?.canonicalText !== true) return arg;
  const dt = column.dataType;
  if (dt !== "timestamp" && dt !== "timestamptz" && dt !== "date") return arg;
  return fragment(arg, dt === "date" ? "::date" : "::timestamp");
}

function renderFunction(fn: WindowFunction<unknown> | AggregateNode, castCanonical: boolean): FragmentPart[] {
  if (isAggregate(fn)) {
    if (!castCanonical) return [fn];
    return [aggregate(fn.op, fn.args.map((a, i) => castCanonicalArg(a, fn.argColumns?.[i])), { distinct: fn.distinct, argColumns: fn.argColumns })];
  }
  const parts: FragmentPart[] = [`${fn.op}(`];
  fn.args.forEach((arg, i) => {
    if (i > 0) parts.push(", ");
    parts.push(castCanonical && i === 0 ? castCanonicalArg(arg, fn.argColumn) : arg);
  });
  parts.push(")");
  return parts;
}

function buildWindowFragment(meta: WindowMeta, castCanonical: boolean): FragmentNode {
  const parts: FragmentPart[] = [...renderFunction(meta.fn, castCanonical), " over ("];
  const clauses: FragmentPart[][] = [];
  if (meta.partitionBy.length > 0) {
    const c: FragmentPart[] = ["partition by "];
    meta.partitionBy.forEach((p, i) => {
      if (i > 0) c.push(", ");
      c.push(p);
    });
    clauses.push(c);
  }
  if (meta.orderBy.length > 0) {
    const c: FragmentPart[] = ["order by "];
    meta.orderBy.forEach((o, i) => {
      if (i > 0) c.push(", ");
      c.push(o.expr, o.direction === "desc" ? " desc" : " asc");
      if (o.nulls !== undefined) c.push(` nulls ${o.nulls}`);
    });
    clauses.push(c);
  }
  if (meta.frame !== undefined) {
    const f = meta.frame;
    const bounds = f.end === undefined ? `${f.mode} ${renderBound(f.start)}` : `${f.mode} between ${renderBound(f.start)} and ${renderBound(f.end)}`;
    clauses.push([f.exclude === undefined ? bounds : `${bounds} exclude ${f.exclude}`]);
  }
  clauses.forEach((c, i) => {
    if (i > 0) parts.push(" ");
    parts.push(...c);
  });
  parts.push(")");
  const node = fragment(...parts);
  WINDOW_META.set(node, meta);
  return node;
}

// ---------------------------------------------------------------------------
// Result typing, projection planning and capabilities
// ---------------------------------------------------------------------------

function metaOf(w: object): WindowMeta {
  const meta = WINDOW_META.get(w);
  if (meta === undefined) throw new Error("window: not a window expression built by over()");
  return meta;
}

/** Source column of the window's value argument (value functions, min/max). */
function sourceColumn(meta: WindowMeta): AnyColumnBuilder | undefined {
  if (isAggregate(meta.fn)) return meta.fn.op === "min" || meta.fn.op === "max" ? meta.fn.argColumns?.[0] : undefined;
  return meta.fn.argColumn;
}

/** The result decode spec of a window expression (PostgreSQL result types):
 *  row_number/rank/dense_rank int8, ntile integer, percent_rank/cume_dist
 *  double, value functions their argument column's type, aggregates exactly
 *  as aggregateResultColumn. */
export function windowResultColumn(w: WindowExpr<unknown> | FragmentNode): WindowResultSpec {
  const meta = metaOf(w);
  const fn = meta.fn;
  if (isAggregate(fn)) return aggregateResultColumn(fn);
  switch (fn.op) {
    case "row_number":
    case "rank":
    case "dense_rank":
      return { dataType: "bigint" };
    case "ntile":
      return { dataType: "integer" };
    case "percent_rank":
    case "cume_dist":
      return { dataType: "double" };
    default: {
      const col = fn.argColumn!;
      return { dataType: col.dataType, readMode: col.readMode as BigintMode | TemporalMode | undefined, valueDecoder: col.valueDecoder };
    }
  }
}

function syntheticColumn(key: string, spec: WindowResultSpec): AnyColumnBuilder {
  const c = new ColumnBuilder(key, spec.dataType);
  if (spec.readMode !== undefined) c.readMode = spec.readMode;
  if (spec.valueDecoder !== undefined) c.valueDecoder = spec.valueDecoder;
  return c;
}

/** Capabilities a window expression requires: window functions, plus the
 *  GROUPS frame mode / EXCLUDE frame exclusion (both PostgreSQL 11) when
 *  used. */
export function windowCapabilities(w: WindowExpr<unknown> | FragmentNode): StatementCapability[] {
  const meta = metaOf(w);
  const caps: StatementCapability[] = ["window-functions"];
  if (meta.frame?.mode === "groups") caps.push("window-frame-groups");
  if (meta.frame?.exclude !== undefined) caps.push("window-frame-exclude");
  return caps;
}

/** Add the capability requirements of every window expression inside `node`
 *  (same scope — subqueries are their own statements). Used for projections
 *  that only CONTAIN a window (arithmetic, fragments) and for ORDER BY
 *  expressions, where the window result needs no decode plan of its own. */
export function collectWindowCapabilities(node: ValueNode, caps: Set<StatementCapability>): void {
  const meta = WINDOW_META.get(node);
  if (meta !== undefined) {
    caps.add("window-functions");
    if (meta.frame?.mode === "groups") caps.add("window-frame-groups");
    if (meta.frame?.exclude !== undefined) caps.add("window-frame-exclude");
    return;
  }
  switch (node.kind) {
    case "expr":
    case "aggregate":
      for (const a of node.args) collectWindowCapabilities(a, caps);
      return;
    case "fragment":
      for (const p of node.parts) if (typeof p !== "string") collectWindowCapabilities(p, caps);
      return;
    default:
      return;
  }
}

/** Projection + decode plan for one projected window expression. Mirrors
 *  aggregateProjection: temporals and bytea acquire the lossless to_jsonb
 *  wire form (jsonb-functions capability), int8/numeric decode natively. */
export function windowProjection(w: WindowExpr<unknown> | FragmentNode, key: string): { node: ProjectionNode; decoder: ProjectionDecoder | null; capabilities: StatementCapability[] } {
  const meta = metaOf(w);
  const spec = windowResultColumn(w);
  const caps = windowCapabilities(w);
  const dt = spec.dataType;
  const src = sourceColumn(meta);
  const decoderFor = (): ProjectionDecoder | null => projectionDecoder(meta.fn.op, syntheticColumn(key, spec), key);
  if (dt === "timestamp" || dt === "timestamptz" || dt === "date") {
    caps.push("jsonb-functions");
    if (src?.canonicalText === true) {
      // Canonical-text pseudo-column: parse back to the temporal type
      // before the window evaluates; the canonical timestamptz text IS the
      // UTC wall clock, so no `at time zone` (see aggregateProjection).
      return { node: projection(fragment("to_jsonb(", buildWindowFragment(meta, true), ")::text"), key), decoder: decoderFor(), capabilities: caps };
    }
    const inner: FragmentPart[] = dt === "timestamptz" ? ["to_jsonb(", w, " at time zone 'UTC')::text"] : ["to_jsonb(", w, ")::text"];
    return { node: projection(fragment(...inner), key), decoder: decoderFor(), capabilities: caps };
  }
  if (dt === "bytea") {
    // bytea needs no lossless wire wrapping: both bundled drivers deliver
    // bytea natively as a Buffer (a Uint8Array) — the same convention as
    // plain bytea column reads (wireReadNode returns null for bytea).
    return { node: projection(w, key), decoder: null, capabilities: caps };
  }
  return { node: projection(w, key), decoder: decoderFor(), capabilities: caps };
}

// ---------------------------------------------------------------------------
// Placement (compile choke point)
// ---------------------------------------------------------------------------

function windowPlacementError(position: string): Error {
  return new Error(
    `window functions are not allowed in ${position} (PostgreSQL evaluates them after WHERE/GROUP BY/HAVING) — compute the window in a derivedTable/cteTable and filter the outer query`,
  );
}

/** Reject windows directly in a restricted position; subqueries inside are
 *  their own statements and are checked with the full statement rules. */
function checkRestricted(node: ValueNode, position: string): void {
  if (containsWindowDirect(node)) throw windowPlacementError(position);
  visitSubqueries(node);
}

/** Recurse into subqueries of an allowed position (projection/order by). */
function visitSubqueries(node: ValueNode): void {
  switch (node.kind) {
    case "subquery":
      assertWindowPlacement(node.select);
      return;
    case "expr":
    case "aggregate":
      for (const a of node.args) visitSubqueries(a);
      return;
    case "fragment":
      for (const p of node.parts) if (typeof p !== "string") visitSubqueries(p);
      return;
    default:
      return;
  }
}

function assertSelectPlacement(stmt: StatementNode): void {
  for (const c of stmt.ctes ?? []) assertWindowPlacement(c.select);
  if (stmt.from?.kind === "subquery") assertWindowPlacement(stmt.from.select);
  for (const j of stmt.joins ?? []) {
    if (j.target.kind === "subquery") assertWindowPlacement(j.target.select);
    if (j.on !== undefined) checkRestricted(j.on, "JOIN ON");
  }
  for (const w of stmt.where ?? []) checkRestricted(w, "WHERE");
  for (const g of stmt.groupBy ?? []) checkRestricted(g, "GROUP BY");
  for (const h of stmt.having ?? []) checkRestricted(h, "HAVING");
  for (const p of stmt.projections ?? []) visitSubqueries(p.expr);
  for (const o of stmt.orderBy ?? []) visitSubqueries(o.expr);
  for (const b of stmt.setOps ?? []) assertWindowPlacement(b.select);
}

/** Compile choke point: window expressions may appear only in select lists
 *  and ORDER BY — never in WHERE, GROUP BY, HAVING, JOIN ON, UPDATE SET,
 *  RETURNING or conflict clauses (PostgreSQL SQLSTATE 42P20). Fails before
 *  any SQL renders. */
export function assertWindowPlacement(stmt: AnyStatementNode): void {
  switch (stmt.kind) {
    case "select":
      assertSelectPlacement(stmt);
      return;
    case "update":
      for (const s of stmt.sets ?? []) checkRestricted(s.value, "UPDATE SET");
      for (const w of stmt.where ?? []) checkRestricted(w, "WHERE");
      for (const p of stmt.returning ?? []) checkRestricted(p.expr, "RETURNING");
      return;
    case "delete":
      for (const w of stmt.where ?? []) checkRestricted(w, "WHERE");
      for (const p of stmt.returning ?? []) checkRestricted(p.expr, "RETURNING");
      return;
    case "insert": {
      const oc = stmt.onConflict;
      if (oc?.target?.kind === "columns") for (const w of oc.target.where ?? []) checkRestricted(w, "ON CONFLICT");
      for (const s of oc?.sets ?? []) checkRestricted(s.value, "ON CONFLICT DO UPDATE SET");
      for (const w of oc?.where ?? []) checkRestricted(w, "ON CONFLICT DO UPDATE WHERE");
      for (const p of stmt.returning ?? []) checkRestricted(p.expr, "RETURNING");
      return;
    }
  }
}

/** True when the node (outside nested subqueries) holds an aggregate call —
 *  used by the locking-clause compatibility check. */
export function containsAggregate(node: ValueNode): boolean {
  switch (node.kind) {
    case "aggregate":
      return true;
    case "expr":
      return node.args.some(containsAggregate);
    case "fragment":
      return node.parts.some((p) => typeof p !== "string" && containsAggregate(p));
    default:
      return false;
  }
}
