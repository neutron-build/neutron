// ---------------------------------------------------------------------------
// @neutron-build/sql — SQL AST nodes (F01 architecture spike)
// ---------------------------------------------------------------------------
// Discriminated, immutable nodes covering the README §3.1 node list:
// identifier, qualified reference, parameter, trusted SQL segment,
// expression, projection, join, subquery, CTE, statement.
//
// Nodes are plain frozen data. All rendering (and all parameter numbering)
// happens in compile.ts as ONE traversal — nothing here produces SQL text
// with $n placeholders, so no consumer ever needs to renumber raw SQL.
//
// The trust boundary: raw SQL text can only enter through `trustSql()`,
// which demands an explicit acknowledgment argument and returns a branded
// `TrustedSql`. Template-literal text in `sqlAst` is trusted by construction
// (it is authored in source); interpolated VALUES are always parameters.

import { getDerivedRecord, isPgTable, tableRefParts, type AnyColumnBuilder, type AnyPgTable, type DerivedRecord } from "./schema.js";

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

export interface IdentifierNode {
  readonly kind: "identifier";
  readonly name: string;
}

/** Qualified reference: `"schema"."table"."column"` — parts are identifiers. */
export interface QualifiedNode {
  readonly kind: "qualified";
  readonly parts: readonly string[];
}

/** A bound value. Renders as the next $n placeholder, never as SQL text.
 *  `cast` (set via paramCast) renders the placeholder as `$n::text::<cast>`
 *  so the parameter stays a text value on both drivers — codecs use it for
 *  temporal and json writes (F03's text-typed bind sites). */
export interface ParamNode {
  readonly kind: "param";
  readonly value: unknown;
  readonly cast?: string;
}

/** An insert cell requesting the column DEFAULT (SQL `default` keyword). */
export interface DefaultNode {
  readonly kind: "default";
}

/** Verbatim SQL text from an acknowledged `TrustedSql`. Never scanned. */
export interface TrustedNode {
  readonly kind: "trusted";
  readonly text: string;
}

/** Ordered mix of trusted template text and structurally spliced nodes. */
export interface FragmentNode {
  readonly kind: "fragment";
  readonly parts: readonly FragmentPart[];
}

export type FragmentPart = string | ValueNode;

/** Structured operator application: binary `(a op b)`, unary `(op a)`, call `op(a, b)`. */
export interface ExpressionNode {
  readonly kind: "expr";
  readonly form: "binary" | "unary" | "call";
  readonly op: string;
  readonly args: readonly ValueNode[];
}

/** Aggregate function application: `op(args…)` with an optional `distinct`
 *  qualifier. `count()` with zero args renders `count(*)`. The phantom
 *  `_.readType` carries the aggregate's result type (Q02) — count/sum over
 *  integer columns return int8, which follows the F03 int8 codec. */
export interface AggregateNode<T = unknown> {
  readonly kind: "aggregate";
  readonly op: AggregateOp;
  readonly args: readonly ValueNode[];
  readonly distinct?: boolean;
  /** Source column of each arg (undefined for non-column args), parallel to
   *  `args` — drives decode planning when the aggregate is projected. */
  readonly argColumns?: readonly (AnyColumnBuilder | undefined)[];
  /** Phantom (type-level only, never present at runtime). */
  readonly _: { readType: T };
}

export type AggregateOp = "count" | "sum" | "avg" | "min" | "max" | "string_agg" | "bool_and" | "bool_or";

/** `expr [as "alias"]` — one SELECT list entry. */
export interface ProjectionNode {
  readonly kind: "projection";
  readonly expr: ValueNode;
  readonly alias?: string;
}

export type JoinType = "inner" | "left" | "right" | "full" | "cross";

export type JoinTarget = IdentifierNode | QualifiedNode | SubqueryNode;

export interface JoinNode {
  readonly kind: "join";
  readonly type: JoinType;
  readonly target: JoinTarget;
  readonly alias?: string;
  /** Required for non-cross joins (validated at construction). */
  readonly on?: ValueNode;
}

/** A full select used as a value: `(select …)` — derived tables, IN lists, scalars. */
export interface SubqueryNode {
  readonly kind: "subquery";
  readonly select: StatementNode;
}

export interface CteNode {
  readonly kind: "cte";
  readonly name: string;
  readonly columns?: readonly string[];
  readonly select: StatementNode;
}

export interface OrderSpec {
  readonly expr: ValueNode;
  readonly direction: "asc" | "desc";
}

export type FromTarget = IdentifierNode | QualifiedNode | SubqueryNode;

/** One trailing set operation: `… <op> (select …)`. Branches render
 *  parenthesized, so chaining is left-associative in call order regardless
 *  of SQL's intersect-over-union precedence. */
export interface SetOpBranch {
  readonly op: SetOpKind;
  readonly select: StatementNode;
}

export type SetOpKind = "union" | "union all" | "intersect" | "intersect all" | "except" | "except all";

/** The statement node: with / select list / from / joins / where / group by /
 *  having / set operations / order / limit. When `setOps` is non-empty the
 *  node's own select is the first branch and orderBy/limit apply to the
 *  whole compound (PostgreSQL semantics). */
export interface StatementNode {
  readonly kind: "select";
  readonly ctes: readonly CteNode[];
  /** Render `with recursive` (required when any CTE self-references). */
  readonly recursive: boolean;
  readonly distinct: boolean;
  readonly projections: readonly ProjectionNode[];
  readonly from?: FromTarget;
  readonly fromAlias?: string;
  readonly joins: readonly JoinNode[];
  readonly where: readonly ValueNode[];
  readonly groupBy: readonly ValueNode[];
  readonly having: readonly ValueNode[];
  readonly setOps: readonly SetOpBranch[];
  readonly orderBy: readonly OrderSpec[];
  readonly limit?: number;
  readonly offset?: number;
}

/** Target of an insert/update/delete statement. */
export type MutationTarget = IdentifierNode | QualifiedNode;

/** One insert row: a cell per listed column, each a bound parameter
 *  (optionally text-cast) or a DEFAULT request. */
export type InsertCell = ParamNode | DefaultNode;

export interface InsertStatementNode {
  readonly kind: "insert";
  readonly table: MutationTarget;
  /** Physical column names, schema-ordered; empty with defaultValues. */
  readonly columns: readonly string[];
  readonly rows: readonly ReadonlyArray<InsertCell>[];
  /** Single-row `insert into … default values` form. */
  readonly defaultValues: boolean;
  readonly returning?: readonly ProjectionNode[];
}

export interface UpdateAssignment {
  readonly column: string;
  readonly value: ValueNode;
}

export interface UpdateStatementNode {
  readonly kind: "update";
  readonly table: MutationTarget;
  readonly sets: readonly UpdateAssignment[];
  /** Required: an update without a predicate is a builder-level error. */
  readonly where: readonly ValueNode[];
  readonly returning?: readonly ProjectionNode[];
}

export interface DeleteStatementNode {
  readonly kind: "delete";
  readonly table: MutationTarget;
  /** Required: a delete without a predicate is a builder-level error. */
  readonly where: readonly ValueNode[];
  readonly returning?: readonly ProjectionNode[];
}

/** Any compilable top-level statement. */
export type AnyStatementNode = StatementNode | InsertStatementNode | UpdateStatementNode | DeleteStatementNode;

/** Nodes valid in expression positions. */
export type ValueNode =
  | IdentifierNode
  | QualifiedNode
  | ParamNode
  | TrustedNode
  | FragmentNode
  | ExpressionNode
  | AggregateNode
  | SubqueryNode;

/** The full frozen union (README §3.1 node list). */
export type SqlNode = ValueNode | ProjectionNode | JoinNode | CteNode | AnyStatementNode | DefaultNode;

// ---------------------------------------------------------------------------
// CTE self-reference detection (Q02): `with recursive` is required exactly
// when a CTE's own statement references the CTE's name. Structural walk over
// node fields only — fragment text is never scanned (no regex over SQL); a
// hand-typed self-reference inside fragment text fails closed at the
// database ("recursive reference to query …") instead of being missed here.
// ---------------------------------------------------------------------------

function nodeReferencesName(node: ValueNode, name: string): boolean {
  switch (node.kind) {
    case "identifier":
      return node.name === name;
    case "qualified":
      return node.parts[0] === name;
    case "expr":
      return node.args.some((a) => nodeReferencesName(a, name));
    case "aggregate":
      return node.args.some((a) => nodeReferencesName(a, name));
    case "fragment":
      return node.parts.some((p) => typeof p !== "string" && nodeReferencesName(p, name));
    case "subquery":
      return statementReferencesName(node.select, name);
    default:
      return false;
  }
}

function targetReferencesName(target: FromTarget | JoinTarget | MutationTarget | undefined, name: string): boolean {
  if (target === undefined) return false;
  if (target.kind === "identifier") return target.name === name;
  if (target.kind === "qualified") return target.parts[0] === name;
  return statementReferencesName(target.select, name);
}

/** True when the statement references `name` as a table-ish identity — a
 *  FROM/JOIN target, a qualified reference's first part, or anywhere inside
 *  a nested statement (subquery, CTE body, set-operation branch). */
export function statementReferencesName(stmt: StatementNode, name: string): boolean {
  if (typeof name !== "string" || name.length === 0) return false;
  if (stmt.ctes.some((c) => statementReferencesName(c.select, name))) return true;
  if (targetReferencesName(stmt.from, name)) return true;
  if (stmt.joins.some((j) => targetReferencesName(j.target, name) || (j.on !== undefined && nodeReferencesName(j.on, name)))) return true;
  if (stmt.where.some((w) => nodeReferencesName(w, name))) return true;
  if (stmt.groupBy.some((g) => nodeReferencesName(g, name))) return true;
  if (stmt.having.some((h) => nodeReferencesName(h, name))) return true;
  if (stmt.setOps.some((b) => statementReferencesName(b.select, name))) return true;
  if (stmt.projections.some((p) => nodeReferencesName(p.expr, name))) return true;
  if (stmt.orderBy.some((o) => nodeReferencesName(o.expr, name))) return true;
  return false;
}

// ---------------------------------------------------------------------------
// CTE identity inside sql fragments (Q02 review MINOR-2): interpolating a
// cteTable handle (or one of its columns) into a `sql` template renders the
// bare name WITHOUT auto-registering the CTE — registration happens in
// from/joins of the consuming statement. The rendered node is branded with
// the handle's DerivedRecord (module-private, non-enumerable), and every
// builder validates the assembled statement: each branded reference must
// resolve, in lexical scope, to a CTE registered from the SAME source
// statement. Unregistered references and same-name shadowing (a different
// statement under the name) fail closed at compile time instead of silently
// binding to whatever relation shares the name.
// ---------------------------------------------------------------------------

const CTE_REF_BRAND = Symbol("@neutron-build/sql.cte-ref");

function brandCteRef<N extends object>(node: N, record: DerivedRecord): N {
  Object.defineProperty(node, CTE_REF_BRAND, {
    value: record,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  return node;
}

/** The CTE identity a fragment-spliced reference carries, or undefined for
 *  plain identifier/qualified nodes. */
export function cteRefRecord(node: object): DerivedRecord | undefined {
  const rec = (node as { [CTE_REF_BRAND]?: unknown })[CTE_REF_BRAND];
  if (typeof rec !== "object" || rec === null) return undefined;
  const r = rec as DerivedRecord;
  return (r.kind === "cte" || r.kind === "derived") && typeof r.name === "string" ? r : undefined;
}

function brandedCteIdent(record: DerivedRecord): IdentifierNode {
  const node = { kind: "identifier", name: validIdent(record.name, "cte reference") } as IdentifierNode;
  return frozen(brandCteRef(node, record));
}

function brandedCteQual(parts: readonly string[], record: DerivedRecord): QualifiedNode {
  const node = { kind: "qualified", parts: parts.map((p) => validIdent(p, "qual")) } as QualifiedNode;
  return frozen(brandCteRef(node, record));
}

type CteScope = ReadonlyMap<string, StatementNode>;

function assertCteRefResolves(record: DerivedRecord, scopes: readonly CteScope[]): void {
  for (const scope of scopes) {
    const defined = scope.get(record.name);
    if (defined === undefined) continue;
    if (defined !== record.select) {
      throw new Error(
        `sql fragment references cte "${record.name}" but a different statement is registered under that name in scope — the reference would silently bind to it; reference the handle from from/joins (its own CTE registers) or rename one`,
      );
    }
    return;
  }
  throw new Error(
    `sql fragment references cte "${record.name}" which is not registered on the consuming statement — reference the handle from from/joins (or register the same source with withCte) so "with" emits its definition`,
  );
}

function visitValueNodeForCteRefs(node: ValueNode, scopes: readonly CteScope[]): void {
  switch (node.kind) {
    case "identifier":
    case "qualified": {
      const record = cteRefRecord(node);
      if (record !== undefined) assertCteRefResolves(record, scopes);
      return;
    }
    case "expr":
      for (const a of node.args) visitValueNodeForCteRefs(a, scopes);
      return;
    case "aggregate":
      for (const a of node.args) visitValueNodeForCteRefs(a, scopes);
      return;
    case "fragment":
      for (const p of node.parts) if (typeof p !== "string") visitValueNodeForCteRefs(p, scopes);
      return;
    case "subquery":
      assertCteRefsResolve(node.select, scopes);
      return;
    default:
      return;
  }
}

/** Validate every branded CTE reference in an assembled statement (select or
 *  mutation): each must resolve in lexical scope — the statement's own WITH
 *  entries, then enclosing statements — to the same source statement the
 *  author's handle carries. Builders call this on the final node; fragment
 *  text is never scanned. */
export function assertCteRefsResolve(stmt: AnyStatementNode, outer: readonly CteScope[] = []): void {
  if (stmt.kind === "insert") return;
  if (stmt.kind === "update" || stmt.kind === "delete") {
    for (const w of stmt.where) visitValueNodeForCteRefs(w, outer);
    if (stmt.returning !== undefined) for (const p of stmt.returning) visitValueNodeForCteRefs(p.expr, outer);
    return;
  }
  const local: CteScope = new Map(stmt.ctes.map((c) => [c.name, c.select] as const));
  const scopes = [local, ...outer];
  for (const c of stmt.ctes) assertCteRefsResolve(c.select, scopes);
  if (stmt.from?.kind === "subquery") assertCteRefsResolve(stmt.from.select, scopes);
  for (const j of stmt.joins) {
    if (j.target.kind === "subquery") assertCteRefsResolve(j.target.select, scopes);
    if (j.on !== undefined) visitValueNodeForCteRefs(j.on, scopes);
  }
  for (const w of stmt.where) visitValueNodeForCteRefs(w, scopes);
  for (const g of stmt.groupBy) visitValueNodeForCteRefs(g, scopes);
  for (const h of stmt.having) visitValueNodeForCteRefs(h, scopes);
  for (const p of stmt.projections) visitValueNodeForCteRefs(p.expr, scopes);
  for (const o of stmt.orderBy) visitValueNodeForCteRefs(o.expr, scopes);
  for (const b of stmt.setOps) assertCteRefsResolve(b.select, scopes);
}

// ---------------------------------------------------------------------------
// Trusted SQL boundary
// ---------------------------------------------------------------------------

declare const trustedSqlBrand: unique symbol;

/** Branded raw SQL. Constructible only through `trustSql` + acknowledgment. */
export interface TrustedSql {
  readonly [trustedSqlBrand]: true;
  readonly text: string;
}

/** The explicit acknowledgment `trustSql` demands. There is one valid value. */
export interface TrustedSqlAcknowledgment {
  readonly iAcknowledgeThisIsTrustedSql: true;
}

export const TRUSTED_SQL_ACK: TrustedSqlAcknowledgment = { iAcknowledgeThisIsTrustedSql: true };

const TRUSTED_MARKER = Symbol("@neutron-build/sql.trusted");

/** Promote raw SQL text to the trusted brand. The acknowledgment argument is
 *  the visible, reviewable moment raw text enters a query. */
export function trustSql(text: string, ack: TrustedSqlAcknowledgment): TrustedSql {
  if (typeof text !== "string") throw new Error("trustSql: text must be a string");
  if (ack !== TRUSTED_SQL_ACK) throw new Error("trustSql: acknowledgment object required (use TRUSTED_SQL_ACK)");
  const branded = { text } as TrustedSql & { [TRUSTED_MARKER]: true };
  (branded as { [TRUSTED_MARKER]?: true })[TRUSTED_MARKER] = true;
  return Object.freeze(branded);
}

// ---------------------------------------------------------------------------
// Node brand (F-R1): the text-bearing node kinds ("trusted", "fragment")
// splice raw SQL text at compile time, so they carry a module-private,
// NON-ENUMERABLE runtime brand attached only by their constructors.
// JSON.parse'd lookalikes, spread copies and hand-built literals all lack the
// brand (spread skips non-enumerable properties) and are rejected wherever
// these kinds are accepted — isValueNode, templatePart, fragment() and the
// compiler itself. Structural kinds stay plain data; the compiler validates
// or quotes their text-bearing fields (see compile.ts).
// ---------------------------------------------------------------------------

const NODE_BRAND = Symbol("@neutron-build/sql.node-brand");

/** The node kinds whose fields carry verbatim SQL text. */
export type TextBearingKind = "trusted" | "fragment";

function brandTextNode<N extends object>(node: N): N {
  Object.defineProperty(node, NODE_BRAND, {
    value: (node as { kind: unknown }).kind,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  return node;
}

/** The claimed kind when `v` claims a text-bearing kind WITHOUT the
 *  constructor brand — i.e. it was forged (JSON.parse, spread copy,
 *  hand-built literal) rather than built by fragment()/sqlAst()/trustSql().
 *  Null for everything else, including correctly branded nodes. */
export function forgedTextKind(v: object): TextBearingKind | null {
  const kind = (v as { kind?: unknown }).kind;
  if (kind !== "trusted" && kind !== "fragment") return null;
  return (v as { [NODE_BRAND]?: unknown })[NODE_BRAND] === kind ? null : (kind as TextBearingKind);
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function validIdent(name: string, what: string): string {
  if (typeof name !== "string" || name.length === 0) throw new Error(`${what}: must be a non-empty string`);
  if (name.includes("\0")) throw new Error(`${what}: NUL bytes are not allowed`);
  return name;
}

const KEYWORD_OR_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
const OPERATORS = new Set(["=", "<>", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||", "and", "or", "like", "ilike", "is", "is not", "in", "not in"]);
const CAST_TYPE_NAME = /^[a-z][a-z0-9_]*$/;

/** Text-typed parameter cast (`$n::text::<cast>`). Only plain lowercase type
 *  names are accepted; validated here and again at the compile choke point. */
export function validParamCast(cast: string): string {
  if (typeof cast !== "string" || !CAST_TYPE_NAME.test(cast)) {
    throw new Error(`paramCast: cast must be a plain lowercase type name, got ${JSON.stringify(cast)}`);
  }
  return cast;
}

export function validOp(op: string, form: ExpressionNode["form"]): string {
  if (typeof op !== "string" || op.length === 0) throw new Error("expr: op must be a non-empty string");
  if (form === "call") {
    if (!KEYWORD_OR_IDENT.test(op)) throw new Error(`expr: call op must be a function identifier, got ${JSON.stringify(op)}`);
  } else if (!KEYWORD_OR_IDENT.test(op) && !OPERATORS.has(op.toLowerCase())) {
    throw new Error(`expr: op ${JSON.stringify(op)} is not an allowed operator`);
  }
  return op;
}

function validParamValue(value: unknown, what: string): void {
  const t = typeof value;
  if (t === "undefined" || t === "symbol" || t === "function") throw new Error(`${what}: ${t} values cannot be bound as parameters`);
}

// ---------------------------------------------------------------------------
// Freezing: nodes are immutable data. Freezes node objects and node arrays,
// never ParamNode.value (user data is borrowed, not owned).
// ---------------------------------------------------------------------------

function deepFreeze(x: unknown): void {
  if (Object.isFrozen(x)) return;
  if (Array.isArray(x)) {
    Object.freeze(x);
    for (const e of x) deepFreeze(e);
    return;
  }
  if (typeof x === "object" && x !== null) {
    Object.freeze(x);
    for (const [k, v] of Object.entries(x)) {
      if (k === "value") continue;
      deepFreeze(v);
    }
  }
}

function frozen<N>(node: N): N {
  deepFreeze(node);
  return node;
}

// ---------------------------------------------------------------------------
// Node constructors (return frozen nodes)
// ---------------------------------------------------------------------------

export function ident(name: string): IdentifierNode {
  return frozen<IdentifierNode>({ kind: "identifier", name: validIdent(name, "ident") });
}

export function qual(...parts: string[]): QualifiedNode {
  if (parts.length === 0) throw new Error("qual: at least one part required");
  return frozen<QualifiedNode>({ kind: "qualified", parts: parts.map((p) => validIdent(p, "qual")) });
}

export function param(value: unknown): ParamNode {
  validParamValue(value, "param");
  return frozen<ParamNode>({ kind: "param", value });
}

/** A parameter rendered at a text-typed site: `$n::text::<cast>`. The codec
 *  layer's canonical temporal/json binds (F03) ride parameters this way. */
export function paramCast(value: unknown, cast: string): ParamNode {
  validParamValue(value, "param");
  return frozen<ParamNode>({ kind: "param", value, cast: validParamCast(cast) });
}

/** An insert cell requesting the column DEFAULT. */
export function defaultCell(): DefaultNode {
  return frozen<DefaultNode>({ kind: "default" });
}

export function expr(form: ExpressionNode["form"], op: string, args: readonly ValueNode[]): ExpressionNode {
  if (form === "binary" && args.length !== 2) throw new Error("expr: binary form takes exactly 2 args");
  if (form === "unary" && args.length !== 1) throw new Error("expr: unary form takes exactly 1 arg");
  return frozen<ExpressionNode>({ kind: "expr", form, op: validOp(op, form), args: [...args] });
}

const AGGREGATE_OPS: ReadonlySet<string> = new Set(["count", "sum", "avg", "min", "max", "string_agg", "bool_and", "bool_or"]);

/** Validate an aggregate op + arity (constructor and compile choke point). */
export function validAggregate(op: string, argCount: number, what: string): AggregateOp {
  if (typeof op !== "string" || !AGGREGATE_OPS.has(op)) {
    throw new Error(`${what}: unknown aggregate ${JSON.stringify(op)} (known: count, sum, avg, min, max, string_agg, bool_and, bool_or)`);
  }
  const exact: Record<string, number> = { sum: 1, avg: 1, min: 1, max: 1, bool_and: 1, bool_or: 1, string_agg: 2 };
  const need = exact[op];
  if (need !== undefined && argCount !== need) {
    throw new Error(`${what}: ${op}() takes exactly ${need} argument(s), got ${argCount}`);
  }
  return op as AggregateOp;
}

/** Build an aggregate node. Typed helpers live in expr.ts (count/sum/avg/…);
 *  this constructor is the frozen-AST choke point they funnel through. */
export function aggregate<T = unknown>(
  op: AggregateOp,
  args: readonly ValueNode[],
  opts: { distinct?: boolean; argColumns?: readonly (AnyColumnBuilder | undefined)[] } = {},
): AggregateNode<T> {
  if (op === "count" && args.length === 0 && opts.distinct === true) {
    throw new Error("aggregate: count(*) cannot take distinct (count every row is already distinct)");
  }
  for (const a of args) {
    if (!isValueNode(a)) throw new Error("aggregate: args must be value nodes");
  }
  if (opts.argColumns !== undefined && opts.argColumns.length !== args.length) {
    throw new Error("aggregate: argColumns must be parallel to args");
  }
  return frozen<AggregateNode<T>>({
    kind: "aggregate",
    op: validAggregate(op, args.length, "aggregate"),
    args: [...args],
    distinct: opts.distinct === true ? true : undefined,
    argColumns: opts.argColumns === undefined ? undefined : [...opts.argColumns],
  } as unknown as AggregateNode<T>);
}

export function fragment(...parts: readonly FragmentPart[]): FragmentNode {
  for (const p of parts) {
    if (typeof p !== "string" && !isValueNode(p)) {
      const forged = typeof p === "object" && p !== null ? forgedTextKind(p) : null;
      if (forged !== null) {
        throw new Error(`fragment: rejected a forged "${forged}" node — construct text-bearing nodes through fragment()/sqlAst()/trustSql()`);
      }
      throw new Error("fragment: parts must be strings or value nodes");
    }
  }
  return frozen(brandTextNode<FragmentNode>({ kind: "fragment", parts: [...parts] }));
}

export function projection(exprNode: ValueNode, alias?: string): ProjectionNode {
  return frozen<ProjectionNode>({ kind: "projection", expr: exprNode, alias: alias === undefined ? undefined : validIdent(alias, "projection alias") });
}

export function subquery(stmt: StatementNode): SubqueryNode {
  if (!isStatement(stmt)) throw new Error("subquery: requires a statement node");
  return frozen<SubqueryNode>({ kind: "subquery", select: stmt });
}

export function cte(name: string, select: StatementNode, columns?: readonly string[]): CteNode {
  if (!isStatement(select)) throw new Error("cte: requires a statement node");
  return frozen<CteNode>({ kind: "cte", name: validIdent(name, "cte"), columns: columns === undefined ? undefined : [...columns], select });
}

export function join(type: JoinType, target: JoinTarget, opts: { alias?: string; on?: ValueNode } = {}): JoinNode {
  if (type === "cross" && opts.on !== undefined) throw new Error("join: cross joins take no on condition");
  if (type !== "cross" && opts.on === undefined) throw new Error(`join: ${type} joins require an on condition`);
  if (opts.on !== undefined && !isValueNode(opts.on)) throw new Error("join: on must be a value node");
  return frozen<JoinNode>({
    kind: "join",
    type: validJoinType(type, "join"),
    target,
    alias: opts.alias === undefined ? undefined : validAlias(opts.alias, "join alias"),
    on: opts.on,
  });
}

export interface StatementInput {
  readonly ctes?: readonly CteNode[];
  /** Render `with recursive`. Builders compute this from CTE
   *  self-references (statementReferencesName); raw authors may set it. */
  readonly recursive?: boolean;
  readonly distinct?: boolean;
  readonly projections?: readonly ProjectionNode[];
  readonly from?: FromTarget;
  readonly fromAlias?: string;
  readonly joins?: readonly JoinNode[];
  readonly where?: readonly ValueNode[];
  readonly groupBy?: readonly ValueNode[];
  readonly having?: readonly ValueNode[];
  readonly setOps?: readonly SetOpBranch[];
  readonly orderBy?: readonly OrderSpec[];
  readonly limit?: number;
  readonly offset?: number;
}

export function validLimit(n: number | undefined, what: string): number | undefined {
  if (n === undefined) return undefined;
  if (!Number.isSafeInteger(n) || n < 0) throw new Error(`${what}: must be a non-negative safe integer`);
  return n;
}

export function validJoinType(type: JoinType, what: string): JoinType {
  if (type !== "inner" && type !== "left" && type !== "right" && type !== "full" && type !== "cross") {
    throw new Error(`${what}: unknown join type ${JSON.stringify(type)}`);
  }
  return type;
}

// The compiler names unnamed derived tables `__q1`, `__q2`, … (deterministic
// alias counter). User-supplied aliases in that namespace are rejected so a
// generated alias can never capture a user's name (F01 decision record §4).
const RESERVED_ALIAS = /^__q\d+$/;

export function validAlias(name: string, what: string): string {
  validIdent(name, what);
  if (name.includes(".")) {
    throw new Error(`${what}: alias "${name}" must not contain "." — an alias is one identifier, not a qualification`);
  }
  if (RESERVED_ALIAS.test(name)) {
    throw new Error(`${what}: alias "${name}" is reserved for compiler-generated derived tables`);
  }
  return name;
}

export function selectStatement(input: StatementInput): StatementNode {
  if (input.fromAlias !== undefined) validAlias(input.fromAlias, "fromAlias");
  return frozen<StatementNode>({
    kind: "select",
    ctes: [...(input.ctes ?? [])],
    recursive: input.recursive === true,
    distinct: input.distinct === true,
    projections: [...(input.projections ?? [])],
    from: input.from,
    fromAlias: input.fromAlias,
    joins: [...(input.joins ?? [])],
    where: [...(input.where ?? [])],
    groupBy: [...(input.groupBy ?? [])],
    having: [...(input.having ?? [])],
    setOps: [...(input.setOps ?? [])],
    orderBy: (input.orderBy ?? []).map((o) => frozen<OrderSpec>({ expr: o.expr, direction: o.direction })),
    limit: validLimit(input.limit, "limit"),
    offset: validLimit(input.offset, "offset"),
  });
}

export interface InsertStatementInput {
  readonly table: MutationTarget;
  readonly columns?: readonly string[];
  readonly rows?: readonly ReadonlyArray<InsertCell>[];
  /** Single-row `default values` form; mutually exclusive with columns/rows. */
  readonly defaultValues?: boolean;
  readonly returning?: readonly ProjectionNode[];
}

export function insertStatement(input: InsertStatementInput): InsertStatementNode {
  const columns = [...(input.columns ?? [])].map((c) => validIdent(c, "insert column"));
  const rows = (input.rows ?? []).map((row) => {
    if (!Array.isArray(row)) throw new Error("insert: rows must be arrays of cells");
    if (row.length !== columns.length) {
      throw new Error(`insert: row has ${row.length} cells but ${columns.length} columns are listed`);
    }
    for (const cell of row) {
      if (typeof cell !== "object" || cell === null || (cell.kind !== "param" && cell.kind !== "default")) {
        throw new Error("insert: cells must be param() or defaultCell() nodes");
      }
    }
    return Object.freeze([...row]) as ReadonlyArray<InsertCell>;
  });
  if (input.defaultValues) {
    if (columns.length > 0 || rows.length > 0) {
      throw new Error("insert: defaultValues takes no columns/rows");
    }
  } else if (columns.length === 0 || rows.length === 0) {
    throw new Error("insert: statements need columns and rows (or defaultValues for one default-only row)");
  }
  return frozen<InsertStatementNode>({
    kind: "insert",
    table: input.table,
    columns: Object.freeze(columns),
    rows: Object.freeze(rows),
    defaultValues: input.defaultValues === true,
    returning: input.returning === undefined ? undefined : [...input.returning],
  });
}

export interface UpdateStatementInput {
  readonly table: MutationTarget;
  readonly sets: readonly UpdateAssignment[];
  readonly where: readonly ValueNode[];
  readonly returning?: readonly ProjectionNode[];
}

export function updateStatement(input: UpdateStatementInput): UpdateStatementNode {
  if (input.sets.length === 0) throw new Error("update: at least one assignment is required");
  const sets = input.sets.map((s) =>
    frozen<UpdateAssignment>({ column: validIdent(s.column, "update column"), value: s.value }),
  );
  if (input.where.length === 0) throw new Error("update: a where predicate is required — builders must not emit all-row updates");
  return frozen<UpdateStatementNode>({
    kind: "update",
    table: input.table,
    sets: Object.freeze(sets),
    where: [...input.where],
    returning: input.returning === undefined ? undefined : [...input.returning],
  });
}

export interface DeleteStatementInput {
  readonly table: MutationTarget;
  readonly where: readonly ValueNode[];
  readonly returning?: readonly ProjectionNode[];
}

export function deleteStatement(input: DeleteStatementInput): DeleteStatementNode {
  if (input.where.length === 0) throw new Error("delete: a where predicate is required — builders must not emit all-row deletes");
  return frozen<DeleteStatementNode>({
    kind: "delete",
    table: input.table,
    where: [...input.where],
    returning: input.returning === undefined ? undefined : [...input.returning],
  });
}

// ---------------------------------------------------------------------------
// Kind guards
// ---------------------------------------------------------------------------

const VALUE_KINDS = new Set(["identifier", "qualified", "param", "trusted", "fragment", "expr", "aggregate", "subquery"]);

function hasKind(v: object): v is { kind: string } {
  return typeof (v as { kind?: unknown }).kind === "string";
}

export function isValueNode(v: unknown): v is ValueNode {
  if (typeof v !== "object" || v === null) return false;
  if (!hasKind(v) || !VALUE_KINDS.has(v.kind)) return false;
  return forgedTextKind(v) === null;
}

export function isStatement(v: unknown): v is StatementNode {
  return typeof v === "object" && v !== null && hasKind(v) && v.kind === "select";
}

// ---------------------------------------------------------------------------
// Schema-aware helpers
// ---------------------------------------------------------------------------

/** `ref(t, users.email)` → `"t"."email"`; `ref(users.email)` → owner-qualified
 *  (schema-qualified when the owner table declares a schema). */
export function ref(tableOrColumn: string | AnyColumnBuilder, column?: AnyColumnBuilder | string): QualifiedNode {
  if (typeof tableOrColumn === "string") {
    if (column === undefined) throw new Error("ref: column required when first argument is an alias");
    const colName = typeof column === "string" ? validIdent(column, "ref column") : column.columnName;
    return qual(tableOrColumn, colName);
  }
  if (column !== undefined) throw new Error("ref: pass either (column) or (tableAlias, column)");
  const owner = tableOrColumn.ownerTable;
  if (!owner) throw new Error("ref: column has no owner table; qualify it with an explicit alias");
  return qual(...tableRefParts(owner), tableOrColumn.columnName);
}

// ---------------------------------------------------------------------------
// sqlAst — the structural template
// ---------------------------------------------------------------------------
// Template text is trusted (authored in source). Interpolations:
//   - plain values        → ParamNode (always bound, never spliced)
//   - TrustedSql          → TrustedNode (verbatim, via the brand)
//   - sqlAst fragments    → spliced structurally, params inherit global numbering
//   - value nodes         → spliced structurally
//   - ColumnBuilder       → owner-qualified reference
//   - PgTable             → identifier
// Legacy {sql, params} fragments are REJECTED: their $n text cannot be
// renumbered without scanning raw SQL, which this compiler never does.

function isColumnBuilder(v: object): v is AnyColumnBuilder {
  return typeof (v as { columnName?: unknown }).columnName === "string";
}

function isLegacyFragment(v: object): boolean {
  return Object.hasOwn(v, "sql") && typeof (v as { sql?: unknown }).sql === "string" && Object.hasOwn(v, "params");
}

// ---------------------------------------------------------------------------
// Legacy {sql, params} fragments — one detection + one rejection, shared by
// every builder slot, connective and template interpolation (F04 MINOR-1).
// Their $n placeholders can only be reused by scanning raw SQL text, which
// this package never does; every entry path fails closed with the same hint.
// ---------------------------------------------------------------------------

/** Structural shape of a legacy fragment (duck-typed: own `sql` string +
 *  own `params` array). */
export interface LegacySqlFragmentShape {
  readonly sql: string;
  readonly params: readonly unknown[];
}

/** True for duck-typed legacy `{sql, params}` fragments — direct-execution
 *  shapes that must never splice into compiled statements. */
export function isLegacySqlFragment(v: unknown): v is LegacySqlFragmentShape {
  return (
    typeof v === "object" &&
    v !== null &&
    Object.hasOwn(v, "sql") &&
    typeof (v as { sql?: unknown }).sql === "string" &&
    Object.hasOwn(v, "params") &&
    Array.isArray((v as { params?: unknown }).params)
  );
}

/** The one rejection message every slot uses for legacy fragments. `slot`
 *  names the entry point ("where", "orderBy", "sqlAst", …). */
export function legacyFragmentError(slot: string): Error {
  return new Error(
    `${slot}: legacy SqlFragment {sql, params} cannot be used here — its $n text would need raw-SQL renumbering, which this compiler never does. ` +
      `Rebuild the expression with the sql template (values bind as parameters), bind a plain value, or execute it directly via raw() + driver.query`,
  );
}

function isTrustedSql(v: object): v is TrustedSql {
  return typeof (v as { text?: unknown }).text === "string" && TRUSTED_MARKER in v;
}

function templatePart(value: unknown): ValueNode {
  const t = typeof value;
  if (t === "string" || t === "boolean" || t === "bigint" || t === "number" || value === null || value instanceof Date || value instanceof Uint8Array) {
    validParamValue(value, "sqlAst interpolation");
    return param(value);
  }
  if (typeof value === "object" && value !== null) {
    if (isTrustedSql(value)) return frozen(brandTextNode<TrustedNode>({ kind: "trusted", text: value.text }));
    if (isValueNode(value)) return value;
    const forged = forgedTextKind(value);
    if (forged !== null) {
      throw new Error(`sqlAst: rejected a forged "${forged}" node — construct text-bearing nodes through fragment()/sqlAst()/trustSql()`);
    }
    if (isColumnBuilder(value)) {
      const owner = value.ownerTable;
      if (!owner) throw new Error("sqlAst: interpolated column has no owner table; use ref(alias, column) inside joins");
      const parts = tableRefParts(owner);
      const rec = getDerivedRecord(owner);
      // A CTE handle's column inside a fragment renders the bare-name
      // qualified reference, branded with the handle's CTE identity so the
      // consuming statement must have that CTE in scope (validated at build).
      return rec?.kind === "cte" ? brandedCteQual([...parts, value.columnName], rec) : qual(...parts, value.columnName);
    }
    if (isPgTable(value)) {
      // Q01 review MINOR-2: a whole-table interpolation renders the table's
      // full reference parts, so pgSchema tables stay schema-qualified here
      // like every other query-layer site (alias handles have a one-part
      // reference and render their alias unchanged).
      const rec = getDerivedRecord(value);
      if (rec?.kind === "derived") {
        throw new Error(
          `sql template: interpolated derived-table handle "${rec.name}" — a derived table exists only at its inline from/join site, so a bare-name reference would bind elsewhere; use a cteTable handle (its CTE registers) or the base table`,
        );
      }
      if (rec?.kind === "cte") {
        return brandedCteIdent(rec);
      }
      const parts = tableRefParts(value);
      return parts.length === 1 ? ident(parts[0]) : qual(...parts);
    }
    if (isLegacyFragment(value)) throw legacyFragmentError("sqlAst");
  }
  throw new Error(`sqlAst: cannot interpolate ${t === "object" ? "an object of this shape" : `a ${t}`} — values bind as parameters; fragments/nodes splice structurally`);
}

export function sqlAst(strings: TemplateStringsArray, ...values: readonly unknown[]): FragmentNode {
  const parts: FragmentPart[] = [];
  for (let i = 0; i < strings.length; i++) {
    if (strings[i] !== "") parts.push(strings[i]);
    if (i < values.length) parts.push(templatePart(values[i]));
  }
  return frozen(brandTextNode<FragmentNode>({ kind: "fragment", parts }));
}
