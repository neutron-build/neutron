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

import { getTableName, isPgTable, type AnyColumnBuilder, type AnyPgTable } from "./schema.js";

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

/** A bound value. Renders as the next $n placeholder, never as SQL text. */
export interface ParamNode {
  readonly kind: "param";
  readonly value: unknown;
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

/** The statement node: with / select list / from / joins / where / order / limit. */
export interface StatementNode {
  readonly kind: "select";
  readonly ctes: readonly CteNode[];
  readonly projections: readonly ProjectionNode[];
  readonly from?: FromTarget;
  readonly fromAlias?: string;
  readonly joins: readonly JoinNode[];
  readonly where: readonly ValueNode[];
  readonly orderBy: readonly OrderSpec[];
  readonly limit?: number;
  readonly offset?: number;
}

/** Nodes valid in expression positions. */
export type ValueNode =
  | IdentifierNode
  | QualifiedNode
  | ParamNode
  | TrustedNode
  | FragmentNode
  | ExpressionNode
  | SubqueryNode;

/** The full frozen union (README §3.1 node list). */
export type SqlNode = ValueNode | ProjectionNode | JoinNode | CteNode | StatementNode;

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
const OPERATORS = new Set(["=", "<>", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||", "like", "ilike", "is", "is not", "in", "not in"]);

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

export function expr(form: ExpressionNode["form"], op: string, args: readonly ValueNode[]): ExpressionNode {
  if (form === "binary" && args.length !== 2) throw new Error("expr: binary form takes exactly 2 args");
  if (form === "unary" && args.length !== 1) throw new Error("expr: unary form takes exactly 1 arg");
  return frozen<ExpressionNode>({ kind: "expr", form, op: validOp(op, form), args: [...args] });
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
    alias: opts.alias === undefined ? undefined : validIdent(opts.alias, "join alias"),
    on: opts.on,
  });
}

export interface StatementInput {
  readonly ctes?: readonly CteNode[];
  readonly projections?: readonly ProjectionNode[];
  readonly from?: FromTarget;
  readonly fromAlias?: string;
  readonly joins?: readonly JoinNode[];
  readonly where?: readonly ValueNode[];
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

export function selectStatement(input: StatementInput): StatementNode {
  if (input.fromAlias !== undefined) validIdent(input.fromAlias, "fromAlias");
  return frozen<StatementNode>({
    kind: "select",
    ctes: [...(input.ctes ?? [])],
    projections: [...(input.projections ?? [])],
    from: input.from,
    fromAlias: input.fromAlias,
    joins: [...(input.joins ?? [])],
    where: [...(input.where ?? [])],
    orderBy: (input.orderBy ?? []).map((o) => frozen<OrderSpec>({ expr: o.expr, direction: o.direction })),
    limit: validLimit(input.limit, "limit"),
    offset: validLimit(input.offset, "offset"),
  });
}

// ---------------------------------------------------------------------------
// Kind guards
// ---------------------------------------------------------------------------

const VALUE_KINDS = new Set(["identifier", "qualified", "param", "trusted", "fragment", "expr", "subquery"]);

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

/** `ref(t, users.email)` → `"t"."email"`; `ref(users.email)` → owner-qualified. */
export function ref(tableOrColumn: string | AnyColumnBuilder, column?: AnyColumnBuilder | string): QualifiedNode {
  if (typeof tableOrColumn === "string") {
    if (column === undefined) throw new Error("ref: column required when first argument is an alias");
    const colName = typeof column === "string" ? validIdent(column, "ref column") : column.columnName;
    return qual(tableOrColumn, colName);
  }
  if (column !== undefined) throw new Error("ref: pass either (column) or (tableAlias, column)");
  const owner = tableOrColumn.ownerTable;
  if (!owner) throw new Error("ref: column has no owner table; qualify it with an explicit alias");
  return qual(getTableName(owner), tableOrColumn.columnName);
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
      return qual(getTableName(owner), value.columnName);
    }
    if (isPgTable(value)) return ident(getTableName(value));
    if (isLegacyFragment(value)) {
      throw new Error(
        "sqlAst: legacy SqlFragment {sql, params} cannot be interpolated structurally — its $n text would need raw-SQL renumbering. Rebuild it with sqlAst or acknowledge it via trustSql",
      );
    }
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
