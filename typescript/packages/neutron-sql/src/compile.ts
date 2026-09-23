// ---------------------------------------------------------------------------
// @neutron-build/sql — AST compiler (F01 architecture spike)
// ---------------------------------------------------------------------------
// ONE traversal renders a statement: text is appended strictly in final order
// and parameters are bound at the moment their placeholder is emitted, so
// $n indexes and the params array can never disagree. There is no regex over
// SQL text anywhere on this path — trusted segments pass through verbatim,
// which is exactly why renumbering-by-regex is impossible here.
//
// compile() is pure: same (frozen) AST → byte-identical SQL + params array.
// Alias generation (`__q1`, `__q2`, …) is a compile-state counter advanced in
// traversal order, so unnamed derived tables get stable, deterministic names.

import { assertExcludedScope, forgedTextKind, validAggregate, validJoinType, validLimit, validNulls, validOp, validParamCast } from "./ast.js";
import type {
  AggregateNode,
  AnyStatementNode,
  CteNode,
  DeleteStatementNode,
  ExpressionNode,
  InsertStatementNode,
  JoinNode,
  OnConflictNode,
  OrderSpec,
  ProjectionNode,
  SqlNode,
  StatementNode,
  UpdateStatementNode,
  ValueNode,
} from "./ast.js";

export interface CompileState {
  /** SQL text chunks, appended in final order. */
  readonly parts: string[];
  /** Bound values; placeholder n is params[n - 1]. */
  readonly params: unknown[];
  /** Deterministic auto-alias counter for unnamed derived tables. */
  aliasCounter: number;
}

export interface CompiledQuery {
  readonly sql: string;
  readonly params: readonly unknown[];
}

/** The single identifier-quote implementation (every identifier-ish field in
 *  every module funnels here or through the AST constructors). */
export function quoteIdent(name: string): string {
  // Every identifier-ish field (names, aliases, qualified parts, CTE columns)
  // funnels through here, including structurally forged nodes — so the text is
  // validated here, not only at construction. A non-string (e.g. an object
  // with a hostile `replace`) or a NUL byte fails closed before quoting.
  if (typeof name !== "string") throw new Error(`compile: identifier text must be a string (got ${typeof name})`);
  if (name.includes("\0")) throw new Error("compile: identifier text must not contain NUL bytes");
  return '"' + name.replace(/"/g, '""') + '"';
}

/** The single string-literal-quote implementation (every module that needs a
 *  quoted SQL text literal funnels here): single quotes doubled, per
 *  standard_conforming_strings. */
export function quoteStringLiteral(text: string): string {
  if (typeof text !== "string") throw new Error(`compile: literal text must be a string (got ${typeof text})`);
  return "'" + text.replace(/'/g, "''") + "'";
}

/** Compile one node into `state`. Exported for direct traversal tests; the
 *  public entry point is compileStatement. */
export function compile(node: SqlNode, state: CompileState): void {
  switch (node.kind) {
    case "identifier":
      state.parts.push(quoteIdent(node.name));
      return;
    case "qualified":
      state.parts.push(node.parts.map(quoteIdent).join("."));
      return;
    case "param": {
      state.params.push(node.value);
      const n = state.params.length;
      state.parts.push(node.cast === undefined ? `$${n}` : `$${n}::text::${validParamCast(node.cast)}`);
      return;
    }
    case "default":
      state.parts.push("default");
      return;
    case "trusted": {
      if (forgedTextKind(node) !== null) {
        throw new Error('compile: rejected a forged "trusted" node — construct trusted SQL through trustSql()/sqlAst()');
      }
      state.parts.push(node.text);
      return;
    }
    case "fragment": {
      if (forgedTextKind(node) !== null) {
        throw new Error('compile: rejected a forged "fragment" node — construct fragments through fragment()/sqlAst()');
      }
      for (const part of node.parts) {
        if (typeof part === "string") state.parts.push(part);
        else compile(part, state);
      }
      return;
    }
    case "expr":
      compileExpr(node, state);
      return;
    case "aggregate":
      compileAggregate(node, state);
      return;
    case "projection":
      compileProjection(node, state);
      return;
    case "join":
      compileJoin(node, state);
      return;
    case "subquery":
      state.parts.push("(");
      compile(node.select, state);
      state.parts.push(")");
      return;
    case "cte":
      compileCte(node, state);
      return;
    case "select":
      compileStatementNode(node, state);
      return;
    case "insert":
      compileInsert(node, state);
      return;
    case "update":
      compileUpdate(node, state);
      return;
    case "delete":
      compileDelete(node, state);
      return;
    default: {
      // Unreachable for the typed union; a structurally forged node (or a
      // non-node) lands here and fails closed instead of compiling silently.
      throw new Error(`compile: unknown node kind ${JSON.stringify((node as { kind?: unknown }).kind)}`);
    }
  }
}

/** Render one operand of a binary/unary operator application. Fragments and
 *  trusted segments carry arbitrary text whose top-level connectives (`or`,
 *  `and`, `not`) would otherwise escape the operator being applied — the
 *  exact rule compilePredicateList applies to where/having items. Call-form arguments
 *  need no wrap: the call's own parentheses and commas delimit each argument. */
function compileOperand(node: ValueNode, state: CompileState): void {
  const wrap = node.kind === "fragment" || node.kind === "trusted";
  if (wrap) state.parts.push("(");
  compile(node, state);
  if (wrap) state.parts.push(")");
}

function compileExpr(node: ExpressionNode, state: CompileState): void {
  validOp(node.op, node.form);
  if (node.form === "binary") {
    state.parts.push("(");
    compileOperand(node.args[0], state);
    state.parts.push(` ${node.op} `);
    compileOperand(node.args[1], state);
    state.parts.push(")");
    return;
  }
  if (node.form === "unary") {
    state.parts.push(`(${node.op} `);
    compileOperand(node.args[0], state);
    state.parts.push(")");
    return;
  }
  state.parts.push(`${node.op}(`);
  for (let i = 0; i < node.args.length; i++) {
    if (i > 0) state.parts.push(", ");
    compile(node.args[i], state);
  }
  state.parts.push(")");
}

/** Aggregates self-delimit (`op(args)`), so operands never need wrapping.
 *  `count()` with zero args renders `count(*)`; `distinct` renders inside
 *  the parentheses. Params inside args (e.g. the string_agg separator)
 *  bind in traversal order. */
function compileAggregate(node: AggregateNode, state: CompileState): void {
  validAggregate(node.op, node.args.length, "compile aggregate");
  state.parts.push(`${node.op}(`);
  if (node.distinct === true) state.parts.push("distinct ");
  if (node.op === "count" && node.args.length === 0) {
    state.parts.push("*");
  } else {
    for (let i = 0; i < node.args.length; i++) {
      if (i > 0) state.parts.push(", ");
      compile(node.args[i], state);
    }
  }
  state.parts.push(")");
}

function compileProjection(node: ProjectionNode, state: CompileState): void {
  compile(node.expr, state);
  if (node.alias !== undefined) state.parts.push(` as ${quoteIdent(node.alias)}`);
}

function nextAlias(state: CompileState): string {
  state.aliasCounter += 1;
  return `__q${state.aliasCounter}`;
}

function compileJoin(node: JoinNode, state: CompileState): void {
  validJoinType(node.type, "compile join");
  const keyword = node.type === "cross" ? "cross join" : `${node.type} join`;
  state.parts.push(keyword);
  state.parts.push(" ");
  compile(node.target, state);
  if (node.alias !== undefined) {
    state.parts.push(` as ${quoteIdent(node.alias)}`);
  } else if (node.target.kind === "subquery") {
    // A derived table is invalid without a name in Postgres; deterministically generated.
    state.parts.push(` as ${quoteIdent(nextAlias(state))}`);
  }
  if (node.type !== "cross" && node.on !== undefined) {
    state.parts.push(" on ");
    compile(node.on, state);
  }
}

function compileCte(node: CteNode, state: CompileState): void {
  state.parts.push(quoteIdent(node.name));
  if (node.columns !== undefined && node.columns.length > 0) {
    state.parts.push(` (${node.columns.map(quoteIdent).join(", ")})`);
  }
  state.parts.push(" as (");
  compile(node.select, state);
  state.parts.push(")");
}

function compileOrder(order: readonly OrderSpec[], state: CompileState): void {
  state.parts.push(" order by ");
  for (let i = 0; i < order.length; i++) {
    if (i > 0) state.parts.push(", ");
    compile(order[i].expr, state);
    state.parts.push(order[i].direction === "desc" ? " desc" : " asc");
    const nulls = validNulls(order[i].nulls, "compile order by");
    if (nulls !== undefined) state.parts.push(` nulls ${nulls}`);
  }
}

function compileStatementNode(stmt: StatementNode, state: CompileState): void {
  // Defensive reads: a structurally forged statement (missing Q02 fields)
  // still reaches the field validations below instead of crashing on
  // undefined arrays — fail closed with the pinned messages.
  const groupBy = stmt.groupBy ?? [];
  const having = stmt.having ?? [];
  const setOps = stmt.setOps ?? [];
  if (stmt.ctes.length > 0) {
    state.parts.push(stmt.recursive === true ? "with recursive " : "with ");
    for (let i = 0; i < stmt.ctes.length; i++) {
      if (i > 0) state.parts.push(", ");
      compile(stmt.ctes[i], state);
    }
    state.parts.push(" ");
  }
  state.parts.push("select ");
  if (stmt.distinct === true) state.parts.push("distinct ");
  if (stmt.projections.length === 0) {
    state.parts.push("*");
  } else {
    for (let i = 0; i < stmt.projections.length; i++) {
      if (i > 0) state.parts.push(", ");
      compile(stmt.projections[i], state);
    }
  }
  if (stmt.from !== undefined) {
    state.parts.push(" from ");
    compile(stmt.from, state);
    if (stmt.fromAlias !== undefined) {
      state.parts.push(` as ${quoteIdent(stmt.fromAlias)}`);
    } else if (stmt.from.kind === "subquery") {
      state.parts.push(` as ${quoteIdent(nextAlias(state))}`);
    }
  }
  for (const j of stmt.joins) {
    state.parts.push(" ");
    compile(j, state);
  }
  compilePredicateList(stmt.where, "where", state);
  if (groupBy.length > 0) {
    state.parts.push(" group by ");
    for (let i = 0; i < groupBy.length; i++) {
      if (i > 0) state.parts.push(", ");
      compile(groupBy[i], state);
    }
  }
  compilePredicateList(having, "having", state);
  // Set-operation branches render parenthesized: chaining is
  // left-associative in call order, never in SQL's intersect-over-union
  // precedence, and a branch keeps its own WITH/ORDER BY/LIMIT inside the
  // parentheses (PostgreSQL semantics).
  for (const so of setOps) {
    state.parts.push(` ${so.op} (`);
    compile(so.select, state);
    state.parts.push(")");
  }
  if (stmt.orderBy.length > 0) compileOrder(stmt.orderBy, state);
  if (stmt.limit !== undefined) state.parts.push(` limit ${validLimit(stmt.limit, "compile limit")}`);
  if (stmt.offset !== undefined) state.parts.push(` offset ${validLimit(stmt.offset, "compile offset")}`);
}

/** where/having share one renderer: items join with `and`, and fragments and
 *  trusted segments are delimited so joining cannot change their meaning
 *  (the F04 Grouping guarantee, applied to HAVING since Q02). */
function compilePredicateList(items: readonly ValueNode[], keyword: "where" | "having", state: CompileState): void {
  if (items.length === 0) return;
  state.parts.push(` ${keyword} `);
  for (let i = 0; i < items.length; i++) {
    if (i > 0) state.parts.push(" and ");
    if (items[i].kind === "fragment" || items[i].kind === "trusted") state.parts.push("(");
    compile(items[i], state);
    if (items[i].kind === "fragment" || items[i].kind === "trusted") state.parts.push(")");
  }
}

function compileReturning(returning: readonly ProjectionNode[] | undefined, state: CompileState): void {
  if (returning === undefined || returning.length === 0) return;
  state.parts.push(" returning ");
  for (let i = 0; i < returning.length; i++) {
    if (i > 0) state.parts.push(", ");
    compile(returning[i], state);
  }
}

/** ON CONFLICT rendering (Q03). The target's index predicate and the DO
 *  UPDATE predicate both go through compilePredicateList, so fragments and
 *  trusted segments are delimited exactly like where/having — the Grouping
 *  guarantee extends to conflict clauses. */
function compileOnConflict(node: OnConflictNode, state: CompileState): void {
  if (node.action !== "nothing" && node.action !== "update") {
    throw new Error(`compile insert: unknown on-conflict action ${JSON.stringify(node.action)}`);
  }
  state.parts.push(" on conflict");
  const target = node.target;
  if (target !== undefined) {
    if (target.kind === "constraint") {
      state.parts.push(` on constraint ${quoteIdent(target.constraint)}`);
    } else {
      if (!Array.isArray(target.columns) || target.columns.length === 0) {
        throw new Error("compile insert: a column-list conflict target needs at least one column");
      }
      state.parts.push(" (");
      for (let i = 0; i < target.columns.length; i++) {
        if (i > 0) state.parts.push(", ");
        state.parts.push(quoteIdent(target.columns[i]));
      }
      state.parts.push(")");
      compilePredicateList(target.where ?? [], "where", state);
    }
  }
  if (node.action === "nothing") {
    if ((node.sets !== undefined && node.sets.length > 0) || (node.where !== undefined && node.where.length > 0)) {
      throw new Error("compile insert: do nothing takes no assignments or predicate");
    }
    state.parts.push(" do nothing");
    return;
  }
  if (target === undefined) {
    throw new Error("compile insert: on conflict do update requires a target — PostgreSQL rejects targetless DO UPDATE");
  }
  const sets = node.sets ?? [];
  if (sets.length === 0) throw new Error("compile insert: on conflict do update requires at least one assignment");
  state.parts.push(" do update set ");
  for (let i = 0; i < sets.length; i++) {
    if (i > 0) state.parts.push(", ");
    state.parts.push(quoteIdent(sets[i].column));
    state.parts.push(" = ");
    compile(sets[i].value, state);
  }
  compilePredicateList(node.where ?? [], "where", state);
}

function compileInsert(node: InsertStatementNode, state: CompileState): void {
  state.parts.push("insert into ");
  compile(node.table, state);
  if (node.defaultValues) {
    state.parts.push(" default values");
  } else {
    state.parts.push(" (");
    for (let i = 0; i < node.columns.length; i++) {
      if (i > 0) state.parts.push(", ");
      state.parts.push(quoteIdent(node.columns[i]));
    }
    state.parts.push(") values ");
    for (let r = 0; r < node.rows.length; r++) {
      if (r > 0) state.parts.push(", ");
      state.parts.push("(");
      const row = node.rows[r];
      for (let i = 0; i < row.length; i++) {
        if (i > 0) state.parts.push(", ");
        compile(row[i], state);
      }
      state.parts.push(")");
    }
  }
  if (node.onConflict !== undefined) compileOnConflict(node.onConflict, state);
  compileReturning(node.returning, state);
}

function compileUpdate(node: UpdateStatementNode, state: CompileState): void {
  state.parts.push("update ");
  compile(node.table, state);
  state.parts.push(" set ");
  for (let i = 0; i < node.sets.length; i++) {
    if (i > 0) state.parts.push(", ");
    state.parts.push(quoteIdent(node.sets[i].column));
    state.parts.push(" = ");
    compile(node.sets[i].value, state);
  }
  compilePredicateList(node.where, "where", state);
  compileReturning(node.returning, state);
}

function compileDelete(node: DeleteStatementNode, state: CompileState): void {
  state.parts.push("delete from ");
  compile(node.table, state);
  compilePredicateList(node.where, "where", state);
  compileReturning(node.returning, state);
}

/** Public entry: compile a statement with fresh state. Pure — same AST gives
 *  a byte-identical SQL string and params array on every call. The excluded()
 *  scope choke point runs first: every statement kind fails closed before any
 *  SQL renders if it references excluded() where PostgreSQL cannot see it. */
export function compileStatement(stmt: AnyStatementNode): CompiledQuery {
  assertExcludedScope(stmt);
  const state: CompileState = { parts: [], params: [], aliasCounter: 0 };
  compile(stmt, state);
  return { sql: state.parts.join(""), params: state.params };
}
