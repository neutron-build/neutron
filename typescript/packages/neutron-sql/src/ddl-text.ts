// ---------------------------------------------------------------------------
// @neutron-build/sql — schema expression text (Q07)
// ---------------------------------------------------------------------------
// Check constraints, index key expressions and predicates, generated column
// expressions and view definitions are SQL text stored verbatim in the
// schema contract. The schema module is trusted project code (it is what the
// exporter runs), so these positions accept either SQL text or a `sql`
// template fragment. Fragments render structurally:
//
//   - column interpolations render as the bare quoted column name inside
//     table-scoped expressions (checks, index parts/predicates, generated
//     columns) — and must belong to that table, so an interpolation can never
//     silently bind to a same-named column of the owning table;
//   - inside view definitions they keep their table qualification;
//   - plain values inline as SQL literals (strings quoted with '' doubling,
//     finite numbers, bigints, booleans, null): DDL has no bind parameters,
//     so the literal becomes part of the stored definition;
//   - anything else (Dates, byte arrays, compiled expressions, subqueries)
//     is rejected with a message naming the position.
//
// Expression equivalence against the catalog's deparse is decided by the
// planner's twin normalizer, never by this renderer.

import { forgedTextKind, isValueNode, type FragmentPart, type ValueNode } from "./ast.js";
import { quoteIdent, quoteStringLiteral } from "./compile.js";
import { tableRefParts, type AnyPgTable } from "./schema.js";

/** SQL text or a `sql` template fragment used as a schema expression. */
export type SchemaExpression = string | ValueNode;

export interface SchemaExpressionScope {
  /** Position description for error messages. */
  readonly what: string;
  /** Table-scoped positions render column references bare and require them
   *  to belong to this table. Omitted for view definitions. */
  readonly table?: AnyPgTable;
}

export function isSchemaExpression(value: unknown): value is SchemaExpression {
  return typeof value === "string" || (typeof value === "object" && value !== null && isValueNode(value) && (value as ValueNode).kind === "fragment");
}

/** Render a schema expression to its verbatim SQL text. */
export function renderSchemaExpression(expr: SchemaExpression, scope: SchemaExpressionScope): string {
  let text: string;
  if (typeof expr === "string") {
    text = expr;
  } else {
    const out: string[] = [];
    renderNode(expr, scope, out);
    text = out.join("");
  }
  if (text.trim().length === 0) throw new Error(`${scope.what}: SQL text must not be empty`);
  if (text.includes("\0")) throw new Error(`${scope.what}: SQL text must not contain NUL`);
  return text;
}

function renderPart(part: FragmentPart, scope: SchemaExpressionScope, out: string[]): void {
  if (typeof part === "string") {
    out.push(part);
    return;
  }
  renderNode(part, scope, out);
}

function renderNode(node: ValueNode, scope: SchemaExpressionScope, out: string[]): void {
  switch (node.kind) {
    case "fragment": {
      if (forgedTextKind(node) !== null) throw new Error(`${scope.what}: rejected a forged fragment node — build expressions with the sql template`);
      for (const p of node.parts) renderPart(p, scope, out);
      return;
    }
    case "trusted": {
      if (forgedTextKind(node) !== null) throw new Error(`${scope.what}: rejected a forged trusted node — use trustSql()`);
      out.push(node.text);
      return;
    }
    case "identifier":
      out.push(quoteIdent(node.name));
      return;
    case "qualified": {
      if (scope.table !== undefined) {
        const tableParts = tableRefParts(scope.table);
        const colName = node.parts[node.parts.length - 1];
        const qualifier = node.parts.slice(0, -1);
        const matches = qualifier.length === tableParts.length && qualifier.every((p, i) => p === tableParts[i]);
        if (!matches) {
          throw new Error(
            `${scope.what}: column reference ${node.parts.map((p) => `"${p}"`).join(".")} does not belong to table ${tableParts.map((p) => `"${p}"`).join(".")} — table-scoped schema expressions may only reference the table's own columns`,
          );
        }
        out.push(quoteIdent(colName));
        return;
      }
      out.push(node.parts.map(quoteIdent).join("."));
      return;
    }
    case "param": {
      if (node.cast !== undefined) throw new Error(`${scope.what}: typed parameters cannot appear in a schema expression — write the literal in the SQL text`);
      out.push(inlineLiteral(node.value, scope));
      return;
    }
    default:
      throw new Error(
        `${scope.what}: ${node.kind} nodes cannot appear in a schema expression — write the SQL text (or a sql template of columns and literals) instead`,
      );
  }
}

function inlineLiteral(value: unknown, scope: SchemaExpressionScope): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "string":
      return quoteStringLiteral(value);
    case "boolean":
      return value ? "true" : "false";
    case "bigint":
      return value.toString();
    case "number":
      if (!Number.isFinite(value)) throw new Error(`${scope.what}: non-finite number ${value} cannot be written as a SQL literal`);
      return String(value);
    default:
      throw new Error(`${scope.what}: ${value instanceof Date ? "Date" : value instanceof Uint8Array ? "Uint8Array" : typeof value} values cannot be inlined into a schema expression — write the literal in the SQL text`);
  }
}
