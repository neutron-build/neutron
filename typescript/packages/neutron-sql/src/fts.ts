// ---------------------------------------------------------------------------
// @neutron-build/sql/fts — optional full-text search module (X01)
// ---------------------------------------------------------------------------
// Import as `@neutron-build/sql/fts`. The SQL-only root never loads this
// file; importing it is the explicit opt-in to the FTS surface.
//
// Everything here is CORE PostgreSQL (integrated full-text search, PG 8.3;
// websearch_to_tsquery, PG 11) — no extension required. Statements carry
// I01 capabilities ("fts-functions", "fts-websearch-tsquery") with version
// facts plus probes, so engines without the functions (Nucleus: no FTS
// evidence in the X00 capability report) reject statements BEFORE any SQL
// runs — fail closed.
//
// Ranking semantics are PostgreSQL's own, verbatim: ts_rank / ts_rank_cd
// return float4 computed by the documented word-frequency/cover-density
// formulas; ordering by them is exact. Text configurations bind as
// parameters cast to regconfig (never spliced into SQL text).
//
// Expression GIN index keys are authored with the `sql` template (DDL
// expressions carry no bind parameters — the configuration must be a
// literal there):
//
//   import { sql } from "@neutron-build/sql";
//   index("posts_body_fts").using("gin").on(sql`to_tsvector('english', ${posts.body})`)
//
// The fragment renders structurally (column interpolation -> the bare
// quoted column, the config string -> an inlined literal), matching the
// immutable two-argument to_tsvector(regconfig, text) form PostgreSQL
// requires in index expressions.

import { expr as exprNode, paramCast, qual as qualNode, withRequirements, type ValueNode } from "./ast.js";
import type { AnyColumnBuilder } from "./schema.js";

export { tsvector } from "./schema.js";

/** A text search configuration name ('english', 'simple', …). Validated as
 *  an identifier-shaped string and bound as a parameter — custom dictionary
 *  names are the server's business. */
export type TsConfig = string;

/** A column reference or value expression: text columns pass as qualified
 *  references, anything else renders as authored. */
export type TsSource = AnyColumnBuilder | ValueNode | string;

function sourceNode(source: TsSource): ValueNode {
  if (typeof source === "object" && source !== null && "columnName" in source && (source as { kind?: unknown }).kind === undefined) {
    const column = source as AnyColumnBuilder;
    const owner = (column as { ownerTable?: unknown }).ownerTable;
    const ownerName =
      owner !== undefined && typeof owner === "object" && owner !== null && "name" in (owner as { name?: unknown })
        ? String((owner as { name: unknown }).name)
        : undefined;
    return ownerName === undefined ? qualNode(column.columnName) : qualNode(ownerName, column.columnName);
  }
  return typeof source === "string" ? paramCast(source, "text") : (source as ValueNode);
}

function configNode(config: TsConfig): ValueNode {
  if (typeof config !== "string" || !/^[a-z_][a-z0-9_]*$/.test(config)) {
    throw new Error(`text search configurations are plain lowercase identifiers (got ${JSON.stringify(config)})`);
  }
  return paramCast(config, "regconfig");
}

function textArg(source: TsSource): ValueNode {
  return sourceNode(source);
}

function ftsCall(name: string, args: readonly ValueNode[], caps: readonly string[]): ValueNode {
  return withRequirements(exprNode("call", name, [...args]), caps);
}

/** `to_tsvector(config, source)` — normalize text (or a text column) into
 *  a tsvector. Use in match predicates, ranking, and expression indexes
 *  (see tsvectorIndexKey). */
export function toTsvector(config: TsConfig, source: TsSource): ValueNode {
  return ftsCall("to_tsvector", [configNode(config), textArg(source)], ["fts-functions"]);
}

/** `to_tsquery(config, query)` — strict query syntax: `cat & dog`, `!mouse`,
 *  `a <-> b`. Invalid syntax is a server error surfaced verbatim. */
export function toTsquery(config: TsConfig, query: string): ValueNode {
  return ftsCall("to_tsquery", [configNode(config), textArg(query)], ["fts-functions"]);
}

/** `plainto_tsquery(config, query)` — every token ANDed, punctuation
 *  ignored. The forgiving form. */
export function plaintoTsquery(config: TsConfig, query: string): ValueNode {
  return ftsCall("plainto_tsquery", [configNode(config), textArg(query)], ["fts-functions"]);
}

/** `phraseto_tsquery(config, query)` — tokens ANDed with `<->` (phrase
 *  adjacency). */
export function phrasetoTsquery(config: TsConfig, query: string): ValueNode {
  return ftsCall("phraseto_tsquery", [configNode(config), textArg(query)], ["fts-functions"]);
}

/** `websearch_to_tsquery(config, query)` — web-engine syntax: quoted
 *  phrases, OR, -exclusions (PG 11+). */
export function websearchToTsquery(config: TsConfig, query: string): ValueNode {
  return ftsCall("websearch_to_tsquery", [configNode(config), textArg(query)], ["fts-functions", "fts-websearch-tsquery"]);
}

/** `tsvector @@ tsquery` — the match predicate. Use inside where(). */
export function matches(tsvectorExpr: TsSource, tsqueryExpr: TsSource): ValueNode {
  const lhs = sourceNode(tsvectorExpr);
  const rhs = sourceNode(tsqueryExpr);
  return withRequirements(exprNode("binary", "@@", [lhs, rhs]), ["fts-functions"]);
}

/** ts_rank normalization flags (PostgreSQL documentation, text search
 *  controlling relevance): OR them together. */
export type TsRankNormalization = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 12 | 16 | 24 | 32;

function rankArgs(
  fn: string,
  tsvectorExpr: ValueNode,
  tsqueryExpr: ValueNode,
  normalization?: TsRankNormalization,
): ValueNode {
  const args: ValueNode[] = [tsvectorExpr, tsqueryExpr];
  if (normalization !== undefined) {
    const n: number = normalization;
    if (!Number.isInteger(n) || n < 0 || n > 32 || (n & 63) !== n) {
      throw new Error(`${fn} normalization is a bitmask 0..32 (1|2|4|8|16|32)`);
    }
    args.push(paramCast(String(n), "int4"));
  }
  return withRequirements(exprNode("call", fn, args), ["fts-functions"]);
}

/** `ts_rank(tsvector, tsquery[, normalization])` — standard relevance
 *  ranking, float4. Project and order by it; the value and ordering are
 *  exactly the server's own computation. */
export function tsRank(tsvectorExpr: TsSource, tsqueryExpr: TsSource, normalization?: TsRankNormalization): ValueNode {
  return rankArgs("ts_rank", sourceNode(tsvectorExpr), sourceNode(tsqueryExpr), normalization);
}

/** `ts_rank_cd(tsvector, tsquery[, normalization])` — cover-density
 *  ranking, float4. Higher is more relevant, exactly as the server
 *  computes it. */
export function tsRankCd(tsvectorExpr: TsSource, tsqueryExpr: TsSource, normalization?: TsRankNormalization): ValueNode {
  return rankArgs("ts_rank_cd", sourceNode(tsvectorExpr), sourceNode(tsqueryExpr), normalization);
}
