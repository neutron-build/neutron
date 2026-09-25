// ---------------------------------------------------------------------------
// @neutron-build/sql/pgvector — optional pgvector capability module (X01)
// ---------------------------------------------------------------------------
// Import as `@neutron-build/sql/pgvector`. The SQL-only root never loads
// this file; importing it is the explicit opt-in to the pgvector surface.
//
// PostgreSQL extension detection is SEPARATE from engine capabilities and
// from Nucleus model capabilities:
//   - `pgvectorExtension(driver)` inspects pg_extension /
//     pg_available_extensions and reports installed / available / absent
//     with the exact version. It never guesses and never installs.
//   - Statements using the distance/similarity expressions carry I01
//     capabilities ("vector-type", "vector-operator-*") resolved by real
//     probes through the normal capability gate: an engine that cannot run
//     the operator rejects the statement BEFORE any SQL runs (fail closed;
//     unknown is not all-enabled). This includes Nucleus engines, whose
//     vector model is a different surface with no proven SQL-column
//     semantics (X00 capability report records none).
//
// Scoring/ordering semantics are pgvector's own, verbatim: `<->` L2
// distance, `<#>` negative inner product, `<=>` cosine distance, `<+>` L1
// distance. Values bind as the exact '[1,2,3]' literal text cast to
// `vector`, with the column's declared dimension validated client-side at
// definition AND write time — a wrong-dimension write is a precise
// client-side error, never a surprise server error or a silent row.

import { expr as exprNode, ident as identNode, param, paramCast, qual as qualNode, withRequirements, type ValueNode } from "./ast.js";
import { capabilityGate, type CapabilityEvidence, type EngineIdentity } from "./engine.js";
import { encodeWriteValue, vectorLiteralText, type ColumnContext } from "./codecs.js";
import type { AnyColumnBuilder, ColumnBuilder, TableIndex } from "./schema.js";
import type { Driver } from "./drivers.js";

export { vector as pgVector } from "./schema.js";

/** pgvector's documented maximum for the `vector` type. HNSW/IVFFlat
 *  indexes accept at most 2000 dimensions — index creation on a higher
 *  dimension column is a server error surfaced verbatim. */
export const MAX_VECTOR_DIMENSIONS = 16000;

/** Typed distance/similarity expression: renders `(col <op> $n::vector)`,
 *  orders/filters by pgvector's own semantics. Phantom-free: the operators
 *  all return double precision, which both drivers hand back as number. */
export type VectorDistanceExpr = ValueNode & { readonly vectorDistance: "l2" | "inner-product" | "cosine" | "l1" };

function assertFiniteNumberArray(value: unknown, what: string): asserts value is number[] {
  if (!Array.isArray(value) || !value.every((e) => typeof e === "number" && Number.isFinite(e))) {
    throw new Error(`${what}: query vectors are arrays of finite numbers`);
  }
}

function vectorColumnRef(col: AnyColumnBuilder | ValueNode, table?: string): { ref: ValueNode; column?: AnyColumnBuilder } {
  if (typeof col === "object" && col !== null && "columnName" in col && (col as { kind?: unknown }).kind === undefined) {
    const column = col as AnyColumnBuilder;
    if (column.dataType !== "vector") {
      throw new Error(`distance expressions apply to vector columns — column "${column.columnName}" is ${column.dataType}`);
    }
    if (table !== undefined) return { ref: qualNode(table, column.columnName), column };
    const owner = (column as { ownerTable?: unknown }).ownerTable;
    const ownerName =
      owner !== undefined && typeof owner === "object" && owner !== null && "name" in (owner as { name?: unknown })
        ? String((owner as { name: unknown }).name)
        : undefined;
    return { ref: ownerName === undefined ? identNode(column.columnName) : qualNode(ownerName, column.columnName), column };
  }
  return { ref: col as ValueNode };
}

function distanceExpr(
  op: "<->" | "<#>" | "<=>" | "<+>",
  capability: "vector-operator-l2" | "vector-operator-inner-product" | "vector-operator-cosine" | "vector-operator-l1",
  col: AnyColumnBuilder | ValueNode,
  query: number[],
  table?: string,
): VectorDistanceExpr {
  assertFiniteNumberArray(query, `pgvector ${capability}`);
  const { ref, column } = vectorColumnRef(col, table);
  // Bind through the column codec when a column is known: the declared
  // dimension is validated here (definition-time validation happened at
  // pgVector()), with the precise expected-vs-got error.
  const bindNode: ValueNode =
    column !== undefined
      ? bindForColumn(column, query, table)
      : paramCast(vectorLiteralText(query), "vector");
  const node = exprNode("binary", op, [ref, bindNode]);
  return withRequirements(node, ["vector-type", capability]) as VectorDistanceExpr;
}

function bindForColumn(column: AnyColumnBuilder, query: number[], table: string | undefined): ValueNode {
  const ctx: ColumnContext = { propertyKey: column.columnName, columnName: column.columnName, tableName: table ?? "vector expression" };
  const enc = encodeWriteValue(column, ctx, query);
  return enc.cast === undefined ? param(enc.bind) : paramCast(enc.bind, enc.cast);
}

/** L2 (Euclidean) distance: `col <-> query`. Orders nearest-first. */
export function l2Distance(col: AnyColumnBuilder | ValueNode, query: number[], table?: string): VectorDistanceExpr {
  return distanceExpr("<->", "vector-operator-l2", col, query, table);
}

/** Negative inner product: `col <#> query`. Orders by greatest similarity
 *  (most positive dot product = most negative operator result). */
export function innerProduct(col: AnyColumnBuilder | ValueNode, query: number[], table?: string): VectorDistanceExpr {
  return distanceExpr("<#>", "vector-operator-inner-product", col, query, table);
}

/** Cosine distance: `col <=> query`. Orders most-similar (distance 0) first. */
export function cosineDistance(col: AnyColumnBuilder | ValueNode, query: number[], table?: string): VectorDistanceExpr {
  return distanceExpr("<=>", "vector-operator-cosine", col, query, table);
}

/** L1 (Manhattan) distance: `col <+> query` (pgvector 0.7.0+). */
export function l1Distance(col: AnyColumnBuilder | ValueNode, query: number[], table?: string): VectorDistanceExpr {
  return distanceExpr("<+>", "vector-operator-l1", col, query, table);
}

// ---------------------------------------------------------------------------
// Index helpers (HNSW / IVFFlat)
// ---------------------------------------------------------------------------

/** Operator classes understood by the pgvector access methods. */
export type VectorOpclass = "vector_ops" | "vector_cosine_ops" | "vector_ip_ops";

/** Mark one HNSW/IVFFlat index's vector key column with an operator class:
 *  `.using("hnsw")` then `withVectorOpclass(idx, embedding, "vector_cosine_ops")`.
 *  The operator class selects which distance the index accelerates:
 *   - vector_ops         <->  (L2, the default)
 *   - vector_cosine_ops   <=>  (cosine)
 *   - vector_ip_ops       <#>  (inner product)
 *  An index whose operator class does not match the query's ORDER BY
 *  operator is simply not used by the planner (EXPLAIN shows a sort) —
 *  PostgreSQL never silently returns wrong distances. */
export function withVectorOpclass(index: TableIndex, col: ColumnBuilder<"vector", boolean, boolean>, opclass: VectorOpclass): TableIndex {
  return index.opclass(col, opclass);
}

// ---------------------------------------------------------------------------
// Extension detection (separate from engine capabilities, X01)
// ---------------------------------------------------------------------------

export interface PgvectorExtensionInfo {
  /** installed: pg_extension carries the extension in this database.
   *  available: not installed, but pg_available_extensions lists it (the
   *  binary is on the server; `create extension vector` would work).
   *  absent: not installed and not available on this server.
   *  unknown: the catalog itself was not usable (e.g. an engine without
   *  pg_extension) — detection fails closed, it never guesses. */
  readonly status: "installed" | "available" | "absent" | "unknown";
  /** The installed extension's version (pg_extension.extversion). */
  readonly installedVersion?: string;
  /** The version pg_available_extensions would install. */
  readonly availableVersion?: string;
  readonly detail: string;
}

/** Detect the pgvector extension in the DATABASE behind `driver`
 * (X01). This is PostgreSQL extension detection — one catalog lookup, no
 * capability inference: an extension row proves the objects exist in this
 * database; it says nothing about Nucleus engines, and it is never used to
 * wave a statement through (statements gate on their own probes). */
export async function pgvectorExtension(driver: Driver): Promise<PgvectorExtensionInfo> {
  let engine: EngineIdentity | undefined;
  try {
    engine = await capabilityGate(driver).engine();
  } catch {
    engine = undefined;
  }
  const engineNote = engine === undefined ? "" : ` (${engine.product} ${engine.version})`;
  try {
    const rows = await driver.query<{ extversion: string }>("select extversion from pg_catalog.pg_extension where extname = 'vector'");
    if (rows.length > 0) {
      const version = String(rows[0].extversion);
      return { status: "installed", installedVersion: version, detail: `pg_extension: vector ${version} is installed in this database${engineNote}` };
    }
  } catch (err) {
    return {
      status: "unknown",
      detail: `pg_extension is not usable on this server (${err instanceof Error ? err.message : String(err)}) — extension detection fails closed`,
    };
  }
  try {
    const rows = await driver.query<{ default_version: string }>("select default_version from pg_catalog.pg_available_extensions where name = 'vector'");
    if (rows.length > 0) {
      const version = String(rows[0].default_version);
      return { status: "available", availableVersion: version, detail: `not installed; the server has pgvector ${version} available — run "create extension vector" to install it into this database` };
    }
    return { status: "absent", detail: `the server has no pgvector extension available${engineNote} — install the pgvector extension package on the server first` };
  } catch (err) {
    return {
      status: "unknown",
      detail: `pg_available_extensions is not usable on this server (${err instanceof Error ? err.message : String(err)}) — extension detection fails closed`,
    };
  }
}

/** Resolve the statement-level vector capabilities against the connected
 *  engine (I01 gate; memoized per driver). Exposed for callers that want
 *  the evidence trail (status + why) before building queries. */
export async function vectorCapabilityEvidence(driver: Driver): Promise<CapabilityEvidence[]> {
  const gate = capabilityGate(driver);
  const caps = ["vector-type", "vector-operator-l2", "vector-operator-inner-product", "vector-operator-cosine", "vector-operator-l1"] as const;
  return Promise.all(caps.map((c) => gate.status(c)));
}
