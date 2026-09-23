// ---------------------------------------------------------------------------
// @neutron-build/sql — derived tables and CTE references (Q02)
// ---------------------------------------------------------------------------
// `derivedTable(name, source)` and `cteTable(name, source)` turn a select
// builder (typed select, AST select, set-operation compound) into a
// table-like handle:
//
//   const agg = cteTable("agg", db.select({ uid: posts.userId, n: count() })
//     .from(posts).groupBy(posts.userId));
//   const rows = await db.select({ uid: agg.uid, n: agg.n }).from(agg)
//     .orderBy(desc(agg.n));
//
// The handle is a pseudo-table whose metadata tableName IS the name and whose
// columns are pseudo-columns synthesized from the source's projections — the
// row type derives from the source's row type (projection outputs), and
// pseudo-columns carry the source's decode metadata so outer queries acquire
// wire forms and decode exactly like base-table columns.
//
//   - derivedTable: `(select …) as "name"` inlines at each reference site.
//   - cteTable: renders the bare name and AUTO-REGISTERS `with "name" as (…)`
//     on the consuming statement (idempotent per statement; conflicting
//     duplicate registrations fail closed). `with recursive` is computed
//     structurally from CTE self-references.
//
// Handles are query-surface identities: from/joins/selects only. Mutations,
// DDL, schema export and relational registration reject them.

import { ColumnBuilder, DERIVED_MARKER, TABLE_SYMBOL } from "./schema.js";
import type { DerivedRecord } from "./schema.js";
import type { AnyColumnBuilder, PgTableCore } from "./schema.js";
import { validAlias } from "./ast.js";
import type { StatementNode } from "./ast.js";
import type { StatementCapability } from "./codecs.js";
import type { AstSelectBuilder, FullSelectPlan, JoinRowOf, PlanColumnSpec, Projection, SelectBuilder, SetOpBuilder } from "./builder.js";

/** Column dataType implied by a read type — drives predicate value typing on
 *  pseudo-columns (string ambiguities resolve to text: comparisons accept
 *  canonical strings; use sql fragments for anything else). */
export type DataTypeOfRead<T> =
  [T] extends [bigint] ? "bigint"
  : [T] extends [number] ? "double"
  : [T] extends [boolean] ? "boolean"
  : [T] extends [Uint8Array] ? "bytea"
  : "text";

/** Pseudo-columns of a derived/CTE handle: the source row's field types,
 *  nullable exactly when the source field is nullable, tagged with the
 *  handle's name so outer-join nullability keys on it like alias() tags. */
export type PseudoColsOf<R, A extends string> = {
  [K in keyof R]: (null extends R[K]
    ? ColumnBuilder<DataTypeOfRead<R[K]>, false, false, R[K]>
    : ColumnBuilder<DataTypeOfRead<R[K]>, true, false, R[K]>) & { readonly aliasTag: A };
};

/** A derived/CTE handle: structurally a table (metadata + column properties)
 *  plus the derived record (kind, name, source statement, capabilities). */
export type DerivedTable<A extends string, R> = PgTableCore<PseudoColsOf<R, A>> &
  PseudoColsOf<R, A> & {
    readonly [DERIVED_MARKER]: DerivedRecord;
  };

/** Structural source accepted by the factories: anything exposing a full
 *  plan (typed select / set-op builders) or an AST + column shape
 *  (astSelect builders). Raw SubqueryNodes carry no column metadata — use a
 *  builder source. */
type DerivedSource = { toPlan(): FullSelectPlan } | { toAST(): StatementNode; derivedColumns(): PlanColumnSpec[] } | { kind: "subquery" };

function extractSource(source: DerivedSource, who: string): { stmt: StatementNode; capabilities: readonly StatementCapability[]; columns: readonly PlanColumnSpec[] } {
  if (typeof source === "object" && source !== null && (source as { kind?: unknown }).kind === "subquery") {
    throw new Error(`${who}: a raw subquery node carries no column metadata — pass the builder itself (derivedTable/cteTable read its projection outputs)`);
  }
  if (typeof (source as { toPlan?: unknown }).toPlan === "function") {
    const plan = (source as { toPlan(): FullSelectPlan }).toPlan();
    return { stmt: plan.stmt, capabilities: plan.capabilities, columns: plan.columns };
  }
  if (typeof (source as { toAST?: unknown }).toAST === "function" && typeof (source as { derivedColumns?: unknown }).derivedColumns === "function") {
    const b = source as { toAST(): StatementNode; derivedColumns(): PlanColumnSpec[] };
    return { stmt: b.toAST(), capabilities: [], columns: b.derivedColumns() };
  }
  throw new Error(`${who}: source must be a select/set-op builder (toPlan) or an astSelect builder (toAST + derivedColumns)`);
}

function makeDerivedHandle(kind: "derived" | "cte", name: string, source: DerivedSource, who: string): AnyColumnBuilderOwner {
  validAlias(name, who);
  const { stmt, capabilities, columns } = extractSource(source, who);
  const cols: Record<string, AnyColumnBuilder> = {};
  const seen = new Set<string>();
  for (const spec of columns) {
    if (seen.has(spec.key)) throw new Error(`${who}: duplicate output key "${spec.key}" in the source projection`);
    seen.add(spec.key);
    const c = new ColumnBuilder(spec.key, spec.dataType);
    if (spec.readMode !== undefined) c.readMode = spec.readMode;
    if (spec.valueDecoder !== undefined) c.valueDecoder = spec.valueDecoder;
    if (spec.canonicalText === true && (spec.dataType === "timestamp" || spec.dataType === "timestamptz" || spec.dataType === "date")) {
      c.canonicalText = true;
    }
    (c as unknown as { aliasTag?: string }).aliasTag = name;
    cols[spec.key] = c;
  }
  const record: DerivedRecord = Object.freeze({ kind, name, select: stmt, capabilities: Object.freeze([...capabilities]) }) as DerivedRecord;
  const handle = {
    [TABLE_SYMBOL]: { tableName: name, columns: cols, indexes: [] },
    $inferSelect: undefined as never,
    $inferInsert: undefined as never,
    ...cols,
    [DERIVED_MARKER]: record,
  } as unknown as Record<PropertyKey, unknown>;
  for (const c of Object.values(cols)) {
    (c as { ownerTable?: unknown }).ownerTable = handle;
    Object.freeze(c);
  }
  Object.freeze(cols);
  Object.freeze(handle);
  return handle as unknown as AnyColumnBuilderOwner;
}

interface AnyColumnBuilderOwner {
  readonly [DERIVED_MARKER]: DerivedRecord;
}

/** A derived table: `(select …) as "name"` inlines at each from/join
 *  reference. The row type derives from the source's row type. */
export function derivedTable<P extends Projection | null, R0, N extends string, F extends boolean, A extends string>(
  name: A,
  source: SelectBuilder<P, R0, N, F>,
): DerivedTable<A, JoinRowOf<P, R0, N, F>>;
export function derivedTable<R extends Record<string, unknown>, A extends string>(name: A, source: SetOpBuilder<R>): DerivedTable<A, R>;
export function derivedTable<A extends string>(name: A, source: AstSelectBuilder): DerivedTable<A, Record<string, unknown>>;
export function derivedTable(name: string, source: DerivedSource): DerivedTable<string, Record<string, unknown>> {
  return makeDerivedHandle("derived", name, source, "derivedTable") as unknown as DerivedTable<string, Record<string, unknown>>;
}

/** A CTE reference: renders the bare `name` and auto-registers
 *  `with "name" as (source)` on the consuming statement. */
export function cteTable<P extends Projection | null, R0, N extends string, F extends boolean, A extends string>(
  name: A,
  source: SelectBuilder<P, R0, N, F>,
): DerivedTable<A, JoinRowOf<P, R0, N, F>>;
export function cteTable<R extends Record<string, unknown>, A extends string>(name: A, source: SetOpBuilder<R>): DerivedTable<A, R>;
export function cteTable<A extends string>(name: A, source: AstSelectBuilder): DerivedTable<A, Record<string, unknown>>;
export function cteTable(name: string, source: DerivedSource): DerivedTable<string, Record<string, unknown>> {
  return makeDerivedHandle("cte", name, source, "cteTable") as unknown as DerivedTable<string, Record<string, unknown>>;
}
