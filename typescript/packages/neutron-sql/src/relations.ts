// ---------------------------------------------------------------------------
// @neutron-build/sql — relational query API (db.query.<table>)
// ---------------------------------------------------------------------------
// Execution model (README §3.4 default): every requested relation edge is an
// INDEPENDENT correlated scalar subquery in the select list. To-many children
// aggregate with jsonb_agg(... order by <target pk>) inside their own
// subquery; to-one parents build a single jsonb object that is NULL when the
// FK misses. No joins between sibling relations can ever exist, so two
// to-many children can never multiply each other. Parent where/order/limit/
// offset apply to parent rows before child expansion (the subqueries run per
// output row). Depth is one level; nested `with` is rejected explicitly.

import { type Condition, type OrderExpression } from "./expr.js";
import { getTableColumns, getTableName } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable, Relation, RelationOne, TableRelations } from "./schema.js";
import type { ExecContext } from "./builder.js";
import { run, whereItems } from "./builder.js";
import {
  applyProjectionDecoders,
  decodeJsonLeaf,
  projectionDecoder,
  wireReadNode,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";
import {
  expr as exprNode,
  fragment,
  ident,
  isLegacySqlFragment,
  legacyFragmentError,
  projection as projectionNode,
  qual,
  selectStatement,
  subquery as subqueryNode,
  type OrderSpec,
  type ProjectionNode,
  type StatementNode,
  type ValueNode,
} from "./ast.js";
import { compileStatement, quoteStringLiteral } from "./compile.js";

export interface RQBArgs {
  where?: Condition;
  orderBy?: OrderExpression[];
  limit?: number;
  offset?: number;
  columns?: string[];
  with?: Record<string, true>;
}

export interface ResolvedRelations {
  /** table name -> relation entries */
  byTable: Map<string, Record<string, Relation>>;
}

/** Wire many() relations to their one() counterparts (FKs live on the one side).
 *  Reverse inference is allowed only when exactly ONE candidate one() exists;
 *  multiple candidates without an explicit relationName pair are an error. */
export function resolveRelations(all: TableRelations[]): ResolvedRelations {
  const byTable = new Map<string, TableRelations>();
  for (const r of all) {
    const existing = byTable.get(getTableName(r.table));
    if (existing) {
      throw new Error(
        `relations for table ${getTableName(r.table)} are declared more than once (duplicate declarations cannot be merged)`,
      );
    }
    byTable.set(getTableName(r.table), r);
  }

  // relationName must be unique among one table's entries: it is the pairing
  // key for reverse resolution and a duplicate makes matching ambiguous.
  for (const r of all) {
    const seen = new Map<string, string>();
    for (const [key, rel] of Object.entries(r.entries)) {
      if (!rel.relationName) continue;
      const prior = seen.get(rel.relationName);
      if (prior !== undefined) {
        throw new Error(
          `duplicate relationName "${rel.relationName}" on ${getTableName(r.table)}: relations "${prior}" and "${key}" declare it`,
        );
      }
      seen.set(rel.relationName, key);
    }
  }

  for (const r of all) {
    for (const [key, rel] of Object.entries(r.entries)) {
      if (rel.kind !== "many") continue;
      const target = rel.targetTable;
      if ((Object.values(getTableColumns(target)) as AnyColumnBuilder[]).every((c) => !c.isPrimaryKey)) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)}: target table ${getTableName(target)} has no primary key; relation ordering undefined`,
        );
      }
      const targetSet = byTable.get(getTableName(target));
      if (!targetSet) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} targets ${getTableName(target)} but that table declares no relations`,
        );
      }
      const candidates: Array<[string, RelationOne]> = [];
      for (const [k, candidate] of Object.entries(targetSet.entries)) {
        if (candidate.kind === "one" && getTableName(candidate.targetTable) === getTableName(r.table)) {
          candidates.push([k, candidate]);
        }
      }
      let source: RelationOne | undefined;
      if (rel.relationName) {
        source = candidates.find(([, candidate]) => candidate.relationName === rel.relationName)?.[1];
        if (!source) {
          throw new Error(
            `relation "${key}" on ${getTableName(r.table)} declares relationName "${rel.relationName}" but no one() on ${getTableName(target)} targeting ${getTableName(r.table)} declares the same relationName — name both sides of the pair`,
          );
        }
      } else if (candidates.length === 1) {
        source = candidates[0][1];
      } else if (candidates.length === 0) {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} has no matching one() on ${getTableName(target)} — declare fields/references there`,
        );
      } else {
        throw new Error(
          `relation "${key}" on ${getTableName(r.table)} is ambiguous: ${getTableName(target)} declares multiple one() relations to ${getTableName(r.table)} (` +
            candidates.map(([k]) => `"${k}"`).join(", ") +
            `) — give the pair an explicit relationName on both sides`,
        );
      }
      rel.source = source;
    }
  }

  const byTableEntries = new Map<string, Record<string, Relation>>();
  for (const r of all) byTableEntries.set(getTableName(r.table), r.entries);
  return { byTable: byTableEntries };
}

function pkColumnsOf(table: AnyPgTable): AnyColumnBuilder[] {
  const pks = (Object.values(getTableColumns(table)) as AnyColumnBuilder[]).filter((c) => c.isPrimaryKey);
  if (pks.length === 0) {
    throw new Error(`target table ${getTableName(table)} has no primary key; relation ordering undefined`);
  }
  return pks;
}

/** Nested JSON objects are labeled with declared property keys; values come
 *  from the physical columns via schema metadata (never name spelling).
 *  int8/numeric leaves render ::text: as jsonb numbers both drivers would
 *  JSON.parse them into doubles (silently corrupting values beyond 2^53 and
 *  dropping numeric scale). timestamptz leaves render their UTC wall clock
 *  (session-timezone independent); other temporals and bytea leaves keep
 *  to_jsonb's exact string forms (microseconds, \x hex). Correlation
 *  predicates and order keys stay raw column references. */
function jsonLeaf(alias: string, column: AnyColumnBuilder): ValueNode {
  const ref = qual(alias, column.columnName);
  if (column.dataType === "bigint" || column.dataType === "numeric") return fragment(ref, "::text");
  if (column.dataType === "timestamptz") return fragment("to_jsonb(", ref, " at time zone 'UTC')");
  return ref;
}

function jsonObjectFor(table: AnyPgTable, alias: string): ValueNode {
  const args: ValueNode[] = [];
  for (const [propertyKey, column] of Object.entries(getTableColumns(table) as Record<string, AnyColumnBuilder>)) {
    args.push(fragment(quoteStringLiteral(propertyKey)));
    args.push(jsonLeaf(alias, column));
  }
  return exprNode("call", "jsonb_build_object", args);
}

/** Normalize driver output: some pgwire servers hand json/jsonb back as strings. */
function normalizeNested(value: unknown): unknown {
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }
  return value;
}

/** Deterministic, collision-free correlation alias for one relation's
 *  subquery. Each subquery is its own scope, so aliases never collide with
 *  each other; the prefix keeps them from shadowing the outer table name. */
function relAlias(relationKey: string, outerTable: string): string {
  let alias = `__rel_${relationKey}`;
  while (alias === outerTable) alias += "_x";
  return alias;
}

/** Validate args against the declared table/relations before building SQL.
 *  Every rejection names the exact shape so callers can see what to fix. */
function validateArgs(table: AnyPgTable, relations: Record<string, Relation>, args: RQBArgs): void {
  const tableName = getTableName(table);

  if (args.columns !== undefined) {
    if (!Array.isArray(args.columns)) throw new Error(`args.columns on ${tableName} must be an array of property keys`);
    if (args.columns.length === 0) {
      throw new Error(`args.columns on ${tableName} is empty — omit columns to select every column`);
    }
    const known = new Set(Object.keys(getTableColumns(table)));
    for (const key of args.columns) {
      if (!known.has(key)) {
        throw new Error(
          `unknown column "${key}" in args.columns on ${tableName} (known property keys: ${[...known].join(", ")})`,
        );
      }
    }
  }

  const withKeys = Object.keys(args.with ?? {});
  if (withKeys.length > 0) {
    const seenTargets = new Map<string, string>();
    for (const key of withKeys) {
      const rel = relations[key];
      if (!rel) {
        const known = Object.keys(relations);
        throw new Error(
          `unknown relation "${key}" on ${tableName} (known relations: ${known.length > 0 ? known.join(", ") : "none"})`,
        );
      }
      const value = (args.with as Record<string, unknown>)[key];
      if (value !== true) {
        throw new Error(
          `relation "${key}" in with on ${tableName}: only \`true\` is supported at one level — got ${typeof value}; ` +
            `nested with / per-relation options are not implemented`,
        );
      }
      const targetName = getTableName(rel.targetTable);
      const prior = seenTargets.get(targetName);
      if (prior !== undefined) {
        throw new Error(
          `relations "${prior}" and "${key}" on ${tableName} both target table "${targetName}" in one with clause — ` +
            `aliasing repeated targets is not supported yet; query them separately`,
        );
      }
      seenTargets.set(targetName, key);
    }
  }
}

export function buildRelationalSQL(
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
): { sql: string; params: unknown[]; decoders: readonly ProjectionDecoder[]; capabilities: readonly StatementCapability[] } {
  validateArgs(table, relations, args);
  if (args.where !== undefined && isLegacySqlFragment(args.where)) throw legacyFragmentError("where");
  for (const o of args.orderBy ?? []) {
    if (isLegacySqlFragment(o)) throw legacyFragmentError("orderBy");
    if (isLegacySqlFragment((o as OrderSpec).expr)) throw legacyFragmentError("orderBy");
  }

  const tableName = getTableName(table);
  const requested = requestedEntries(table, args);

  const projections: ProjectionNode[] = [];
  const decoders: ProjectionDecoder[] = [];
  let usesJsonb = false;

  // Top-level rows are keyed by property keys (alias when the names differ).
  // Lossy-native columns (temporals) project their lossless text wire form,
  // exactly like the flat select path.
  for (const { propertyKey, column } of requested) {
    const ref = qual(tableName, column.columnName);
    const wire = wireReadNode(column.dataType, ref);
    if (wire !== null) {
      projections.push(projectionNode(wire, propertyKey));
      usesJsonb = true;
    } else {
      projections.push(projectionNode(ref, propertyKey === column.columnName ? undefined : propertyKey));
    }
    const decoder = projectionDecoder(tableName, column, propertyKey);
    if (decoder) decoders.push(decoder);
  }

  for (const key of Object.keys(args.with ?? {})) {
    const rel = relations[key];
    const target = rel.targetTable;
    const alias = relAlias(key, tableName);

    let inner: StatementNode;
    if (rel.kind === "many") {
      const source = rel.source!;
      // source.fields live on target; source.references live on this table
      const correlation = andChain(
        source.fields.map((f, i) => [qual(alias, f.columnName), qual(tableName, source.references[i].columnName)] as const),
      );
      const orderKeys = pkColumnsOf(target).map((c) => qual(alias, c.columnName));
      const agg = aggWithOrder(jsonObjectFor(target, alias), orderKeys);
      inner = selectStatement({
        projections: [projectionNode(agg)],
        from: ident(getTableName(target)),
        fromAlias: alias,
        where: [correlation],
      });
    } else {
      const correlation = andChain(
        rel.fields.map((f, i) => [qual(alias, rel.references[i].columnName), qual(tableName, f.columnName)] as const),
      );
      inner = selectStatement({
        projections: [projectionNode(jsonObjectFor(target, alias))],
        from: ident(getTableName(target)),
        fromAlias: alias,
        where: [correlation],
      });
    }
    projections.push(projectionNode(subqueryNode(inner), key));
    usesJsonb = true;
  }

  const stmt = selectStatement({
    projections,
    from: ident(tableName),
    where: args.where ? whereItems([args.where]) : [],
    orderBy: (args.orderBy ?? []).map((o) =>
      typeof (o as OrderSpec).direction === "string" ? (o as OrderSpec) : { expr: o as ValueNode, direction: "asc" as const },
    ),
    limit: args.limit,
    offset: args.offset,
  });
  const compiled = compileStatement(stmt);
  const capabilities: StatementCapability[] = usesJsonb ? ["jsonb-functions"] : [];
  return { sql: compiled.sql, params: compiled.params as unknown[], decoders, capabilities };
}

/** Fold correlation pairs with `=` and `and` (raw column comparisons — the
 *  JSON-only casts never apply to predicates). */
function andChain(pairs: ReadonlyArray<readonly [ValueNode, ValueNode]>): ValueNode {
  const eqs = pairs.map(([a, b]) => exprNode("binary", "=", [a, b]));
  return eqs.reduce((acc, c) => exprNode("binary", "and", [acc, c]));
}

/** `coalesce(jsonb_agg(obj order by keys), '[]'::jsonb)` as one fragment —
 *  aggregate ORDER BY lives inside the call, so it interleaves trusted text
 *  with the object expression and the raw order keys. */
function aggWithOrder(obj: ValueNode, orderKeys: readonly ValueNode[]): ValueNode {
  const parts: Array<string | ValueNode> = ["coalesce(jsonb_agg(", obj];
  if (orderKeys.length > 0) {
    parts.push(" order by ");
    orderKeys.forEach((k, i) => {
      if (i > 0) parts.push(", ");
      parts.push(k);
    });
  }
  parts.push("), '[]'::jsonb)");
  return fragment(...parts);
}

/** args.columns are property keys; map them to physical columns through
 *  metadata. Unselected columns default to the full declaration order. */
function requestedEntries(table: AnyPgTable, args: RQBArgs): Array<{ propertyKey: string; column: AnyColumnBuilder }> {
  const allEntries = Object.entries(getTableColumns(table) as Record<string, AnyColumnBuilder>).map(([propertyKey, column]) => ({
    propertyKey,
    column,
  }));
  if (args.columns !== undefined && args.columns.length > 0) {
    return args.columns.map((key) => {
      const entry = allEntries.find(({ propertyKey }) => propertyKey === key);
      return { propertyKey: key, column: entry!.column };
    });
  }
  return allEntries;
}

export async function findMany(
  ctx: ExecContext,
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
): Promise<Array<Record<string, unknown>>> {
  const built = buildRelationalSQL(table, relations, args);
  const rows = (await run(ctx, built.sql, built.params, "query")) as Array<Record<string, unknown>>;
  // Parent columns decode through the compiled statement's decode plan,
  // exactly like the flat select path.
  applyProjectionDecoders(rows, built.decoders);
  // Children decode per target-table column: the JSON projection renders
  // int8/numeric ::text, timestamptz as its UTC wall clock and bytea as \x
  // hex text, so precision survives JSON.parse and decodes through the same
  // codecs as the flat path (mode-aware).
  const childColumns = new Map<string, Array<{ propertyKey: string; column: AnyColumnBuilder }>>();
  for (const key of Object.keys(args.with ?? {})) {
    const target = relations[key].targetTable;
    childColumns.set(
      key,
      Object.entries(getTableColumns(target) as Record<string, AnyColumnBuilder>).map(([propertyKey, column]) => ({
        propertyKey,
        column,
      })),
    );
  }
  for (const row of rows) {
    for (const [key, columns] of childColumns) {
      if (!(key in row)) continue;
      row[key] = normalizeNested(row[key]);
      const children = Array.isArray(row[key]) ? row[key] : [row[key]];
      for (const child of children) {
        if (child === null || typeof child !== "object") continue;
        const target = relations[key].targetTable;
        const targetName = getTableName(target);
        for (const { propertyKey, column } of columns) {
          const raw = (child as Record<string, unknown>)[propertyKey];
          if (raw === null || raw === undefined) continue;
          (child as Record<string, unknown>)[propertyKey] = decodeJsonLeaf(
            column,
            { propertyKey, columnName: column.columnName, tableName: targetName },
            raw,
          );
        }
      }
    }
  }
  return rows;
}

export async function findFirst(
  ctx: ExecContext,
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
): Promise<Record<string, unknown> | undefined> {
  const rows = await findMany(ctx, table, relations, { ...args, limit: 1 });
  return rows[0];
}
