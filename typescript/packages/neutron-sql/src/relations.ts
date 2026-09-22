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

import { type Condition, type OrderExpression, qident, qualify } from "./expr.js";
import type { AnyColumnBuilder, AnyPgTable, Relation, RelationOne, TableRelations } from "./schema.js";
import type { ExecContext } from "./builder.js";
import { run } from "./builder.js";

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
    const existing = byTable.get(r.table.tableName);
    if (existing) {
      throw new Error(
        `relations for table ${r.table.tableName} are declared more than once (duplicate declarations cannot be merged)`,
      );
    }
    byTable.set(r.table.tableName, r);
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
          `duplicate relationName "${rel.relationName}" on ${r.table.tableName}: relations "${prior}" and "${key}" declare it`,
        );
      }
      seen.set(rel.relationName, key);
    }
  }

  for (const r of all) {
    for (const [key, rel] of Object.entries(r.entries)) {
      if (rel.kind !== "many") continue;
      const target = rel.targetTable;
      if ((Object.values(target.columns) as AnyColumnBuilder[]).every((c) => !c.isPrimaryKey)) {
        throw new Error(
          `relation "${key}" on ${r.table.tableName}: target table ${target.tableName} has no primary key; relation ordering undefined`,
        );
      }
      const targetSet = byTable.get(target.tableName);
      if (!targetSet) {
        throw new Error(
          `relation "${key}" on ${r.table.tableName} targets ${target.tableName} but that table declares no relations`,
        );
      }
      const candidates: Array<[string, RelationOne]> = [];
      for (const [k, candidate] of Object.entries(targetSet.entries)) {
        if (candidate.kind === "one" && candidate.targetTable.tableName === r.table.tableName) {
          candidates.push([k, candidate]);
        }
      }
      let source: RelationOne | undefined;
      if (rel.relationName) {
        source = candidates.find(([, candidate]) => candidate.relationName === rel.relationName)?.[1];
        if (!source) {
          throw new Error(
            `relation "${key}" on ${r.table.tableName} declares relationName "${rel.relationName}" but no one() on ${target.tableName} targeting ${r.table.tableName} declares the same relationName — name both sides of the pair`,
          );
        }
      } else if (candidates.length === 1) {
        source = candidates[0][1];
      } else if (candidates.length === 0) {
        throw new Error(
          `relation "${key}" on ${r.table.tableName} has no matching one() on ${target.tableName} — declare fields/references there`,
        );
      } else {
        throw new Error(
          `relation "${key}" on ${r.table.tableName} is ambiguous: ${target.tableName} declares multiple one() relations to ${r.table.tableName} (` +
            candidates.map(([k]) => `"${k}"`).join(", ") +
            `) — give the pair an explicit relationName on both sides`,
        );
      }
      rel.source = source;
    }
  }

  const byTableEntries = new Map<string, Record<string, Relation>>();
  for (const r of all) byTableEntries.set(r.table.tableName, r.entries);
  return { byTable: byTableEntries };
}

function pkColumnsOf(table: AnyPgTable): AnyColumnBuilder[] {
  const pks = (Object.values(table.columns) as AnyColumnBuilder[]).filter((c) => c.isPrimaryKey);
  if (pks.length === 0) {
    throw new Error(`target table ${table.tableName} has no primary key; relation ordering undefined`);
  }
  return pks;
}

/** Nested JSON objects are labeled with declared property keys; values come
 *  from the physical columns via schema metadata (never name spelling).
 *  int8/numeric leaves render ::text: as jsonb numbers both drivers would
 *  JSON.parse them into doubles (silently corrupting values beyond 2^53 and
 *  dropping numeric scale); as text they arrive exactly, matching the declared
 *  string read types. The cast applies to the JSON projection only —
 *  correlation predicates and order keys stay raw column references. */
function jsonLeaf(alias: string, column: AnyColumnBuilder): string {
  const ref = qualify(alias, column.columnName);
  return column.dataType === "bigint" || column.dataType === "numeric" ? `${ref}::text` : ref;
}

function jsonObjectFor(table: AnyPgTable, alias: string): string {
  const parts = Object.entries(table.columns as Record<string, AnyColumnBuilder>).map(
    ([propertyKey, column]) => `'${propertyKey.replace(/'/g, "''")}', ${jsonLeaf(alias, column)}`,
  );
  return `jsonb_build_object(${parts.join(", ")})`;
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
  const tableName = table.tableName;

  if (args.columns !== undefined) {
    if (!Array.isArray(args.columns)) throw new Error(`args.columns on ${tableName} must be an array of property keys`);
    if (args.columns.length === 0) {
      throw new Error(`args.columns on ${tableName} is empty — omit columns to select every column`);
    }
    const known = new Set(Object.keys(table.columns));
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
      const targetName = rel.targetTable.tableName;
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
): { sql: string; params: unknown[] } {
  validateArgs(table, relations, args);

  const params: unknown[] = [];
  const tableName = table.tableName;
  // args.columns are property keys; map them to physical columns through
  // metadata. Unselected columns default to the full declaration order.
  const allEntries = Object.entries(table.columns as Record<string, AnyColumnBuilder>).map(([propertyKey, column]) => ({
    propertyKey,
    column,
  }));
  const requested =
    args.columns !== undefined && args.columns.length > 0
      ? args.columns.map((key) => {
          const entry = allEntries.find(({ propertyKey }) => propertyKey === key);
          return { propertyKey: key, column: entry!.column };
        })
      : allEntries;

  const extras: string[] = [];
  for (const key of Object.keys(args.with ?? {})) {
    const rel = relations[key];
    const target = rel.targetTable;
    const alias = relAlias(key, tableName);

    if (rel.kind === "many") {
      const source = rel.source!;
      // source.fields live on target; source.references live on this table
      const correlation = source.fields
        .map((f, i) => `${qualify(alias, f.columnName)} = ${qualify(tableName, source.references[i].columnName)}`)
        .join(" and ");
      const orderBy = pkColumnsOf(target)
        .map((c) => qualify(alias, c.columnName))
        .join(", ");
      extras.push(
        `(select coalesce(jsonb_agg(${jsonObjectFor(target, alias)} order by ${orderBy}), '[]'::jsonb) ` +
          `from ${qident(target.tableName)} as ${qident(alias)} where ${correlation}) as ${qident(key)}`,
      );
    } else {
      const correlation = rel.fields
        .map((f, i) => `${qualify(alias, rel.references[i].columnName)} = ${qualify(tableName, f.columnName)}`)
        .join(" and ");
      extras.push(
        `(select ${jsonObjectFor(target, alias)} from ${qident(target.tableName)} as ${qident(alias)} where ${correlation}) as ${qident(key)}`,
      );
    }
  }

  // Top-level rows are keyed by property keys (alias when the names differ).
  const selectParts = requested.map(({ propertyKey, column }) => {
    const ref = qualify(tableName, column.columnName);
    return propertyKey === column.columnName ? ref : `${ref} as ${qident(propertyKey)}`;
  });
  if (extras.length > 0) selectParts.push(...extras);

  let sqlText = `select ${selectParts.join(", ")} from ${qident(tableName)}`;

  if (args.where) {
    sqlText += ` where ${inlineInto(args.where, params)}`;
  }
  if (args.orderBy && args.orderBy.length > 0) {
    sqlText += ` order by ${args.orderBy.map((o) => o.sql).join(", ")}`;
  }
  if (args.limit !== undefined) sqlText += ` limit ${args.limit}`;
  if (args.offset !== undefined) sqlText += ` offset ${args.offset}`;

  return { sql: sqlText, params };
}

function inlineInto(fragment: { sql: string; params: readonly unknown[] }, params: unknown[]): string {
  let n = 0;
  return fragment.sql.replace(/\$(\d+)/g, () => {
    params.push(fragment.params[n]);
    n++;
    return `$${params.length}`;
  });
}

export async function findMany(
  ctx: ExecContext,
  table: AnyPgTable,
  relations: Record<string, Relation>,
  args: RQBArgs,
): Promise<Array<Record<string, unknown>>> {
  const { sql: sqlText, params } = buildRelationalSQL(table, relations, args);
  const rows = (await run(ctx, sqlText, params, "query")) as Array<Record<string, unknown>>;
  for (const row of rows) {
    for (const key of Object.keys(args.with ?? {})) {
      if (key in row) row[key] = normalizeNested(row[key]);
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
