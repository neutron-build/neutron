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
import { getTableColumns, getTableName } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable, Relation, RelationOne, TableRelations } from "./schema.js";
import type { ExecContext } from "./builder.js";
import { run } from "./builder.js";
import { decodeJsonLeaf, decodeNativeValue, decodeTextWire, wireReadExpr, type ColumnContext } from "./codecs.js";

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
function jsonLeaf(alias: string, column: AnyColumnBuilder): string {
  const ref = qualify(alias, column.columnName);
  if (column.dataType === "bigint" || column.dataType === "numeric") return `${ref}::text`;
  if (column.dataType === "timestamptz") return `to_jsonb(${ref} at time zone 'UTC')`;
  return ref;
}

function jsonObjectFor(table: AnyPgTable, alias: string): string {
  const parts = Object.entries(getTableColumns(table) as Record<string, AnyColumnBuilder>).map(
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
): { sql: string; params: unknown[] } {
  validateArgs(table, relations, args);

  const params: unknown[] = [];
  const tableName = getTableName(table);
  const requested = requestedEntries(table, args);

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
          `from ${qident(getTableName(target))} as ${qident(alias)} where ${correlation}) as ${qident(key)}`,
      );
    } else {
      const correlation = rel.fields
        .map((f, i) => `${qualify(alias, rel.references[i].columnName)} = ${qualify(tableName, f.columnName)}`)
        .join(" and ");
      extras.push(
        `(select ${jsonObjectFor(target, alias)} from ${qident(getTableName(target))} as ${qident(alias)} where ${correlation}) as ${qident(key)}`,
      );
    }
  }

  // Top-level rows are keyed by property keys (alias when the names differ).
  // Lossy-native columns (temporals) project their lossless text wire form,
  // exactly like the flat select path.
  const selectParts = requested.map(({ propertyKey, column }) => {
    const ref = qualify(tableName, column.columnName);
    const wire = wireReadExpr(column.dataType, ref);
    if (wire) return `${wire} as ${qident(propertyKey)}`;
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
  const tableName = getTableName(table);
  // Parent columns decode exactly like the flat select path.
  for (const { propertyKey, column } of requestedEntries(table, args)) {
    const wire = wireReadExpr(column.dataType, "x");
    for (const row of rows) {
      const raw = row[propertyKey];
      if (raw === null || raw === undefined) continue;
      const cctx: ColumnContext = { propertyKey, columnName: column.columnName, tableName };
      row[propertyKey] =
        wire !== null
          ? decodeTextWire(column, cctx, raw)
          : decodeNativeValue(column, cctx, raw);
    }
  }
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
