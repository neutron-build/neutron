// ---------------------------------------------------------------------------
// @neutron-build/sql — database entry point
// ---------------------------------------------------------------------------

import { loadDriver, type Driver, type LoadDriverOptions } from "./drivers.js";
import { capabilityGate, type CapabilityEvidence, type EngineIdentity } from "./engine.js";
import type { StatementCapability } from "./codecs.js";
import { resolveLogger, type LoggerOption } from "./logger.js";
import {
  DeleteBuilder,
  InsertBuilder,
  SelectBuilder,
  UpdateBuilder,
  type ExecContext,
  type InferSelectModelOfRecord,
  type Projection,
} from "./builder.js";
import {
  buildRelationalPlan,
  buildRelationalSQL,
  findFirst,
  findMany,
  resolveRelations,
  MAX_RELATION_DEPTH,
  type RQBArgs,
  type RelationalExplainPlan,
  type RelationalStatementPlan,
  type RelationEdgePlan,
} from "./relations.js";
export type { RelationalExplainPlan, RelationalStatementPlan, RelationEdgePlan };
import {
  getTableName,
  getTableSchema,
  isPgTable,
  isTableRelations,
  rejectDerivedTable,
  type AnyColumnBuilder,
  type AnyPgTable,
  type PgTable,
  type PgTableCore,
  type Relation,
  type RelationMany,
  type RelationOne,
  type RelationSelectTypeOf,
  type SelectTypeOf,
  type TableRelations,
} from "./schema.js";

export interface TablesInput {
  [name: string]: AnyPgTable;
}

export interface RelationsInput {
  [name: string]: TableRelations;
}

export interface DatabaseOptions<
  T extends TablesInput = TablesInput,
  R extends RelationsInput = RelationsInput,
> {
  /** Postgres or Nucleus (pgwire) connection URL. The package creates the
   *  adapter and OWNS it: `close()` terminates it. Provide either `url` or
   *  `driver`, not both. */
  url?: string;
  /** Injected adapter (from `wrapPgPool` / `wrapPostgresJs` / `loadDriver`).
   *  Its lifecycle governs disposal: wrapped pools/clients are BORROWED by
   *  default, so `close()` leaves the resource you injected functional. */
  driver?: Driver;
  /** Driver kind and pool tuning when `url` is used (renamed from the
   *  pre-I01 `driver` option, which now injects an adapter). */
  driverOptions?: LoadDriverOptions;
  /** Tables; enables db.query.<name>. */
  tables?: T;
  /** Relations keyed by the same names as `tables`. */
  relations?: R;
  logger?: LoggerOption;
}

/** Row shape of a relation child as it arrives through the JSON path.
 *  Leaves decode through the same codecs as the flat path (mode-aware). */
export type RelationChildModelOf<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]: RelationSelectTypeOf<Cols[K]>;
};

/** Runtime value of one relation edge: to-many -> array (missing -> []),
 *  to-one -> object or null. */
export type RelationValue<R> = R extends RelationMany<infer T>
  ? T extends PgTableCore<infer Cols>
    ? Array<RelationChildModelOf<Cols>>
    : never[]
  : R extends RelationOne<infer T>
    ? T extends PgTableCore<infer Cols>
      ? RelationChildModelOf<Cols> | null
      : never
    : never;

type Simplify<T> = { [K in keyof T]: T[K] } & {};

/** Columns of a relation's target table (extracted from its table type). */
type TargetColsOf<Rel> =
  Rel extends RelationOne<infer T>
    ? T extends PgTableCore<infer Cols>
      ? Cols
      : never
    : Rel extends RelationMany<infer T>
      ? T extends PgTableCore<infer Cols>
        ? Cols
        : never
      : never;

/** The target table type of one relation. */
type TargetTableOf<Rel> =
  Rel extends RelationOne<infer T> ? T : Rel extends RelationMany<infer T> ? T : never;

/** Relation entries declared for one table, resolved through the whole
 *  relations input by table type (the input is keyed by table name — the
 *  runtime convention). Unresolvable targets degrade to the permissive map,
 *  matching runtime rejection of unknown relations. */
type EntriesForTarget<R extends RelationsInput, T> = UnionizeEntries<{
  [K in keyof R]: R[K] extends TableRelations<infer TT, infer E> ? ([T] extends [TT] ? E : never) : never;
}>;
type UnionizeEntries<T> = T[keyof T] extends infer U ? (U extends Record<string, Relation> ? U : never) : never;

/** Nested args for one relation edge: to-many edges take the full
 *  RelationalArgs; to-one edges reject limit/offset/orderBy at the type
 *  level too (a to-one matches at most one row — limit/offset would bound
 *  nothing and an ordering could not change the single-row result; the
 *  runtime enforces the same rules for untyped callers). */
type NestedArgsOf<Rel, R extends RelationsInput> = Rel extends RelationOne<any>
  ? OneRelationArgs<TargetColsOf<Rel>, EntriesForTarget<R, TargetTableOf<Rel>>, R>
  : RelationalArgs<TargetColsOf<Rel>, EntriesForTarget<R, TargetTableOf<Rel>>, R>;

export interface OneRelationArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> extends Omit<RelationalArgs<Cols, Entries, R>, "limit" | "offset" | "orderBy"> {}

/** Typed relational args, recursive through `with`. `columns` accepts only
 *  the table's property keys; `with` accepts only declared relation names,
 *  each `true` or a nested RelationalArgs applying to that relation's rows
 *  (per-child filter/order/limit/offset/columns and further nesting up to
 *  MAX_RELATION_DEPTH). */
export interface RelationalArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
  C extends keyof Cols = keyof Cols,
> {
  where?: RQBArgs["where"];
  orderBy?: RQBArgs["orderBy"];
  limit?: number;
  offset?: number;
  columns?: ReadonlyArray<C>;
  with?: {
    [K in keyof Entries]?: true | NestedArgsOf<Entries[K], R>;
  };
}

/** The with-map of an args value ({} when absent — `true` and plain args
 *  without `with` both select every column and no nested edges). */
type WithOf<A> = A extends { with?: infer W } ? (W extends Record<string, unknown> ? W : {}) : {};

/** Column keys selected by an args value: its `columns` subset intersected
 *  with the table's keys, or every key when omitted. */
type ColumnsSelected<A, Cols extends Record<string, AnyColumnBuilder>> =
  A extends { columns?: ReadonlyArray<infer CC> } ? (CC & keyof Cols) : keyof Cols;

/** Exact result row for one nesting level: the selected columns plus exactly
 *  the requested relation edges, each typed by the edge's cardinality and
 *  its own nested selection. */
export type RelationalRow<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
  A = RelationalArgs<Cols, Entries, R>,
> = Simplify<
  { [K in ColumnsSelected<A, Cols>]: SelectTypeOf<Cols[K]> } & {
    [K in keyof WithOf<A> & keyof Entries]: RelationEdgeValue<Entries[K], NonNullable<WithOf<A>[K & keyof WithOf<A>]>, R>;
  }
>;

/** Value of one relation edge given its nested args: array of child rows
 *  (to-many; missing -> []) or child row | null (to-one). */
type RelationEdgeValue<Rel, W, R extends RelationsInput> = Rel extends RelationMany<infer T>
  ? T extends PgTableCore<infer Cols>
    ? Array<RelationalRow<Cols, EntriesForTarget<R, T>, R, W>>
    : never[]
  : Rel extends RelationOne<infer T>
    ? T extends PgTableCore<infer Cols>
      ? RelationalRow<Cols, EntriesForTarget<R, T>, R, W> | null
      : never
    : never;

/**
 * Relational query API per table. Types are exact at every nesting depth:
 * requesting `with: { posts: { with: { comments: true } } }` types `posts`
 * as an array of post rows carrying `comments` arrays; unrequested relation
 * keys are absent. Unknown relation names in `with` and unknown property
 * keys in `columns` are compile errors at every level. `toSQL` and
 * `explainQuery` compile WITHOUT a connection (pure inspection).
 */
export interface QueryApiFor<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> {
  findMany<const A extends RelationalArgs<Cols, Entries, R> = {}>(
    args?: A,
  ): Promise<Array<RelationalRow<Cols, Entries, R, A>>>;
  findFirst<const A extends RelationalArgs<Cols, Entries, R> = {}>(
    args?: A,
  ): Promise<RelationalRow<Cols, Entries, R, A> | undefined>;
  /** Pure compile of the query — no driver round-trip, no execution. */
  toSQL(args?: RQBArgs): { sql: string; params: unknown[] };
  /** Structured pure compile plan: statements, parameters, decode plans,
   *  capability requirements and the relation edge tree. */
  explainQuery(args?: RQBArgs): RelationalExplainPlan;
}

type QueryApiOf<T extends TablesInput, R extends RelationsInput> = {
  [K in keyof T]: T[K] extends PgTableCore<infer Cols>
    ? R[K & keyof R] extends TableRelations<any, infer E>
      ? E extends Record<string, Relation>
        ? QueryApiFor<Cols, E, R>
        : QueryApiFor<Cols, Record<string, Relation>, R>
      : QueryApiFor<Cols, Record<string, Relation>, R>
    : never;
};

export interface SelectFrom {
  from<TCols extends Record<string, AnyColumnBuilder>>(table: PgTable<TCols>): SelectBuilder<null, InferSelectModelOfRecord<TCols>>;
  /** Select from a derived table or CTE reference: the row type is the
   *  source query's row type (Q02). */
  from<R extends Record<string, unknown>>(table: import("./subqueries.js").DerivedTable<string, R>): SelectBuilder<null, R>;
}
export interface SelectProjectedFrom<P extends Projection> {
  from(table: AnyPgTable): SelectBuilder<P, unknown>;
}

export interface NeutronDatabase<
  T extends TablesInput = TablesInput,
  R extends RelationsInput = RelationsInput,
> {
  driver: Driver;
  /** Close per the driver's ownership: owned adapters terminate exactly
   *  once (idempotent); borrowed adapters are never closed — their owner
   *  stays functional. */
  close(): Promise<void>;
  /** Engine identity of the connected server (memoized SELECT VERSION();
   *  FRAMEWORK_CONTRACT.md §1 — product is tri-state, see engine.ts). */
  engine(): Promise<EngineIdentity>;
  /** Tri-state capability status with evidence (supported / unsupported /
   *  unknown; unknown fails closed for statements that require it). */
  capability(name: StatementCapability): Promise<CapabilityEvidence>;
  select(): SelectFrom;
  select<P extends Projection>(projection: P): SelectProjectedFrom<P>;
  insert<TCols extends Record<string, AnyColumnBuilder>>(table: PgTable<TCols>): InsertBuilder<TCols, number>;
  update<TCols extends Record<string, AnyColumnBuilder>>(table: PgTable<TCols>): UpdateBuilder<TCols, number>;
  delete<TCols extends Record<string, AnyColumnBuilder>>(table: PgTable<TCols>): DeleteBuilder<TCols, number>;
  transaction<Tx>(fn: (tx: Omit<NeutronDatabase<T, R>, "transaction" | "close" | "driver">) => Promise<Tx>): Promise<Tx>;
  query: QueryApiOf<T, R>;
}

type Crud = Pick<NeutronDatabase, "select" | "insert" | "update" | "delete">;
type QueryApi = Record<string, {
  findMany: (args?: RQBArgs) => Promise<unknown>;
  findFirst: (args?: RQBArgs) => Promise<unknown>;
  toSQL: (args?: RQBArgs) => { sql: string; params: unknown[] };
  explainQuery: (args?: RQBArgs) => RelationalExplainPlan;
}>;
type TxScope = Omit<NeutronDatabase, "transaction" | "close" | "driver">;

export async function createDatabase<
  T extends TablesInput = TablesInput,
  R extends RelationsInput = RelationsInput,
>(options: DatabaseOptions<T, R>): Promise<NeutronDatabase<T, R>> {
  if ((options.url === undefined) === (options.driver === undefined)) {
    throw new Error("createDatabase requires exactly one of `url` or `driver` (inject an adapter wrapped via wrapPgPool/wrapPostgresJs)");
  }
  const driver = options.driver ?? (await loadDriver(options.url!, options.driverOptions));
  const logger = resolveLogger(options.logger);
  const capabilities = capabilityGate(driver);

  const tables = new Map<string, { key: string; table: AnyPgTable }>();
  for (const [key, value] of Object.entries(options.tables ?? {})) {
    if (isPgTable(value)) {
      rejectDerivedTable(value, `tables.${key}`);
      const schema = getTableSchema(value);
      if (schema !== undefined) {
        throw new Error(
          `tables.${key}: "${schema}"."${getTableName(value)}" declares a schema — relational reads (db.query) on schema-qualified tables land with Q05/Q07. ` +
            `CRUD select/insert/update/delete and alias joins support them without registering them in \`tables\``,
        );
      }
      tables.set(getTableName(value), { key, table: value });
    }
  }

  const relationSets: TableRelations[] = [];
  const relationsByName = new Map<string, TableRelations>();
  for (const [key, value] of Object.entries(options.relations ?? {})) {
    if (!isTableRelations(value)) continue;
    rejectDerivedTable(value.table, `relations.${key}`);
    const schema = getTableSchema(value.table);
    if (schema !== undefined) {
      throw new Error(
        `relations.${key}: "${schema}"."${getTableName(value.table)}" declares a schema — relational reads on schema-qualified tables land with Q05/Q07`,
      );
    }
    relationsByName.set(getTableName(value.table), value);
    relationSets.push(value);
  }
  const resolved = resolveRelations(relationSets);
  const relationsByTable = new Map<string, Record<string, Relation>>();
  for (const r of relationSets) relationsByTable.set(getTableName(r.table), r.entries);
  void resolved;

  const ctx: ExecContext = { driver, logger, capabilities };

  const makeCrud = (context: ExecContext): Crud => ({
    select: ((projection?: Projection) => ({
      from: (table: AnyPgTable) => new SelectBuilder(context, table, projection ?? null),
    })) as Crud["select"],
    insert: ((table: AnyPgTable) => new InsertBuilder(context, table)) as Crud["insert"],
    update: ((table: AnyPgTable) => new UpdateBuilder(context, table)) as Crud["update"],
    delete: ((table: AnyPgTable) => new DeleteBuilder(context, table)) as Crud["delete"],
  });

  const makeQuery = (context: ExecContext): QueryApi => {
    const api: QueryApi = {};
    for (const { key, table } of tables.values()) {
      const entries = relationsByTable.get(getTableName(table)) ?? {};
      api[key] = {
        findMany: (args: RQBArgs = {}) => findMany(context, table, entries, args, relationsByTable),
        findFirst: (args: RQBArgs = {}) => findFirst(context, table, entries, args, relationsByTable),
        toSQL: (args: RQBArgs = {}) => {
          const built = buildRelationalSQL(table, entries, args, relationsByTable);
          return { sql: built.sql, params: built.params };
        },
        explainQuery: (args: RQBArgs = {}) => buildRelationalPlan(table, entries, args, relationsByTable),
      };
    }
    return api;
  };

  const crud = makeCrud(ctx);
  const query = makeQuery(ctx);

  const db = {
    driver,
    close: (): Promise<void> => driver.close(),
    engine: (): Promise<EngineIdentity> => capabilities.engine(),
    capability: (name: StatementCapability): Promise<CapabilityEvidence> => capabilities.status(name),
    ...crud,
    transaction: async <Tx>(fn: (tx: TxScope) => Promise<Tx>): Promise<Tx> => {
      return driver.begin(async (txDriver) => {
        const txCtx: ExecContext = { driver: txDriver, logger, capabilities };
        return fn({ ...makeCrud(txCtx), query: makeQuery(txCtx) } as TxScope);
      });
    },
    query,
  };

  return db as unknown as NeutronDatabase<T, R>;
}

export { buildRelationalSQL, buildRelationalPlan, MAX_RELATION_DEPTH };
