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
  type ProjectionResult,
} from "./builder.js";
import { buildRelationalSQL, findFirst, findMany, resolveRelations, type RQBArgs } from "./relations.js";
import {
  getTableName,
  isPgTable,
  isTableRelations,
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
 *  Leaves are typed honestly per RelationLeafTypeOf: int8/numeric (rendered
 *  ::text) and temporal/bytea (to_jsonb string forms) are `string`. */
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

type WithSelection<Entries extends Record<string, Relation>> = { [K in keyof Entries]?: true };

/** Typed relational args. `columns` accepts only the table's property keys;
 *  `with` accepts only declared relation names, each `true` (one level). */
export interface RelationalArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  C extends keyof Cols = keyof Cols,
  W extends keyof Entries = never,
> {
  where?: RQBArgs["where"];
  orderBy?: RQBArgs["orderBy"];
  limit?: number;
  offset?: number;
  columns?: Array<C>;
  with?: { [K in W]?: true };
}

type Simplify<T> = { [K in keyof T]: T[K] } & {};

/** Exact one-level result row: the selected columns (all of them when
 *  `columns` is omitted) plus exactly the requested relation edges. */
export type RelationalRow<
  Cols extends Record<string, AnyColumnBuilder>,
  C extends keyof Cols,
  Entries extends Record<string, Relation>,
  W extends keyof Entries,
> = Simplify<{ [K in C]: SelectTypeOf<Cols[K]> } & { [K in Extract<W, keyof Entries>]: RelationValue<Entries[K]> }>;

/**
 * Relational query API per table. Types are exact at one level: requesting
 * `with: { posts: true }` makes `posts` a required key of the result row and
 * unrequested relation keys are absent. Unknown relation names in `with` and
 * unknown property keys in `columns` are compile errors.
 */
export type QueryApiFor<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
> = {
  findMany<C extends keyof Cols = keyof Cols, W extends keyof Entries = never>(
    args?: RelationalArgs<Cols, Entries, C, W>,
  ): Promise<Array<RelationalRow<Cols, C, Entries, W>>>;
  findFirst<C extends keyof Cols = keyof Cols, W extends keyof Entries = never>(
    args?: RelationalArgs<Cols, Entries, C, W>,
  ): Promise<RelationalRow<Cols, C, Entries, W> | undefined>;
};

type QueryApiOf<T extends TablesInput, R extends RelationsInput> = {
  [K in keyof T]: T[K] extends PgTableCore<infer Cols>
    ? R[K & keyof R] extends TableRelations<any, infer E>
      ? E extends Record<string, Relation>
        ? QueryApiFor<Cols, E>
        : QueryApiFor<Cols, Record<string, Relation>>
      : QueryApiFor<Cols, Record<string, Relation>>
    : never;
};

export interface SelectFrom {
  from<TCols extends Record<string, AnyColumnBuilder>>(table: PgTable<TCols>): SelectBuilder<InferSelectModelOfRecord<TCols>>;
}
export interface SelectProjectedFrom<P extends Projection> {
  from(table: AnyPgTable): SelectBuilder<ProjectionResult<P>>;
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
type QueryApi = Record<string, { findMany: (args?: RQBArgs) => Promise<unknown>; findFirst: (args?: RQBArgs) => Promise<unknown> }>;
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
    if (isPgTable(value)) tables.set(getTableName(value), { key, table: value });
  }

  const relationSets: TableRelations[] = [];
  const relationsByName = new Map<string, TableRelations>();
  for (const [key, value] of Object.entries(options.relations ?? {})) {
    if (!isTableRelations(value)) continue;
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
        findMany: (args: RQBArgs = {}) => findMany(context, table, entries, args),
        findFirst: (args: RQBArgs = {}) => findFirst(context, table, entries, args),
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

export { buildRelationalSQL };
