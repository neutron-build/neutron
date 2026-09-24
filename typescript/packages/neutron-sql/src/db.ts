// ---------------------------------------------------------------------------
// @neutron-build/sql — database entry point
// ---------------------------------------------------------------------------

import { loadDriver, assertNodeRuntime, type Driver, type LoadDriverOptions } from "./drivers.js";
import { capabilityGate, type CapabilityEvidence, type EngineIdentity } from "./engine.js";
import type { StatementCapability } from "./codecs.js";
import { resolveLogger, type Logger, type LoggerOption, type SqlEvent } from "./logger.js";
import {
  hasModes,
  renderBeginSql,
  runRetriedTransaction,
  runTransaction,
  validateRetryOptions,
  type QueryExecutionOptions,
  type Savepoint,
  type TransactionModes,
  type TransactionRetryOptions,
  type TransactionScope,
} from "./transactions.js";
import { NeutronSqlError } from "./errors.js";
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
  compileNestedCreate,
  compileNestedUpdate,
  compileNestedDelete,
  executeNestedWrite,
  type NestedCreateInput,
  type NestedUpdateInput,
  type NestedDeleteInput,
  type NestedWriteOptions,
  type NestedWritePlan,
  type CompiledNestedWrite,
} from "./nested.js";
import type { ValueNode } from "./ast.js";
import {
  getTableName,
  getTableSchema,
  isPgTable,
  isTableRelations,
  rejectDerivedTable,
  type AnyColumnBuilder,
  type AnyPgTable,
  type ColumnBuilder,
  type InsertTypeOf,
  type JsWriteTypeOf,
  type UpdateTypeOf,
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

// ---------------------------------------------------------------------------
// Nested writes (Q06) — typed input shapes
// ---------------------------------------------------------------------------

type OneOrMany<T> = T | ReadonlyArray<T>;
type ColumnWriteOf<C> = C extends ColumnBuilder<infer D, boolean, boolean, unknown> ? JsWriteTypeOf<D> : never;

/** A unique-key selector: property keys of ONE declared unique key (the
 *  possibly composite primary key, a unique column, or a unique index) with
 *  non-null values. Which keys form a unique key is checked at runtime before
 *  any SQL (the key sets are not visible to the type system). */
export type UniqueSelector<Cols extends Record<string, AnyColumnBuilder>> = {
  [K in keyof Cols]?: ColumnWriteOf<Cols[K]>;
};

/** Relation keys of a data object. Untyped relation maps (no relations
 *  generic) accept any key and defer to runtime validation. */
type RelationOpsMap<Entries extends Record<string, Relation>, R extends RelationsInput, M extends "create" | "update"> =
  string extends keyof Entries
    ? { [key: string]: unknown }
    : { [K in keyof Entries]?: M extends "create" ? CreateOpsOf<Entries[K], R> : UpdateOpsOf<Entries[K], R> };

/** Data for a created row: column values (every key optional at the type
 *  level — foreign-key columns may be supplied by a relation operation, so
 *  required-column presence is checked at runtime before any SQL) plus
 *  relation operations. */
export type NestedCreateData<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> = { [K in keyof Cols]?: InsertTypeOf<Cols[K]> } & RelationOpsMap<Entries, R, "create">;

/** Data for an updated row: column assignments (undefined ignored, null for
 *  nullable columns, expressions allowed like update().set()) plus relation
 *  operations. */
export type NestedUpdateData<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> = { [K in keyof Cols]?: UpdateTypeOf<Cols[K]> | ValueNode } & RelationOpsMap<Entries, R, "update">;

type CreateOpsOf<Rel, R extends RelationsInput> = Rel extends RelationMany<infer T>
  ? T extends PgTableCore<infer TC>
    ? {
        create?: OneOrMany<NestedCreateData<TC, EntriesForTarget<R, T>, R>>;
        connect?: OneOrMany<UniqueSelector<TC>>;
      }
    : never
  : Rel extends RelationOne<infer T>
    ? T extends PgTableCore<infer TC>
      ? {
          create?: NestedCreateData<TC, EntriesForTarget<R, T>, R>;
          connect?: UniqueSelector<TC>;
        }
      : never
    : never;

type UpdateOpsOf<Rel, R extends RelationsInput> = Rel extends RelationMany<infer T>
  ? T extends PgTableCore<infer TC>
    ? {
        create?: OneOrMany<NestedCreateData<TC, EntriesForTarget<R, T>, R>>;
        connect?: OneOrMany<UniqueSelector<TC>>;
        update?: OneOrMany<{ where: UniqueSelector<TC>; data: NestedUpdateData<TC, EntriesForTarget<R, T>, R> }>;
        disconnect?: OneOrMany<UniqueSelector<TC>>;
        delete?: OneOrMany<UniqueSelector<TC>>;
      }
    : never
  : Rel extends RelationOne<infer T>
    ? T extends PgTableCore<infer TC>
      ? {
          create?: NestedCreateData<TC, EntriesForTarget<R, T>, R>;
          connect?: UniqueSelector<TC>;
          update?: NestedUpdateData<TC, EntriesForTarget<R, T>, R>;
          disconnect?: true;
          delete?: true;
        }
      : never
    : never;

export interface NestedCreateArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> {
  data: NestedCreateData<Cols, Entries, R>;
}

export interface NestedUpdateArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> {
  /** One unique key of the row to update (exactly one row must match). */
  where: UniqueSelector<Cols>;
  data: NestedUpdateData<Cols, Entries, R>;
}

/** Dependent-row disposition for a delete cascade (to-many edges only):
 *  `"disconnect"` keeps the rows and nulls their foreign key (nullable FKs
 *  only), `"delete"` removes them, `{ delete: {...} }` removes them with
 *  dispositions for THEIR dependents. */
type CascadeOf<Rel, R extends RelationsInput> = Rel extends RelationMany<infer T>
  ? T extends PgTableCore
    ? "disconnect" | "delete" | { delete: NestedCascadeSpec<EntriesForTarget<R, T>, R> }
    : never
  : never;

/** Dispositions keyed by relation name. An edge with existing rows and no
 *  declared disposition aborts the whole delete. */
export type NestedCascadeSpec<Entries extends Record<string, Relation>, R extends RelationsInput = RelationsInput> = {
  [K in keyof Entries]?: CascadeOf<Entries[K], R>;
};

export interface NestedDeleteArgs<
  Cols extends Record<string, AnyColumnBuilder>,
  Entries extends Record<string, Relation>,
  R extends RelationsInput = RelationsInput,
> {
  /** One unique key of the row to delete (exactly one row must match). */
  where: UniqueSelector<Cols>;
  cascade?: NestedCascadeSpec<Entries, R>;
}

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
    options?: QueryExecutionOptions,
  ): Promise<Array<RelationalRow<Cols, Entries, R, A>>>;
  findFirst<const A extends RelationalArgs<Cols, Entries, R> = {}>(
    args?: A,
    options?: QueryExecutionOptions,
  ): Promise<RelationalRow<Cols, Entries, R, A> | undefined>;
  /** Pure compile of the query — no driver round-trip, no execution. */
  toSQL(args?: RQBArgs): { sql: string; params: unknown[] };
  /** Structured pure compile plan: statements, parameters, decode plans,
   *  capability requirements and the relation edge tree. */
  explainQuery(args?: RQBArgs): RelationalExplainPlan;
  /** Nested create (Q06): insert one row plus the relation operations in
   *  `data`, atomically (one transaction; a savepoint inside db.transaction).
   *  Resolves to the created row. Never retried automatically. */
  create(args: NestedCreateArgs<Cols, Entries, R>, options?: NestedWriteOptions): Promise<InferSelectModelOfRecord<Cols>>;
  /** Nested update (Q06) of exactly one row named by a unique key, plus the
   *  relation operations in `data`, atomically. Resolves to the updated row. */
  update(args: NestedUpdateArgs<Cols, Entries, R>, options?: NestedWriteOptions): Promise<InferSelectModelOfRecord<Cols>>;
  /** Nested delete (Q06) of exactly one row named by a unique key, with
   *  explicit dispositions for every dependent edge (`cascade`). Dependent
   *  edges with rows and no declared disposition abort the write. Resolves
   *  to the deleted row. Never retried automatically. */
  delete(args: NestedDeleteArgs<Cols, Entries, R>, options?: NestedWriteOptions): Promise<InferSelectModelOfRecord<Cols>>;
  /** Dry compilation of a nested create: every planned statement in order,
   *  with parameters (step-output references for generated keys),
   *  dependencies and edge ownership. Pure — no connection. */
  explainCreate(args: NestedCreateArgs<Cols, Entries, R>): NestedWritePlan;
  /** Dry compilation of a nested update (pure). */
  explainUpdate(args: NestedUpdateArgs<Cols, Entries, R>): NestedWritePlan;
  /** Dry compilation of a nested delete (pure). */
  explainDelete(args: NestedDeleteArgs<Cols, Entries, R>): NestedWritePlan;
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

/** Options for `db.transaction` (I02): transaction modes plus the opt-in
 *  whole-transaction retry. */
export interface TransactionOptions extends TransactionModes {
  /** Retry the WHOLE transaction on serialization failure (40001) or
   *  deadlock (40P01) only, up to maxAttempts, with backoff between
   *  attempts. Requires `idempotent: true` — a retried attempt re-executes
   *  the callback from the top. A CommitAmbiguityError (unknown commit
   *  outcome) is never retried. */
  retry?: TransactionRetryOptions;
}

/** The scope passed to a `db.transaction` callback: the full database
 *  surface (CRUD, relational queries) pinned to the transaction's
 *  connection, plus nested transactions (real savepoints) and explicit
 *  savepoint control. */
export interface TransactionTxScope<
  T extends TablesInput = TablesInput,
  R extends RelationsInput = RelationsInput,
> extends Omit<NeutronDatabase<T, R>, "transaction" | "close" | "driver"> {
  /** Nested transaction: a real SAVEPOINT on the same connection. On error
   *  the savepoint is rolled back and released and the error rethrows; the
   *  outer transaction stays usable. Modes are properties of the outer
   *  BEGIN — a nested call takes none. */
  transaction<Tx>(fn: (tx: TransactionTxScope<T, R>) => Promise<Tx>): Promise<Tx>;
  /** Create an explicitly controlled savepoint (`rollbackTo` keeps it,
   *  `release` destroys it). Omit the name for an auto-generated one. */
  savepoint(name?: string): Promise<Savepoint>;
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
  /** Run `fn` in one transaction. Options select isolation/read-only/
   *  deferrable modes (rendered into BEGIN) and the opt-in retry. A
   *  transport failure while COMMIT is in flight throws
   *  CommitAmbiguityError — the outcome is unknown and never replayed. */
  transaction<Tx>(fn: (tx: TransactionTxScope<T, R>) => Promise<Tx>, options?: TransactionOptions): Promise<Tx>;
  query: QueryApiOf<T, R>;
}

type Crud = Pick<NeutronDatabase, "select" | "insert" | "update" | "delete">;
type QueryApi = Record<string, {
  findMany: (args?: RQBArgs, options?: QueryExecutionOptions) => Promise<unknown>;
  findFirst: (args?: RQBArgs, options?: QueryExecutionOptions) => Promise<unknown>;
  toSQL: (args?: RQBArgs) => { sql: string; params: unknown[] };
  explainQuery: (args?: RQBArgs) => RelationalExplainPlan;
  create: (args: NestedCreateInput, options?: NestedWriteOptions) => Promise<unknown>;
  update: (args: NestedUpdateInput, options?: NestedWriteOptions) => Promise<unknown>;
  delete: (args: NestedDeleteInput, options?: NestedWriteOptions) => Promise<unknown>;
  explainCreate: (args: NestedCreateInput) => NestedWritePlan;
  explainUpdate: (args: NestedUpdateInput) => NestedWritePlan;
  explainDelete: (args: NestedDeleteInput) => NestedWritePlan;
}>;
type TxScope = TransactionTxScope;

/** Runs a nested-write body atomically on a transaction-scoped context. */
type AtomicRunner = <T>(body: (context: ExecContext) => Promise<T>, isolation: NestedWriteOptions["isolation"]) => Promise<T>;

export async function createDatabase<
  T extends TablesInput = TablesInput,
  R extends RelationsInput = RelationsInput,
>(options: DatabaseOptions<T, R>): Promise<NeutronDatabase<T, R>> {
  assertNodeRuntime("createDatabase");
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

  const makeQuery = (context: ExecContext, atomic: AtomicRunner): QueryApi => {
    const api: QueryApi = {};
    for (const { key, table } of tables.values()) {
      const entries = relationsByTable.get(getTableName(table)) ?? {};
      // Nested writes compile (and validate) BEFORE any connection is touched;
      // only a valid plan opens the transaction. The compile itself runs
      // inside an async entry so planning rejections surface as promise
      // rejections (never synchronous throws off an await expression).
      const nestedExec = (compiled: CompiledNestedWrite, options: NestedWriteOptions = {}): Promise<unknown> => {
        const { isolation, ...statementOptions } = options;
        return atomic((txContext) => executeNestedWrite(txContext, compiled, statementOptions), isolation);
      };
      api[key] = {
        create: async (args: NestedCreateInput, options?: NestedWriteOptions) =>
          nestedExec(compileNestedCreate(table, args, relationsByTable), options),
        update: async (args: NestedUpdateInput, options?: NestedWriteOptions) =>
          nestedExec(compileNestedUpdate(table, args, relationsByTable), options),
        delete: async (args: NestedDeleteInput, options?: NestedWriteOptions) =>
          nestedExec(compileNestedDelete(table, args, relationsByTable), options),
        explainCreate: (args: NestedCreateInput) => compileNestedCreate(table, args, relationsByTable).plan,
        explainUpdate: (args: NestedUpdateInput) => compileNestedUpdate(table, args, relationsByTable).plan,
        explainDelete: (args: NestedDeleteInput) => compileNestedDelete(table, args, relationsByTable).plan,
        findMany: (args: RQBArgs = {}, options?: QueryExecutionOptions) => findMany(context, table, entries, args, relationsByTable, options),
        findFirst: (args: RQBArgs = {}, options?: QueryExecutionOptions) => findFirst(context, table, entries, args, relationsByTable, options),
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

  /** Top level: a nested write opens its own transaction (the shared I02
   *  runner on a pinned connection; the adapter's begin() for custom
   *  adapters). Never retried — a failure or an ambiguous commit surfaces
   *  unchanged. */
  const topLevelAtomic: AtomicRunner = async (body, isolation) => {
    const modes: TransactionModes = isolation === undefined ? {} : { isolation };
    renderBeginSql(modes);
    if (typeof driver.pin !== "function") {
      if (isolation !== undefined) {
        throw new NeutronSqlError("nested write isolation requires a pinnable adapter (Driver.pin) — the injected custom adapter does not provide one");
      }
      return driver.begin((txDriver) => body({ driver: txDriver, logger, capabilities }));
    }
    const hooks = { onEvent: (event: SqlEvent) => logger?.(event) };
    return runTransaction(await driver.pin(), (scope) => body({ driver: scope, logger, capabilities }), modes, hooks);
  };
  const query = makeQuery(ctx, topLevelAtomic);

  /** Adapt a driver-level transaction scope into the full database-shaped
   *  tx scope (CRUD + relational queries pinned to the transaction's
   *  connection, nested savepoint transactions, explicit savepoints). */
  const adaptScope = (scope: TransactionScope): TxScope => {
    const txCtx: ExecContext = { driver: scope, logger, capabilities };
    // Inside a transaction a nested write runs in its own SAVEPOINT: a failed
    // graph rolls back to it and the outer transaction stays usable.
    const savepointAtomic: AtomicRunner = (body, isolation) => {
      if (isolation !== undefined) {
        return Promise.reject(
          new NeutronSqlError("nested write isolation is a property of the outer BEGIN — pass it to db.transaction(fn, options); inside a transaction the write runs in a savepoint"),
        );
      }
      return scope.transaction((inner) => body({ driver: inner, logger, capabilities }));
    };
    return {
      ...makeCrud(txCtx),
      query: makeQuery(txCtx, savepointAtomic),
      // Nested transaction: a real SAVEPOINT on the same connection. Modes
      // are properties of the outer BEGIN — a JS caller passing an options
      // argument (reachable only without types; the declared surface takes
      // none) gets a precise runtime rejection instead of a silent drop.
      transaction: <Tx>(nested: (tx: TxScope) => Promise<Tx>, ...extra: unknown[]): Promise<Tx> => {
        const modes = extra[0] as TransactionModes | undefined;
        if (modes !== undefined && hasModes(modes)) {
          throw new NeutronSqlError(
            "isolation/read-only/deferrable are properties of the outer BEGIN — pass them to db.transaction(fn, options) or the driver-scoped begin(fn, modes); a nested transaction is a savepoint and takes no modes",
          );
        }
        return scope.transaction((inner) => nested(adaptScope(inner)));
      },
      savepoint: (name?: string): Promise<Savepoint> => scope.savepoint(name),
    } as TxScope;
  };

  /** Legacy scope for custom adapters without Driver.pin: transactions run
   *  through their begin(), without savepoints or transaction options. */
  const legacyScope = (txDriver: Driver): TxScope => {
    const txCtx: ExecContext = { driver: txDriver, logger, capabilities };
    const noSavepoints: AtomicRunner = () =>
      Promise.reject(
        new NeutronSqlError(
          "nested writes inside a transaction need savepoints to stay atomic (a failed graph must not leave partial work in your transaction) — this custom adapter has no Driver.pin; run the nested write outside db.transaction or use a bundled adapter",
        ),
      );
    return {
      ...makeCrud(txCtx),
      query: makeQuery(txCtx, noSavepoints),
      transaction: async <Tx>(_fn: (tx: TxScope) => Promise<Tx>): Promise<Tx> => {
        throw new NeutronSqlError("nested transactions (savepoints) require a pinnable adapter (Driver.pin) — this custom adapter's begin() scope does not expose them");
      },
      savepoint: async (_name?: string): Promise<Savepoint> => {
        throw new NeutronSqlError("savepoints require a pinnable adapter (Driver.pin) — this custom adapter's begin() scope does not expose them");
      },
    } as TxScope;
  };

  const db = {
    driver,
    close: (): Promise<void> => driver.close(),
    engine: (): Promise<EngineIdentity> => capabilities.engine(),
    capability: (name: StatementCapability): Promise<CapabilityEvidence> => capabilities.status(name),
    ...crud,
    transaction: async <Tx>(fn: (tx: TxScope) => Promise<Tx>, options: TransactionOptions = {}): Promise<Tx> => {
      const retry = options.retry;
      if (retry !== undefined) validateRetryOptions(retry);
      // Honest pre-SQL validation of modes (and pre-pin: nothing touches a
      // connection for an unsupported combination).
      renderBeginSql(options);
      const hooks = { onEvent: (event: SqlEvent) => logger?.(event) };
      if (typeof driver.pin !== "function") {
        if (hasModes(options) || retry !== undefined) {
          throw new NeutronSqlError(
            "transaction options (isolation/read-only/deferrable/retry) require a pinnable adapter (Driver.pin) — the injected custom adapter does not provide one; both bundled drivers do",
          );
        }
        return driver.begin(async (txDriver) => fn(legacyScope(txDriver)));
      }
      const pinFactory = (): Promise<import("./transactions.js").PinnedExecutor> => driver.pin!();
      if (retry === undefined) {
        return runTransaction(await pinFactory(), (scope) => fn(adaptScope(scope)), options, hooks);
      }
      return runRetriedTransaction(pinFactory, (scope) => fn(adaptScope(scope)), options, retry, hooks);
    },
    query,
  };

  return db as unknown as NeutronDatabase<T, R>;
}

export { buildRelationalSQL, buildRelationalPlan, MAX_RELATION_DEPTH };
