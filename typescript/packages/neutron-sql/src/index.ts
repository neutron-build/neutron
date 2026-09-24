// ---------------------------------------------------------------------------
// @neutron-build/sql — SQL-only entry point
// ---------------------------------------------------------------------------
// This module pulls in no multi-model (Nucleus) code. Tree-shaking starts here.

export {
  pgTable,
  pgSchema,
  pgView,
  pgEnum,
  isPgTable,
  isPgView,
  isPgEnum,
  alias,
  relations,
  isAliasHandle,
  isTableRelations,
  getTableName,
  getTableSchema,
  getTableColumns,
  getTableIndexes,
  getTableConstraints,
  getEnumDefinition,
  index,
  uniqueIndex,
  primaryKey,
  unique,
  check,
  foreignKey,
  TABLE_SYMBOL,
  RELATIONS_SYMBOL,
  ALIAS_MARKER,
  DERIVED_MARKER,
  isDerivedTableHandle,
  getDerivedRecord,
  type PgTable,
  type PgTableCore,
  type PgSchemaBuilder,
  type TableMetadata,
  type ViewDefinition,
  type ViewOptions,
  type PgEnum,
  type AnyPgEnum,
  type PgEnumDefinition,
  type CustomCodec,
  type AliasedTable,
  type AliasedCols,
  type AliasRecord,
  type DerivedRecord,
  type TableRelations,
  type Relation,
  type RelationOne,
  type RelationMany,
  type RelationConfig,
  type ManyConfig,
  type ColumnBuilder,
  type AnyColumnBuilder,
  type ColumnDataType,
  type JsTypeOf,
  type JsWriteTypeOf,
  type WriteTypeOf,
  type PredicateValueOf,
  type BigintRead,
  type TemporalRead,
  type SelectTypeOf,
  type RelationLeafTypeOf,
  type RelationSelectTypeOf,
  type InsertTypeOf,
  type UpdateTypeOf,
  type TableIndex,
  type IndexKeyPartDef,
  type IndexMethod,
  type TableConstraint,
  type TablePrimaryKeyDef,
  type TableUniqueDef,
  type TableCheckDef,
  type TableForeignKeyDef,
  type ForeignKeyMatch,
  type TableExtra,
  type ReferentialAction,
  serial,
  integer,
  smallint,
  bigint,
  double,
  real,
  numeric,
  text,
  varchar,
  boolean,
  timestamp,
  timestamptz,
  date,
  json,
  jsonb,
  uuid,
  bytea,
  vector,
} from "./schema.js";

// The structural template. `sql` is the primary name (F04 collapsed the
// F01-era `sqlAst` split); `sqlAst` remains as a deprecated alias so existing
// code keeps compiling. Both interpolate VALUES as parameters and splice
// nodes/fragments/TrustedSql structurally — legacy {sql, params} fragments
// are rejected (their $n text can never be renumbered here).
export { sqlAst, sqlAst as sql } from "./ast.js";

export {
  raw,
  eq,
  ne,
  lt,
  lte,
  gt,
  gte,
  like,
  ilike,
  inArray,
  isNull,
  isNotNull,
  and,
  or,
  not,
  asc,
  desc,
  ascNullsLast,
  ascNullsFirst,
  descNullsLast,
  descNullsFirst,
  exists,
  excluded,
  count,
  countDistinct,
  sum,
  avg,
  min,
  max,
  stringAgg,
  boolAnd,
  boolOr,
  qident,
  type SqlFragment,
  type Condition,
  type OrderExpression,
  type SumResult,
  type AvgResult,
  type MinMaxResult,
} from "./expr.js";

export {
  SelectBuilder,
  InsertBuilder,
  UpdateBuilder,
  DeleteBuilder,
  SetOpBuilder,
  astSelect,
  AstSelectBuilder,
  whereItems,
  type Projection,
  type ProjectionResult,
  type AstProjection,
  type AstJoinTarget,
  type JoinFieldType,
  type JoinRowOf,
  type CompiledStatement,
  type FullSelectPlan,
  type PlanColumnSpec,
  type SubquerySource,
  type ConflictTargetSpec,
  type ReturningSubsetOf,
  type LockOptions,
} from "./builder.js";

// Q02: derived tables and CTE references — table-like handles over a select
// builder, with row types derived from the source's projection outputs.
export {
  derivedTable,
  cteTable,
  type DerivedTable,
  type PseudoColsOf,
  type DataTypeOfRead,
} from "./subqueries.js";

// F01 architecture spike: frozen SQL AST nodes, the structural sql template,
// the trusted-SQL boundary and the one-traversal compiler. Since F04 every
// CRUD path (select/insert/update/delete, relational aggregation) compiles
// through compileStatement — the single SQL assembly point.
export {
  trustSql,
  TRUSTED_SQL_ACK,
  ident,
  qual,
  ref,
  param,
  paramCast,
  defaultCell,
  expr,
  fragment,
  projection,
  subquery,
  cte,
  aggregate,
  join,
  selectStatement,
  statementReferencesName,
  insertStatement,
  updateStatement,
  deleteStatement,
  onConflictClause,
  assertDistinctPhysicalColumns,
  assertOnConflictNodeValid,
  assertNoExcludedRefs,
  assertExcludedScope,
  collectExcludedRefs,
  isValueNode,
  isStatement,
  lockingClause,
  assertLockingClauseValid,
  type IdentifierNode,
  type QualifiedNode,
  type ParamNode,
  type DefaultNode,
  type TrustedNode,
  type FragmentNode,
  type FragmentPart,
  type ExpressionNode,
  type ProjectionNode,
  type JoinNode,
  type JoinType,
  type JoinTarget,
  type SubqueryNode,
  type CteNode,
  type AggregateOp,
  type AggregateNode,
  type SetOpKind,
  type SetOpBranch,
  type OrderSpec,
  type FromTarget,
  type StatementNode,
  type StatementInput,
  type InsertStatementNode,
  type InsertStatementInput,
  type OnConflictNode,
  type OnConflictClauseInput,
  type ConflictTarget,
  type UpdateStatementNode,
  type UpdateStatementInput,
  type UpdateAssignment,
  type DeleteStatementNode,
  type DeleteStatementInput,
  type MutationTarget,
  type AnyStatementNode,
  type ValueNode,
  type SqlNode,
  type TrustedSql,
  type TrustedSqlAcknowledgment,
  type LockStrength,
  type LockWaitPolicy,
  type LockingClause,
} from "./ast.js";

// Q08: window functions — structural `fn(...) over (partition by / order by /
// frame)` expressions with PostgreSQL result typing (row_number/rank int8,
// value functions their argument column's codec), placement enforced at the
// compile choke point (select list and ORDER BY only) and capability
// requirements (window-functions, window-frame-groups).
export {
  over,
  isWindowExpr,
  rowNumber,
  rank,
  denseRank,
  percentRank,
  cumeDist,
  ntile,
  lag,
  lead,
  firstValue,
  lastValue,
  nthValue,
  containsWindow,
  type WindowFunction,
  type WindowFunctionOp,
  type WindowExpr,
  type WindowSpec,
  type WindowOrderTerm,
  type WindowFrame,
  type FrameBound,
  type FrameExclude,
  type WindowValueResult,
  type WindowResultSpec,
} from "./window.js";

// Q08: bounded streaming over server-side cursors — DECLARE / FETCH FORWARD
// / CLOSE, at most one batch buffered client-side, early exit returns the
// connection promptly (owned transaction rolls back, enclosing scope closes
// the cursor), cancellation reaches the server on every round trip.
export {
  CursorStream,
  DEFAULT_STREAM_BATCH_SIZE,
  MAX_STREAM_BATCH_SIZE,
  type StreamOptions,
  type StreamPlan,
  type StreamStatementRunner,
} from "./stream.js";

// Q08: explicit batch query plans — a fixed list of compiled statements run
// sequentially on one connection in one transaction (REPEATABLE READ
// snapshot by default), opt-in whole-batch retry with I02 semantics.
export {
  BatchQuery,
  type Batchable,
  type BatchOptions,
  type BatchResults,
  type BatchPlan,
  type BatchStatementPlan,
} from "./batch.js";

export { compile, compileStatement, quoteIdent, type CompileState, type CompiledQuery } from "./compile.js";

export { schemaToDDL, createTableSQL, createIndexSQL, dropTableSQL, addForeignKeySQL, sqlTypeOf } from "./ddl.js";

export {
  jsonNull,
  isJsonNull,
  wireReadNode,
  canonicalTextWireNode,
  projectionDecoder,
  applyProjectionDecoders,
  aggregateProjection,
  aggregateResultColumn,
  type BigintMode,
  type BigintOptions,
  type TemporalMode,
  type TemporalOptions,
  type NumericOptions,
  type JsonNullValue,
  type ColumnCodec,
  type CodecRead,
  type ColumnContext,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";

export {
  createDatabase,
  buildRelationalSQL,
  buildRelationalPlan,
  MAX_RELATION_DEPTH,
  type DatabaseOptions,
  type TablesInput,
  type RelationsInput,
  type NeutronDatabase,
  type TransactionOptions,
  type TransactionTxScope,
  type QueryApiFor,
  type RelationalArgs,
  type OneRelationArgs,
  type RelationalRow,
  type RelationValue,
  type RelationChildModelOf,
  type RelationalExplainPlan,
  type RelationalStatementPlan,
  type RelationEdgePlan,
  type UniqueSelector,
  type NestedCreateData,
  type NestedUpdateData,
  type NestedCreateArgs,
  type NestedUpdateArgs,
  type NestedDeleteArgs,
  type NestedCascadeSpec,
} from "./db.js";

export {
  NestedWriteError,
  uniqueKeysOf,
  type NestedWritePlan,
  type NestedWriteStep,
  type NestedWriteOp,
  type NestedWriteAction,
  type NestedWriteExpect,
  type NestedWriteOptions,
  type NestedEdgePlan,
  type StepOutputRef,
} from "./nested.js";

export {
  loadDriver,
  assertNodeRuntime,
  wrapPgPool,
  wrapPostgresJs,
  makeLifecycle,
  preparedStatement,
  pgStatementName,
  type Driver,
  type DriverKind,
  type LoadDriverOptions,
  type AdapterOwnership,
  type DriverLifecycle,
  type WrapAdapterOptions,
  type PgPoolLike,
  type PgPoolClientLike,
  type PgQueryConfig,
  type PostgresJsClient,
  type PostgresJsExecutor,
  type PostgresJsReserved,
  type CancelablePromise,
  type PreparedStatement,
} from "./drivers.js";

// I02: transaction control, cancellation and observability. One shared
// runner drives BEGIN/COMMIT/ROLLBACK/savepoints on pinned connections for
// both bundled drivers; deadlines and AbortSignals cancel at the server
// (pg_cancel_backend side channel / postgres.js native Query.cancel);
// CommitAmbiguityError is never auto-replayed; structured events are
// redacted by default (params only under NEUTRON_SQL_LOG_PARAMS=1).
export {
  renderBeginSql,
  hasModes,
  runTransaction,
  runRetriedTransaction,
  validateRetryOptions,
  type PinnedExecutor,
  type QueryExecutionOptions,
  type TransactionModes,
  type TransactionRetryOptions,
  type TransactionScope,
  type TransactionHooks,
  type Savepoint,
} from "./transactions.js";

// Q04: keyset pagination — unique tie-breakers, explicit null ordering,
// mixed directions, versioned opaque cursors with strict validation.
export {
  keyset,
  CursorError,
  type KeysetPager,
  type KeysetColumn,
  type KeysetOptions,
  type KeysetPage,
  type KeysetQueryable,
} from "./pagination.js";

// I01: stable driver error taxonomy. SQLSTATE survives every wrapper
// (ServerSqlError.sqlstate / getSqlState); connection failures and missing
// driver packages are distinct classes, never conflated. I02 adds the
// cancellation state (QueryCanceledError, SQLSTATE 57014 with the cancel
// reason) and the unknown-commit-outcome state (CommitAmbiguityError,
// never retried automatically).
export {
  NeutronSqlError,
  MissingDriverError,
  ConnectionFailedError,
  ServerSqlError,
  QueryCanceledError,
  CommitAmbiguityError,
  classifyDriverError,
  getSqlState,
  isConnectionError,
  isMissingDriverError,
  isFatalConnectionLoss,
  isRetriableTransactionError,
} from "./errors.js";

// I01: engine identity (FRAMEWORK_CONTRACT.md §1) and the tri-state
// capability contract — supported / unsupported / unknown, and unknown fails
// closed. Plain Postgres gains no model dependency: detection is one
// `select version()` through whichever driver you brought.
export {
  parseVersionString,
  resolveCapabilityStatus,
  capabilityGate,
  CapabilityRequirementError,
  type EngineProduct,
  type EngineIdentity,
  type CapabilityStatus,
  type CapabilityEvidence,
  type CapabilityGate,
} from "./engine.js";

export {
  resolveLogger,
  paramsLoggingEnabled,
  statementIdOf,
  errorSummary,
  type Logger,
  type LoggerOption,
  type LogEvent,
  type SqlEvent,
  type SqlEventKind,
  type IsolationLevel,
} from "./logger.js";

export { type RQBArgs } from "./relations.js";

export {
  exportSchema,
  exportTable,
  exportSchemaV2,
  canonicalSchemaJson,
  readSchemaDocumentV1,
  type ExportedSchema,
  type ExportedTable,
  type ExportedColumn,
  type ExportedIndex,
  type SchemaDocumentV2,
  type V2Identity,
  type V2TypeRef,
  type V2Default,
  type V2Column,
  type V2Constraint,
  type V2Index,
  type V2Table,
} from "./export.js";
