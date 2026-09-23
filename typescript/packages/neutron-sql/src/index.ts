// ---------------------------------------------------------------------------
// @neutron-build/sql — SQL-only entry point
// ---------------------------------------------------------------------------
// This module pulls in no multi-model (Nucleus) code. Tree-shaking starts here.

export {
  pgTable,
  relations,
  isPgTable,
  isTableRelations,
  getTableName,
  getTableColumns,
  getTableIndexes,
  index,
  uniqueIndex,
  TABLE_SYMBOL,
  RELATIONS_SYMBOL,
  type PgTable,
  type PgTableCore,
  type TableMetadata,
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
  type BigintRead,
  type TemporalRead,
  type SelectTypeOf,
  type RelationLeafTypeOf,
  type RelationSelectTypeOf,
  type InsertTypeOf,
  type UpdateTypeOf,
  type TableIndex,
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
  qident,
  type SqlFragment,
  type Condition,
  type OrderExpression,
} from "./expr.js";

export {
  SelectBuilder,
  InsertBuilder,
  UpdateBuilder,
  DeleteBuilder,
  astSelect,
  AstSelectBuilder,
  whereItems,
  type Projection,
  type ProjectionResult,
  type AstProjection,
  type CompiledStatement,
} from "./builder.js";

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
  join,
  selectStatement,
  insertStatement,
  updateStatement,
  deleteStatement,
  isValueNode,
  isStatement,
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
  type OrderSpec,
  type FromTarget,
  type StatementNode,
  type StatementInput,
  type InsertStatementNode,
  type InsertStatementInput,
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
} from "./ast.js";

export { compile, compileStatement, quoteIdent, type CompileState, type CompiledQuery } from "./compile.js";

export { schemaToDDL, createTableSQL, createIndexSQL, dropTableSQL, addForeignKeySQL, sqlTypeOf } from "./ddl.js";

export {
  jsonNull,
  isJsonNull,
  wireReadNode,
  projectionDecoder,
  applyProjectionDecoders,
  type BigintMode,
  type BigintOptions,
  type TemporalMode,
  type TemporalOptions,
  type NumericOptions,
  type JsonNullValue,
  type ColumnCodec,
  type CodecRead,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";

export {
  createDatabase,
  buildRelationalSQL,
  type DatabaseOptions,
  type TablesInput,
  type RelationsInput,
  type NeutronDatabase,
  type QueryApiFor,
  type RelationalArgs,
  type RelationalRow,
  type RelationValue,
  type RelationChildModelOf,
} from "./db.js";

export {
  loadDriver,
  wrapPgPool,
  wrapPostgresJs,
  makeLifecycle,
  type Driver,
  type DriverKind,
  type LoadDriverOptions,
  type AdapterOwnership,
  type DriverLifecycle,
  type WrapAdapterOptions,
  type PgPoolLike,
  type PgPoolClientLike,
  type PostgresJsClient,
} from "./drivers.js";

// I01: stable driver error taxonomy. SQLSTATE survives every wrapper
// (ServerSqlError.sqlstate / getSqlState); connection failures and missing
// driver packages are distinct classes, never conflated.
export {
  NeutronSqlError,
  MissingDriverError,
  ConnectionFailedError,
  ServerSqlError,
  classifyDriverError,
  getSqlState,
  isConnectionError,
  isMissingDriverError,
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

export { resolveLogger, type Logger, type LoggerOption, type LogEvent } from "./logger.js";

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
