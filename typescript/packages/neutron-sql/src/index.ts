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

export {
  sql,
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
  type Projection,
  type ProjectionResult,
  type AstProjection,
} from "./builder.js";

// F01 architecture spike: frozen SQL AST nodes, the structural sqlAst
// template, the trusted-SQL boundary and the one-traversal compiler.
// astSelect().from(...).toSQL() is the proven builder integration; the
// remaining CRUD paths migrate onto this compiler in F04.
export {
  sqlAst,
  trustSql,
  TRUSTED_SQL_ACK,
  ident,
  qual,
  ref,
  param,
  expr,
  fragment,
  projection,
  subquery,
  cte,
  join,
  selectStatement,
  isValueNode,
  isStatement,
  type IdentifierNode,
  type QualifiedNode,
  type ParamNode,
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
  type ValueNode,
  type SqlNode,
  type TrustedSql,
  type TrustedSqlAcknowledgment,
} from "./ast.js";

export { compile, compileStatement, type CompileState, type CompiledQuery } from "./compile.js";

export { schemaToDDL, createTableSQL, createIndexSQL, dropTableSQL, addForeignKeySQL, sqlTypeOf } from "./ddl.js";

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

export { loadDriver, type Driver, type DriverKind, type LoadDriverOptions } from "./drivers.js";

export { resolveLogger, type Logger, type LoggerOption, type LogEvent } from "./logger.js";

export { type RQBArgs } from "./relations.js";

export { exportSchema, exportTable, type ExportedSchema, type ExportedTable, type ExportedColumn, type ExportedIndex } from "./export.js";
