// ---------------------------------------------------------------------------
// @neutron-build/sql — nested writes (Q06)
// ---------------------------------------------------------------------------
// `db.query.<table>.create({ data })` / `.update({ where, data })` accept
// relation operations inside `data` (create / connect / disconnect / update /
// delete) and compile the whole write graph into an explicit, ordered list of
// single-row statements that runs inside ONE transaction (a savepoint when
// already inside one).
//
// Ownership of every relation edge is explicit and derived from the schema,
// never guessed:
//   - one() relation: the DECLARING table owns the edge — its `fields` are the
//     foreign-key columns, pointing at `references` on the target. Writing the
//     edge assigns the owner's FK columns (create/connect/disconnect), so the
//     target row must exist before the owner row is written.
//   - many() relation: the TARGET table owns the edge through the paired one()
//     resolved by resolveRelations. Writing the edge assigns the CHILD rows'
//     FK columns, so the parent row must exist first.
//
// Planning is pure (no driver): every statement's SQL text is final at plan
// time. Values that only exist after an earlier statement ran (generated
// keys) are parameters bound to a named output of that step — the plan shows
// them as `{ kind: "step-output", step, key }`. Execution binds them through
// the consuming column's codec (lossless: keys are captured in their exact
// wire forms, never through a user decoder or Date mode).
//
// Every step affects or returns EXACTLY one row; zero or several is a
// NestedWriteError thrown inside the transaction, which rolls the whole graph
// back. Nothing is retried: the plan has no callbacks with external side
// effects, and a failed or ambiguous transaction surfaces unchanged (an
// explicit whole-transaction retry stays the caller's opt-in on
// db.transaction).
//
// Cross references inside one graph (a connect naming a row created by the
// same call) are recognized only through PREALLOCATED keys: the create must
// supply every column of a unique key explicitly and the connect must name
// the same key with the same values. Such references order the statements; a
// reference cycle is broken only by deferring a NULLABLE owned foreign key of
// an inserted row to a follow-up link update, and is otherwise rejected
// before any SQL runs.

import { MAX_RELATION_DEPTH } from "./relations.js";
import type { ExecContext } from "./builder.js";
import { run, tableTargetNode } from "./builder.js";
import {
  applyProjectionDecoders,
  encodeWriteValue,
  projectionDecoder,
  wireReadNode,
  writeCastTarget,
  type EncodedValue,
  type ProjectionDecoder,
  type StatementCapability,
} from "./codecs.js";
import {
  assertNoExcludedRefs,
  deleteStatement,
  expr as exprNode,
  fragment,
  insertStatement,
  isLegacySqlFragment,
  isValueNode,
  legacyFragmentError,
  param as paramNode,
  paramCast,
  projection as projectionNode,
  qual,
  selectStatement,
  subquery as subqueryNode,
  updateStatement,
  type AnyStatementNode,
  type InsertCell,
  type ProjectionNode,
  type ValueNode,
} from "./ast.js";
import { compileStatement } from "./compile.js";
import { NeutronSqlError } from "./errors.js";
import type { IsolationLevel } from "./logger.js";
import type { QueryExecutionOptions } from "./transactions.js";
import { getTableColumns, getTableIndexes, getTableName, tableRefParts } from "./schema.js";
import type { AnyColumnBuilder, AnyPgTable, Relation, RelationOne } from "./schema.js";

// ---------------------------------------------------------------------------
// Public plan shape
// ---------------------------------------------------------------------------

/** What a step does for the graph. `lookup` resolves the updated row
 *  (existence + current key values); `link` is the deferred foreign-key
 *  assignment that breaks a preallocated-key reference cycle; `scan`
 *  counts a delete cascade's dependents (and enforces declared
 *  ownership over them). */
export type NestedWriteOp = "create" | "connect" | "disconnect" | "update" | "delete" | "lookup" | "link" | "scan";

/** What a step demands of its row count. Row writes and unique-key
 *  lookups affect or return exactly one row. A `scan` returns
 *  zero-or-more rows (its count is later asserted against the
 *  mutation it guards); a cascade mutation's affected-row count must
 *  equal the scan it follows (`affectedMatchesStep` names it). */
export type NestedWriteExpect = "exactly-one-row" | "zero-or-more-rows" | { readonly affectedMatchesStep: number };

export type NestedWriteAction = "insert" | "update" | "delete" | "select";

/** A parameter whose value is an output of an earlier step (a generated or
 *  current key). `step` is that step's index in `steps`. */
export interface StepOutputRef {
  readonly kind: "step-output";
  readonly step: number;
  readonly key: string;
}

/** The relation edge a step writes, with explicit ownership. */
export interface NestedEdgePlan {
  /** Relation key on the table that declares it. */
  readonly relation: string;
  /** Table declaring the relation (the side the write enters from). */
  readonly source: string;
  readonly target: string;
  readonly cardinality: "one" | "many";
  /** The table whose rows carry the foreign-key columns. */
  readonly owner: string;
  /** Physical FK columns on `owner`, in declared (positional) order. */
  readonly foreignKey: readonly string[];
  /** Physical referenced columns on the other side, zipped positionally
   *  with `foreignKey`. */
  readonly references: readonly string[];
}

export interface NestedWriteStep {
  readonly index: number;
  /** Position in the input graph: `users`, `users.posts[0]`,
   *  `users.posts.connect[1]`, `posts.author`, … */
  readonly path: string;
  readonly op: NestedWriteOp;
  readonly action: NestedWriteAction;
  readonly table: string;
  /** Final SQL text — identical to what executes. */
  readonly sql: string;
  /** Bind values in placeholder order: literal values, or StepOutputRef
   *  entries resolved from earlier steps at execution. */
  readonly params: readonly unknown[];
  /** Indices of the steps this one must run after. */
  readonly dependsOn: readonly number[];
  /** The row-count contract (see NestedWriteExpect). */
  readonly expect: NestedWriteExpect;
  /** Property keys this step captures for later steps. */
  readonly outputs: readonly string[];
  /** True for the step whose row is the call's result. */
  readonly returnsResult: boolean;
  readonly edge?: NestedEdgePlan;
  readonly capabilities: readonly StatementCapability[];
}

/** Dry compilation of a nested write: every statement that will run, in
 *  order, with its parameters, dependencies and edge ownership. Pure — no
 *  driver, no connection. */
export interface NestedWritePlan {
  readonly table: string;
  readonly operation: "create" | "update" | "delete";
  /** All steps run in one transaction (a savepoint inside an existing one). */
  readonly atomic: true;
  readonly statementCount: number;
  readonly steps: readonly NestedWriteStep[];
  readonly capabilities: readonly StatementCapability[];
  /** Reference cycles broken by deferring a nullable owned foreign key. */
  readonly deferredLinks: number;
}

/** Execution options for a nested write. Deadlines/signals apply to EACH
 *  statement of the plan. `isolation` sets the transaction's isolation level
 *  (top level only — inside db.transaction the outer BEGIN owns it). */
export interface NestedWriteOptions extends QueryExecutionOptions {
  isolation?: IsolationLevel;
}

/** Loose (untyped) argument shapes — the typed surface lives in db.ts. */
export interface NestedCreateInput {
  data: Record<string, unknown>;
}
export interface NestedUpdateInput {
  where: Record<string, unknown>;
  data: Record<string, unknown>;
}
export interface NestedDeleteInput {
  where: Record<string, unknown>;
  /** Dependent-row dispositions keyed by relation name: `"disconnect"`
   *  keeps the rows and nulls their foreign key (nullable FKs only),
   *  `"delete"` removes them, `{ delete: {...} }` removes them with
   *  dispositions for THEIR dependents. A dependent edge with existing
   *  rows and no declared disposition aborts the whole write. */
  cascade?: Record<string, unknown>;
}

/** A plan step matched zero or several rows where exactly one was required,
 *  a delete cascade found undeclared dependents, or a cascade mutation's
 *  affected-row count diverged from its scan. Thrown inside the
 *  transaction: the whole graph is rolled back. */
export class NestedWriteError extends NeutronSqlError {
  readonly step: number;
  readonly path: string;
  readonly op: NestedWriteOp;
  readonly table: string;
  /** not-found: 0 rows; cardinality: more than 1 row; no-connected-row: the
   *  owner's foreign key is NULL, so there is no related row to act on;
   *  undeclared-dependents: rows reference a deleted row through an edge
   *  with no disposition; row-set-changed: a cascade mutation affected a
   *  different number of rows than its scan counted. */
  readonly reason: "not-found" | "cardinality" | "no-connected-row" | "undeclared-dependents" | "row-set-changed";
  readonly rowCount: number;
  constructor(
    message: string,
    fields: { step: number; path: string; op: NestedWriteOp; table: string; reason: NestedWriteError["reason"]; rowCount: number },
  ) {
    super(message);
    this.step = fields.step;
    this.path = fields.path;
    this.op = fields.op;
    this.table = fields.table;
    this.reason = fields.reason;
    this.rowCount = fields.rowCount;
  }
}

// ---------------------------------------------------------------------------
// Internal graph model
// ---------------------------------------------------------------------------

type ValueSpec =
  /** A bound literal (already codec-encoded) or a user expression node. */
  | { readonly kind: "node"; readonly node: ValueNode; readonly encoded?: EncodedValue }
  /** An output of another step, bound through `column`'s codec. */
  | { readonly kind: "ref"; readonly draft: Draft; readonly key: string }
  /** Position `index` of a to-one connect whose target is resolved at
   *  finalize (same-graph create or a lookup statement). */
  | { readonly kind: "intent"; readonly intent: ConnectIntent; readonly index: number };

interface Assignment {
  readonly propertyKey: string;
  readonly column: AnyColumnBuilder;
  value: ValueSpec;
}

/** Owned to-one FK columns an insert assigns for one edge (cycle breaking
 *  can defer them). */
interface FkGroup {
  readonly relation: string;
  readonly edge: NestedEdgePlan;
  readonly assignments: Assignment[];
  readonly nullable: boolean;
}

interface Draft {
  seq: number;
  readonly path: string;
  readonly op: NestedWriteOp;
  readonly action: NestedWriteAction;
  readonly table: AnyPgTable;
  readonly sets: Assignment[];
  readonly where: Assignment[];
  /** Delete-cascade predicates (correlated EXISTS chains) that cannot be
   *  expressed as column-equality assignments. Built lazily at compile
   *  time so step-output placeholders bind to final step indices. */
  rawWhere?: (late: LateFn, selfParts: string[]) => ValueNode[];
  readonly outputs: Set<string>;
  result: boolean;
  /** Scan steps tolerate zero-or-more rows (count asserted downstream). */
  zeroOrMore?: boolean;
  /** A scan whose rows are dependents with no declared disposition: any
   *  row aborts the write (explicit ownership of every dependent edge). */
  scanGuard?: { readonly edge: string };
  /** Cascade mutations: affected rows must equal this scan step's count. */
  affectedEquals?: Draft;
  readonly orderDeps: Set<Draft>;
  readonly edge?: NestedEdgePlan;
  readonly fkGroups: FkGroup[];
  /** Link drafts: the inserted row whose deferred FK they assign. */
  linkOf?: Draft;
}

interface ConnectIntent {
  readonly seq: number;
  readonly path: string;
  readonly target: AnyPgTable;
  readonly selector: Assignment[];
  readonly signature: string;
  /** Referenced property keys on the target, positional with the owner's FK. */
  readonly referenceKeys: readonly string[];
  readonly edge: NestedEdgePlan;
  /** to-many connects: the update draft that must follow a same-graph create. */
  readonly orderedDraft?: Draft;
  /** to-one connects: the assignments to rewrite once resolved. */
  readonly assignments: Assignment[];
}

interface PlanCtx {
  readonly relationsByTable: Map<string, Record<string, Relation>>;
  readonly drafts: Draft[];
  readonly intents: ConnectIntent[];
  /** Preallocated-key signatures of created rows. */
  readonly createdKeys: Map<string, Draft>;
  nextSeq: number;
}

function planError(path: string, reason: string): NeutronSqlError {
  return new NeutronSqlError(`nested write at ${path}: ${reason}`);
}

function newDraft(
  pc: PlanCtx,
  init: {
    path: string;
    op: NestedWriteOp;
    action: NestedWriteAction;
    table: AnyPgTable;
    sets?: Assignment[];
    where?: Assignment[];
    edge?: NestedEdgePlan;
    seq?: number;
  },
): Draft {
  const draft: Draft = {
    seq: init.seq ?? pc.nextSeq++,
    path: init.path,
    op: init.op,
    action: init.action,
    table: init.table,
    sets: init.sets ?? [],
    where: init.where ?? [],
    outputs: new Set<string>(),
    result: false,
    orderDeps: new Set<Draft>(),
    edge: init.edge,
    fkGroups: [],
  };
  pc.drafts.push(draft);
  return draft;
}

/** Register that `draft` must expose `key` and return a reference to it. */
function outputRef(draft: Draft, key: string): ValueSpec {
  draft.outputs.add(key);
  return { kind: "ref", draft, key };
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

function columnsOf(table: AnyPgTable): Record<string, AnyColumnBuilder> {
  return getTableColumns(table) as Record<string, AnyColumnBuilder>;
}

function tableId(table: AnyPgTable): string {
  return tableRefParts(table).join(".");
}

/** Property key of a column object on `table` (identity first, then the
 *  physical name — relation configs hold the table's own column objects). */
function propertyKeyOf(table: AnyPgTable, column: AnyColumnBuilder, path: string): string {
  const entries = Object.entries(columnsOf(table));
  const byIdentity = entries.find(([, c]) => c === column);
  if (byIdentity) return byIdentity[0];
  const byName = entries.filter(([, c]) => c.columnName === column.columnName);
  if (byName.length === 1) return byName[0][0];
  throw planError(path, `relation column "${column.columnName}" is not a column of ${getTableName(table)}`);
}

function isNullableColumn(column: AnyColumnBuilder): boolean {
  return !column.isNotNull && !column.isPrimaryKey;
}

function pkKeysOf(table: AnyPgTable): string[] {
  return Object.entries(columnsOf(table)).filter(([, c]) => c.isPrimaryKey).map(([k]) => k);
}

/** Unique keys of a table as property-key sets: the (possibly composite)
 *  primary key, every single-column unique, and every unique index whose
 *  columns all map to declared properties. */
export function uniqueKeysOf(table: AnyPgTable): Array<{ readonly name: string; readonly keys: readonly string[] }> {
  const cols = columnsOf(table);
  const out: Array<{ name: string; keys: string[] }> = [];
  const pk = pkKeysOf(table);
  if (pk.length > 0) out.push({ name: "primary key", keys: pk });
  for (const [key, column] of Object.entries(cols)) {
    if (column.isUnique) out.push({ name: `unique ${key}`, keys: [key] });
  }
  const byPhysical = new Map(Object.entries(cols).map(([k, c]) => [c.columnName, k] as const));
  for (const idx of getTableIndexes(table)) {
    if (!idx.unique || idx.columns.length === 0) continue;
    const keys = idx.columns.map((physical) => byPhysical.get(physical));
    if (keys.some((k) => k === undefined)) continue;
    out.push({ name: `unique index ${idx.indexName}`, keys: keys as string[] });
  }
  return out;
}

function sameKeySet(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((k) => set.has(k));
}

const UUID_TEXT = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

/** Canonical text of an encoded key value for same-graph matching: numbers,
 *  bigints and integer strings compare by decimal text, UUIDs
 *  case-insensitively, bytes by hex. */
function normalizedBind(encoded: EncodedValue): string {
  const v = encoded.bind;
  if (v instanceof Uint8Array) return `x:${Buffer.from(v).toString("hex")}`;
  if (typeof v === "string") {
    if (/^[+-]?\d+$/.test(v)) return `n:${BigInt(v).toString()}`;
    return `s:${encoded.cast ?? ""}:${UUID_TEXT.test(v) ? v.toLowerCase() : v}`;
  }
  if (typeof v === "number" && Number.isInteger(v)) return `n:${BigInt(v).toString()}`;
  if (typeof v === "bigint") return `n:${v.toString()}`;
  return `v:${String(v)}`;
}

function keySignature(table: AnyPgTable, entries: ReadonlyArray<{ propertyKey: string; encoded: EncodedValue }>): string {
  const sorted = [...entries].sort((a, b) => (a.propertyKey < b.propertyKey ? -1 : a.propertyKey > b.propertyKey ? 1 : 0));
  return `${tableId(table)}|${sorted.map((e) => `${e.propertyKey}=${normalizedBind(e.encoded)}`).join("|")}`;
}

function literalAssignment(tableName: string, propertyKey: string, column: AnyColumnBuilder, value: unknown, path: string): Assignment {
  if (isLegacySqlFragment(value)) throw legacyFragmentError("nested write value");
  const encoded = encodeValue(tableName, propertyKey, column, value, path);
  return { propertyKey, column, value: { kind: "node", node: bindNode(encoded), encoded } };
}

function encodeValue(tableName: string, propertyKey: string, column: AnyColumnBuilder, value: unknown, path: string): EncodedValue {
  try {
    return encodeWriteValue(column, { propertyKey, columnName: column.columnName, tableName }, value);
  } catch (err) {
    throw planError(path, err instanceof Error ? err.message : String(err));
  }
}

function bindNode(encoded: EncodedValue): ValueNode {
  return encoded.cast === undefined ? paramNode(encoded.bind) : paramCast(encoded.bind, encoded.cast);
}

function nullAssignment(propertyKey: string, column: AnyColumnBuilder): Assignment {
  return { propertyKey, column, value: { kind: "node", node: paramNode(null) } };
}

/** Validate a unique-key selector: its keys must be exactly one declared
 *  unique key of the table, every value non-null and codec-valid. */
function uniqueSelector(table: AnyPgTable, selector: unknown, path: string, what: string): Assignment[] {
  const tableName = getTableName(table);
  if (typeof selector !== "object" || selector === null || Array.isArray(selector)) {
    throw planError(path, `${what} expects a unique-key object ({ key: value, … }) for ${tableName}`);
  }
  const cols = columnsOf(table);
  const given = Object.keys(selector).filter((k) => (selector as Record<string, unknown>)[k] !== undefined);
  for (const key of given) {
    if (!Object.hasOwn(cols, key)) throw planError(path, `${what}: unknown column "${key}" on ${tableName}`);
  }
  const keys = uniqueKeysOf(table);
  const match = keys.find((k) => sameKeySet(k.keys, given));
  if (!match) {
    const known = keys.length === 0 ? "none declared" : keys.map((k) => `{${k.keys.join(", ")}}`).join(", ");
    throw planError(
      path,
      `${what} on ${tableName} must name exactly one unique key (got {${given.join(", ")}}; unique keys: ${known}) — non-unique selectors could match several rows`,
    );
  }
  return match.keys.map((key) => {
    const value = (selector as Record<string, unknown>)[key];
    if (value === null) throw planError(path, `${what}: "${key}" is null — NULL never identifies a row`);
    return literalAssignment(tableName, key, cols[key], value, path);
  });
}

function selectorSignature(table: AnyPgTable, selector: Assignment[]): string {
  return keySignature(
    table,
    selector.map((a) => {
      if (a.value.kind !== "node" || a.value.encoded === undefined) throw new Error("internal: selector values are encoded literals");
      return { propertyKey: a.propertyKey, encoded: a.value.encoded };
    }),
  );
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

interface ResolvedEdge {
  readonly plan: NestedEdgePlan;
  /** FK property keys on the owner (declaring table for one(), child for many()). */
  readonly fkKeys: readonly string[];
  /** Referenced property keys on the other side, positional with fkKeys. */
  readonly refKeys: readonly string[];
  readonly fkNullable: boolean;
}

function resolveEdge(table: AnyPgTable, key: string, rel: Relation, path: string): ResolvedEdge {
  const target = rel.targetTable;
  if (rel.kind === "one") {
    return edgeFromOne(rel, table, target, key, getTableName(table), path);
  }
  const source: RelationOne | undefined = rel.source;
  if (!source) {
    throw planError(path, `many() relation "${key}" on ${getTableName(table)} is unresolved — register both tables' relations with createDatabase`);
  }
  // The paired one() lives on the target: its fields are the child's FK
  // columns and its references are this (parent) table's columns.
  const inner = edgeFromOne(source, target, table, key, getTableName(table), path);
  return {
    plan: { ...inner.plan, cardinality: "many", source: getTableName(table), target: getTableName(target) },
    fkKeys: inner.fkKeys,
    refKeys: inner.refKeys,
    fkNullable: inner.fkNullable,
  };
}

function edgeFromOne(rel: RelationOne, owner: AnyPgTable, referenced: AnyPgTable, key: string, sourceName: string, path: string): ResolvedEdge {
  if (rel.fields.length === 0 || rel.fields.length !== rel.references.length) {
    throw planError(path, `relation "${key}" declares ${rel.fields.length} field(s) and ${rel.references.length} reference(s) — they must pair positionally`);
  }
  const fkKeys = rel.fields.map((c) => propertyKeyOf(owner, c, path));
  const refKeys = rel.references.map((c) => propertyKeyOf(referenced, c, path));
  const ownerCols = columnsOf(owner);
  return {
    plan: {
      relation: key,
      source: sourceName,
      target: getTableName(referenced),
      cardinality: "one",
      owner: getTableName(owner),
      foreignKey: rel.fields.map((c) => c.columnName),
      references: rel.references.map((c) => c.columnName),
    },
    fkKeys,
    refKeys,
    fkNullable: fkKeys.every((k) => isNullableColumn(ownerCols[k])),
  };
}

// ---------------------------------------------------------------------------
// Input splitting and validation
// ---------------------------------------------------------------------------

type Mode = "create" | "update";

const CREATE_OPS = new Set(["create", "connect"]);
const UPDATE_OPS = new Set(["create", "connect", "disconnect", "update", "delete"]);

interface SplitData {
  readonly scalars: Array<[string, unknown]>;
  /** Relation ops in RELATION DECLARATION order (input key order never
   *  changes the plan). */
  readonly relations: Array<[string, Relation, Record<string, unknown>]>;
}

function splitData(pc: PlanCtx, table: AnyPgTable, data: unknown, mode: Mode, path: string): SplitData {
  const tableName = getTableName(table);
  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    throw planError(path, `data must be an object of column values and relation operations for ${tableName}`);
  }
  const cols = columnsOf(table);
  const entries = pc.relationsByTable.get(tableName) ?? {};
  const input = data as Record<string, unknown>;
  for (const key of Object.keys(input)) {
    const isColumn = Object.hasOwn(cols, key);
    const isRelation = Object.hasOwn(entries, key);
    if (isColumn && isRelation) {
      throw planError(path, `"${key}" is both a column and a relation of ${tableName} — nested writes cannot disambiguate it`);
    }
    if (!isColumn && !isRelation) {
      const relKeys = Object.keys(entries);
      throw planError(
        path,
        `unknown key "${key}" on ${tableName} (columns: ${Object.keys(cols).join(", ")}; relations: ${relKeys.length > 0 ? relKeys.join(", ") : "none"})`,
      );
    }
  }
  const scalars: Array<[string, unknown]> = [];
  for (const key of Object.keys(cols)) {
    if (Object.hasOwn(input, key) && input[key] !== undefined) scalars.push([key, input[key]]);
  }
  const relations: Array<[string, Relation, Record<string, unknown>]> = [];
  const allowed = mode === "create" ? CREATE_OPS : UPDATE_OPS;
  for (const [key, rel] of Object.entries(entries)) {
    if (!Object.hasOwn(input, key) || input[key] === undefined) continue;
    const ops = input[key];
    const relPath = `${path}.${key}`;
    if (typeof ops !== "object" || ops === null || Array.isArray(ops)) {
      throw planError(relPath, `relation "${key}" expects an operations object ({ ${[...allowed].join(" | ")} })`);
    }
    const present = Object.keys(ops).filter((op) => (ops as Record<string, unknown>)[op] !== undefined);
    for (const op of present) {
      if (!allowed.has(op)) {
        const hint = mode === "create" && UPDATE_OPS.has(op) ? ` — "${op}" needs an existing row (use update)` : "";
        throw planError(relPath, `unknown or disallowed relation operation "${op}" (allowed here: ${[...allowed].join(", ")})${hint}`);
      }
    }
    if (present.length === 0) throw planError(relPath, `relation "${key}" has no operation`);
    if (rel.kind === "one" && present.length > 1) {
      throw planError(relPath, `to-one relation "${key}" takes exactly one operation (got ${present.join(", ")})`);
    }
    relations.push([key, rel, ops as Record<string, unknown>]);
  }
  return { scalars, relations };
}

function asList(value: unknown, path: string, what: string): unknown[] {
  const list = Array.isArray(value) ? value : [value];
  if (list.length === 0) throw planError(path, `${what} received an empty array`);
  return list;
}

function checkDepth(depth: number, path: string): void {
  if (depth > MAX_RELATION_DEPTH) {
    throw planError(path, `nesting depth ${depth} exceeds the maximum of ${MAX_RELATION_DEPTH} — split the write`);
  }
}

/** Physical-column bookkeeping for one row write: every column is assigned
 *  by exactly one source (scalar data, the parent edge, or a relation op). */
class AssignmentSet {
  private readonly byPhysical = new Map<string, string>();
  readonly list: Assignment[] = [];
  constructor(
    private readonly tableName: string,
    private readonly path: string,
  ) {}
  add(a: Assignment, origin: string): void {
    const prior = this.byPhysical.get(a.column.columnName);
    if (prior !== undefined) {
      throw planError(
        this.path,
        `column "${a.propertyKey}" ("${a.column.columnName}") on ${this.tableName} is assigned by both ${prior} and ${origin} — assign each column once`,
      );
    }
    this.byPhysical.set(a.column.columnName, origin);
    this.list.push(a);
  }
  /** Schema-ordered assignments (deterministic SQL). */
  ordered(table: AnyPgTable): Assignment[] {
    const order = Object.keys(columnsOf(table));
    return [...this.list].sort((a, b) => order.indexOf(a.propertyKey) - order.indexOf(b.propertyKey));
  }
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

interface ParentLink {
  readonly assignments: Assignment[];
  readonly origin: string;
  readonly edge: NestedEdgePlan;
}

function planCreate(
  pc: PlanCtx,
  table: AnyPgTable,
  data: unknown,
  path: string,
  depth: number,
  parent?: ParentLink,
  edgePlan?: NestedEdgePlan,
): Draft {
  checkDepth(depth, path);
  const tableName = getTableName(table);
  const cols = columnsOf(table);
  const split = splitData(pc, table, data, "create", path);
  const set = new AssignmentSet(tableName, path);

  for (const [key, value] of split.scalars) {
    const column = cols[key];
    if (value === null) {
      if (!isNullableColumn(column)) {
        throw planError(path, `null is not allowed for NOT NULL column "${key}" ("${column.columnName}") on ${tableName}`);
      }
      set.add(nullAssignment(key, column), `data.${key}`);
      continue;
    }
    set.add(literalAssignment(tableName, key, column, value, path), `data.${key}`);
  }
  if (parent) for (const a of parent.assignments) set.add(a, parent.origin);

  const fkGroups: FkGroup[] = [];
  const toMany: Array<[string, ResolvedEdge, Relation, Record<string, unknown>]> = [];
  for (const [key, rel, ops] of split.relations) {
    const relPath = `${path}.${key}`;
    const edge = resolveEdge(table, key, rel, relPath);
    if (rel.kind === "many") {
      toMany.push([key, edge, rel, ops]);
      continue;
    }
    const assignments = ownedToOneAssignments(pc, table, rel, edge, ops, relPath, depth);
    for (const a of assignments) set.add(a, `relation ${key}`);
    fkGroups.push({ relation: key, edge: edge.plan, assignments, nullable: edge.fkNullable });
  }

  // Required-key check after every source contributed: NOT NULL without a
  // default (serial always defaults).
  const assigned = new Set(set.list.map((a) => a.propertyKey));
  const missing = Object.entries(cols).filter(
    ([key, c]) => !assigned.has(key) && (c.isNotNull || c.isPrimaryKey) && !c.hasDefault && c.dataType !== "serial",
  );
  if (missing.length > 0) {
    throw planError(
      path,
      `create on ${tableName} is missing required column(s) ${missing.map(([k, c]) => `"${k}" ("${c.columnName}")`).join(", ")} — NOT NULL without a default (a relation operation may supply foreign-key columns)`,
    );
  }

  const draft = newDraft(pc, { path, op: "create", action: "insert", table, sets: set.ordered(table), edge: parent?.edge ?? edgePlan });
  draft.fkGroups.push(...fkGroups);
  registerCreatedKeys(pc, draft, path);

  for (const [key, edge, rel, ops] of toMany) {
    planToManyOps(pc, draft, table, key, rel, edge, ops, `${path}.${key}`, depth, "create");
  }
  return draft;
}

/** Record every unique key whose columns this create supplies as explicit
 *  literals: connects naming the same key/values resolve to this row. */
function registerCreatedKeys(pc: PlanCtx, draft: Draft, path: string): void {
  const literals = new Map<string, EncodedValue>();
  for (const a of draft.sets) {
    if (a.value.kind === "node" && a.value.encoded !== undefined) literals.set(a.propertyKey, a.value.encoded);
  }
  for (const uk of uniqueKeysOf(draft.table)) {
    if (!uk.keys.every((k) => literals.has(k))) continue;
    const signature = keySignature(draft.table, uk.keys.map((k) => ({ propertyKey: k, encoded: literals.get(k)! })));
    const prior = pc.createdKeys.get(signature);
    if (prior !== undefined && prior !== draft) {
      throw planError(path, `two creates in one write supply the same ${uk.name} on ${getTableName(draft.table)} (${prior.path} and ${path}) — the second would violate it`);
    }
    pc.createdKeys.set(signature, draft);
  }
}

/** Owned to-one edge ops that assign the owner's FK columns: create (target
 *  first), connect (target resolved at finalize), disconnect (NULL). */
function ownedToOneAssignments(
  pc: PlanCtx,
  owner: AnyPgTable,
  rel: RelationOne,
  edge: ResolvedEdge,
  ops: Record<string, unknown>,
  relPath: string,
  depth: number,
): Assignment[] {
  const ownerCols = (fkKey: string): AnyColumnBuilder => columnsOf(owner)[fkKey];
  if (ops.create !== undefined) {
    if (Array.isArray(ops.create)) throw planError(relPath, `to-one relation "${edge.plan.relation}" creates exactly one row — pass an object, not an array`);
    // The target is inserted first; the owner's FK reads its referenced columns.
    const target = planCreate(pc, rel.targetTable, ops.create, relPath, depth + 1, undefined, edge.plan);
    return edge.fkKeys.map((fk, i) => ({ propertyKey: fk, column: ownerCols(fk), value: outputRef(target, edge.refKeys[i]) }));
  }
  if (ops.connect !== undefined) {
    const selector = uniqueSelector(rel.targetTable, ops.connect, relPath, "connect");
    const assignments: Assignment[] = [];
    const intent: ConnectIntent = {
      seq: pc.nextSeq++,
      path: `${relPath}.connect`,
      target: rel.targetTable,
      selector,
      signature: selectorSignature(rel.targetTable, selector),
      referenceKeys: edge.refKeys,
      edge: edge.plan,
      assignments,
    };
    edge.fkKeys.forEach((fk, i) => assignments.push({ propertyKey: fk, column: ownerCols(fk), value: { kind: "intent", intent, index: i } }));
    pc.intents.push(intent);
    return assignments;
  }
  if (ops.disconnect !== undefined) {
    if (ops.disconnect !== true) throw planError(relPath, "disconnect on a to-one relation takes `true`");
    requireNullableFk(edge, relPath, "disconnect");
    return edge.fkKeys.map((fk) => nullAssignment(fk, ownerCols(fk)));
  }
  throw new Error("internal: ownedToOneAssignments called without an assigning op");
}

function requireNullableFk(edge: ResolvedEdge, path: string, op: string): void {
  if (!edge.fkNullable) {
    throw planError(
      path,
      `${op} on relation "${edge.plan.relation}" would set NOT NULL foreign-key column(s) ${edge.plan.foreignKey.map((c) => `"${c}"`).join(", ")} on ${edge.plan.owner} to NULL — the relation is required; ${op === "disconnect" ? "connect another row or delete the owning row instead" : "delete the owning row instead"}`,
    );
  }
}

// ---------------------------------------------------------------------------
// To-many edge operations (the child rows own the FK)
// ---------------------------------------------------------------------------

function parentScope(parentDraft: Draft, childTable: AnyPgTable, edge: ResolvedEdge): Assignment[] {
  const childCols = columnsOf(childTable);
  return edge.fkKeys.map((fk, i) => ({ propertyKey: fk, column: childCols[fk], value: outputRef(parentDraft, edge.refKeys[i]) }));
}

function planToManyOps(
  pc: PlanCtx,
  parentDraft: Draft,
  parentTable: AnyPgTable,
  key: string,
  rel: Relation,
  edge: ResolvedEdge,
  ops: Record<string, unknown>,
  relPath: string,
  depth: number,
  mode: Mode,
): void {
  const child = rel.targetTable;
  const childName = getTableName(child);
  const seenSelectors = new Map<string, string>();
  const noteSelector = (selector: Assignment[], where: string): void => {
    const sig = selectorSignature(child, selector);
    const prior = seenSelectors.get(sig);
    if (prior !== undefined) {
      throw planError(relPath, `the same ${childName} row is targeted by both ${prior} and ${where} — one operation per row`);
    }
    seenSelectors.set(sig, where);
  };

  // Fixed execution order per edge, independent of input key order:
  // create, connect, update, disconnect, delete.
  if (ops.create !== undefined) {
    asList(ops.create, relPath, "create").forEach((item, i) => {
      planCreate(pc, child, item, `${relPath}[${i}]`, depth + 1, {
        assignments: parentScope(parentDraft, child, edge),
        origin: `the parent edge ${getTableName(parentTable)}.${key}`,
        edge: edge.plan,
      });
    });
  }
  if (ops.connect !== undefined) {
    asList(ops.connect, relPath, "connect").forEach((item, i) => {
      const path = `${relPath}.connect[${i}]`;
      const selector = uniqueSelector(child, item, path, "connect");
      noteSelector(selector, `connect[${i}]`);
      const draft = newDraft(pc, { path, op: "connect", action: "update", table: child, sets: parentScope(parentDraft, child, edge), where: selector, edge: edge.plan });
      pc.intents.push({
        seq: draft.seq,
        path,
        target: child,
        selector,
        signature: selectorSignature(child, selector),
        referenceKeys: [],
        edge: edge.plan,
        orderedDraft: draft,
        assignments: [],
      });
    });
  }
  if (mode === "create") return;
  if (ops.update !== undefined) {
    asList(ops.update, relPath, "update").forEach((item, i) => {
      const path = `${relPath}.update[${i}]`;
      if (typeof item !== "object" || item === null || Array.isArray(item) || !("where" in item) || !("data" in item)) {
        throw planError(path, "to-many update entries take { where: <unique key>, data }");
      }
      const extra = Object.keys(item).filter((k) => k !== "where" && k !== "data");
      if (extra.length > 0) throw planError(path, `unknown field(s) ${extra.join(", ")} in a to-many update entry (expected where, data)`);
      const selector = uniqueSelector(child, (item as { where: unknown }).where, path, "update where");
      noteSelector(selector, `update[${i}]`);
      planUpdateRow(pc, child, [...selector, ...scopeWhere(parentDraft, child, edge)], (item as { data: unknown }).data, path, depth + 1, "update", edge.plan);
    });
  }
  if (ops.disconnect !== undefined) {
    requireNullableFk(edge, relPath, "disconnect");
    asList(ops.disconnect, relPath, "disconnect").forEach((item, i) => {
      const path = `${relPath}.disconnect[${i}]`;
      const selector = uniqueSelector(child, item, path, "disconnect");
      noteSelector(selector, `disconnect[${i}]`);
      const childCols = columnsOf(child);
      newDraft(pc, {
        path,
        op: "disconnect",
        action: "update",
        table: child,
        sets: edge.fkKeys.map((fk) => nullAssignment(fk, childCols[fk])),
        where: [...selector, ...scopeWhere(parentDraft, child, edge)],
        edge: edge.plan,
      });
    });
  }
  if (ops.delete !== undefined) {
    asList(ops.delete, relPath, "delete").forEach((item, i) => {
      const path = `${relPath}.delete[${i}]`;
      const selector = uniqueSelector(child, item, path, "delete");
      noteSelector(selector, `delete[${i}]`);
      newDraft(pc, { path, op: "delete", action: "delete", table: child, where: [...selector, ...scopeWhere(parentDraft, child, edge)], edge: edge.plan });
    });
  }
}

/** Child-side predicate `fk = parent.refs`: to-many update/disconnect/delete
 *  only touch rows currently connected to THIS parent. Emitted in the
 *  child's schema column order (deterministic SQL regardless of the
 *  relation's declared field order). */
function scopeWhere(parentDraft: Draft, child: AnyPgTable, edge: ResolvedEdge): Assignment[] {
  return orderAssignments(child, parentScope(parentDraft, child, edge));
}

/** Schema-ordered assignments (deterministic SQL). */
function orderAssignments(table: AnyPgTable, list: readonly Assignment[]): Assignment[] {
  const order = new Map(Object.keys(columnsOf(table)).map((k, i) => [k, i] as const));
  return [...list].sort((a, b) => (order.get(a.propertyKey) ?? 0) - (order.get(b.propertyKey) ?? 0));
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

/** Plan an update of exactly one existing row identified by `where` (a
 *  unique key, optionally scoped). Returns the draft that exposes the row's
 *  (post-update) values to dependents. */
function planUpdateRow(
  pc: PlanCtx,
  table: AnyPgTable,
  where: Assignment[],
  data: unknown,
  path: string,
  depth: number,
  op: "update" | "root",
  edge?: NestedEdgePlan,
): Draft {
  checkDepth(depth, path);
  const tableName = getTableName(table);
  const cols = columnsOf(table);
  const split = splitData(pc, table, data, "update", path);
  if (split.scalars.length === 0 && split.relations.length === 0) {
    throw planError(path, `update data for ${tableName} is empty — nothing to write`);
  }
  const set = new AssignmentSet(tableName, path);
  for (const [key, value] of split.scalars) {
    const column = cols[key];
    if (value === null) {
      if (!isNullableColumn(column)) {
        throw planError(path, `null is not allowed for NOT NULL column "${key}" ("${column.columnName}") on ${tableName}`);
      }
      set.add(nullAssignment(key, column), `data.${key}`);
      continue;
    }
    const isJson = column.dataType === "json" || column.dataType === "jsonb";
    if (!isJson && isValueNode(value)) {
      assertNoExcludedRefs(value, `nested update on ${tableName} column "${key}"`);
      set.add({ propertyKey: key, column, value: { kind: "node", node: value } }, `data.${key}`);
      continue;
    }
    set.add(literalAssignment(tableName, key, column, value, path), `data.${key}`);
  }

  interface PostOp {
    readonly key: string;
    readonly rel: Relation;
    readonly edge: ResolvedEdge;
    readonly ops: Record<string, unknown>;
  }
  const post: PostOp[] = [];
  const fkGroups: FkGroup[] = [];
  let needOldValues = false;
  for (const [key, rel, ops] of split.relations) {
    const relPath = `${path}.${key}`;
    const edge = resolveEdge(table, key, rel, relPath);
    if (rel.kind === "many") {
      // The parent's referenced columns scope the children; changing them in
      // the same call would make "the parent" ambiguous (old vs new key).
      for (const refKey of edge.refKeys) {
        if (split.scalars.some(([k]) => k === refKey)) {
          throw planError(relPath, `data assigns "${refKey}", which relation "${key}" references — change the key and the relation in separate writes`);
        }
      }
      post.push({ key, rel, edge, ops });
      continue;
    }
    if (ops.create !== undefined || ops.connect !== undefined || ops.disconnect !== undefined) {
      const assignments = ownedToOneAssignments(pc, table, rel, edge, ops, relPath, depth);
      for (const a of assignments) set.add(a, `relation ${key}`);
      fkGroups.push({ relation: key, edge: edge.plan, assignments, nullable: edge.fkNullable });
      continue;
    }
    if (ops.delete !== undefined) {
      if (ops.delete !== true) throw planError(relPath, "delete on a to-one relation takes `true`");
      requireNullableFk(edge, relPath, "delete");
      for (const fk of edge.fkKeys) set.add(nullAssignment(fk, cols[fk]), `relation ${key}`);
      needOldValues = true;
    } else if (ops.update !== undefined) {
      // The FK is read from this row; the op is alone on its edge, so the
      // column is unchanged by this call — unless data assigns it.
      for (const fk of edge.fkKeys) {
        if (split.scalars.some(([k]) => k === fk)) {
          throw planError(relPath, `data assigns "${fk}", the foreign key of relation "${key}" — update the related row and the key in separate writes`);
        }
      }
    }
    post.push({ key, rel, edge, ops });
  }

  const sets = set.ordered(table);
  const lookup =
    sets.length === 0 || needOldValues
      ? newDraft(pc, { path, op: "lookup", action: "select", table, where, edge: op === "update" ? edge : undefined })
      : undefined;
  const update =
    sets.length > 0
      ? newDraft(pc, { path, op: "update", action: "update", table, sets, where, edge: op === "update" ? edge : undefined })
      : undefined;
  if (update && lookup) update.orderDeps.add(lookup);
  if (update) update.fkGroups.push(...fkGroups);
  const provider = (update ?? lookup)!;
  if (op === "root") provider.result = true;

  for (const { key, rel, edge, ops } of post) {
    const relPath = `${path}.${key}`;
    if (rel.kind === "many") {
      planToManyOps(pc, provider, table, key, rel, edge, ops, relPath, depth, "update");
      continue;
    }
    const targetCols = columnsOf(rel.targetTable);
    if (ops.delete !== undefined) {
      // Old FK values come from the lookup (the update nulls them first so
      // the delete cannot violate the owner's FK).
      const del = newDraft(pc, {
        path: `${relPath}.delete`,
        op: "delete",
        action: "delete",
        table: rel.targetTable,
        where: edge.refKeys.map((rk, i) => ({ propertyKey: rk, column: targetCols[rk], value: outputRef(lookup!, edge.fkKeys[i]) })),
        edge: edge.plan,
      });
      if (update) del.orderDeps.add(update);
      continue;
    }
    // to-one update: the currently connected target row.
    planUpdateRow(
      pc,
      rel.targetTable,
      edge.refKeys.map((rk, i) => ({ propertyKey: rk, column: targetCols[rk], value: outputRef(provider, edge.fkKeys[i]) })),
      ops.update,
      `${relPath}.update`,
      depth + 1,
      "update",
      edge.plan,
    );
  }
  return provider;
}

// ---------------------------------------------------------------------------
// Delete (root) — predicate-driven dependent handling
// ---------------------------------------------------------------------------

/** Compile-time maker for a step-output parameter inside a raw predicate:
 *  binds `key` of `draft`'s row through `column`'s codec (the consuming
 *  column governs the cast). */
export type LateFn = (draft: Draft, key: string, column: AnyColumnBuilder) => ValueNode;

/** Predicate items selecting the children (rows of the edge's target table)
 * of the rows matched by the parent level. `selfParts` qualifies the child
 * table — the scan statement's own reference. Rebound per statement, so a
 * nested EXISTS re-aliases every level (self-edges and A→B→A chains never
 * shadow: each subquery FROM carries a distinct compiler alias). */
type ChildPredicate = (late: LateFn, selfParts: string[]) => ValueNode[];

type Disposition = "disconnect" | "delete" | { readonly nested: Record<string, unknown> };

function parseDisposition(value: unknown, path: string, edge: string): Disposition | undefined {
  if (value === undefined) return undefined;
  if (value === "disconnect" || value === "delete") return value;
  if (typeof value === "object" && value !== null && !Array.isArray(value) && "delete" in value) {
    const nested = (value as Record<string, unknown>).delete;
    if (typeof nested !== "object" || nested === null || Array.isArray(nested)) {
      throw planError(path, `cascade "${edge}": delete takes a nested dispositions object`);
    }
    return { nested: nested as Record<string, unknown> };
  }
  throw planError(path, `cascade "${edge}" must be "disconnect", "delete" or { delete: {...} } — got ${JSON.stringify(value)}`);
}

/** One level's predicate over `parentTable` rows, keyed off the deleted root
 *  (level 1: direct equality to the root row's referenced columns). */
function rootLevelPredicate(
  source: RelationOne,
  child: AnyPgTable,
  lookup: Draft,
  edge: ResolvedEdge,
  rootTable: AnyPgTable,
): ChildPredicate {
  const rootPk = pkKeysOf(rootTable);
  const rootCols = columnsOf(rootTable);
  const childIsRoot = tableId(child) === tableId(rootTable);
  return (late, selfParts) => {
    const items = source.fields.map((f, i) => exprNode("binary", "=", [qual(...selfParts, f.columnName), late(lookup, edge.refKeys[i], f)]));
    if (childIsRoot) {
      const exclusions = rootPk.map((k) => exprNode("binary", "=", [qual(...selfParts, rootCols[k].columnName), late(lookup, k, rootCols[k])]));
      const joined = exclusions.reduce((acc, c) => exprNode("binary", "and", [acc, c]));
      items.push(exprNode("unary", "not", [joined]));
    }
    return items;
  };
}

/** Deeper level: children of the parent level's rows — a correlated EXISTS
 *  over the parent predicate, rebound to a distinct alias. When the child
 *  table IS the deletion root's table, the root row itself is excluded
 *  (data cycles through the root must not count the root as its own
 *  dependent — it is being deleted). */
function nestedLevelPredicate(
  source: RelationOne,
  parentTable: AnyPgTable,
  parentPred: ChildPredicate,
  alias: string,
  rootTable: AnyPgTable,
  lookup: Draft,
): ChildPredicate {
  const childTable = source.fields[0]?.ownerTable;
  if (!childTable) throw new Error("internal: relation one() fields have no owner table");
  const childIsRoot = tableId(childTable) === tableId(rootTable);
  const rootPk = pkKeysOf(rootTable);
  const rootCols = columnsOf(rootTable);
  return (late, selfParts) => {
    const correlation = source.fields.map((f, i) =>
      exprNode("binary", "=", [qual(alias, source.references[i].columnName), qual(...selfParts, f.columnName)]),
    );
    const items = [
      exprNode("call", "exists", [
        subqueryNode(
          selectStatement({
            projections: [projectionNode(fragment("1"))],
            from: tableTargetNode(parentTable),
            fromAlias: alias,
            where: [...parentPred(late, [alias]), ...correlation],
          }),
        ),
      ]),
    ];
    if (childIsRoot) {
      const exclusions = rootPk.map((k) => exprNode("binary", "=", [qual(...selfParts, rootCols[k].columnName), late(lookup, k, rootCols[k])]));
      items.push(exprNode("unary", "not", [exclusions.reduce((acc, c) => exprNode("binary", "and", [acc, c]))]));
    }
    return items;
  };
}

interface DeleteCtx {
  readonly pc: PlanCtx;
  readonly root: AnyPgTable;
  readonly lookup: Draft;
  aliasSeq: number;
}

/** Emit scan + mutation steps for the dependents of one level's rows.
 *  Every declared to-many edge of the level's table gets a scan; an edge
 *  with rows and no declared disposition aborts the write (explicit
 *  ownership of every dependent edge). Mutations run deepest-first. */
function planDependentLevel(
  dctx: DeleteCtx,
  levelTable: AnyPgTable,
  parentPred: ChildPredicate | undefined,
  dispositions: Record<string, unknown> | undefined,
  depth: number,
): void {
  const { pc, root } = dctx;
  const levelName = getTableName(levelTable);
  checkDepth(depth, `${levelName}.cascade`);
  const entries = pc.relationsByTable.get(levelName) ?? {};
  for (const [key, rel] of Object.entries(entries)) {
    if (rel.kind !== "many") continue;
    const source = rel.source;
    // An unresolved many() has no known FK columns: it cannot be scanned.
    // The database's own FK enforcement aborts the delete if such rows
    // exist (documented) — nothing is silently dropped.
    if (!source) continue;
    const child = rel.targetTable;
    const childName = getTableName(child);
    const relPath = `${levelName}.${key}`;
    const edge = resolveEdge(levelTable, key, rel, relPath);
    const disposition = parseDisposition(dispositions?.[key], relPath, key);
    if (disposition === "disconnect") requireNullableFk(edge, relPath, "disconnect");

    const pred: ChildPredicate =
      parentPred === undefined
        ? rootLevelPredicate(source, child, dctx.lookup, edge, root)
        : nestedLevelPredicate(source, levelTable, parentPred, `__np${dctx.aliasSeq++}`, root, dctx.lookup);

    const scan = newDraft(pc, {
      path: `${relPath}.scan`,
      op: "scan",
      action: "select",
      table: child,
    });
    scan.rawWhere = pred;
    scan.zeroOrMore = true;
    if (disposition === undefined) scan.scanGuard = { edge: key };
    scan.orderDeps.add(dctx.lookup);

    if (disposition === "disconnect") {
      const upd = newDraft(pc, {
        path: `${relPath}.disconnect`,
        op: "disconnect",
        action: "update",
        table: child,
        sets: edge.fkKeys.map((fk) => nullAssignment(fk, columnsOf(child)[fk])),
        edge: edge.plan,
      });
      upd.rawWhere = pred;
      upd.affectedEquals = scan;
      upd.orderDeps.add(scan);
      continue;
    }
    const nestedSpec = typeof disposition === "object" ? disposition.nested : undefined;
    if (disposition !== undefined) {
      // delete (optionally with nested dispositions): grandchildren FIRST.
      planDependentLevel(dctx, child, pred, nestedSpec, depth + 1);
      const del = newDraft(pc, { path: `${relPath}.delete`, op: "delete", action: "delete", table: child, edge: edge.plan });
      del.rawWhere = pred;
      del.affectedEquals = scan;
      del.orderDeps.add(scan);
    }
  }
}

// ---------------------------------------------------------------------------
// Finalize: resolve connects, order, break cycles, compile
// ---------------------------------------------------------------------------

function resolveIntents(pc: PlanCtx): void {
  for (const intent of pc.intents) {
    const created = pc.createdKeys.get(intent.signature);
    if (intent.orderedDraft) {
      // to-many connect: an UPDATE of the child; a same-graph child must be
      // inserted first.
      if (created !== undefined) intent.orderedDraft.orderDeps.add(created);
      continue;
    }
    let provider: Draft;
    if (created !== undefined) {
      provider = created;
    } else {
      provider = newDraft(pc, {
        path: intent.path,
        op: "connect",
        action: "select",
        table: intent.target,
        where: intent.selector,
        edge: intent.edge,
        seq: intent.seq,
      });
    }
    for (const a of intent.assignments) {
      if (a.value.kind !== "intent") continue;
      a.value = outputRef(provider, intent.referenceKeys[a.value.index]);
    }
  }
}

function depsOf(d: Draft): Set<Draft> {
  const out = new Set<Draft>(d.orderDeps);
  for (const a of [...d.sets, ...d.where]) {
    if (a.value.kind === "ref") out.add(a.value.draft);
    if (a.value.kind === "intent") throw new Error("internal: unresolved connect intent");
  }
  return out;
}

/** Kahn's algorithm, lowest seq first (traversal order when unconstrained).
 *  Returns the order, or the drafts left in cycles. */
function topoOrder(drafts: readonly Draft[]): { order: Draft[]; stuck: Draft[] } {
  const deps = new Map<Draft, Set<Draft>>(drafts.map((d) => [d, depsOf(d)]));
  const done = new Set<Draft>();
  const order: Draft[] = [];
  const remaining = new Set(drafts);
  for (;;) {
    let next: Draft | undefined;
    for (const d of remaining) {
      if ([...deps.get(d)!].every((x) => done.has(x)) && (next === undefined || d.seq < next.seq)) next = d;
    }
    if (next === undefined) break;
    remaining.delete(next);
    done.add(next);
    order.push(next);
  }
  return { order, stuck: [...remaining].sort((a, b) => a.seq - b.seq) };
}

/** One dependency cycle among `stuck` drafts: c[i] depends on c[i+1]. */
function findCycle(stuck: readonly Draft[]): Draft[] {
  const inStuck = new Set(stuck);
  const start = stuck[0];
  const visitOrder: Draft[] = [];
  const index = new Map<Draft, number>();
  let cur: Draft | undefined = start;
  while (cur !== undefined && !index.has(cur)) {
    index.set(cur, visitOrder.length);
    visitOrder.push(cur);
    const nexts: Draft[] = [...depsOf(cur)].filter((d) => inStuck.has(d)).sort((a, b) => a.seq - b.seq);
    cur = nexts[0];
  }
  if (cur === undefined) return [start];
  return visitOrder.slice(index.get(cur)!);
}

/** Try to break the cycle edge d -> n by deferring d's owned, nullable FK
 *  assignments that reference n into a follow-up link update. */
function tryBreak(pc: PlanCtx, d: Draft, n: Draft): boolean {
  if (d.action !== "insert" || d.orderDeps.has(n)) return false;
  const groups = d.fkGroups.filter((g) => g.assignments.some((a) => a.value.kind === "ref" && a.value.draft === n));
  if (groups.length === 0 || groups.some((g) => !g.nullable)) return false;
  const inGroups = new Set(groups.flatMap((g) => g.assignments));
  const otherRef = [...d.sets, ...d.where].some((a) => !inGroups.has(a) && a.value.kind === "ref" && a.value.draft === n);
  if (otherRef) return false;
  const pk = pkKeysOf(d.table);
  if (pk.length === 0) return false;
  const cols = columnsOf(d.table);
  for (const g of groups) {
    const deferred: Assignment[] = g.assignments.map((a) => ({ propertyKey: a.propertyKey, column: a.column, value: a.value }));
    for (const a of g.assignments) a.value = { kind: "node", node: paramNode(null) };
    const link = newDraft(pc, {
      path: `${d.path}.${g.relation}.link`,
      op: "link",
      action: "update",
      table: d.table,
      sets: deferred,
      where: pk.map((k) => ({ propertyKey: k, column: cols[k], value: outputRef(d, k) })),
      edge: g.edge,
      seq: Math.max(d.seq, n.seq) + 0.5,
    });
    link.linkOf = d;
    d.fkGroups.splice(d.fkGroups.indexOf(g), 1);
  }
  return true;
}

function orderWithCycleBreaking(pc: PlanCtx): { order: Draft[]; links: number } {
  let links = 0;
  for (let guard = 0; guard <= pc.drafts.length + 1; guard++) {
    const { order, stuck } = topoOrder(pc.drafts);
    if (stuck.length === 0) return { order, links };
    const cycle = findCycle(stuck);
    let broken = false;
    for (let i = 0; i < cycle.length && !broken; i++) {
      if (tryBreak(pc, cycle[i], cycle[(i + 1) % cycle.length])) {
        broken = true;
        links++;
      }
    }
    if (!broken) {
      const trail = [...cycle, cycle[0]].map((d) => `${d.path} (${d.op} ${getTableName(d.table)})`).join(" -> needs -> ");
      throw new NeutronSqlError(
        `nested write graph has a reference cycle that cannot be ordered: ${trail}. ` +
          "A cycle is only executable when one inserted row's owned foreign key is nullable (it is written as NULL and linked by a follow-up update); " +
          "otherwise split the write: create one side first, then connect it in a second write inside the same transaction.",
      );
    }
  }
  throw new Error("internal: cycle breaking did not converge");
}

/** Lossless capture column: exact wire decode regardless of the declared
 *  read mode or user decoder (keys re-bind through the consumer's codec). */
function losslessColumn(column: AnyColumnBuilder): AnyColumnBuilder {
  const clone = Object.create(Object.getPrototypeOf(column)) as AnyColumnBuilder;
  Object.assign(clone, column);
  if (column.dataType === "bigint") clone.readMode = "bigint";
  if (column.dataType === "timestamp" || column.dataType === "timestamptz") clone.readMode = "string";
  clone.valueDecoder = undefined;
  return clone;
}

interface Binding {
  readonly step: number;
  readonly key: string;
  readonly column: AnyColumnBuilder;
  readonly propertyKey: string;
  readonly tableName: string;
  readonly inWhere: boolean;
}

interface ExecStep {
  readonly plan: NestedWriteStep;
  readonly returnsRows: boolean;
  readonly bindings: ReadonlyMap<number, Binding>;
  /** Lossless decoders for captured outputs. */
  readonly capture: readonly ProjectionDecoder[];
  /** User-facing decoders (result step only). */
  readonly resultDecoders: readonly ProjectionDecoder[];
  /** Scans whose rows abort the write when no disposition was declared. */
  readonly scanGuard?: { readonly edge: string };
}

/** A compiled nested write: the public plan plus the execution bindings. */
export interface CompiledNestedWrite {
  readonly plan: NestedWritePlan;
  /** @internal */
  readonly steps: readonly ExecStep[];
}

function projectionsFor(table: AnyPgTable, keys: readonly string[]): { nodes: ProjectionNode[]; usesJsonb: boolean } {
  const cols = columnsOf(table);
  const refParts = tableRefParts(table);
  const nodes: ProjectionNode[] = [];
  let usesJsonb = false;
  for (const key of keys) {
    const column = cols[key];
    const ref = qual(...refParts, column.columnName);
    const wire = wireReadNode(column.dataType, ref);
    if (wire !== null) {
      nodes.push(projectionNode(wire, key));
      usesJsonb = true;
    } else {
      nodes.push(projectionNode(ref, key === column.columnName ? undefined : key));
    }
  }
  return { nodes, usesJsonb };
}

function compileDrafts(order: readonly Draft[], operation: "create" | "update" | "delete", tableName: string, links: number): CompiledNestedWrite {
  const indexOf = new Map<Draft, number>(order.map((d, i) => [d, i]));
  const execSteps: ExecStep[] = [];
  const planSteps: NestedWriteStep[] = [];
  const allCaps = new Set<StatementCapability>();

  order.forEach((d, index) => {
    const table = d.table;
    const name = getTableName(table);
    const cols = columnsOf(table);
    const refParts = tableRefParts(table);
    const pending: Array<{ placeholder: StepOutputRef; binding: Binding }> = [];
    const valueNode = (a: Assignment, inWhere: boolean): ValueNode => {
      const v = a.value;
      if (v.kind === "node") return v.node;
      if (v.kind === "intent") throw new Error("internal: unresolved connect intent");
      const placeholder: StepOutputRef = Object.freeze({ kind: "step-output", step: indexOf.get(v.draft)!, key: v.key });
      pending.push({
        placeholder,
        binding: { step: placeholder.step, key: v.key, column: a.column, propertyKey: a.propertyKey, tableName: name, inWhere },
      });
      const cast = writeCastTarget(a.column.dataType);
      return cast === undefined ? paramNode(placeholder) : paramCast(placeholder, cast);
    };
    const late: LateFn = (draft, key, column) => {
      const placeholder: StepOutputRef = Object.freeze({ kind: "step-output", step: indexOf.get(draft)!, key });
      pending.push({ placeholder, binding: { step: placeholder.step, key, column, propertyKey: key, tableName: name, inWhere: true } });
      const cast = writeCastTarget(column.dataType);
      return cast === undefined ? paramNode(placeholder) : paramCast(placeholder, cast);
    };
    const whereNodes = [
      ...d.where.map((a) => exprNode("binary", "=", [qual(...refParts, a.column.columnName), valueNode(a, true)])),
      ...(d.rawWhere ? d.rawWhere(late, refParts) : []),
    ];

    const schemaOrder = Object.keys(cols);
    const outputKeys = d.result ? schemaOrder : schemaOrder.filter((k) => d.outputs.has(k));
    const proj = projectionsFor(table, outputKeys);
    const returning = proj.nodes.length > 0 ? proj.nodes : undefined;

    let stmt: AnyStatementNode;
    switch (d.action) {
      case "insert": {
        stmt =
          d.sets.length === 0
            ? insertStatement({ table: tableTargetNode(table), defaultValues: true, returning })
            : insertStatement({
                table: tableTargetNode(table),
                columns: d.sets.map((a) => a.column.columnName),
                rows: [d.sets.map((a) => valueNode(a, false) as InsertCell)],
                returning,
              });
        break;
      }
      case "update":
        stmt = updateStatement({
          table: tableTargetNode(table),
          sets: d.sets.map((a) => ({ column: a.column.columnName, value: valueNode(a, false) })),
          where: whereNodes,
          returning,
        });
        break;
      case "delete":
        stmt = deleteStatement({ table: tableTargetNode(table), where: whereNodes, returning });
        break;
      case "select":
        stmt = selectStatement({
          // No captured columns: project a constant so a missing row still
          // yields zero rows (an aggregate would always yield one).
          projections: returning ?? [projectionNode(fragment("1"), "matched")],
          from: tableTargetNode(table),
          where: whereNodes,
        });
        break;
    }
    // Insert cells must be param nodes; user expressions only reach update
    // SET (planUpdateRow) — assert the insert invariant structurally.
    if (d.action === "insert") {
      for (const a of d.sets) {
        if (a.value.kind === "node" && a.value.node.kind !== "param") throw new Error("internal: insert cells must be parameters");
      }
    }
    const compiled = compileStatement(stmt);
    const bindings = new Map<number, Binding>();
    compiled.params.forEach((p, i) => {
      const hit = pending.find((x) => x.placeholder === p);
      if (hit) bindings.set(i, hit.binding);
    });
    const caps: StatementCapability[] = proj.usesJsonb ? ["jsonb-functions"] : [];
    for (const c of caps) allCaps.add(c);
    const dependsOn = [...depsOf(d)].map((x) => indexOf.get(x)!).sort((a, b) => a - b);
    const capture = outputKeys
      .filter((k) => d.outputs.has(k))
      .map((k) => projectionDecoder(name, losslessColumn(cols[k]), k))
      .filter((x): x is ProjectionDecoder => x !== null);
    const resultDecoders = d.result
      ? schemaOrder.map((k) => projectionDecoder(name, cols[k], k)).filter((x): x is ProjectionDecoder => x !== null)
      : [];
    const plan: NestedWriteStep = Object.freeze({
      index,
      path: d.path,
      op: d.op,
      action: d.action,
      table: name,
      sql: compiled.sql,
      params: Object.freeze([...compiled.params]),
      dependsOn: Object.freeze(dependsOn),
      expect:
        d.affectedEquals !== undefined
          ? Object.freeze({ affectedMatchesStep: indexOf.get(d.affectedEquals)! })
          : d.zeroOrMore === true
            ? ("zero-or-more-rows" as const)
            : ("exactly-one-row" as const),
      outputs: Object.freeze(schemaOrder.filter((k) => d.outputs.has(k))),
      returnsResult: d.result,
      ...(d.edge ? { edge: Object.freeze({ ...d.edge }) } : {}),
      capabilities: Object.freeze(caps),
    });
    planSteps.push(plan);
    execSteps.push({
      plan,
      returnsRows: d.action === "select" || returning !== undefined,
      bindings,
      capture,
      resultDecoders,
      ...(d.scanGuard ? { scanGuard: d.scanGuard } : {}),
    });
  });

  const plan: NestedWritePlan = Object.freeze({
    table: tableName,
    operation,
    atomic: true as const,
    statementCount: planSteps.length,
    steps: Object.freeze(planSteps),
    capabilities: Object.freeze([...allCaps]),
    deferredLinks: links,
  });
  return { plan, steps: execSteps };
}

function newPlanCtx(relationsByTable: Map<string, Record<string, Relation>>): PlanCtx {
  return { relationsByTable, drafts: [], intents: [], createdKeys: new Map(), nextSeq: 0 };
}

function finalize(pc: PlanCtx, operation: "create" | "update" | "delete", tableName: string): CompiledNestedWrite {
  resolveIntents(pc);
  const { order, links } = orderWithCycleBreaking(pc);
  // A link that completes the RESULT row runs after its insert: the last such
  // link returns the final row (the insert's RETURNING still shows the
  // deferred NULL).
  const resultDraft = order.find((d) => d.result);
  const resultLinks = order.filter((d) => d.linkOf !== undefined && d.linkOf === resultDraft);
  if (resultDraft !== undefined && resultLinks.length > 0) {
    resultDraft.result = false;
    resultLinks[resultLinks.length - 1].result = true;
  }
  return compileDrafts(order, operation, tableName, links);
}

/** Compile a nested create (pure). */
export function compileNestedCreate(
  table: AnyPgTable,
  args: NestedCreateInput,
  relationsByTable: Map<string, Record<string, Relation>>,
): CompiledNestedWrite {
  const tableName = getTableName(table);
  if (typeof args !== "object" || args === null || !("data" in args)) {
    throw new NeutronSqlError(`create on ${tableName}: expected { data }`);
  }
  const extra = Object.keys(args).filter((k) => k !== "data");
  if (extra.length > 0) throw new NeutronSqlError(`create on ${tableName}: unknown argument(s) ${extra.join(", ")} (expected data)`);
  const pc = newPlanCtx(relationsByTable);
  const root = planCreate(pc, table, args.data, tableName, 0);
  root.result = true;
  return finalize(pc, "create", tableName);
}

/** Compile a nested update of exactly one row (pure). */
export function compileNestedUpdate(
  table: AnyPgTable,
  args: NestedUpdateInput,
  relationsByTable: Map<string, Record<string, Relation>>,
): CompiledNestedWrite {
  const tableName = getTableName(table);
  if (typeof args !== "object" || args === null || !("where" in args) || !("data" in args)) {
    throw new NeutronSqlError(`update on ${tableName}: expected { where, data } — where names one unique key`);
  }
  const extra = Object.keys(args).filter((k) => k !== "where" && k !== "data");
  if (extra.length > 0) throw new NeutronSqlError(`update on ${tableName}: unknown argument(s) ${extra.join(", ")} (expected where, data)`);
  const pc = newPlanCtx(relationsByTable);
  const where = uniqueSelector(table, args.where, tableName, "update where");
  planUpdateRow(pc, table, where, args.data, tableName, 0, "root");
  return finalize(pc, "update", tableName);
}

/** Compile a nested delete of exactly one row (pure): one lookup, one scan
 *  per declared dependent edge of every disposition level (predicate-driven,
 *  statement count independent of row counts), mutations deepest-first, and
 *  the row's own DELETE ... RETURNING last. */
export function compileNestedDelete(
  table: AnyPgTable,
  args: NestedDeleteInput,
  relationsByTable: Map<string, Record<string, Relation>>,
): CompiledNestedWrite {
  const tableName = getTableName(table);
  if (typeof args !== "object" || args === null || !("where" in args)) {
    throw new NeutronSqlError(`delete on ${tableName}: expected { where, cascade? } — where names one unique key`);
  }
  const extra = Object.keys(args).filter((k) => k !== "where" && k !== "cascade");
  if (extra.length > 0) throw new NeutronSqlError(`delete on ${tableName}: unknown argument(s) ${extra.join(", ")} (expected where, cascade?)`);
  const cols = columnsOf(table);
  const pk = pkKeysOf(table);
  if (pk.length === 0) {
    throw new NeutronSqlError(`delete on ${tableName}: the table declares no primary key — a delete must name the row by one`);
  }
  const cascade = args.cascade;
  if (cascade !== undefined && (typeof cascade !== "object" || cascade === null || Array.isArray(cascade))) {
    throw new NeutronSqlError(`delete on ${tableName}: cascade must be an object keyed by relation names`);
  }
  const entries = relationsByTable.get(tableName) ?? {};
  if (cascade !== undefined) {
    for (const key of Object.keys(cascade).filter((k) => (cascade as Record<string, unknown>)[k] !== undefined)) {
      const rel = entries[key];
      if (rel === undefined) {
        throw planError(`delete on ${tableName}.cascade`, `unknown relation "${key}" (known: ${Object.keys(entries).join(", ") || "none"})`);
      }
      if (rel.kind !== "many") {
        throw planError(`delete on ${tableName}.cascade.${key}`, `dispositions apply to dependent (to-many) edges — "${key}" is a to-one relation`);
      }
      parseDisposition((cascade as Record<string, unknown>)[key], `delete on ${tableName}.cascade.${key}`, key);
    }
  }

  const pc = newPlanCtx(relationsByTable);
  const where = uniqueSelector(table, args.where, tableName, "delete where");
  // The lookup exposes the root primary key (self-edge exclusion + the final
  // DELETE) and every referenced column of the root's declared edges
  // (level-1 predicates).
  const lookup = newDraft(pc, { path: tableName, op: "lookup", action: "select", table, where });
  for (const k of pk) lookup.outputs.add(k);
  for (const [, rel] of Object.entries(entries)) {
    if (rel.kind !== "many" || !rel.source) continue;
    for (const ref of rel.source.references) {
      lookup.outputs.add(propertyKeyOf(table, ref, tableName));
    }
  }
  const dctx: DeleteCtx = { pc, root: table, lookup, aliasSeq: 1 };
  planDependentLevel(dctx, table, undefined, cascade, 0);

  const del = newDraft(pc, { path: tableName, op: "delete", action: "delete", table });
  del.rawWhere = (late, selfParts) => pk.map((k) => exprNode("binary", "=", [qual(...selfParts, cols[k].columnName), late(lookup, k, cols[k])]));
  // The database's own FK constraints require every dependent mutation to
  // land first; make that explicit in the dependency graph.
  for (const d of pc.drafts) if (d !== del) del.orderDeps.add(d);
  del.result = true;
  return finalize(pc, "delete", tableName);
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

function bindCaptured(binding: Binding, value: unknown): unknown {
  const ctx = { propertyKey: binding.propertyKey, columnName: binding.column.columnName, tableName: binding.tableName };
  let v = value;
  // A captured int8 feeding an int2/int4 FK column (mismatched key types):
  // narrow only when exact.
  const dt = binding.column.dataType;
  if (typeof v === "bigint" && (dt === "integer" || dt === "smallint" || dt === "serial")) {
    if (v > BigInt(Number.MAX_SAFE_INTEGER) || v < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new NeutronSqlError(`column "${ctx.propertyKey}" on ${ctx.tableName}: captured key ${v} does not fit ${dt}`);
    }
    v = Number(v);
  }
  const encoded = encodeWriteValue(binding.column, ctx, v);
  if (encoded.cast !== writeCastTarget(dt)) throw new Error("internal: captured value encoded with an unexpected cast");
  return encoded.bind;
}

/** Run a compiled nested write on `ctx` — which MUST already be scoped to a
 *  transaction (the caller owns BEGIN/COMMIT or the savepoint). Returns the
 *  result row (decoded with the table's declared codecs). */
export async function executeNestedWrite(
  ctx: ExecContext,
  compiled: CompiledNestedWrite,
  options?: QueryExecutionOptions,
): Promise<Record<string, unknown>> {
  // Capability requirements are checked for the WHOLE plan before the first
  // statement: an unsupported engine fails before any partial work.
  if (compiled.plan.capabilities.length > 0 && ctx.capabilities) {
    await ctx.capabilities.assert(compiled.plan.capabilities);
  }
  const outputs: Array<Map<string, unknown>> = [];
  const stepCounts: number[] = [];
  let result: Record<string, unknown> | undefined;
  for (const step of compiled.steps) {
    const { plan } = step;
    const params = plan.params.map((p, i) => {
      const binding = step.bindings.get(i);
      if (binding === undefined) return p;
      const captured = outputs[binding.step]?.get(binding.key);
      if (captured === undefined) throw new Error(`internal: step ${plan.index} reads output "${binding.key}" of step ${binding.step} before it ran`);
      if (captured === null) {
        if (binding.inWhere) {
          throw new NestedWriteError(
            `nested write step ${plan.index} (${plan.op} at ${plan.path}): the key it is scoped by ("${binding.key}" of step ${binding.step}) is NULL — there is no connected ${plan.table} row`,
            { step: plan.index, path: plan.path, op: plan.op, table: plan.table, reason: "no-connected-row", rowCount: 0 },
          );
        }
        return null;
      }
      return bindCaptured(binding, captured);
    });
    let count: number;
    let row: Record<string, unknown> | undefined;
    if (step.returnsRows) {
      const rows = (await run(ctx, plan.sql, params, "query", plan.capabilities, options)) as Array<Record<string, unknown>>;
      count = rows.length;
      row = rows[0];
    } else {
      count = (await run(ctx, plan.sql, params, "execute", plan.capabilities, options)) as number;
    }
    if (plan.expect === "exactly-one-row" && count !== 1) {
      const reason = count === 0 ? "not-found" : "cardinality";
      throw new NestedWriteError(
        `nested write step ${plan.index} (${plan.op} at ${plan.path}) ${count === 0 ? "matched no" : `matched ${count}`} ${plan.table} row${count === 1 ? "" : "s"} — exactly one is required; the whole write is rolled back`,
        { step: plan.index, path: plan.path, op: plan.op, table: plan.table, reason, rowCount: count },
      );
    }
    if (typeof plan.expect === "object" && count !== stepCounts[plan.expect.affectedMatchesStep]) {
      const scan = plan.expect.affectedMatchesStep;
      throw new NestedWriteError(
        `nested write step ${plan.index} (${plan.op} at ${plan.path}) affected ${count} ${plan.table} row${count === 1 ? "" : "s"} but its scan (step ${scan}) counted ${stepCounts[scan]} — the dependent row set changed under the write; the whole write is rolled back`,
        { step: plan.index, path: plan.path, op: plan.op, table: plan.table, reason: "row-set-changed", rowCount: count },
      );
    }
    if (step.scanGuard !== undefined && count > 0) {
      throw new NestedWriteError(
        `nested write step ${plan.index} (${plan.op} at ${plan.path}): ${count} ${plan.table} row${count === 1 ? "" : "s"} reference the deleted row through "${step.scanGuard.edge}" and no disposition was declared — pass cascade: { ${step.scanGuard.edge}: "disconnect" | "delete" | { delete: {...} } }; the whole write is rolled back`,
        { step: plan.index, path: plan.path, op: plan.op, table: plan.table, reason: "undeclared-dependents", rowCount: count },
      );
    }
    const captured = new Map<string, unknown>();
    if (row !== undefined) {
      const raw = { ...row };
      const decoders = new Map(step.capture.map((c) => [c.key, c] as const));
      for (const key of plan.outputs) {
        const v = raw[key];
        const dec = decoders.get(key);
        captured.set(key, v === null || v === undefined ? null : dec ? dec.decode(v) : v);
      }
      if (plan.returnsResult) {
        const out = [{ ...row }];
        applyProjectionDecoders(out, step.resultDecoders);
        result = out[0];
      }
    }
    outputs.push(captured);
    stepCounts.push(count);
  }
  if (result === undefined) throw new Error("internal: nested write produced no result row");
  return result;
}
