// ---------------------------------------------------------------------------
// @neutron-build/sql — keyset pagination (Q04)
// ---------------------------------------------------------------------------
// Cursor (seek) pagination over an explicitly ordered keyset:
//
//   const pager = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.rank)]);
//   const p1 = await pager.page(db.select().from(events), undefined, { perPage: 20 });
//   const p2 = await pager.page(db.select().from(events), p1.nextCursor, 20);
//
// Contracts (card Q04 / VERIFICATION V13+V16):
// - The keyset must form a UNIQUE ordering: a schema-declared unique key
//   (single-column PK/unique, composite PK, unique index) must be contained
//   in the keyset columns. When it is not, a unique tie-breaker column is
//   AUTO-APPENDED (asc nulls last) when the schema declares one; otherwise
//   keyset() errors. Non-unique keysets duplicate/omit rows on ties.
// - Every keyset term carries EXPLICIT null ordering (ascNullsLast() etc.);
//   a spec without one is rejected.
// - The seek predicate decomposes the row comparison with per-column
//   direction and null placement, tying with IS NOT DISTINCT FROM (null =
//   null under a nullable tie column).
// - Cursors are opaque, versioned strings: base64url(version byte + JSON
//   payload). Values are tagged and precision-exact (int8 as decimal
//   strings — never through JS Number; microsecond temporal canonical text;
//   NULL positions explicit). Malformed input, unknown versions, unknown
//   envelope/value fields and keyset mismatches fail with CursorError
//   before any SQL runs.
// - apply()/page() compose onto a FRESH builder only: a builder that
//   already carries ORDER BY terms or an OFFSET is rejected through
//   KeysetQueryable.keysetBuilderState() before any SQL runs — composed
//   silently, a leading user order or a persisting offset duplicates and
//   omits rows. Filter a fresh builder with .where(); the pager applies
//   ordering and limit exclusively.
// - Concurrency: each page is its own statement. Rows inserted between pages
//   appear iff they sort AFTER the then-current cursor; rows inserted before
//   the cursor never reappear, so the pre-existing snapshot is never
//   duplicated or omitted. For one snapshot across a whole walk, wrap it in
//   a single REPEATABLE READ transaction. See README "Keyset pagination".

import { expr as exprNode, fragment, param as paramNode, paramCast, qual, type OrderSpec, type ValueNode } from "./ast.js";
import { encodeWriteValue, type ColumnContext } from "./codecs.js";
import {
  getTableColumns,
  getTableName,
  getTableIndexes,
  tableRefParts,
  type AnyColumnBuilder,
  type AnyPgTable,
  type ColumnDataType,
} from "./schema.js";
import type { Condition, OrderExpression } from "./expr.js";

/** Strict, clearly-named cursor validation failure. */
export class CursorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

// ---------------------------------------------------------------------------
// Cursor wire format: base64url( [version byte] ++ utf8(JSON payload) )
// ---------------------------------------------------------------------------

const CURSOR_VERSION = 1;

interface CursorOrderTerm {
  readonly c: string;
  readonly d: "asc" | "desc";
  readonly n: "first" | "last";
}

/** JSON-safe tagged cursor value. `t` discriminates; payloads are plain
 *  strings/numbers/booleans so the whole cursor round-trips JSON losslessly
 *  (int8/numeric ride exact decimal strings, temporals canonical text). */
interface CursorValue {
  readonly t: "null" | "i8" | "dec" | "int" | "flt" | "str" | "ts" | "tstz" | "date" | "bool";
  readonly s?: string;
  readonly n?: number;
  readonly b?: boolean;
}

interface CursorPayload {
  readonly v: typeof CURSOR_VERSION;
  readonly o: readonly CursorOrderTerm[];
  readonly k: readonly CursorValue[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlDecode(text: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(text)) {
    throw new CursorError("cursor is not valid base64url");
  }
  let binary: string;
  try {
    binary = atob(text.replace(/-/g, "+").replace(/_/g, "/"));
  } catch {
    throw new CursorError("cursor is not valid base64url");
  }
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function encodeCursorPayload(payload: CursorPayload): string {
  const json = JSON.stringify(payload);
  const body = encoder.encode(json);
  const bytes = new Uint8Array(body.length + 1);
  bytes[0] = CURSOR_VERSION;
  bytes.set(body, 1);
  return base64UrlEncode(bytes);
}

function decodeCursorPayload(cursor: string): CursorPayload {
  if (typeof cursor !== "string" || cursor.length === 0) {
    throw new CursorError("cursor must be a non-empty string");
  }
  const bytes = base64UrlDecode(cursor);
  if (bytes.length < 2) {
    throw new CursorError("cursor payload is too short to carry a version byte and payload");
  }
  if (bytes[0] !== CURSOR_VERSION) {
    throw new CursorError(
      `cursor version ${bytes[0]} is not supported by this build (supported: ${CURSOR_VERSION}) — the cursor was produced by a different version and is rejected rather than guess-decoded`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(bytes.subarray(1)));
  } catch {
    throw new CursorError("cursor payload is not valid JSON");
  }
  const p = parsed as Record<string, unknown>;
  if (typeof p !== "object" || p === null || p.v !== CURSOR_VERSION || !Array.isArray(p.o) || !Array.isArray(p.k)) {
    throw new CursorError("cursor payload does not have the expected shape ({ v, o, k })");
  }
  const extraEnvelopeKeys = Object.keys(p).filter((key) => key !== "v" && key !== "o" && key !== "k");
  if (extraEnvelopeKeys.length > 0) {
    throw new CursorError(`cursor payload carries unexpected fields (${extraEnvelopeKeys.join(", ")})`);
  }
  if (p.o.length !== p.k.length) {
    throw new CursorError(`cursor ordering has ${p.o.length} terms but ${p.k.length} values`);
  }
  const o: CursorOrderTerm[] = [];
  for (const rawOrder of p.o) {
    const t = rawOrder as Record<string, unknown>;
    if (typeof t !== "object" || t === null || typeof t.c !== "string" || (t.d !== "asc" && t.d !== "desc") || (t.n !== "first" && t.n !== "last")) {
      throw new CursorError("cursor ordering term does not have the expected shape ({ c, d, n })");
    }
    o.push({ c: t.c, d: t.d, n: t.n });
  }
  const k: CursorValue[] = [];
  for (const rawValue of p.k) {
    const v = rawValue as Record<string, unknown>;
    if (typeof v !== "object" || v === null) throw new CursorError("cursor value is not an object");
    const tag = v.t;
    const extra = Object.keys(v).filter((key) => key !== "t" && key !== "s" && key !== "n" && key !== "b");
    if (extra.length > 0) throw new CursorError(`cursor value carries unexpected fields (${extra.join(", ")})`);
    if (tag === "null") {
      k.push({ t: "null" });
      continue;
    }
    if (tag === "bool") {
      if (typeof v.b !== "boolean") throw new CursorError("cursor boolean value is malformed");
      k.push({ t: "bool", b: v.b });
      continue;
    }
    if (tag === "int" || tag === "flt") {
      if (typeof v.n !== "number" || !Number.isFinite(v.n)) throw new CursorError("cursor numeric value is malformed");
      k.push({ t: tag, n: v.n });
      continue;
    }
    if (tag === "i8" || tag === "dec" || tag === "str" || tag === "ts" || tag === "tstz" || tag === "date") {
      if (typeof v.s !== "string") throw new CursorError("cursor string value is malformed");
      k.push({ t: tag, s: v.s });
      continue;
    }
    throw new CursorError(`cursor value carries unknown tag ${JSON.stringify(tag)}`);
  }
  return { v: CURSOR_VERSION, o, k };
}

// ---------------------------------------------------------------------------
// Keyset column resolution
// ---------------------------------------------------------------------------

/** One resolved keyset term: the schema column plus its explicit ordering. */
export interface KeysetColumn {
  /** Property key of the column (cursorOf reads result rows by this key). */
  readonly propertyKey: string;
  readonly column: AnyColumnBuilder;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
  /** True when this term was auto-appended as the unique tie-breaker. */
  readonly autoAppended: boolean;
}

const UNSUPPORTED_KEYSET_TYPES: ReadonlySet<ColumnDataType> = new Set(["json", "jsonb", "bytea", "vector"]);

const CANONICAL_TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$/;
const CANONICAL_TIMESTAMPTZ_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/;
const CANONICAL_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TEMPORAL_INFINITY_RE = /^(infinity|-infinity)$/;
const INT8_RE = /^[+-]?\d+$/;
const DECIMAL_RE = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;
const INT8_MIN = -9223372036854775808n;
const INT8_MAX = 9223372036854775807n;

function valueTagOf(dataType: ColumnDataType): CursorValue["t"] | null {
  switch (dataType) {
    case "bigint": return "i8";
    case "numeric": return "dec";
    case "serial":
    case "integer":
    case "smallint": return "int";
    case "double":
    case "real": return "flt";
    case "text":
    case "varchar":
    case "uuid": return "str";
    case "timestamp": return "ts";
    case "timestamptz": return "tstz";
    case "date": return "date";
    case "boolean": return "bool";
    default: return null;
  }
}

/** All schema-declared unique keys of a table, as physical-column sets:
 *  single-column PK/unique marks, the composite PK, and unique indexes. */
function uniqueKeySets(table: AnyPgTable): string[][] {
  const entries = Object.entries(getTableColumns(table));
  const sets: string[][] = [];
  for (const [, col] of entries) {
    if (col.isPrimaryKey || col.isUnique) sets.push([col.columnName]);
  }
  const pk = entries.filter(([, c]) => c.isPrimaryKey).map(([, c]) => c.columnName);
  if (pk.length > 1) sets.push(pk);
  for (const idx of getTableIndexes(table)) {
    if (idx.unique && idx.columns.length > 0) sets.push([...idx.columns]);
  }
  return sets;
}

function containedIn(superset: readonly string[], subset: readonly string[]): boolean {
  return subset.every((c) => superset.includes(c));
}

function describeUniqueKeys(sets: readonly string[][]): string {
  if (sets.length === 0) return "the schema declares no unique key for this table";
  return sets.map((s) => `(${s.join(", ")})`).join(" or ");
}

export interface KeysetOptions {
  /** Default page size for page() (positive integer; overridden per call).
   *  Default 50. */
  perPage?: number;
  /** Auto-append a unique tie-breaker column when the keyset is not unique
   *  (default true). When false, a non-unique keyset errors instead. */
  autoAppendTiebreaker?: boolean;
}

/** Structural type page()/apply() accept: the typed select builder satisfies
 *  it — where/orderBy/limit fork immutably, execute() runs the compiled
 *  statement, and keysetBuilderState() exposes the order/offset state the
 *  pager's fail-closed guard reads. */
export interface KeysetQueryable<R> {
  where(condition: Condition): KeysetQueryable<R>;
  orderBy(...exprs: OrderExpression[]): KeysetQueryable<R>;
  limit(n: number): KeysetQueryable<R>;
  execute(): Promise<R[]>;
  /** Introspection the pager's guard reads before composing: does this
   *  builder already carry ORDER BY terms or an OFFSET? apply()/page()
   *  reject such builders — the seek predicate is only correct when the
   *  keyset ordering is the statement's total ORDER BY, and a pre-existing
   *  OFFSET skips rows on every page. */
  keysetBuilderState(): { ordered: boolean; offset: boolean };
}

export interface KeysetPage<R> {
  readonly rows: readonly R[];
  /** Cursor to the next page, or null when the walk is complete. */
  readonly nextCursor: string | null;
}

/** Reduce value nodes with a binary operator, left-associatively. */
function reduceBinary(op: "and" | "or", parts: readonly ValueNode[]): ValueNode {
  return parts.reduce((acc, p) => exprNode("binary", op, [acc, p]));
}

// ---------------------------------------------------------------------------
// Pager
// ---------------------------------------------------------------------------

export class KeysetPager {
  private readonly table: AnyPgTable;
  private readonly tableName: string;
  private readonly terms: readonly KeysetColumn[];
  private readonly defaultPerPage: number;

  private constructor(table: AnyPgTable, terms: readonly KeysetColumn[], defaultPerPage: number) {
    this.table = table;
    this.tableName = getTableName(table);
    this.terms = Object.freeze([...terms]);
    this.defaultPerPage = defaultPerPage;
  }

  /** Build a pager for `table` ordered by explicit order specs. Each spec
   *  must reference a plain column of `table` and carry explicit null
   *  ordering (ascNullsLast/ascNullsFirst/descNullsLast/descNullsFirst). */
  static create(table: AnyPgTable, order: readonly OrderSpec[], options: KeysetOptions = {}): KeysetPager {
    if (!Array.isArray(order) || order.length === 0) {
      throw new Error(`keyset on ${getTableName(table)}: at least one order term is required`);
    }
    const tableName = getTableName(table);
    const columns = getTableColumns(table);
    const refParts = tableRefParts(table);
    const byPhysical = new Map<string, { propertyKey: string; column: AnyColumnBuilder }>();
    for (const [propertyKey, column] of Object.entries(columns)) {
      byPhysical.set(column.columnName, { propertyKey, column });
    }
    const resolved: KeysetColumn[] = [];
    const seenPhysical = new Set<string>();
    for (const spec of order) {
      if (typeof spec !== "object" || spec === null || typeof spec.direction !== "string") {
        throw new Error(`keyset on ${tableName}: order terms must be asc()/desc() order specs`);
      }
      if (spec.nulls !== "first" && spec.nulls !== "last") {
        throw new Error(
          `keyset on ${tableName}: every order term needs EXPLICIT null ordering — use ascNullsLast()/ascNullsFirst()/descNullsLast()/descNullsFirst() (PostgreSQL's implicit default depends on direction and hides the boundary semantics)`,
        );
      }
      const e = spec.expr as ValueNode | undefined;
      if (typeof e !== "object" || e === null || e.kind !== "qualified" || e.parts.length !== refParts.length + 1 || !refParts.every((p, i) => e.parts[i] === p)) {
        throw new Error(
          `keyset on ${tableName}: every order term must be a plain column reference of ${tableName} (pass the table's column, e.g. ascNullsLast(events.occurredAt))`,
        );
      }
      const physical = e.parts[e.parts.length - 1];
      const found = byPhysical.get(physical);
      if (found === undefined) {
        throw new Error(`keyset on ${tableName}: order term "${physical}" is not a column of the table`);
      }
      if (seenPhysical.has(physical)) {
        throw new Error(`keyset on ${tableName}: column "${physical}" appears twice in the keyset`);
      }
      seenPhysical.add(physical);
      const dt = found.column.dataType;
      if (UNSUPPORTED_KEYSET_TYPES.has(dt)) {
        throw new Error(`keyset on ${tableName}: column "${found.propertyKey}" (${dt}) has no total btree ordering usable for keyset pagination`);
      }
      if (dt === "numeric" && found.column.valueDecoder !== undefined) {
        throw new Error(
          `keyset on ${tableName}: numeric column "${found.propertyKey}" carries a user decoder — its decoded row values cannot re-encode losslessly into a cursor; paginate the exact decimal string instead`,
        );
      }
      if ((dt === "timestamp" || dt === "timestamptz") && found.column.readMode === "date") {
        throw new Error(
          `keyset on ${tableName}: temporal column "${found.propertyKey}" is in Date read mode, which truncates to milliseconds — cursor boundaries would duplicate/omit microsecond rows; use the default string mode for keyset columns`,
        );
      }
      resolved.push({ propertyKey: found.propertyKey, column: found.column, direction: spec.direction, nulls: spec.nulls, autoAppended: false });
    }

    // Unique ordering: some declared unique key must be contained in the
    // keyset; otherwise auto-append a tie-breaker when one exists.
    const uniqueSets = uniqueKeySets(table);
    const current = resolved.map((t) => t.column.columnName);
    if (!uniqueSets.some((u) => containedIn(current, u))) {
      if (options.autoAppendTiebreaker === false) {
        throw new Error(
          `keyset on ${tableName}: the keyset (${current.join(", ")}) is not a unique ordering and autoAppendTiebreaker is false — a tie would make page boundaries repeat or skip rows; include a unique key (${describeUniqueKeys(uniqueSets)})`,
        );
      }
      const candidate =
        uniqueSets.find((u) => u.length === 1 && u.every((c) => byPhysical.get(c)?.column.isPrimaryKey)) ??
        uniqueSets.find((u) => u.length === 1 && u.every((c) => byPhysical.get(c) !== undefined)) ??
        uniqueSets.find((u) => u.every((c) => byPhysical.get(c) !== undefined)) ??
        null;
      if (candidate === null) {
        throw new Error(
          `keyset on ${tableName}: the keyset (${current.join(", ")}) is not a unique ordering and no schema-declared unique key can be auto-appended as a tie-breaker — include a unique key (${describeUniqueKeys(uniqueSets)})`,
        );
      }
      for (const physical of candidate) {
        if (seenPhysical.has(physical)) continue;
        const found = byPhysical.get(physical)!;
        resolved.push({ propertyKey: found.propertyKey, column: found.column, direction: "asc", nulls: "last", autoAppended: true });
        seenPhysical.add(physical);
      }
    }
    return new KeysetPager(table, resolved, validPerPage(options.perPage ?? 50));
  }

  /** The resolved keyset terms (including the auto-appended tie-breaker). */
  get columns(): readonly KeysetColumn[] {
    return this.terms;
  }

  /** Order specs for the statement's ORDER BY — the same explicit ordering
   *  the seek predicate decomposes. */
  orderExpressions(): OrderSpec[] {
    return this.terms.map((t) => ({
      expr: qual(...tableRefParts(this.table), t.column.columnName),
      direction: t.direction,
      nulls: t.nulls,
    }));
  }

  /** Encode a cursor from one result row (keyed by property keys — the
   *  default projection satisfies this). The row's keyset values run through
   *  the column codecs' canonical forms: int8 exact decimals, microsecond
   *  temporal text, explicit null positions. */
  cursorOf(row: Record<string, unknown>): string {
    const values: CursorValue[] = [];
    for (const term of this.terms) {
      const v = row[term.propertyKey];
      if (v === undefined) {
        throw new Error(
          `cursor on ${this.tableName}: row does not project keyset column "${term.propertyKey}" — project the keyset columns (the default projection does)`,
        );
      }
      values.push(encodeRowValue(term, v, this.tableName));
    }
    return encodeCursorPayload({
      v: CURSOR_VERSION,
      o: this.terms.map((t) => ({ c: t.column.columnName, d: t.direction, n: t.nulls })),
      k: values,
    });
  }

  /** Decode and strictly validate a cursor against THIS pager's keyset, then
   *  build the seek predicate: rows strictly after the cursor position in
   *  the keyset ordering (mixed directions + null placement decomposed; ties
   *  compared with IS NOT DISTINCT FROM). */
  seekCondition(cursor: string): Condition {
    const payload = decodeCursorPayload(cursor);
    if (payload.o.length !== this.terms.length) {
      throw new CursorError(
        `cursor was created for a ${payload.o.length}-term keyset but this pager orders ${this.terms.length} terms`,
      );
    }
    for (let i = 0; i < payload.o.length; i++) {
      const want = this.terms[i];
      const got = payload.o[i];
      if (got.c !== want.column.columnName || got.d !== want.direction || got.n !== want.nulls) {
        const expected = this.terms.map((t) => `${t.column.columnName} ${t.direction} nulls ${t.nulls}`).join(", ");
        const actual = payload.o.map((t) => `${t.c} ${t.d} nulls ${t.n}`).join(", ");
        throw new CursorError(`cursor was not created for this keyset ordering — cursor: (${actual}); this pager: (${expected})`);
      }
    }
    // Validate every value against its column BEFORE building branches —
    // skipped branches (trailing nulls) must not skip validation, and a
    // cursor claiming null on a NOT NULL keyset column is malformed rather
    // than a legal "end of ordering".
    for (let i = 0; i < this.terms.length; i++) {
      validateCursorValue(this.terms[i], payload.k[i], this.tableName);
    }
    const branches: ValueNode[] = [];
    for (let i = 0; i < this.terms.length; i++) {
      // Ties on every earlier term, then strictly past term i. When term i's
      // value sits at the absolute end of ITS ordering (a trailing null),
      // nothing advances at i — but later branches can still tie through i
      // and advance later, so only this branch is skipped.
      const parts: ValueNode[] = [];
      for (let j = 0; j < i; j++) {
        parts.push(this.notDistinctFrom(this.terms[j], payload.k[j]));
      }
      const advance = this.advanceCondition(this.terms[i], payload.k[i]);
      if (advance === null) continue;
      parts.push(advance);
      branches.push(reduceBinary("and", parts));
    }
    if (branches.length === 0) {
      // The cursor sits at the absolute end of the whole ordering: nothing
      // can follow any tie-prefix.
      return fragment("1 = 0");
    }
    return reduceBinary("or", branches) as Condition;
  }

  /** Compose one page query: seek + order + limit(perPage + 1). `cursor`
   *  undefined starts from the beginning. Returns the forked builder.
   *  Fails closed when the builder already carries order terms or an
   *  offset (see keysetBuilderState). */
  apply<R>(builder: KeysetQueryable<R>, cursor: string | undefined, perPage?: number): KeysetQueryable<R> {
    assertFreshKeysetBuilder(builder, this.tableName);
    const size = validPerPage(perPage ?? this.defaultPerPage);
    let b: KeysetQueryable<R> = cursor === undefined ? builder : builder.where(this.seekCondition(cursor));
    b = b.orderBy(...this.orderExpressions());
    return b.limit(size + 1);
  }

  /** Run one page. Fetches perPage + 1 rows so hasMore is known without a
   *  count query; the returned page holds the first perPage rows and
   *  nextCursor encodes the page's LAST included row. */
  async page<R>(builder: KeysetQueryable<R>, cursor: string | undefined, perPage?: number): Promise<KeysetPage<R>> {
    const size = validPerPage(perPage ?? this.defaultPerPage);
    const rows = await this.apply(builder, cursor, perPage).execute();
    if (rows.length > size) {
      const pageRows = rows.slice(0, size);
      return { rows: pageRows, nextCursor: this.cursorOf(pageRows[pageRows.length - 1] as Record<string, unknown>) };
    }
    return { rows, nextCursor: null };
  }

  /** The tie comparison for one keyset term: plain `=` on NOT NULL columns
   *  (btree-friendly), `IS NOT DISTINCT FROM` on nullable ones (null = null
   *  ties). */
  private notDistinctFrom(term: KeysetColumn, v: CursorValue): ValueNode {
    const ref = qual(...tableRefParts(this.table), term.column.columnName);
    const param = this.valueParam(term, v);
    if (term.column.isNotNull || term.column.isPrimaryKey) {
      return exprNode("binary", "=", [ref, param]);
    }
    return fragment(ref, " is not distinct from ", param);
  }

  /** The advance predicate for one keyset term: rows strictly after `v` in
   *  this term's (direction, nulls) ordering, or null when nothing can
   *  follow (v is null at the trailing end position). `nulls last` places
   *  NULL at the END of the ordering in BOTH directions (and `nulls first`
   *  at the start) — the trailing check is direction-independent. NOT NULL
   *  columns get the plain comparison — the is-null disjunct would be dead
   *  text. */
  private advanceCondition(term: KeysetColumn, v: CursorValue): ValueNode | null {
    const ref = qual(...tableRefParts(this.table), term.column.columnName);
    const nullable = !(term.column.isNotNull || term.column.isPrimaryKey);
    const nullIsTrailing = term.nulls === "last";
    if (v.t === "null") {
      if (!nullable) return null; // a NOT NULL column never carries null values
      return nullIsTrailing ? null : fragment(ref, " is not null");
    }
    const op = term.direction === "asc" ? ">" : "<";
    const cmp = exprNode("binary", op, [ref, this.valueParam(term, v)]);
    return nullable && nullIsTrailing ? exprNode("binary", "or", [cmp, fragment(ref, " is null")]) : cmp;
  }

  /** A bind-ready param node for one keyset value (runs the column codec —
   *  validation plus canonical text casts for temporals — exactly like
   *  expr.ts comparison predicates). */
  private valueParam(term: KeysetColumn, v: CursorValue): ValueNode {
    const value = validateCursorValue(term, v, this.tableName);
    if (value === null) return paramNode(null);
    const ctx: ColumnContext = { propertyKey: term.propertyKey, columnName: term.column.columnName, tableName: this.tableName };
    const enc = encodeWriteValue(term.column, ctx, value);
    return enc.cast === undefined ? paramNode(enc.bind) : paramCast(enc.bind, enc.cast);
  }
}

/** Fail-closed composition guard (review-1 MAJOR-1): the seek predicate is
 *  only correct when the keyset ordering is the statement's TOTAL order —
 *  a leading user ORDER BY term or a persisting OFFSET silently duplicates
 *  and omits rows. Builders report their state through
 *  KeysetQueryable.keysetBuilderState(); anything that does not expose it
 *  cannot be verified and is rejected too. */
function assertFreshKeysetBuilder(builder: KeysetQueryable<unknown>, tableName: string): void {
  if (typeof builder.keysetBuilderState !== "function") {
    throw new Error(
      `keyset on ${tableName}: the builder passed to apply()/page() does not expose keysetBuilderState() — the pager cannot verify it is fresh; pass a select builder (db.select().from(table)) or expose the introspection on your wrapper`,
    );
  }
  const state = builder.keysetBuilderState();
  if (state.ordered || state.offset) {
    const found = [state.ordered ? "ORDER BY terms" : "", state.offset ? "an OFFSET" : ""].filter((s) => s !== "").join(" and ");
    throw new Error(
      `keyset on ${tableName}: the builder passed to apply()/page() already carries ${found} — the seek predicate is only correct when the keyset ordering is the statement's total ORDER BY, and a pre-existing OFFSET skips rows on every page. Combine instead: filter a FRESH builder (db.select().from(table).where(...)) and let the pager apply ordering and limit exclusively; to keep your own ordering, paginate the builder directly with .limit()/.offset() instead of the pager`,
    );
  }
}

function validPerPage(n: number): number {
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1) {
    throw new Error(`keyset perPage must be a positive safe integer, got ${JSON.stringify(n)}`);
  }
  return n;
}

function encodeRowValue(term: KeysetColumn, v: unknown, tableName: string): CursorValue {
  const dt = term.column.dataType;
  if (v === null) {
    if (term.column.isNotNull || term.column.isPrimaryKey) {
      throw new Error(`cursor on ${tableName}: keyset column "${term.propertyKey}" is NOT NULL but the row carries null — the row does not match this table's shape`);
    }
    return { t: "null" };
  }
  const who = `cursor on ${tableName} column "${term.propertyKey}"`;
  switch (dt) {
    case "bigint": {
      let s: string;
      if (typeof v === "bigint") s = v.toString();
      else if (typeof v === "string") s = v;
      else if (typeof v === "number" && Number.isSafeInteger(v)) s = String(v);
      else throw new Error(`${who}: expected a bigint/string/number int8 value, got ${typeof v}`);
      if (!INT8_RE.test(s)) throw new Error(`${who}: "${s}" is not an integer decimal string`);
      const asBig = BigInt(s);
      if (asBig < INT8_MIN || asBig > INT8_MAX) throw new Error(`${who}: int8 value ${s} is out of range`);
      return { t: "i8", s };
    }
    case "numeric": {
      if (typeof v !== "string" || !DECIMAL_RE.test(v)) {
        throw new Error(`${who}: expected the exact decimal string form, got ${JSON.stringify(v)}`);
      }
      return { t: "dec", s: v };
    }
    case "serial":
    case "integer":
    case "smallint": {
      if (typeof v !== "number" || !Number.isInteger(v)) throw new Error(`${who}: expected an integer number, got ${JSON.stringify(v)}`);
      return { t: "int", n: v };
    }
    case "double":
    case "real": {
      if (typeof v !== "number" || !Number.isFinite(v)) throw new Error(`${who}: expected a finite number, got ${JSON.stringify(v)}`);
      return { t: "flt", n: v };
    }
    case "text":
    case "varchar":
    case "uuid": {
      if (typeof v !== "string") throw new Error(`${who}: expected a string, got ${typeof v}`);
      return { t: "str", s: v };
    }
    case "timestamp": {
      if (typeof v !== "string" || (!CANONICAL_TIMESTAMP_RE.test(v) && !TEMPORAL_INFINITY_RE.test(v))) {
        throw new Error(`${who}: expected the canonical timestamp string (microseconds preserved), got ${JSON.stringify(v)}`);
      }
      return { t: "ts", s: v };
    }
    case "timestamptz": {
      if (typeof v !== "string" || (!CANONICAL_TIMESTAMPTZ_RE.test(v) && !TEMPORAL_INFINITY_RE.test(v))) {
        throw new Error(`${who}: expected the canonical UTC timestamptz string (microseconds preserved), got ${JSON.stringify(v)}`);
      }
      return { t: "tstz", s: v };
    }
    case "date": {
      if (typeof v !== "string" || !CANONICAL_DATE_RE.test(v)) {
        throw new Error(`${who}: expected a canonical YYYY-MM-DD date string, got ${JSON.stringify(v)}`);
      }
      return { t: "date", s: v };
    }
    case "boolean": {
      if (typeof v !== "boolean") throw new Error(`${who}: expected a boolean, got ${typeof v}`);
      return { t: "bool", b: v };
    }
    default:
      throw new Error(`${who}: ${dt} columns cannot serve as keyset columns`);
  }
}

/** Validate a decoded cursor value against its column's expected tag and
 *  string form; return the value to re-encode for binding. */
function validateCursorValue(term: KeysetColumn, v: CursorValue, tableName: string): unknown {
  const expected = valueTagOf(term.column.dataType);
  if (expected === null) throw new CursorError(`${tableName}: ${term.column.dataType} columns cannot serve as keyset columns`);
  if (v.t === "null") {
    if (term.column.isNotNull || term.column.isPrimaryKey) {
      throw new CursorError(`cursor on ${tableName}: column "${term.propertyKey}" is NOT NULL but the cursor carries null`);
    }
    return null;
  }
  if (v.t !== expected) {
    throw new CursorError(`cursor value for "${term.propertyKey}" carries tag "${v.t}" but the column is ${term.column.dataType} (expected "${expected}")`);
  }
  const who = `cursor on ${tableName} column "${term.propertyKey}"`;
  switch (v.t) {
    case "i8":
      if (!INT8_RE.test(v.s!)) throw new CursorError(`${who}: "${v.s}" is not an integer decimal string`);
      if (BigInt(v.s!) < INT8_MIN || BigInt(v.s!) > INT8_MAX) throw new CursorError(`${who}: int8 value ${v.s} is out of range`);
      return v.s!;
    case "dec":
      if (!DECIMAL_RE.test(v.s!)) throw new CursorError(`${who}: "${v.s}" is not a decimal string`);
      return v.s!;
    case "ts":
      if (!CANONICAL_TIMESTAMP_RE.test(v.s!) && !TEMPORAL_INFINITY_RE.test(v.s!)) throw new CursorError(`${who}: "${v.s}" is not a canonical timestamp string`);
      return v.s!;
    case "tstz":
      if (!CANONICAL_TIMESTAMPTZ_RE.test(v.s!) && !TEMPORAL_INFINITY_RE.test(v.s!)) throw new CursorError(`${who}: "${v.s}" is not a canonical timestamptz string`);
      return v.s!;
    case "date":
      if (!CANONICAL_DATE_RE.test(v.s!)) throw new CursorError(`${who}: "${v.s}" is not a canonical date string`);
      return v.s!;
    case "str":
      return v.s!;
    case "int":
      if (!Number.isInteger(v.n)) throw new CursorError(`${who}: cursor integer is not an integer`);
      return v.n;
    case "flt":
      if (!Number.isFinite(v.n)) throw new CursorError(`${who}: cursor float is not finite`);
      return v.n;
    case "bool":
      return v.b!;
  }
}

/** Build a keyset pager (see the module header for the full contract). */
export function keyset(table: AnyPgTable, order: readonly OrderSpec[], options?: KeysetOptions): KeysetPager {
  return KeysetPager.create(table, order, options);
}
