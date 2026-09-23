import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  integer,
  smallint,
  text,
  bigint,
  numeric,
  double,
  timestamp,
  timestamptz,
  date,
  boolean,
  jsonb,
  uniqueIndex,
  index,
  asc,
  desc,
  ascNullsLast,
  ascNullsFirst,
  descNullsLast,
  descNullsFirst,
  keyset,
  createDatabase,
  sql,
  CursorError,
  type KeysetPager,
  type KeysetQueryable,
} from "./index.js";

// Q04 unit suite: keyset construction/validation, seek-predicate SQL shapes,
// cursor encode/decode round-trips (precision + JSON-safety), malformed
// cursor rejection, and page() mechanics. Live behavior (no dup/omit walks,
// oracle twins, prepared statements) lives in live.pagination/live.prepared.

const events = pgTable("q04_events", {
  id: serial("id").primaryKey(),
  occurredAt: timestamptz("occurred_at"),
  rank: integer("rank"),
  label: text("label").notNull(),
});

const logs = pgTable("q04_logs", {
  id: serial("id").primaryKey(),
  seq: integer("seq"),
  day: date("day"),
  note: text("note"),
});

const big = pgTable("q04_big", {
  id: bigint("id", { mode: "string" }).primaryKey(),
  amt: numeric("amt"),
  when: timestamp("when"),
  label: text("label").notNull(),
});

const duo = pgTable(
  "q04_duo",
  {
    a: integer("a").notNull(),
    b: integer("b").notNull(),
    v: text("v"),
  },
  (t) => [uniqueIndex("q04_duo_ab").on(t.a, t.b)],
);

const plain = pgTable("q04_plain", {
  x: integer("x"),
  y: integer("y"),
});

const noKeys = pgTable("q04_nokeys", {
  x: integer("x"),
});

const weird = pgTable("q04_weird", {
  id: serial("id").primaryKey(),
  payload: text("payload"),
  doc: jsonb("doc"),
  tsDate: timestamp("ts_date", { mode: "date" }),
  amt: numeric("amt", { decoder: (s) => Number(s) }),
  ratio: double("ratio"),
  flag: boolean("flag"),
  small: smallint("small"),
});

const db = await createDatabase({ url: "postgres://snapshot:nouser@127.0.0.1:1/none", driverOptions: { driver: "postgres" } });

// ---------------------------------------------------------------------------
// Nulls-explicit ORDER BY rendering
// ---------------------------------------------------------------------------

test("pagination: order specs with explicit nulls render nulls first/last", () => {
  const { sql } = db
    .select()
    .from(events)
    .orderBy(ascNullsLast(events.occurredAt), descNullsFirst(events.rank), asc(events.label))
    .toSQL();
  assert.equal(
    sql.slice(sql.indexOf(" order by ")),
    ' order by "q04_events"."occurred_at" asc nulls last, "q04_events"."rank" desc nulls first, "q04_events"."label" asc',
  );
});

// ---------------------------------------------------------------------------
// keyset() validation
// ---------------------------------------------------------------------------

test("pagination: keyset requires explicit null ordering on every term", () => {
  assert.throws(
    () => keyset(events, [asc(events.occurredAt)]),
    /needs EXPLICIT null ordering/,
  );
  assert.throws(
    () => keyset(events, [desc(events.id)]),
    /needs EXPLICIT null ordering/,
  );
});

test("pagination: keyset order terms must be plain columns of the table", () => {
  assert.throws(() => keyset(events, [ascNullsLast(plain.x)]), /must be a plain column reference of q04_events/);
  assert.throws(() => keyset(events, [ascNullsLast(events.label), ascNullsFirst(events.label)]), /appears twice/);
});

test("pagination: unsupported keyset column types and modes fail closed", () => {
  assert.throws(() => keyset(weird, [ascNullsFirst(weird.doc)]), /no total btree ordering/);
  assert.throws(() => keyset(weird, [ascNullsFirst(weird.tsDate)]), /Date read mode/);
  assert.throws(() => keyset(weird, [ascNullsFirst(weird.amt)]), /user decoder/);
});

test("pagination: unique tie-breaker auto-append (single-column PK)", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.rank)]);
  assert.deepEqual(
    pager.columns.map((c) => [c.propertyKey, c.direction, c.nulls, c.autoAppended]),
    [["occurredAt", "asc", "last", false], ["rank", "desc", "first", false], ["id", "asc", "last", true]],
  );
});

test("pagination: composite unique index contained in the keyset means unique", () => {
  const pager = keyset(duo, [ascNullsFirst(duo.b), descNullsLast(duo.a)]);
  assert.deepEqual(pager.columns.map((c) => c.propertyKey), ["b", "a"], "no tie-breaker appended: (a,b) unique index is contained");
});

test("pagination: composite unique index auto-appends its missing columns", () => {
  const pager = keyset(duo, [ascNullsFirst(duo.v)]);
  assert.deepEqual(pager.columns.map((c) => c.propertyKey), ["v", "a", "b"]);
});

test("pagination: non-unique keyset without any unique key errors", () => {
  assert.throws(
    () => keyset(noKeys, [ascNullsLast(noKeys.x)]),
    /not a unique ordering and no schema-declared unique key/,
  );
  assert.throws(
    () => keyset(plain, [ascNullsLast(plain.x)]),
    /not a unique ordering and no schema-declared unique key can be auto-appended/,
  );
});

test("pagination: autoAppendTiebreaker false demands a unique keyset", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.id)], { autoAppendTiebreaker: false });
  assert.equal(pager.columns.length, 2);
  assert.throws(
    () => keyset(events, [ascNullsLast(events.occurredAt)], { autoAppendTiebreaker: false }),
    /autoAppendTiebreaker is false/,
  );
});

// ---------------------------------------------------------------------------
// Seek predicate SQL shapes (hand-written expectations)
// ---------------------------------------------------------------------------

test("pagination: seek decomposition, mixed directions + nulls, pins SQL", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.rank)]);
  const cursor = pager.cursorOf({ id: 7, occurredAt: "2026-09-23T10:00:00.123456Z", rank: null, label: "x" });
  const { sql, params } = db.select().from(events).where(pager.seekCondition(cursor)).toSQL();
  assert.equal(
    sql,
    'select "q04_events"."id", to_jsonb("q04_events"."occurred_at" at time zone \'UTC\')::text as "occurredAt", "q04_events"."rank", "q04_events"."label" from "q04_events" ' +
      "where ((((" +
      '"q04_events"."occurred_at" > $1::text::timestamptz) or ("q04_events"."occurred_at" is null)) or ' +
      '(("q04_events"."occurred_at" is not distinct from $2::text::timestamptz) and ("q04_events"."rank" is not null))) or ' +
      '((("q04_events"."occurred_at" is not distinct from $3::text::timestamptz) and ("q04_events"."rank" is not distinct from $4)) and ("q04_events"."id" > $5)))',
  );
  assert.deepEqual(params, ["2026-09-23T10:00:00.123456Z", "2026-09-23T10:00:00.123456Z", "2026-09-23T10:00:00.123456Z", null, 7]);
});

test("pagination: seek with a null first-term value ties through it", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.rank)]);
  const cursor = pager.cursorOf({ id: 7, occurredAt: null, rank: 5, label: "x" });
  const { sql, params } = db.select().from(events).where(pager.seekCondition(cursor)).toSQL();
  assert.equal(
    sql,
    'select "q04_events"."id", to_jsonb("q04_events"."occurred_at" at time zone \'UTC\')::text as "occurredAt", "q04_events"."rank", "q04_events"."label" from "q04_events" ' +
      'where ((("q04_events"."occurred_at" is not distinct from $1) and ("q04_events"."rank" < $2)) or ' +
      '((("q04_events"."occurred_at" is not distinct from $3) and ("q04_events"."rank" is not distinct from $4)) and ("q04_events"."id" > $5)))',
  );
  assert.deepEqual(params, [null, 5, null, 5, 7]);
});

test("pagination: trailing-null cursor under nulls-last-at-end yields 1 = 0", () => {
  const t2 = pgTable("q04_t2", {
    id: serial("id").primaryKey(),
    v: integer("v").notNull().unique(),
  });
  const pager = keyset(t2, [ascNullsLast(t2.v)]);
  const cursor = pager.cursorOf({ id: 3, v: 42 });
  void cursor;
  // v is unique: (v asc nulls last) with v NOT NULL never reaches a trailing
  // null — use the nullable variant to reach the absolute end.
  const t3 = pgTable("q04_t3", {
    id: serial("id").primaryKey(),
    v: integer("v").unique(),
  });
  const pager3 = keyset(t3, [ascNullsLast(t3.v)]);
  const cursor3 = pager3.cursorOf({ id: 3, v: null });
  const { sql } = db.select().from(t3).where(pager3.seekCondition(cursor3)).toSQL();
  assert.ok(sql.includes("where (1 = 0)"), sql);
});

test("pagination: desc nulls-first advance is < without nulls (they lead)", () => {
  const pager = keyset(events, [descNullsFirst(events.rank)]);
  const cursor = pager.cursorOf({ id: 1, occurredAt: null, rank: 4, label: "x" });
  const { sql } = db.select().from(events).where(pager.seekCondition(cursor)).toSQL();
  assert.equal(
    sql.slice(sql.indexOf("where")),
    'where (("q04_events"."rank" < $1) or (("q04_events"."rank" is not distinct from $2) and ("q04_events"."id" > $3)))',
  );
});

test("pagination: all four (direction, nulls) placements decompose per PG orderings", () => {
  const t = pgTable("q04_place", { id: serial("id").primaryKey(), v: integer("v").unique() });
  const cases: Array<[string, ReturnType<typeof ascNullsLast>, number | null, string]> = [
    ["asc/last", ascNullsLast(t.v), null, "where (1 = 0)"],
    ["asc/last", ascNullsLast(t.v), 5, 'where (("q04_place"."v" > $1) or ("q04_place"."v" is null))'],
    ["asc/first", ascNullsFirst(t.v), null, 'where ("q04_place"."v" is not null)'],
    ["asc/first", ascNullsFirst(t.v), 5, 'where ("q04_place"."v" > $1)'],
    ["desc/last", descNullsLast(t.v), null, "where (1 = 0)"],
    ["desc/last", descNullsLast(t.v), 5, 'where (("q04_place"."v" < $1) or ("q04_place"."v" is null))'],
    ["desc/first", descNullsFirst(t.v), null, 'where ("q04_place"."v" is not null)'],
    ["desc/first", descNullsFirst(t.v), 5, 'where ("q04_place"."v" < $1)'],
  ];
  for (const [name, spec, v, expectedWhere] of cases) {
    const pager = keyset(t, [spec]);
    const cursor = pager.cursorOf({ id: 1, v });
    const { sql } = db.select().from(t).where(pager.seekCondition(cursor)).toSQL();
    assert.equal(sql.slice(sql.indexOf("where")), expectedWhere, `${name} after ${v}`);
  }
});

// ---------------------------------------------------------------------------
// Cursor precision + JSON safety
// ---------------------------------------------------------------------------

test("pagination: cursor round-trips int8 max exactly (string mode)", () => {
  const pager = keyset(big, [descNullsLast(big.id)]);
  const row = { id: "9223372036854775807", amt: null, when: null, label: "x" };
  const cursor = pager.cursorOf(row);
  const { params } = db.select().from(big).where(pager.seekCondition(cursor)).toSQL();
  assert.deepEqual(params, ["9223372036854775807"]);
});

test("pagination: cursor round-trips int8 max exactly (bigint mode)", () => {
  const t8 = pgTable("q04_t8", { id: bigint("id").primaryKey() });
  const pager = keyset(t8, [descNullsLast(t8.id)]);
  const cursor = pager.cursorOf({ id: 9223372036854775807n });
  const { params } = db.select().from(t8).where(pager.seekCondition(cursor)).toSQL();
  assert.deepEqual(params, ["9223372036854775807"]);
});

test("pagination: cursor round-trips 40-digit numerics and microsecond temporals", () => {
  const pager = keyset(big, [ascNullsFirst(big.amt), descNullsLast(big.when)]);
  const row = { id: 1n, amt: "-9998887776665554443332221110009998887776.5", when: "2027-07-14T13:45:22.123456", label: "x" };
  const cursor = pager.cursorOf(row);
  const { params } = db.select().from(big).where(pager.seekCondition(cursor)).toSQL();
  assert.deepEqual(params, [
    "-9998887776665554443332221110009998887776.5",
    "-9998887776665554443332221110009998887776.5",
    "2027-07-14T13:45:22.123456",
    "-9998887776665554443332221110009998887776.5",
    "2027-07-14T13:45:22.123456",
    "1",
  ]);
});

test("pagination: cursors are JSON-safe base64url and deterministic", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)]);
  const row = { id: 7, occurredAt: "2026-09-23T10:00:00.123456Z", rank: null, label: "a\"b\\c\n\d" };
  const cursor = pager.cursorOf(row);
  assert.match(cursor, /^[A-Za-z0-9_-]+$/);
  assert.equal(JSON.parse(JSON.stringify({ cursor })).cursor, cursor, "cursor survives JSON.stringify/parse unchanged");
  assert.equal(pager.cursorOf(row), cursor, "encoding is deterministic");
});

test("pagination: NOT NULL keyset column with a null row value fails", () => {
  const pager = keyset(events, [ascNullsLast(events.label)]);
  assert.throws(() => pager.cursorOf({ id: 1, occurredAt: null, rank: null, label: null }), /NOT NULL but the row carries null/);
});

test("pagination: cursorOf rejects rows that do not project a keyset column", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)]);
  assert.throws(() => pager.cursorOf({ id: 1 }), /does not project keyset column/);
});

// ---------------------------------------------------------------------------
// Malformed / mismatched cursors
// ---------------------------------------------------------------------------

function assertCursorError(pager: KeysetPager, cursor: string, re: RegExp): void {
  assert.throws(() => pager.seekCondition(cursor), (err: unknown) => {
    assert.ok(err instanceof CursorError, `expected CursorError, got ${err}`);
    assert.match(err.message, re);
    return true;
  });
}

test("pagination: malformed cursor variants fail clearly", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)]);
  assertCursorError(pager, "", /non-empty string/);
  assertCursorError(pager, "!!not base64!!", /not valid base64url/);
  assertCursorError(pager, "AA", /too short/);
  assertCursorError(pager, "aGVsbG8", /version 104 is not supported/); // 'h' = 104
  assertCursorError(pager, "AQ", /not valid JSON|too short/); // v1 + 0x10 junk
  // v1 + "{\"v\":2,...}" — payload v mismatches the version byte
  assertCursorError(pager, BufferLike('{"v":2,"o":[],"k":[]}'), /does not have the expected shape/);
  // valid envelope, but the value tag lies about the column type
  const lie = pager.cursorOf({ id: 1, occurredAt: null, rank: null, label: "x" });
  void lie;
});

function BufferLike(json: string): string {
  // version byte 1 + json, base64url — hand-built without the encoder
  const bytes = [1, ...Array.from(new TextEncoder().encode(json))];
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

test("pagination: tag-forged and shape-forged cursors fail", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)]);
  // occurred_at is timestamptz (tag tstz) — a str tag is rejected
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"str","s":"2026-01-01T00:00:00Z"},{"t":"int","n":1}]}'), /carries tag "str" but the column is timestamptz/);
  // non-canonical timestamp text
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"tstz","s":"Jan 1 2026"},{"t":"int","n":1}]}'), /not a canonical timestamptz string/);
  // count mismatch
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[]}'), /2 terms but 0 values/);
  // unknown tag
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"bogus","s":"x"},{"t":"int","n":1}]}'), /unknown tag/);
  // extra fields
  // extra fields on a value object
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"tstz","s":"2026-01-01T00:00:00Z","z":1},{"t":"int","n":1}]}'), /unexpected fields/);
  // extra fields on the top-level envelope (review-1 LOW-1: same strictness)
  assertCursorError(pager, BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"null"},{"t":"int","n":1}],"x":1}'), /unexpected fields \(x\)/);
  // NOT NULL column carrying null (id is the auto tie-breaker)
  const twoTerm = BufferLike('{"v":1,"o":[{"c":"occurred_at","d":"asc","n":"last"},{"c":"id","d":"asc","n":"last"}],"k":[{"t":"null"},{"t":"null"}]}');
  assertCursorError(pager, twoTerm, /is NOT NULL but the cursor carries null/);
});

test("pagination: cursors from a different keyset ordering are rejected", () => {
  const pagerA = keyset(events, [ascNullsLast(events.occurredAt), descNullsFirst(events.rank)]);
  const pagerB = keyset(events, [ascNullsLast(events.occurredAt), ascNullsFirst(events.rank)]);
  const cursor = pagerA.cursorOf({ id: 1, occurredAt: null, rank: 2, label: "x" });
  assertCursorError(pagerB, cursor, /was not created for this keyset ordering/);
  const pagerC = keyset(events, [ascNullsFirst(events.rank), ascNullsLast(events.occurredAt)]);
  assertCursorError(pagerC, cursor, /was not created for this keyset ordering|terms/);
});

// ---------------------------------------------------------------------------
// apply()/page() mechanics
// ---------------------------------------------------------------------------

test("pagination: apply composes seek + order + limit(perPage + 1) immutably", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)], { perPage: 20 });
  const base = db.select().from(events);
  const before = base.toSQL().sql;
  const cursor = pager.cursorOf({ id: 3, occurredAt: "2026-01-01T00:00:00Z", rank: null, label: "x" });
  const applied = pager.apply(base, cursor);
  const { sql } = (applied as unknown as { toSQL(): { sql: string } }).toSQL();
  assert.ok(sql.includes("order by") && sql.includes("asc nulls last"));
  assert.ok(sql.endsWith(" limit 21"), sql);
  assert.equal(base.toSQL().sql, before, "apply must not mutate the source builder");
  const first = pager.apply(base, undefined, 7);
  assert.ok((first as unknown as { toSQL(): { sql: string } }).toSQL().sql.endsWith(" limit 8"));
});

test("pagination: page() fetches one extra row and derives nextCursor from the last included row", async () => {
  const pager = keyset(events, [ascNullsLast(events.id)], { perPage: 2 });
  const rows = [
    { id: 1, occurredAt: null, rank: null, label: "a" },
    { id: 2, occurredAt: null, rank: null, label: "b" },
    { id: 3, occurredAt: null, rank: null, label: "c" },
  ];
  let sawLimit = -1;
  const fake: KeysetQueryable<Record<string, unknown>> = {
    where(condition) {
      void condition;
      return fake;
    },
    orderBy(...exprs) {
      void exprs;
      return fake;
    },
    limit(n) {
      sawLimit = n;
      return fake;
    },
    keysetBuilderState() {
      return { ordered: false, offset: false };
    },
    async execute() {
      return rows;
    },
  };
  const page = await pager.page(fake, undefined);
  assert.equal(sawLimit, 3, "perPage 2 -> limit 3");
  assert.equal(page.rows.length, 2);
  assert.deepEqual(page.rows, rows.slice(0, 2));
  assert.equal(page.nextCursor, pager.cursorOf(rows[1]), "nextCursor encodes the LAST INCLUDED row");
  // last page: no extra row -> null cursor
  const shortRows = [rows[0]];
  const fakeShort: KeysetQueryable<Record<string, unknown>> = {
    where: () => fakeShort,
    orderBy: () => fakeShort,
    limit: () => fakeShort,
    keysetBuilderState: () => ({ ordered: false, offset: false }),
    async execute() {
      return shortRows;
    },
  };
  const last = await pager.page(fakeShort, undefined);
  assert.equal(last.nextCursor, null);
  assert.equal(last.rows.length, 1);
});

test("pagination: perPage is validated", () => {
  const pager = keyset(events, [ascNullsLast(events.id)]);
  assert.throws(() => pager.apply(db.select().from(events), undefined, 0), /positive safe integer/);
  assert.throws(() => keyset(events, [ascNullsLast(events.id)], { perPage: 1.5 }), /positive safe integer/);
});

// ---------------------------------------------------------------------------
// Fail-closed composition guard (review-1 MAJOR-1)
// ---------------------------------------------------------------------------

test("pagination: apply()/page() fail closed on builders carrying order or offset", async () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)], { perPage: 5 });
  assert.throws(
    () => pager.apply(db.select().from(events).orderBy(events.id), undefined),
    /already carries ORDER BY terms/,
  );
  assert.throws(
    () => pager.apply(db.select().from(events).offset(5), undefined),
    /already carries an OFFSET/,
  );
  assert.throws(
    () => pager.apply(db.select().from(events).orderBy(events.id).offset(5), undefined),
    /already carries ORDER BY terms and an OFFSET/,
  );
  await assert.rejects(
    () => pager.page(db.select().from(events).orderBy(events.id), undefined),
    /already carries ORDER BY terms/,
  );
  // set-op compounds share the hazard: user order on the compound leads the keyset
  const compound = db.select().from(events).union(db.select().from(events)).orderBy(events.id);
  assert.throws(
    () => pager.apply(compound as unknown as KeysetQueryable<Record<string, unknown>>, undefined),
    /already carries ORDER BY terms/,
  );
  // a foreign queryable without the introspection hook gets a precise error
  // (not a TypeError deep in composition)
  const bare = { where: () => bare, orderBy: () => bare, limit: () => bare, execute: async () => [] };
  assert.throws(
    () => pager.apply(bare as unknown as KeysetQueryable<Record<string, unknown>>, undefined),
    /does not expose keysetBuilderState/,
  );
});

test("pagination: documented combination — where filters compose, the pager's ordering leads exclusively", () => {
  const pager = keyset(events, [ascNullsLast(events.occurredAt)], { perPage: 5 });
  const cursor = pager.cursorOf({ id: 3, occurredAt: "2026-01-01T00:00:00Z", rank: null, label: "x" });
  const applied = pager.apply(db.select().from(events).where(sql`${events.id} > ${2}`), cursor);
  const composed = (applied as unknown as { toSQL(): { sql: string } }).toSQL().sql;
  assert.ok(composed.includes('"q04_events"."id" > $1'), "the user filter survives composition");
  const orderClause = composed.slice(composed.indexOf(" order by "), composed.indexOf(" limit "));
  assert.equal(
    orderClause,
    ' order by "q04_events"."occurred_at" asc nulls last, "q04_events"."id" asc nulls last',
    "ORDER BY is exactly the keyset ordering — no user term leads it",
  );
  assert.ok(composed.endsWith(" limit 6"), "the pager's limit (perPage + 1) applies exclusively");
});

test("pagination: floats, booleans, smallints and dates encode and re-bind", () => {
  const pager = keyset(weird, [ascNullsLast(weird.ratio), descNullsFirst(weird.flag), ascNullsLast(weird.small)]);
  const row = { id: 1, payload: null, doc: null, tsDate: null, amt: null, ratio: 1.5, flag: null, small: -3 };
  const cursor = pager.cursorOf(row);
  const { params } = db.select().from(weird).where(pager.seekCondition(cursor)).toSQL();
  assert.deepEqual(params, [1.5, 1.5, 1.5, null, -3, 1.5, null, -3, 1]);
});

test("pagination: date keyset column round-trips", () => {
  const pager = keyset(logs, [ascNullsFirst(logs.day), descNullsLast(logs.seq)]);
  const row = { id: 9, seq: 4, day: "2024-02-29", note: "leap" };
  const cursor = pager.cursorOf(row);
  const { params } = db.select().from(logs).where(pager.seekCondition(cursor)).toSQL();
  assert.deepEqual(params, ["2024-02-29", "2024-02-29", 4, "2024-02-29", 4, 9]);
});
