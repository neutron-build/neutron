import assert from "node:assert/strict";
import test from "node:test";
import {
  asc,
  countDistinct,
  createDatabase,
  desc,
  eq,
  integer,
  pgTable,
  serial,
  sql,
  sum,
  text,
  timestamp,
  timestamptz,
  bigint,
  numeric,
  cteTable,
  lte,
  compileStatement,
  updateStatement,
  projection,
  param,
  alias,
  over,
  rowNumber,
  rank,
  descNullsLast,
  ntile,
  lag,
  nthValue,
  firstValue,
  isWindowExpr,
  containsWindow,
  type Projection,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q08 — window functions (unit leg). Deterministic SQL text, structural
// validation, placement enforcement at the compile choke point, capability
// collection and decode-plan wiring. Live V13 oracles against hand-written
// SQL live in live.q08.postgres.test.ts.
// ---------------------------------------------------------------------------

const events = pgTable("w_events", {
  id: serial("id").primaryKey(),
  actor: integer("actor").notNull(),
  day: integer("day").notNull(),
  amount: bigint("amount"),
  price: numeric("price"),
  note: text("note"),
  seen: timestamptz("seen"),
  logged: timestamp("logged"),
});

const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { w_events: events },
});

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

test("window: row_number over partition/order renders as hand-written SQL with exact params", () => {
  const q = db
    .select({
      id: events.id,
      rn: over(rowNumber(), { partitionBy: [events.actor], orderBy: [desc(events.amount), asc(events.id)] }),
    })
    .from(events)
    .toSQL();
  assert.equal(
    q.sql,
    'select "w_events"."id", row_number() over (partition by "w_events"."actor" order by "w_events"."amount" desc, "w_events"."id" asc) as "rn" from "w_events"',
  );
  assert.deepEqual(q.params, []);
});

test("window: lag renders its offset as a parameter", () => {
  const q = db
    .select({ prev: over(lag(events.amount, 2), { orderBy: [asc(events.day)] }) })
    .from(events)
    .toSQL();
  assert.equal(
    q.sql,
    'select lag("w_events"."amount", $1) over (order by "w_events"."day" asc) as "prev" from "w_events"',
  );
  assert.deepEqual(q.params, [2]);
});

test("window: ntile binds the bucket count", () => {
  const q = db.select({ b: over(ntile(4), { orderBy: [asc(events.day)] }) }).from(events).toSQL();
  assert.equal(q.sql, 'select ntile($1) over (order by "w_events"."day" asc) as "b" from "w_events"');
  assert.deepEqual(q.params, [4]);
});

test("window: frame variants render exactly (rows offsets, range, groups, exclude)", () => {
  const rows = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "rows", start: { preceding: 1 }, end: { following: 2 } } }) })
    .from(events)
    .toSQL();
  assert.equal(
    rows.sql,
    'select sum("w_events"."amount") over (order by "w_events"."day" asc rows between 1 preceding and 2 following) as "s" from "w_events"',
  );

  const range = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "range", start: "unbounded preceding", end: "current row" } }) })
    .from(events)
    .toSQL();
  assert.equal(
    range.sql,
    'select sum("w_events"."amount") over (order by "w_events"."day" asc range between unbounded preceding and current row) as "s" from "w_events"',
  );

  const groups = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "groups", start: "unbounded preceding", exclude: "ties" } }) })
    .from(events)
    .toSQL();
  assert.equal(
    groups.sql,
    'select sum("w_events"."amount") over (order by "w_events"."day" asc groups unbounded preceding exclude ties) as "s" from "w_events"',
  );

  const endOmitted = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "rows", start: "unbounded preceding" } }) })
    .from(events)
    .toSQL();
  assert.equal(
    endOmitted.sql,
    'select sum("w_events"."amount") over (order by "w_events"."day" asc rows unbounded preceding) as "s" from "w_events"',
  );

  const excludeExplicit = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "rows", start: "unbounded preceding", end: "current row", exclude: "current row" } }) })
    .from(events)
    .toSQL();
  assert.equal(
    excludeExplicit.sql,
    'select sum("w_events"."amount") over (order by "w_events"."day" asc rows between unbounded preceding and current row exclude current row) as "s" from "w_events"',
  );
});

test("window: nulls ordering in window order terms", () => {
  const q = db
    .select({ rn: over(rowNumber(), { orderBy: [descNullsLast(events.amount)] }) })
    .from(events)
    .toSQL();
  assert.equal(q.sql, 'select row_number() over (order by "w_events"."amount" desc nulls last) as "rn" from "w_events"');
});

test("window: aggregate over () with empty spec", () => {
  const q = db.select({ total: over(sum(events.amount)) }).from(events).toSQL();
  assert.equal(q.sql, 'select sum("w_events"."amount") over () as "total" from "w_events"');
});

// ---------------------------------------------------------------------------
// Structural validation (fails before any SQL renders)
// ---------------------------------------------------------------------------

test("window: frame validation rejects impossible and unsupported shapes", () => {
  const s = { orderBy: [asc(events.day)] };
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: "unbounded following" } }), /cannot start at unbounded following/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: { following: 1 }, end: { preceding: 1 } } }), /comes after/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "range", start: { preceding: 1 } } }), /range mode with value offsets/);
  assert.throws(() => over(sum(events.amount), { frame: { mode: "groups", start: "unbounded preceding" } }), /groups mode requires an orderBy/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "wobbly" as never, start: "current row" } }), /mode must be/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: "somewhere" as never } }), /unknown frame bound/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: { preceding: 1.5 } } }), /safe integer/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: { preceding: -1 } } }), /safe integer/);
  assert.throws(() => over(sum(events.amount), { ...s, frame: { mode: "rows", start: "current row", exclude: "peers" as never } }), /exclude must be/);
  assert.throws(() => over(sum(events.amount), { unknown: 1 } as object), /unknown spec key/);
});

test("window: over() rejects non-window/non-aggregate input", () => {
  assert.throws(() => over(42 as never, {}), /takes a window function/);
  assert.throws(() => over(events.amount as never, {}), /takes a window function/);
});

test("window: DISTINCT aggregate cannot be a window function (PostgreSQL limitation)", () => {
  assert.throws(() => over(countDistinct(events.actor), {}), /DISTINCT aggregates cannot be window functions/);
});

test("window: nested window calls are rejected (PostgreSQL 42P20 class)", () => {
  const inner = over(rowNumber(), { orderBy: [asc(events.day)] });
  assert.throws(() => over(rowNumber(), { orderBy: [asc(events.day)], partitionBy: [inner as never] }), /cannot be nested/);
  const ranked = over(rank(), {});
  assert.throws(() => over(sum(events.amount), { partitionBy: [ranked as never] }), /cannot be nested/);
});

test("window: forged window-function nodes fail over()", () => {
  const forged = { kind: "window-function", op: "count_all", args: [] };
  assert.throws(() => over(forged as never, {}), /takes a window function/);
});

test("window: lag/lead/first/last/nth require a schema column", () => {
  assert.throws(() => lag(sql`x` as never), /takes a schema column/);
  assert.throws(() => nthValue(events.amount, 0), /positive safe integer/);
  assert.throws(() => lag(events.amount, -1), /non-negative safe integer/);
  assert.throws(() => ntile(0), /positive safe integer/);
});

// ---------------------------------------------------------------------------
// Placement: the compile choke point (SQLSTATE 42P20 class)
// ---------------------------------------------------------------------------

test("window: allowed in select list and ORDER BY", () => {
  const rn = over(rowNumber(), { orderBy: [asc(events.day)] });
  const q = db.select({ id: events.id, rn }).from(events).orderBy(desc(rn)).toSQL();
  assert.match(q.sql, /row_number\(\) over/);
  assert.match(q.sql, /order by row_number\(\) over/);
});

test("window: rejected in WHERE, GROUP BY, HAVING", () => {
  const rn = over(rowNumber(), {});
  assert.throws(() => db.select().from(events).where(sql`${rn} > ${1}`).toSQL(), /not allowed in WHERE/);
  assert.throws(() => db.select({ a: events.actor }).from(events).groupBy(rn as never).toSQL(), /not allowed in GROUP BY/);
  assert.throws(() => db.select({ a: events.actor }).from(events).groupBy(events.actor).having(sql`${rn} > ${1}`).toSQL(), /not allowed in HAVING/);
});

test("window: rejected in JOIN ON", () => {
  const rn = over(rowNumber(), {});
  const other = pgTable("w_other", { id: integer("id").primaryKey() });
  assert.throws(
    () =>
      db.select({ id: events.id }).from(events).leftJoin(alias(other, "o"), sql`${rn} = ${1}`).toSQL(),
    /not allowed in JOIN ON/,
  );
});

test("window: rejected in UPDATE SET / WHERE, DELETE WHERE, and DML RETURNING via AST nodes", () => {
  const rn = over(rowNumber(), {});
  assert.throws(() => db.update(events).set({ note: rn } as never).where(eq(events.id, 1)).toSQL(), /not allowed in UPDATE SET/);
  assert.throws(() => db.update(events).set({ note: "x" }).where(sql`${rn} > ${1}`).toSQL(), /not allowed in WHERE/);
  assert.throws(() => db.delete(events).where(sql`${rn} > ${1}`).toSQL(), /not allowed in WHERE/);
  // RETURNING placement is enforced at the compile choke point: a window in
  // an UPDATE's returning projection fails compileStatement before SQL runs.
  const stmt = updateStatement({
    table: sql`${events}` as never,
    sets: [{ column: "note", value: param("x") }],
    where: [sql`${events.id} = ${1}`],
    returning: [projection(rn, "rn")],
  });
  assert.throws(() => compileStatement(stmt), /not allowed in RETURNING/);
});

test("window: a subquery in a projection carries its own placement rules", () => {
  const inner = db.select({ rn: over(rowNumber(), { orderBy: [asc(events.day)] }), id: events.id }).from(events);
  const q = db.select({ id: inner.subquery() }).from(events).toSQL();
  assert.match(q.sql, /row_number\(\) over/);
  // Window inside the subquery's own WHERE is still rejected at compile time.
  const badInner = db.select().from(events).where(sql`${over(rowNumber(), {})} > ${1}`);
  assert.throws(() => badInner.toSQL(), /not allowed in WHERE/);
});

// ---------------------------------------------------------------------------
// Capabilities and decode plans
// ---------------------------------------------------------------------------

test("window: capability requirements collect into the compiled statement", () => {
  const plain = db.select({ rn: over(rowNumber(), {}) }).from(events);
  assert.deepEqual(plain.toCompiled().capabilities, ["window-functions"]);

  const grouped = db
    .select({ s: over(sum(events.amount), { orderBy: [asc(events.day)], frame: { mode: "groups", start: "unbounded preceding", end: "current row", exclude: "ties" } }) })
    .from(events);
  assert.deepEqual(new Set(grouped.toCompiled().capabilities), new Set(["window-functions", "window-frame-groups", "window-frame-exclude"]));
});

test("window: windows nested in larger expressions still add capabilities", () => {
  const rn = over(rowNumber(), {});
  const q = db.select({ bucket: sql`case when ${rn} <= ${3} then 1 else 0 end` }).from(events);
  assert.deepEqual(q.toCompiled().capabilities, ["window-functions"]);
});

test("window: window-only functions cannot project bare (must be wrapped by over)", () => {
  assert.throws(
    () => db.select({ rn: rowNumber() as never as Projection[string] }).from(events).toSQL(),
    /cannot render bare.*over\(\)/s,
  );
});

test("window: temporal value functions take the lossless to_jsonb wire form", () => {
  const q = db.select({ prev: over(lag(events.seen), { orderBy: [asc(events.id)] }) }).from(events).toSQL();
  assert.match(q.sql, /to_jsonb\(lag\("w_events"."seen", \$1\) over \(order by "w_events"\."id" asc\) at time zone 'UTC'\)::text/);
  const q2 = db.select({ first: over(firstValue(events.logged), { orderBy: [asc(events.id)] }) }).from(events).toSQL();
  assert.match(q2.sql, /to_jsonb\(first_value\("w_events"\."logged"\) over \(order by "w_events"\."id" asc\)\)::text/);
});

test("window: Q02 composition — window over a CTE, outer query filters the window result", () => {
  const ranked = cteTable(
    "ranked",
    db.select({
      id: events.id,
      actor: events.actor,
      rn: over(rowNumber(), { partitionBy: [events.actor], orderBy: [desc(events.amount)] }),
    }).from(events),
  );
  const q = db.select({ id: ranked.id, actor: ranked.actor }).from(ranked).where(lte(ranked.rn, 2)).toSQL();
  assert.equal(
    q.sql,
    'with "ranked" as (select "w_events"."id", "w_events"."actor", row_number() over (partition by "w_events"."actor" order by "w_events"."amount" desc) as "rn" from "w_events") select "ranked"."id", "ranked"."actor" from "ranked" where ("ranked"."rn" <= $1)',
  );
  assert.deepEqual(q.params, [2]);
});

test("window: aggregates compose with windows in one projection", () => {
  const q = db
    .select({
      actor: events.actor,
      total: sum(events.amount),
      shareRank: over(rank(), { partitionBy: [events.actor] }),
    })
    .from(events)
    .groupBy(events.actor)
    .toSQL();
  assert.match(q.sql, /group by "w_events"\."actor"/);
  assert.match(q.sql, /rank\(\) over \(partition by "w_events"\."actor"\)/);
});

test("window: isWindowExpr/containsWindow identify window expressions structurally", () => {
  const w = over(rowNumber(), {});
  assert.equal(isWindowExpr(w), true);
  assert.equal(isWindowExpr(sql`row_number() over ()`), false);
  assert.equal(containsWindow(w), true);
  assert.equal(containsWindow(sql`1 + ${w}` as never), true);
  assert.equal(containsWindow(events.amount as never), false);
});
