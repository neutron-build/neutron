import assert from "node:assert/strict";
import test from "node:test";
import {
  aggregate,
  alias,
  astSelect,
  boolean,
  bigint,
  count,
  countDistinct,
  createDatabase,
  cteTable,
  derivedTable,
  desc,
  eq,
  exists,
  exportSchema,
  exportSchemaV2,
  ident,
  integer,
  isDerivedTableHandle,
  numeric,
  pgTable,
  projection,
  qual,
  schemaToDDL,
  serial,
  sql,
  statementReferencesName,
  stringAgg,
  subquery,
  sum,
  avg,
  min,
  max,
  boolAnd,
  boolOr,
  date,
  text,
  timestamp,
  timestamptz,
  type AggregateNode,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q02 — subqueries, CTEs, aggregates and set operations (unit leg).
// Deterministic SQL text, parameter composition through three levels,
// aggregate decode semantics, recursive detection, set-operation shape,
// fail-closed rejections and typing fixtures. Live V13 oracles against
// hand-written SQL live in live.subqueries.postgres.test.ts.
// ---------------------------------------------------------------------------

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  name: text("name"),
});

const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  total: numeric("total"),
  ref: bigint("ref"),
  note: timestamp("note"),
  paid: boolean("paid"),
  label: text("label"),
});

const nodes = pgTable("nodes", {
  id: serial("id").primaryKey(),
  parentId: integer("parent_id"),
});

const accountsTz = pgTable("tzsrc", {
  id: serial("id").primaryKey(),
  seen: timestamptz("seen"),
});

const ordersDay = pgTable("orders_day", {
  oid: integer("oid").notNull(),
  day: date("day"),
});

// Snapshot URL is inert: postgres.js connects lazily and no query here runs.
const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { users, orders, nodes },
});

// ---------------------------------------------------------------------------
// Group by / having / distinct / aggregates — deterministic SQL
// ---------------------------------------------------------------------------

test("aggregates: group/having/distinct compile to exact SQL with aggregate projections", () => {
  const q = db.select({ userId: orders.userId, n: count(), revenue: sum(orders.total) })
    .from(orders)
    .groupBy(orders.userId)
    .having(sql`${count()} > ${1}`)
    .orderBy(desc(count()))
    .toSQL();
  assert.equal(
    q.sql,
    'select "orders"."user_id" as "userId", count(*) as "n", sum("orders"."total") as "revenue" ' +
      'from "orders" group by "orders"."user_id" having (count(*) > $1) order by count(*) desc',
  );
  assert.deepEqual(q.params, [1]);
});

test("aggregates: every form renders its PostgreSQL spelling", () => {
  const q = db.select({
    n: count(),
    nc: count(orders.ref),
    nd: countDistinct(orders.userId),
    s: sum(orders.total),
    a: avg(orders.total),
    mn: min(orders.note),
    mx: max(orders.ref),
    sa: stringAgg(orders.label, ", "),
    ba: boolAnd(orders.paid),
    bo: boolOr(orders.paid),
  })
    .from(orders)
    .toSQL();
  assert.equal(
    q.sql,
    'select count(*) as "n", count("orders"."ref") as "nc", count(distinct "orders"."user_id") as "nd", ' +
      'sum("orders"."total") as "s", avg("orders"."total") as "a", to_jsonb(min("orders"."note"))::text as "mn", ' +
      'max("orders"."ref") as "mx", string_agg("orders"."label", $1) as "sa", bool_and("orders"."paid") as "ba", bool_or("orders"."paid") as "bo" from "orders"',
  );
  assert.deepEqual(q.params, [", "]);
});

test("aggregates: min/max over timestamptz acquires the UTC wire form and the jsonb capability", () => {
  const tz = pgTable("tz", { at: timestamptz("at") });
  const compiled = db.select({ lo: min(tz.at) }).from(tz).toCompiled();
  assert.match(compiled.sql, /to_jsonb\(min\("tz"\."at"\) at time zone 'UTC'\)::text as "lo"/);
  assert.deepEqual(compiled.capabilities, ["jsonb-functions"]);
  assert.equal(compiled.decoders.length, 1);
  assert.equal(compiled.decoders[0].key, "lo");
});

test("aggregates: min/max over a derived/CTE timestamptz column casts the arg back and skips at time zone", () => {
  // Q02 review MAJOR-1: the derived column materializes canonical naive-UTC
  // text — the aggregate must parse it (::timestamp, quote-stripping) BEFORE
  // aggregating and must NOT apply `at time zone` (timezone(unknown, text)
  // does not exist; the canonical text IS the UTC wall clock).
  const d = derivedTable("d", db.select({ id: accountsTz.id, seen: accountsTz.seen }).from(accountsTz));
  const compiled = db.select({ lo: min(d.seen), hi: max(d.seen) }).from(d).toCompiled();
  assert.equal(compiled.sql, 'select to_jsonb(min("d"."seen"::timestamp))::text as "lo", to_jsonb(max("d"."seen"::timestamp))::text as "hi" from (select "tzsrc"."id", to_jsonb("tzsrc"."seen" at time zone \'UTC\')::text as "seen" from "tzsrc") as "d"');
  assert.deepEqual(compiled.capabilities, ["jsonb-functions"]);
  // Same shape through a CTE handle (canonicalText marker rides the handle).
  const c = cteTable("c", db.select({ id: accountsTz.id, seen: accountsTz.seen }).from(accountsTz));
  const cteCompiled = db.select({ lo: min(c.seen) }).from(c).toCompiled();
  assert.match(cteCompiled.sql, /^with "c" as \(select "tzsrc"."id", to_jsonb\("tzsrc"\."seen" at time zone 'UTC'\)::text as "seen" from "tzsrc"\) select to_jsonb\(min\("c"\."seen"::timestamp\)\)::text as "lo" from "c"$/);
  // Naive timestamp and date over a derived table: arg cast, no at time zone.
  const od = alias(ordersDay, "od");
  const nd = derivedTable("nd", db.select({ note: orders.note, day: od.day }).from(orders).innerJoin(od, sql`${od.oid} = ${orders.id}`));
  const ndCompiled = db.select({ mn: min(nd.note), md: min(nd.day) }).from(nd).toCompiled();
  assert.match(ndCompiled.sql, /to_jsonb\(min\("nd"\."note"::timestamp\)\)::text as "mn"/);
  assert.match(ndCompiled.sql, /to_jsonb\(min\("nd"\."day"::date\)\)::text as "md"/);
});

test("aggregates: count/sum decode through the int8 codec, empty-input semantics per PostgreSQL", async () => {
  const { applyProjectionDecoders } = await import("./index.js");
  const compiled = db.select({ n: count(), s: sum(orders.total), mx: max(orders.ref) }).from(orders).toCompiled();
  assert.deepEqual(compiled.capabilities, []);
  // int8 results (count, max over int8) decode via the bigint codec; numeric
  // sums arrive as exact strings natively and need no decoder.
  const keys = compiled.decoders.map((d) => d.key).sort();
  assert.deepEqual(keys, ["mx", "n"]);
  // Empty input: count -> 0, sum/max -> null (decoders leave null cells
  // untouched; the driver delivers SQL NULL as null).
  const emptyRow: Record<string, unknown> = { n: "0", s: null, mx: null };
  applyProjectionDecoders([emptyRow], compiled.decoders);
  assert.deepEqual(emptyRow, { n: 0n, s: null, mx: null });
  // Non-empty: int8 strings become bigint per the default mode.
  const row: Record<string, unknown> = { n: "2", s: null, mx: "9007199254740993" };
  applyProjectionDecoders([row], compiled.decoders);
  assert.deepEqual(row, { n: 2n, s: null, mx: 9007199254740993n });
});

test("aggregates: constructor and compile validations fail closed", () => {
  // @ts-expect-error unknown aggregate op is rejected at the type level too
  assert.throws(() => aggregate("median", []), /unknown aggregate/);
  assert.throws(() => aggregate("sum", []), /sum\(\) takes exactly 1/);
  assert.throws(() => aggregate("string_agg", [ident("a")]), /string_agg\(\) takes exactly 2/);
  assert.throws(() => aggregate("count", [], { distinct: true }), /count\(\*\) cannot take distinct/);
  // Forged aggregate nodes re-validate at the compile choke point.
  const forged = { kind: "aggregate", op: "percentile", args: [] } as unknown as AggregateNode;
  const stmt = { kind: "select", ctes: [], recursive: false, distinct: false, projections: [projection(forged)], joins: [], where: [], groupBy: [], having: [], setOps: [], orderBy: [] };
  assert.throws(() => db.select({ x: forged }).from(orders).toSQL(), /unknown aggregate/);
  void stmt;
});

test("groupBy: order specs are rejected; columns and expressions group deterministically", () => {
  assert.throws(
    () => db.select({ n: count() }).from(orders).groupBy(desc(orders.userId) as never),
    /order specs are not group terms/,
  );
  const q = db.select({ n: count() })
    .from(orders)
    .groupBy(orders.userId, sql`lower(${orders.note})`)
    .toSQL();
  assert.equal(q.sql, 'select count(*) as "n" from "orders" group by "orders"."user_id", lower("orders"."note")');
});

test("having: fragments are delimited when joined — the F04 grouping guarantee applies", () => {
  const q = db.select({ userId: orders.userId })
    .from(orders)
    .groupBy(orders.userId)
    .having(sql`${count()} > ${1} or ${sum(orders.total)} is null`)
    .having(sql`${count()} < ${10}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "orders"."user_id" as "userId" from "orders" group by "orders"."user_id" ' +
      'having (count(*) > $1 or sum("orders"."total") is null) and (count(*) < $2)',
  );
  assert.deepEqual(q.params, [1, 10]);
});

test("distinct: renders select distinct without touching anything else", () => {
  const q = db.select({ userId: orders.userId }).from(orders).where(eq(orders.userId, 1)).distinct().toSQL();
  assert.equal(q.sql, 'select distinct "orders"."user_id" as "userId" from "orders" where ("orders"."user_id" = $1)');
});

// ---------------------------------------------------------------------------
// CTEs and derived tables — three-level alias + parameter composition
// ---------------------------------------------------------------------------

test("cte/derived: three levels compose with exact aliases and parameter ordering", () => {
  const lvl1 = db.select({ userId: orders.userId, n: count() })
    .from(orders)
    .where(sql`${orders.total} >= ${"5.00"}`)
    .groupBy(orders.userId);
  const agg = cteTable("agg", lvl1);
  const lvl2 = db.select({ userId: agg.userId, n: agg.n }).from(agg).where(sql`${agg.n} > ${2}`);
  const wrapped = derivedTable("wrapped", lvl2);
  const lvl3 = db.select({ userId: wrapped.userId, n: wrapped.n, bumped: sql`${wrapped.n} * ${10}` })
    .from(wrapped)
    .orderBy(desc(wrapped.n));

  const q = lvl3.toSQL();
  assert.equal(
    q.sql,
    'select "wrapped"."userId", "wrapped"."n", "wrapped"."n" * $1 as "bumped" ' +
      'from (with "agg" as (select "orders"."user_id" as "userId", count(*) as "n" from "orders" ' +
      'where ("orders"."total" >= $2) group by "orders"."user_id") ' +
      'select "agg"."userId", "agg"."n" from "agg" where ("agg"."n" > $3)) as "wrapped" order by "wrapped"."n" desc',
  );
  assert.deepEqual(q.params, [10, "5.00", 2]);

  // Recompiles byte-identical; placeholders stay $1..$n sequential.
  const again = lvl3.toSQL();
  assert.equal(again.sql, q.sql);
  assert.deepEqual(again.params, q.params);
  const placeholders = [...q.sql.matchAll(/\$(\d+)/g)].map((m) => Number(m[1]));
  assert.deepEqual(placeholders, placeholders.map((_, k) => k + 1));
});

test("cte/derived: hoisting the CTE to the outer statement yields one with-clause", () => {
  const lvl1 = db.select({ userId: orders.userId, n: count() }).from(orders).groupBy(orders.userId);
  const agg = cteTable("agg", lvl1);
  const q = db.select({ userId: agg.userId, n: agg.n }).from(agg).orderBy(desc(agg.n)).toSQL();
  assert.equal(
    q.sql,
    'with "agg" as (select "orders"."user_id" as "userId", count(*) as "n" from "orders" group by "orders"."user_id") ' +
      'select "agg"."userId", "agg"."n" from "agg" order by "agg"."n" desc',
  );
});

test("cte/derived: joins on handles join under their own name; outer-join typing keys on it", () => {
  const big = cteTable("big", db.select({ id: orders.id, total: orders.total }).from(orders));
  const q = db.select({ email: users.email, total: big.total })
    .from(users)
    .innerJoin(big, sql`${big.id} = ${users.id}`)
    .toSQL();
  assert.equal(
    q.sql,
    'with "big" as (select "orders"."id", "orders"."total" from "orders") ' +
      'select "users"."email", "big"."total" from "users" inner join "big" as "big" on "big"."id" = "users"."id"',
  );

  const wrapped = derivedTable("wrapped", db.select({ id: orders.id, note: orders.note }).from(orders));
  const q2 = db.select({ email: users.email, note: wrapped.note })
    .from(users)
    .leftJoin(wrapped, sql`${wrapped.id} = ${users.id}`)
    .toSQL();
  assert.equal(
    q2.sql,
    'select "users"."email", to_jsonb("wrapped"."note"::timestamp)::text as "note" from "users" ' +
      'left join (select "orders"."id", to_jsonb("orders"."note")::text as "note" from "orders") as "wrapped" on "wrapped"."id" = "users"."id"',
  );
});

test("cte/derived: timestamptz composition acquires the NAIVE pass-through wire (session-TZ independent)", () => {
  // Q02 review BLOCKER: the canonical timestamptz text IS the UTC wall
  // clock, so the outer acquisition must re-parse with ::timestamp (no
  // timezone interpretation) — ::timestamptz would consult the session
  // timezone and shift the value per composition level.
  const inner = derivedTable("inner_tz", db.select({ id: accountsTz.id, seen: accountsTz.seen }).from(accountsTz));
  const q = db.select({ id: inner.id, seen: inner.seen }).from(inner).toSQL();
  assert.equal(
    q.sql,
    'select "inner_tz"."id", to_jsonb("inner_tz"."seen"::timestamp)::text as "seen" ' +
      'from (select "tzsrc"."id", to_jsonb("tzsrc"."seen" at time zone \'UTC\')::text as "seen" from "tzsrc") as "inner_tz"',
  );
  // Idempotent through the next level: the CTE consumer renders the same
  // naive acquisition — never a timestamptz re-parse of the composed column.
  const cte = cteTable("mid_tz", db.select({ id: inner.id, seen: inner.seen }).from(inner));
  const q2 = db.select({ seen: cte.seen }).from(cte).toSQL();
  assert.match(q2.sql, /to_jsonb\("mid_tz"\."seen"::timestamp\)::text as "seen"/);
  assert.doesNotMatch(q2.sql, /"mid_tz"\."seen"::timestamptz|"mid_tz"\."seen" at time zone/);
});

test("cte: duplicate registration is idempotent per statement; conflicting duplicates fail closed", () => {
  const src = db.select({ id: orders.id }).from(orders);
  const a = cteTable("shared", src);
  // Two references to the same handle in one statement: one WITH entry.
  const q = db.select({ id: a.id }).from(a).where(sql`${a.id} > ${1}`).toSQL();
  assert.equal(q.sql.match(/with "shared" as/g)?.length, 1);

  assert.throws(
    () => db.select({ x: a.id }).from(a).withCte("shared", db.select({ id: users.id }).from(users)).toSQL(),
    /registered twice with different statements/,
  );
});

test("fragments: cteTable references inside sql fragments resolve or fail closed (no silent shadow binding)", () => {
  // Q02 review MINOR-2: a fragment-spliced cteTable reference must resolve
  // to the handle's OWN CTE on the consuming statement.
  const c = cteTable("fragc", db.select({ id: orders.id }).from(orders));

  // Registered via from: the fragment reference composes.
  const okQ = db.select({ id: c.id, n: sql`(select ${count()} from ${c})` })
    .from(c)
    .where(sql`${c.id} > ${1}`)
    .toSQL();
  assert.equal(
    okQ.sql,
    'with "fragc" as (select "orders"."id" from "orders") ' +
      'select "fragc"."id", (select count(*) from "fragc") as "n" from "fragc" where ("fragc"."id" > $1)',
  );

  // Unregistered reference: compilation fails closed (before, this rendered
  // a bare name that only failed — or shadow-bound — at the database).
  assert.throws(
    () => db.select({ n: sql`(select count(*) from ${c})` }).from(users).toSQL(),
    /references cte "fragc" which is not registered/,
  );

  // Shadowing: a DIFFERENT same-name CTE in scope would silently capture the
  // fragment reference — fail closed instead.
  assert.throws(
    () => db.select({ n: sql`(select count(*) from ${c})` })
      .from(users)
      .withCte("fragc", db.select({ id: users.id }).from(users))
      .toSQL(),
    /references cte "fragc" but a different statement is registered/,
  );

  // Column references carry the same identity: unregistered fails closed.
  assert.throws(
    () => db.select({ x: sql`${c.id} + 1` }).from(users).toSQL(),
    /references cte "fragc" which is not registered/,
  );

  // astSelect consumers auto-register CTE handles from from/joins (same
  // rule as the typed builder) instead of emitting a dangling bare name.
  const astQ = astSelect({ id: qual("fragc", "id") }).from(c).toSQL();
  assert.equal(
    astQ.sql,
    'with "fragc" as (select "orders"."id" from "orders") select "fragc"."id" as "id" from "fragc"',
  );

  // A whole derived-table handle in a fragment is rejected at the template:
  // a derived table exists only at its inline from/join site.
  const d = derivedTable("fragd", db.select({ id: orders.id }).from(orders));
  assert.throws(() => sql`(select 1 from ${d})`, /interpolated derived-table handle/);

  // Mutations validate fragment references too (they register no CTEs).
  assert.throws(
    () => db.update(users).set({ name: sql`${c.id}::text` }).where(sql`${users.id} = ${c.id}`).toSQL(),
    /references cte "fragc" which is not registered/,
  );
});

test("derived: name validation mirrors alias rules", () => {
  assert.throws(() => derivedTable("a.b", db.select({ id: orders.id }).from(orders)), /must not contain "\."/);
  assert.throws(() => derivedTable("__q1", db.select({ id: orders.id }).from(orders)), /reserved/);
  assert.throws(() => cteTable("", db.select({ id: orders.id }).from(orders)), /non-empty/);
});

test("cte/derived: handles fail closed outside the query surface", () => {
  const h = cteTable("agg", db.select({ id: orders.id }).from(orders));
  const d = derivedTable("der", db.select({ id: orders.id }).from(orders));
  assert.equal(isDerivedTableHandle(h), true);
  assert.equal(isDerivedTableHandle(d), true);
  assert.equal(isDerivedTableHandle(orders), false);
  assert.throws(() => db.insert(h).values({ id: 1 }), /query-surface identities/);
  assert.throws(() => db.update(d).set({ id: 1 }).where(eq(orders.id, 1)), /query-surface identities/);
  assert.throws(() => db.delete(h).where(eq(orders.id, 1)), /query-surface identities/);
  assert.throws(() => schemaToDDL([h]), /query-surface identities/);
  assert.throws(() => exportSchema({ t: d }), /query-surface identities/);
  assert.throws(() => exportSchemaV2({ t: h }), /query-surface identities/);
  assert.rejects(
    createDatabase({ url: "postgres://snapshot:nouser@127.0.0.1:1/none", driverOptions: { driver: "postgres" }, tables: { t: h } }),
    /query-surface identities/,
  );
});

test("alias() rejects derived/CTE handles — the subquery would be lost", () => {
  const h = derivedTable("d", db.select({ id: orders.id }).from(orders));
  assert.throws(() => alias(h, "x"), /subquery would be lost/);
  const c = cteTable("c", db.select({ id: orders.id }).from(orders));
  assert.throws(() => alias(c, "x"), /subquery would be lost/);
});

test("raw subquery nodes are rejected as factory sources (no column metadata)", () => {
  const sub = subquery(db.select({ id: orders.id }).from(orders).toAST());
  assert.throws(() => derivedTable("d", sub as never), /no column metadata/);
  assert.throws(() => cteTable("c", sub as never), /no column metadata/);
});

// ---------------------------------------------------------------------------
// Recursive CTEs
// ---------------------------------------------------------------------------

test("recursive: with recursive is computed from structural CTE self-references", () => {
  assert.equal(statementReferencesName(db.select({ id: nodes.id }).from(nodes).toAST(), "tree"), false);
  // Conservative by design: a base table sharing the CTE's name still flags
  // (with recursive on a non-recursive CTE is valid SQL).
  const tree = cteTable("tree", astSelect({ id: nodes.id, depth: sql`0` })
    .from(nodes)
    .where(sql`${nodes.parentId} is null`)
    .unionAll(astSelect({ id: nodes.id, depth: sql`${qual("tree", "depth")} + 1` })
      .from(nodes)
      .innerJoin(ident("tree"), "tree", sql`${nodes.parentId} = ${qual("tree", "id")}`)));
  const q = db.select({ id: tree.id, depth: tree.depth }).from(tree).toSQL();
  assert.equal(
    q.sql,
    'with recursive "tree" as (select "nodes"."id", 0 as "depth" from "nodes" where ("nodes"."parent_id" is null) ' +
      'union all (select "nodes"."id", "tree"."depth" + 1 as "depth" from "nodes" ' +
      'inner join "tree" as "tree" on "nodes"."parent_id" = "tree"."id")) select "tree"."id", "tree"."depth" from "tree"',
  );
  // A non-self-referencing CTE stays ordinary.
  const plain = cteTable("plain", db.select({ id: nodes.id }).from(nodes));
  assert.equal(db.select({ id: plain.id }).from(plain).toSQL().sql.includes("with recursive"), false);
  assert.equal(statementReferencesName(db.select({ id: nodes.id }).from(nodes).toAST(), "nodes"), true);
});

test("recursive: astSelect withCte + union compose at the AST layer", () => {
  const base = astSelect({ id: nodes.id }).from(nodes).where(sql`${nodes.parentId} is null`);
  const rec = astSelect({ id: nodes.id }).from(ident("t")).where(sql`${qual("t", "id")} < ${100}`);
  const q = astSelect({ id: qual("t", "id") }).from(ident("t")).withCte("t", base.union(rec)).toSQL();
  assert.equal(
    q.sql,
    'with recursive "t" as (select "nodes"."id" from "nodes" where ("nodes"."parent_id" is null) ' +
      'union (select "nodes"."id" from "t" where ("t"."id" < $1))) select "t"."id" as "id" from "t"',
  );
  assert.deepEqual(q.params, [100]);
});

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

test("setops: union/union all/intersect/except and their ALL variants render parenthesized branches", () => {
  const a = () => db.select({ id: orders.id }).from(orders);
  const b = () => db.select({ id: orders.id }).from(orders);
  assert.equal(a().union(b()).toSQL().sql, 'select "orders"."id" from "orders" union (select "orders"."id" from "orders")');
  assert.equal(a().unionAll(b()).toSQL().sql, 'select "orders"."id" from "orders" union all (select "orders"."id" from "orders")');
  assert.equal(a().intersect(b()).toSQL().sql, 'select "orders"."id" from "orders" intersect (select "orders"."id" from "orders")');
  assert.equal(a().intersectAll(b()).toSQL().sql, 'select "orders"."id" from "orders" intersect all (select "orders"."id" from "orders")');
  assert.equal(a().except(b()).toSQL().sql, 'select "orders"."id" from "orders" except (select "orders"."id" from "orders")');
  assert.equal(a().exceptAll(b()).toSQL().sql, 'select "orders"."id" from "orders" except all (select "orders"."id" from "orders")');
});

test("setops: chaining is left-associative in call order; compound order/limit bind to the whole", () => {
  const q = db.select({ id: orders.id }).from(orders)
    .where(sql`${orders.id} > ${1}`)
    .union(db.select({ id: orders.id }).from(orders).where(sql`${orders.id} < ${5}`))
    .except(db.select({ id: orders.id }).from(orders).where(sql`${orders.id} = ${3}`))
    .orderBy(sql`id`)
    .limit(10)
    .toSQL();
  assert.equal(
    q.sql,
    'select "orders"."id" from "orders" where ("orders"."id" > $1) ' +
      'union (select "orders"."id" from "orders" where ("orders"."id" < $2)) ' +
      'except (select "orders"."id" from "orders" where ("orders"."id" = $3)) order by id asc limit 10',
  );
  assert.deepEqual(q.params, [1, 5, 3]);
});

test("setops: a first branch carrying orderBy/limit fails closed (SQL would rebind them)", () => {
  const ordered = db.select({ id: orders.id }).from(orders).orderBy(sql`id`).limit(3);
  assert.throws(() => ordered.union(db.select({ id: orders.id }).from(orders)), /silently bind to the compound/);
  // A trailing branch keeps its own order/limit inside the parentheses.
  const ok = db.select({ id: orders.id }).from(orders).union(ordered).toSQL();
  assert.equal(
    ok.sql,
    'select "orders"."id" from "orders" union (select "orders"."id" from "orders" order by id asc limit 3)',
  );
});

test("setops: capabilities merge across branches and the first branch's decode plan carries", () => {
  const tz = pgTable("tz2", { at: timestamp("at") });
  const first = db.select({ id: orders.id }).from(orders);
  const withTemporal = db.select({ at: tz.at }).from(tz);
  const compiled = first.union(withTemporal).toCompiled();
  assert.deepEqual(compiled.capabilities, ["jsonb-functions"]);
  const onlyTemporal = db.select({ at: tz.at }).from(tz).union(first).toCompiled();
  assert.equal(onlyTemporal.decoders.length, 1);
  assert.equal(onlyTemporal.decoders[0].key, "at");
});

test("setops: compounds nest as subqueries and CTE sources", () => {
  const compound = db.select({ id: orders.id }).from(orders).union(db.select({ id: users.id }).from(users));
  const t = cteTable("both", compound);
  const q = db.select({ id: t.id }).from(t).toSQL();
  assert.equal(
    q.sql,
    'with "both" as (select "orders"."id" from "orders" union (select "users"."id" from "users")) select "both"."id" from "both"',
  );
});

// ---------------------------------------------------------------------------
// Correlated subqueries
// ---------------------------------------------------------------------------

test("correlated: subquery fragments and exists() compile with inline correlated references", () => {
  const inner = astSelect({ n: count() }).from(orders).where(sql`${orders.userId} = ${users.id}`);
  const q = db.select({ email: users.email, n: sql`${inner.subquery()}` })
    .from(users)
    .where(exists(astSelect({ one: sql`1` }).from(orders).where(sql`${orders.userId} = ${users.id}`)))
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."email", (select count(*) as "n" from "orders" where ("orders"."user_id" = "users"."id")) as "n" ' +
      'from "users" where (exists (select 1 as "one" from "orders" where ("orders"."user_id" = "users"."id")))',
  );
  assert.deepEqual(q.params, []);
});

test("correlated: a subquery in a join ON binds its parameters in ON position", () => {
  const big = astSelect({ id: orders.id }).from(orders).where(sql`${orders.total} > ${20}`);
  const q = db.select({ email: users.email })
    .from(users)
    .innerJoin(derivedTable("bigd", big), sql`${qual("bigd", "id")} = ${users.id}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."email" from "users" inner join (select "orders"."id" from "orders" where ("orders"."total" > $1)) as "bigd" on "bigd"."id" = "users"."id"',
  );
  assert.deepEqual(q.params, [20]);
});

// ---------------------------------------------------------------------------
// Typing fixtures — aggregate nullability, CTE/derived row types, set-op rows
// ---------------------------------------------------------------------------

async function typeFixtures(): Promise<void> {
  const g = db.select({ userId: orders.userId, n: count(), s: sum(orders.total), a: avg(orders.total), mx: max(orders.ref), mn: min(orders.note), sa: stringAgg(orders.label, ", "), ba: boolAnd(orders.paid) })
    .from(orders)
    .groupBy(orders.userId);
  const eqRow: AssertEq<
    Awaited<typeof g>[number],
    { userId: number; n: bigint; s: string | null; a: string | null; mx: bigint | null; mn: string | null; sa: string | null; ba: boolean | null }
  > = true;
  // @ts-expect-error count is never null; sum is nullable on empty input
  const badRow: { n: bigint | null; s: string } = ({} as Awaited<typeof g>[number]);

  const lvl1 = db.select({ userId: orders.userId, n: count() }).from(orders).groupBy(orders.userId);
  const agg = cteTable("agg", lvl1);
  const rows = db.select({ userId: agg.userId, n: agg.n }).from(agg).where(sql`${agg.n} > ${1}`);
  // CTE/derived row types derive from the source's projections.
  const eqCte: AssertEq<Awaited<typeof rows>[number], { userId: number; n: bigint }> = true;
  // @ts-expect-error the derived column keeps its source type (count -> bigint)
  const badCte: { n: number } = ({} as Awaited<typeof rows>[number]);

  const wrapped = derivedTable("wrapped", db.select({ userId: agg.userId, n: agg.n }).from(agg));
  const dsel = db.select().from(wrapped);
  const eqDerived: AssertEq<Awaited<typeof dsel>[number], { userId: number; n: bigint }> = true;

  // leftJoin on a derived handle: its columns widen to null.
  const lj = db.select({ email: users.email, n: wrapped.n }).from(users).leftJoin(wrapped, sql`${wrapped.userId} = ${users.id}`);
  const eqLeft: AssertEq<Awaited<typeof lj>[number], { email: string; n: bigint | null }> = true;
  // @ts-expect-error left-joined derived columns are nullable
  const badLeft: { email: string; n: bigint } = ({} as Awaited<typeof lj>[number]);

  // expression outputs are unknown through a derived table.
  const exprSrc = db.select({ bumped: sql`${orders.total} * 2` }).from(orders);
  const dexpr = derivedTable("dexpr", exprSrc);
  const dexprRows = db.select().from(dexpr);
  const eqExpr: AssertEq<Awaited<typeof dexprRows>[number], { bumped: unknown }> = true;

  // set operations type rows by the first branch.
  const u = db.select({ id: orders.id }).from(orders).union(db.select({ id: users.id }).from(users));
  const eqUnion: AssertEq<Awaited<typeof u>[number], { id: number }> = true;

  // nullable source columns stay nullable through derived tables.
  const nullableSrc = db.select({ note: orders.note, ref: orders.ref }).from(orders);
  const dn = derivedTable("dn", nullableSrc);
  const dnRows = db.select().from(dn);
  const eqNull: AssertEq<Awaited<typeof dnRows>[number], { note: string | null; ref: bigint | null }> = true;

  // bigint column modes survive: mode string reads string through the handle.
  const sRef = pgTable("s_ref", { v: bigint("v", { mode: "string" }) });
  const dstr = derivedTable("dstr", db.select({ v: sRef.v }).from(sRef));
  const dstrRows = db.select().from(dstr);
  const eqMode: AssertEq<Awaited<typeof dstrRows>[number], { v: string | null }> = true;

  void [eqRow, badRow, eqCte, badCte, eqDerived, eqLeft, badLeft, eqExpr, eqUnion, eqNull, eqMode];
  void [g, rows, lj, u, dsel, dexprRows, dnRows, dstrRows];
}
void typeFixtures;

// ---------------------------------------------------------------------------
// Seeded determinism fuzz over the new surface
// ---------------------------------------------------------------------------

test("fuzz: 120 seeded composed selects compile deterministically with sequential placeholders", () => {
  function mulberry32(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const rng = mulberry32(0x20260924);
  const pick = <T>(arr: readonly T[]): T => arr[Math.floor(rng() * arr.length)];

  let composed = 0;
  for (let i = 0; i < 120; i++) {
    const lvl1 = db.select({ userId: orders.userId, n: count(), mx: max(orders.ref) })
      .from(orders)
      .where(sql`${orders.total} > ${`1.${i}`}`)
      .groupBy(orders.userId);
    const handle = pick([cteTable("c1", lvl1), derivedTable("d1", lvl1)]);
    let q: { toSQL(): { sql: string; params: unknown[] } } = db.select({ userId: handle.userId, n: handle.n })
      .from(handle)
      .where(sql`${handle.n} > ${i}`);
    if (rng() < 0.4) {
      q = db.select({ userId: handle.userId }).from(handle).union(db.select({ userId: orders.userId }).from(orders)).limit(5);
    }
    const first = q.toSQL();
    const second = q.toSQL();
    assert.equal(second.sql, first.sql, `case ${i}: recompile differs`);
    assert.deepEqual(second.params, first.params, `case ${i}: params differ on recompile`);
    const placeholders = [...first.sql.matchAll(/\$(\d+)/g)].map((m) => Number(m[1]));
    assert.deepEqual(placeholders, placeholders.map((_, k) => k + 1), `case ${i}: placeholders must be $1..$n`);
    assert.equal(first.params.length, placeholders.length, `case ${i}: params count matches placeholders`);
    composed++;
  }
  assert.ok(composed >= 120);
});
