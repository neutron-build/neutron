import assert from "node:assert/strict";
import test from "node:test";
import {
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
  ident,
  integer,
  max,
  min,
  numeric,
  pgTable,
  qual,
  serial,
  sql,
  stringAgg,
  sum,
  avg,
  boolAnd,
  boolOr,
  text,
  timestamp,
  timestamptz,
  date,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Q02 live suite (V13): every query built through the new surface is
// executed against real Postgres and compared row-for-row with independently
// authored hand-written SQL (never compiler output). Covers grouping/having,
// every aggregate's empty-input semantics (count -> 0, the rest -> null),
// the int8 count codec, distinct and set operations with duplicate data,
// CTEs over joins, derived tables with outer-join nullability, correlated
// subqueries two levels up, recursive CTEs on a CYCLICAL graph (UNION
// deduplication is the cycle safety), and three-level alias+parameter
// composition with exact params arrays. Both drivers run against a uniquely
// named q02_ throwaway database.

const DB_NAME = uniqueDbName("q02_subq");

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
  seen: timestamptz("seen"),
  day: date("day"),
  paid: boolean("paid"),
  label: text("label"),
});

const emptyT = pgTable("empty_t", {
  id: serial("id").primaryKey(),
  total: numeric("total"),
  ref: bigint("ref"),
  note: timestamp("note"),
  paid: boolean("paid"),
  label: text("label"),
});

const edges = pgTable("edges", {
  id: serial("id").primaryKey(),
  fromId: integer("from_id").notNull(),
  toId: integer("to_id").notNull(),
});

type Tables = { users: typeof users; orders: typeof orders; empty_t: typeof emptyT; edges: typeof edges };
type TestDb = NeutronDatabase<Tables>;

// Hand-written fixture DDL and seed — never through the ORM. Real NULLs
// (order 13 all-NULL payload), int8 boundary refs, microsecond timestamps,
// duplicate labels for distinct/set-op semantics, a cyclic edge graph
// (1 -> 2 -> 3 -> 1) and an empty table.
async function seed(driver: { query: (s: string, p?: unknown[]) => Promise<unknown>; execute: (s: string, p?: unknown[]) => Promise<unknown> }): Promise<void> {
  await driver.execute(`create table "users" (
    "id" serial primary key,
    "email" text not null,
    "name" text
  )`);
  await driver.execute(`create table "orders" (
    "id" serial primary key,
    "user_id" integer not null,
    "total" numeric,
    "ref" bigint,
    "note" timestamp,
    "seen" timestamptz,
    "day" date,
    "paid" boolean,
    "label" text
  )`);
  await driver.execute(`create table "empty_t" (
    "id" serial primary key,
    "total" numeric,
    "ref" bigint,
    "note" timestamp,
    "paid" boolean,
    "label" text
  )`);
  await driver.execute(`create table "edges" (
    "id" serial primary key,
    "from_id" integer not null,
    "to_id" integer not null
  )`);
  await driver.execute(`insert into "users" ("id", "email", "name") values
    (1, 'alice@x.com', 'Alice'),
    (2, 'bob@x.com', 'Bob'),
    (3, 'carol@x.com', null),
    (4, 'dave@x.com', 'Dave')`);
  await driver.execute(`insert into "orders" ("id", "user_id", "total", "ref", "note", "seen", "day", "paid", "label") values
    (10, 1, '19.50', 9007199254740993, '2026-01-02T03:04:05.678912', '2026-01-02T03:04:05.678912+00', '2026-01-02', true, 'hot'),
    (11, 1, '0.010', null, null, null, null, false, 'cold'),
    (12, 2, '120.75', 42, '2026-02-03T04:05:06.000001', '2026-02-03T04:05:06.000001+00', '2026-02-03', true, 'hot'),
    (13, 3, null, null, null, null, null, null, null),
    (14, 2, '7.00', 7, '2026-03-04T05:06:07.000001', '2026-03-04T05:06:07.000001+00', '2026-03-04', false, 'warm'),
    (15, 9, '30.00', -9007199254740993, null, '2025-12-31T23:59:59.999999+00', '2025-12-31', true, 'hot')`);
  await driver.execute(`insert into "edges" ("id", "from_id", "to_id") values
    (1, 1, 2),
    (2, 2, 3),
    (3, 3, 1),
    (4, 1, 4),
    (5, 2, 2)`);
}

interface DriverLike {
  query<T = Record<string, unknown>>(s: string, p?: unknown[]): Promise<T[]>;
  execute(s: string, p?: unknown[]): Promise<unknown>;
}

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb, driver: DriverLike) => Promise<void>, sessionTimeZone?: string): Promise<void> {
  if (!(await ensureLive(`live subqueries (${driverKind})`))) {
    return;
  }
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  // Session-timezone matrix (Q02 review BLOCKER): the fix must hold for the
  // SERVER session timezone, which process TZ does not influence. Setting
  // the database default before any pooled connection opens pins every
  // session this suite creates to the requested zone.
  if (sessionTimeZone !== undefined) {
    await admin.query(`alter database "${DB_NAME}" set timezone to '${sessionTimeZone.replace(/'/g, "''")}'`);
  }
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const db = (await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
  })) as unknown as TestDb;
  const driver = db.driver as unknown as DriverLike;
  try {
    await seed(driver);
    await fn(db, driver);
  } finally {
    await db.close();
    if (/^q02_subq_[0-9_]+$/.test(DB_NAME)) {
      const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
      await admin2.query(
        "select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()",
        [DB_NAME],
      );
      await admin2.query(`drop database if exists "${DB_NAME}"`);
      await admin2.end();
    }
  }
}

type Row = Record<string, unknown>;

/** Execute the hand-written oracle and deep-compare rows. */
async function compare(
  driver: DriverLike,
  label: string,
  got: Row[],
  oracleSql: string,
  oracleParams: unknown[],
  normalize?: (row: Row) => Row,
): Promise<void> {
  const want = (await driver.query(oracleSql, oracleParams)) as Row[];
  const n = normalize ?? ((r: Row) => r);
  assert.deepEqual(got.map(n), want.map(n), `${label}: compiled results differ from hand-written SQL`);
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live subqueries (${driverKind}): V13 group by / having / every aggregate matches hand SQL`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const rows = await db.select({
        userId: orders.userId,
        n: count(),
        distinctLabels: countDistinct(orders.label),
        revenue: sum(orders.total),
        meanTotal: avg(orders.total),
        lastNote: max(orders.note),
        firstNote: min(orders.note),
        biggestRef: max(orders.ref),
        labels: stringAgg(orders.label, " | "),
        allPaid: boolAnd(orders.paid),
        anyPaid: boolOr(orders.paid),
      })
        .from(orders)
        .groupBy(orders.userId)
        .having(sql`${count()} >= ${1}`)
        .orderBy(orders.userId);

      // Int8 aggregate results follow the F03 codec: count/max(ref) are
      // bigint, never Number.
      assert.equal(typeof rows[0].n, "bigint");
      assert.equal(rows[0].biggestRef, 9007199254740993n);
      assert.equal(rows[2].biggestRef, null);

      await compare(
        driver,
        "group/having aggregates",
        rows as Row[],
        `select "user_id" as "userId",
           count(*) as "n",
           count(distinct "label") as "distinctLabels",
           sum("total")::text as "revenue",
           avg("total")::text as "meanTotal",
           to_char(max("note"), 'YYYY-MM-DD"T"HH24:MI:SS.US') as "lastNote",
           to_char(min("note"), 'YYYY-MM-DD"T"HH24:MI:SS.US') as "firstNote",
           max("ref")::text as "biggestRef",
           string_agg("label", ' | ') as "labels",
           bool_and("paid") as "allPaid",
           bool_or("paid") as "anyPaid"
         from "orders" group by "user_id" having count(*) >= $1 order by "user_id"`,
        [1],
        (r) => ({
          userId: r.userId,
          n: BigInt(r.n as string | bigint),
          distinctLabels: BigInt(r.distinctLabels as string | bigint),
          revenue: r.revenue,
          meanTotal: r.meanTotal,
          lastNote: r.lastNote,
          firstNote: r.firstNote,
          biggestRef: r.biggestRef === null ? null : BigInt(r.biggestRef as string),
          labels: r.labels,
          allPaid: r.allPaid,
          anyPaid: r.anyPaid,
        }),
      );

      // Grouped query over data whose only group is filtered away by
      // HAVING: zero rows (no groups survive).
      const none = await db.select({ userId: orders.userId, n: count() })
        .from(orders)
        .groupBy(orders.userId)
        .having(sql`${count()} > ${99}`);
      assert.equal(none.length, 0);
    });
  });

  test(`live subqueries (${driverKind}): V13 empty-input aggregate semantics match PostgreSQL exactly`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const rows = await db.select({
        n: count(),
        nc: count(emptyT.total),
        nd: countDistinct(emptyT.label),
        s: sum(emptyT.total),
        a: avg(emptyT.total),
        mn: min(emptyT.note),
        mx: max(emptyT.ref),
        sa: stringAgg(emptyT.label, ", "),
        ba: boolAnd(emptyT.paid),
        bo: boolOr(emptyT.paid),
      }).from(emptyT);

      // count -> 0 (bigint); every other aggregate -> null on empty input.
      assert.equal(rows.length, 1);
      const r = rows[0];
      assert.equal(r.n, 0n);
      assert.equal(typeof r.n, "bigint");
      assert.equal(r.nc, 0n);
      assert.equal(r.nd, 0n);
      assert.equal(r.s, null);
      assert.equal(r.a, null);
      assert.equal(r.mn, null);
      assert.equal(r.mx, null);
      assert.equal(r.sa, null);
      assert.equal(r.ba, null);
      assert.equal(r.bo, null);

      await compare(
        driver,
        "empty-input aggregates",
        rows as Row[],
        `select count(*) as "n", count("total") as "nc", count(distinct "label") as "nd",
           sum("total")::text as "s", avg("total")::text as "a",
           min("note") as "mn", max("ref")::text as "mx",
           string_agg("label", ', ') as "sa", bool_and("paid") as "ba", bool_or("paid") as "bo"
         from "empty_t"`,
        [],
        (r) => ({ ...r, n: BigInt(r.n as string | bigint), nc: BigInt(r.nc as string | bigint), nd: BigInt(r.nd as string | bigint) }),
      );

      // Grouped over an empty table: ZERO rows (no groups exist).
      const grouped = await db.select({ n: count() }).from(emptyT).groupBy(emptyT.id);
      assert.equal(grouped.length, 0);
    });
  });

  test(`live subqueries (${driverKind}): V13 distinct and set operations over duplicate data`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      // distinct labels: duplicates collapse; SQL NULL label is one distinct value.
      const d = await db.select({ label: orders.label }).from(orders).distinct().orderBy(sql`label`);
      assert.deepEqual(d.map((r) => r.label), ["cold", "hot", "warm", null]);
      await compare(
        driver,
        "distinct labels",
        d as Row[],
        `select distinct "label" from "orders" order by "label"`,
        [],
      );

      // union dedups (hot appears in both branches), union all keeps both.
      const hotPaid = () => db.select({ id: orders.id }).from(orders).where(eq(orders.label, "hot"));
      const bigRef = () => db.select({ id: orders.id }).from(orders).where(sql`${orders.ref} > ${5}`);
      const u = await hotPaid().union(bigRef()).orderBy(sql`id`);
      assert.deepEqual(u.map((r) => r.id), [10, 12, 14, 15]);
      const ua = await hotPaid().unionAll(bigRef()).orderBy(sql`id`);
      assert.deepEqual(ua.map((r) => r.id), [10, 10, 12, 12, 14, 15]);
      await compare(
        driver,
        "union dedup vs union all duplicates",
        ua as Row[],
        `select "id" from "orders" where "label" = 'hot'
         union all select "id" from "orders" where "ref" > 5 order by "id"`,
        [],
      );

      // intersect / except / except all (multiset semantics) vs hand SQL.
      const userOrders = () => db.select({ id: orders.id }).from(orders).where(sql`${orders.userId} = ${1}`);
      const hotOrders = () => db.select({ id: orders.id }).from(orders).where(eq(orders.label, "hot"));
      const i = await userOrders().intersect(hotOrders());
      assert.deepEqual(i.map((r) => r.id), [10]);
      const e = await userOrders().except(hotOrders());
      assert.deepEqual(e.map((r) => r.id), [11]);
      await compare(
        driver,
        "except all multiset",
        await hotOrders().exceptAll(userOrders()).orderBy(sql`id`) as Row[],
        `select "id" from "orders" where "label" = 'hot'
         except all select "id" from "orders" where "user_id" = 1 order by "id"`,
        [],
      );

      // Chaining composes left-associatively in call order (parenthesized
      // branches): (A union B) except C — call-order, not SQL precedence.
      const chained = await hotPaid().union(bigRef()).except(userOrders()).orderBy(sql`id`);
      assert.deepEqual(chained.map((r) => r.id), [12, 14, 15]);
      await compare(
        driver,
        "chained set operations",
        chained as Row[],
        `(select "id" from "orders" where "label" = 'hot'
          union (select "id" from "orders" where "ref" > 5))
         except (select "id" from "orders" where "user_id" = 1) order by "id"`,
        [],
      );
    });
  });

  test(`live subqueries (${driverKind}): V13 three-level alias and parameter composition with exact params`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const lvl1 = db.select({ userId: orders.userId, n: count() })
        .from(orders)
        .where(sql`${orders.total} > ${"1.00"}`)
        .groupBy(orders.userId);
      const agg = cteTable("agg", lvl1);
      const lvl2 = db.select({ userId: agg.userId, n: agg.n }).from(agg).where(sql`${agg.n} >= ${1}`);
      const wrapped = derivedTable("wrapped", lvl2);
      const lvl3 = db.select({ userId: wrapped.userId, n: wrapped.n, scaled: sql`${wrapped.n} * ${100}` })
        .from(wrapped)
        .orderBy(desc(wrapped.n), sql`${wrapped.userId}`);

      // Exact parameter array, deterministic order across three levels.
      const compiled = lvl3.toSQL();
      assert.deepEqual(compiled.params, [100, "1.00", 1]);
      const placeholders = [...compiled.sql.matchAll(/\$(\d+)/g)].map((m) => Number(m[1]));
      assert.deepEqual(placeholders, placeholders.map((_, k) => k + 1));

      const rows = await lvl3;
      // scaled is an expression projection: no decoder, so int8*int arrives
      // as the driver-native exact string (documented expression posture).
      assert.deepEqual([...rows], [
        { userId: 2, n: 2n, scaled: "200" },
        { userId: 1, n: 1n, scaled: "100" },
        { userId: 9, n: 1n, scaled: "100" },
      ]);
      await compare(
        driver,
        "three-level composition",
        rows as Row[],
        `select "userId", "n", "n" * $1 as "scaled" from (
           with "agg" as (
             select "user_id" as "userId", count(*) as "n" from "orders"
             where "total" > $2 group by "user_id"
           )
           select "agg"."userId", "agg"."n" from "agg" where "agg"."n" >= $3
         ) as "wrapped" order by "wrapped"."n" desc, "wrapped"."userId"`,
        [100, "1.00", 1],
        (r) => ({ userId: r.userId, n: BigInt(r.n as string | bigint), scaled: r.scaled }),
      );
      // scaled is an expression over an int8 derived column: int8 * int.
      void compiled;
    });
  });

  test(`live subqueries (${driverKind}): V13 CTE over a join and outer-join nullability on derived tables`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      // CTE whose source query joins users to orders (Q01 composition):
      // the CTE body is a left join against a derived table.
      const o = derivedTable("o", astSelect({ id: orders.id, userId: orders.userId, total: orders.total, note: orders.note }).from(orders));
      const src = db.select({ email: users.email, total: o.total, note: o.note, oid: o.id })
        .from(users)
        .leftJoin(o, sql`${o.userId} = ${users.id}`);
      const perUser = cteTable("per_user", src);
      const rows = await db.select({ email: perUser.email, note: perUser.note })
        .from(perUser)
        .where(sql`${perUser.total} > ${"0.00"}`)
        .orderBy(sql`"email"`);
      assert.deepEqual([...rows], [
        { email: "alice@x.com", note: "2026-01-02T03:04:05.678912" },
        { email: "alice@x.com", note: null },
        { email: "bob@x.com", note: "2026-02-03T04:05:06.000001" },
        { email: "bob@x.com", note: "2026-03-04T05:06:07.000001" },
      ]);
      await compare(
        driver,
        "CTE over left join, temporal round-trip through the CTE",
        rows as Row[],
        `select "users"."email", to_jsonb("o"."note")::text as "note"
         from "users" left join "orders" as "o" on "o"."user_id" = "users"."id"
         where "o"."total" > $1 order by "email", "note"`,
        ["0.00"],
        (r) => ({ email: r.email, note: r.note === null ? null : (r.note as string).replace(/^"|"$/g, "").replace(" ", "T") }),
      );

      // Outer-join nullability on a derived table: left-join the derived
      // handle; unmatched users read null (typed and actual).
      const counts = derivedTable("counts", db.select({ userId: orders.userId, n: count() }).from(orders).groupBy(orders.userId));
      const lj = await db.select({ email: users.email, orderCount: counts.n })
        .from(users)
        .leftJoin(counts, sql`${counts.userId} = ${users.id}`)
        .orderBy(sql`${users.id}`);
      assert.deepEqual([...lj], [
        { email: "alice@x.com", orderCount: 2n },
        { email: "bob@x.com", orderCount: 2n },
        { email: "carol@x.com", orderCount: 1n },
        { email: "dave@x.com", orderCount: null },
      ]);
      await compare(
        driver,
        "outer-join nullability on a derived table",
        lj as Row[],
        `select "users"."email", "counts"."n"::text as "orderCount" from "users"
         left join (select "user_id", count(*) as "n" from "orders" group by "user_id") as "counts"
           on "counts"."user_id" = "users"."id" order by "users"."id"`,
        [],
        (r) => ({ email: r.email, orderCount: r.orderCount === null ? null : BigInt(r.orderCount as string) }),
      );

      // Subquery in a join ON (CTE handle) with parameters bound in ON position.
      const big = cteTable("big", db.select({ id: orders.id }).from(orders).where(sql`${orders.total} > ${"100.00"}`));
      const onSub = await db.select({ email: users.email })
        .from(users)
        .innerJoin(big, sql`${big.id} in (${astSelect({ id: orders.id }).from(orders).where(sql`${orders.userId} = ${users.id}`).subquery()})`)
        .orderBy(sql`${users.id}`);
      assert.deepEqual(onSub.map((r) => r.email), ["bob@x.com"]);
      await compare(
        driver,
        "subquery in ON",
        onSub as Row[],
        `select "users"."email" from "users" inner join (
           select "id" from "orders" where "total" > $1
         ) as "big" on "big"."id" in (select "orders"."id" from "orders" where "orders"."user_id" = "users"."id")
         order by "users"."id"`,
        ["100.00"],
      );
    });
  });

  test(`live subqueries (${driverKind}): V13 correlated subquery two levels up`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      // users."id" is referenced from TWO subquery levels down: the scalar
      // count subquery and, inside it, the exists subquery.
      const inner = astSelect({ one: sql`1` })
        .from(edges)
        .where(sql`${edges.fromId} = ${orders.userId} and ${edges.toId} = ${users.id}`);
      const rows = await db.select({
        email: users.email,
        n: sql`(select ${count()} from ${orders} where ${orders.userId} = ${users.id} and exists ${inner.subquery()})`,
      })
        .from(users)
        .orderBy(sql`${users.id}`);
      assert.deepEqual([...rows], [
        { email: "alice@x.com", n: "0" },
        { email: "bob@x.com", n: "2" },
        { email: "carol@x.com", n: "0" },
        { email: "dave@x.com", n: "0" },
      ]);
      await compare(
        driver,
        "correlated two levels up",
        rows as Row[],
        `select "users"."email",
           (select count(*) from "orders"
            where "orders"."user_id" = "users"."id"
              and exists (select 1 from "edges" where "edges"."from_id" = "orders"."user_id" and "edges"."to_id" = "users"."id")) as "n"
         from "users" order by "users"."id"`,
        [],
        (r) => ({ email: r.email, n: r.n }),
      );

      // exists() as a typed where condition over a correlated subquery.
      const withOrders = await db.select({ email: users.email })
        .from(users)
        .where(exists(astSelect({ one: sql`1` }).from(orders).where(sql`${orders.userId} = ${users.id}`)))
        .orderBy(sql`${users.id}`);
      assert.deepEqual(withOrders.map((r) => r.email), ["alice@x.com", "bob@x.com", "carol@x.com"]);
    });
  });

  test(`live subqueries (${driverKind}): V13 recursive CTE terminates on a cyclic graph (cycle safety)`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      // edges: 1 -> 2 -> 3 -> 1 (a cycle) plus 1 -> 4. UNION (not ALL)
      // deduplicates, so traversal terminates; reachable(1) = {1,2,3,4}.
      const tree = cteTable("reach", astSelect({ node: edges.fromId })
        .from(edges)
        .where(sql`${edges.fromId} = ${1}`)
        .union(astSelect({ node: sql`${qual("e", "to_id")}` })
          .from(ident("reach"))
          .innerJoin(edges, "e", sql`${qual("e", "from_id")} = ${qual("reach", "node")}`)));
      const rows = await db.select({ node: tree.node }).from(tree).orderBy(sql`node`);
      assert.deepEqual(rows.map((r) => r.node).sort(), [1, 2, 3, 4]);
      await compare(
        driver,
        "recursive reachability over a cycle",
        rows as Row[],
        `with recursive "reach" as (
           select "from_id" as "node" from "edges" where "from_id" = $1
           union
           select "e"."to_id" as "node" from "reach"
             inner join "edges" as "e" on "e"."from_id" = "reach"."node"
         )
         select "node" from "reach" order by "node"`,
        [1],
      );

      // A depth-bounded UNION ALL traversal also terminates (explicit bound).
      const depths = cteTable("walk", astSelect({ node: edges.fromId, depth: sql`0` })
        .from(edges)
        .where(sql`${edges.fromId} = ${1}`)
        .distinct()
        .unionAll(astSelect({ node: sql`${qual("e", "to_id")}`, depth: sql`${qual("walk", "depth")} + 1` })
          .from(ident("walk"))
          .innerJoin(edges, "e", sql`${qual("e", "from_id")} = ${qual("walk", "node")}`)
          .where(sql`${qual("walk", "depth")} < ${2}`)));
      const walked = await db.select({ node: depths.node, depth: depths.depth }).from(depths).orderBy(sql`node`, sql`depth`);
      assert.deepEqual([...walked], [
        { node: 1, depth: 0 },
        { node: 2, depth: 1 },
        { node: 2, depth: 2 },
        { node: 3, depth: 2 },
        { node: 4, depth: 1 },
      ]);
      await compare(
        driver,
        "depth-bounded union all walk",
        walked as Row[],
        `with recursive "walk" as (
           select distinct "from_id" as "node", 0 as "depth" from "edges" where "from_id" = $1
           union all
           select "e"."to_id" as "node", "walk"."depth" + 1 as "depth" from "walk"
             inner join "edges" as "e" on "e"."from_id" = "walk"."node"
           where "walk"."depth" < $2
         )
         select "node", "depth" from "walk" order by "node", "depth"`,
        [1, 2],
      );
    });
  });

  // Q02 review BLOCKER-1 regression: timestamptz values must survive
  // derived/CTE composition byte-exactly — the canonical materialized text
  // is the UTC wall clock, and re-parsing it with ::timestamptz would
  // consult the SESSION timezone (the server's, not the process's), shifting
  // the value per composition level (+8h/level in Vancouver, -9h in Tokyo).
  // Both legs run against real session timezones: the server default AND an
  // explicitly pinned Asia/Tokyo database default.
  for (const sessionTz of [undefined, "Asia/Tokyo"] as const) {
    test(`live subqueries (${driverKind}): V13 timestamptz composes byte-exactly through 3 levels (session TZ ${sessionTz ?? "server default"})`, async () => {
      await withSuite(driverKind, async (db, driver) => {
        const zone = (await driver.query("show timezone")) as Row[];
        if (sessionTz !== undefined) {
          assert.equal(String(zone[0].TimeZone).toLowerCase(), sessionTz.toLowerCase());
        }

        const d1 = derivedTable("d1", db.select({ id: orders.id, seen: orders.seen, note: orders.note, day: orders.day }).from(orders));
        const d2 = cteTable("d2", db.select({ id: d1.id, seen: d1.seen, note: d1.note, day: d1.day }).from(d1));
        const d3 = derivedTable("d3", db.select({ id: d2.id, seen: d2.seen, note: d2.note, day: d2.day }).from(d2));
        const rows = await db.select({ id: d3.id, seen: d3.seen, note: d3.note, day: d3.day })
          .from(d3)
          .where(sql`${d3.id} in (${12}, ${14}, ${15})`)
          .orderBy(sql`${d3.id}`);

        // Byte-exact canonical strings with real microsecond data — under
        // either session timezone (before the fix: +8h per level in a
        // Vancouver session, e.g. 2026-02-04T04:05:06.000001Z after 3).
        assert.deepEqual([...rows], [
          { id: 12, seen: "2026-02-03T04:05:06.000001Z", note: "2026-02-03T04:05:06.000001", day: "2026-02-03" },
          { id: 14, seen: "2026-03-04T05:06:07.000001Z", note: "2026-03-04T05:06:07.000001", day: "2026-03-04" },
          { id: 15, seen: "2025-12-31T23:59:59.999999Z", note: null, day: "2025-12-31" },
        ]);

        // Independent hand-written oracle: the composed read must equal the
        // direct base-table read rendered through to_char (a different path
        // than the codec wire forms) — lossless round-trip fidelity.
        await compare(
          driver,
          `timestamptz through 3 levels (session TZ ${sessionTz ?? "default"})`,
          rows as Row[],
          `select "id",
             to_char("seen" at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "seen",
             to_char("note", 'YYYY-MM-DD"T"HH24:MI:SS.US') as "note",
             to_char("day", 'YYYY-MM-DD') as "day"
           from "orders" where "id" in ($1, $2, $3) order by "id"`,
          [12, 14, 15],
          (r) => {
            const norm6 = (s: unknown): string | null =>
              s === null ? null : String(s).replace(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$/, "$1.000000");
            return { id: r.id, seen: r.seen, note: norm6(r.note), day: r.day };
          },
        );
      }, sessionTz === undefined ? undefined : String(sessionTz));
    });
  }

  // Q02 review MAJOR-1 regression: min()/max() over derived/CTE temporal
  // columns — timestamptz (the review's failing case: timezone(unknown, text)
  // does not exist), naive timestamp, and date — execute and round-trip
  // exactly against hand SQL.
  test(`live subqueries (${driverKind}): V13 min/max over derived and CTE temporal columns (timestamptz, timestamp, date)`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const tsrc = derivedTable("tsrc", db.select({ id: orders.id, seen: orders.seen, note: orders.note, day: orders.day }).from(orders));
      const derivedRows = await db.select({
        loSeen: min(tsrc.seen),
        hiSeen: max(tsrc.seen),
        loNote: min(tsrc.note),
        hiNote: max(tsrc.note),
        loDay: min(tsrc.day),
        hiDay: max(tsrc.day),
      }).from(tsrc);
      assert.deepEqual([...derivedRows], [{
        loSeen: "2025-12-31T23:59:59.999999Z",
        hiSeen: "2026-03-04T05:06:07.000001Z",
        loNote: "2026-01-02T03:04:05.678912",
        hiNote: "2026-03-04T05:06:07.000001",
        loDay: "2025-12-31",
        hiDay: "2026-03-04",
      }]);
      await compare(
        driver,
        "min/max over derived temporal columns",
        derivedRows as Row[],
        `select
           to_char(min("seen") at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "loSeen",
           to_char(max("seen") at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "hiSeen",
           to_char(min("note"), 'YYYY-MM-DD"T"HH24:MI:SS.US') as "loNote",
           to_char(max("note"), 'YYYY-MM-DD"T"HH24:MI:SS.US') as "hiNote",
           min("day")::text as "loDay",
           max("day")::text as "hiDay"
         from "orders"`,
        [],
        (r) => {
          const norm6 = (s: unknown): string | null =>
            s === null ? null : String(s).replace(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$/, "$1.000000");
          return {
            loSeen: r.loSeen, hiSeen: r.hiSeen,
            loNote: norm6(r.loNote), hiNote: norm6(r.hiNote),
            loDay: r.loDay, hiDay: r.hiDay,
          };
        },
      );

      // Same shape through a CTE handle, grouped (the MAJOR-1 repro shape:
      // min over a derived/CTE timestamptz column previously failed at
      // execution with timezone(unknown, text)).
      const csrc = cteTable("csrc", db.select({ userId: orders.userId, seen: orders.seen, note: orders.note }).from(orders));
      const cteRows = await db.select({ userId: csrc.userId, first: min(csrc.seen), last: max(csrc.seen), firstNote: min(csrc.note) })
        .from(csrc)
        .groupBy(csrc.userId)
        .orderBy(sql`${csrc.userId}`);
      assert.deepEqual([...cteRows], [
        { userId: 1, first: "2026-01-02T03:04:05.678912Z", last: "2026-01-02T03:04:05.678912Z", firstNote: "2026-01-02T03:04:05.678912" },
        { userId: 2, first: "2026-02-03T04:05:06.000001Z", last: "2026-03-04T05:06:07.000001Z", firstNote: "2026-02-03T04:05:06.000001" },
        { userId: 3, first: null, last: null, firstNote: null },
        { userId: 9, first: "2025-12-31T23:59:59.999999Z", last: "2025-12-31T23:59:59.999999Z", firstNote: null },
      ]);
      await compare(
        driver,
        "min/max over CTE temporal columns (grouped)",
        cteRows as Row[],
        `select "user_id" as "userId",
           to_char(min("seen") at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "first",
           to_char(max("seen") at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "last",
           to_char(min("note"), 'YYYY-MM-DD"T"HH24:MI:SS.US') as "firstNote"
         from "orders" group by "user_id" order by "user_id"`,
        [],
        (r) => {
          const norm6 = (s: unknown): string | null =>
            s === null ? null : String(s).replace(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$/, "$1.000000");
          return { userId: r.userId, first: r.first, last: r.last, firstNote: norm6(r.firstNote) };
        },
      );
    });
  });
}
