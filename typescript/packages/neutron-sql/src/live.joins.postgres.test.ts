import assert from "node:assert/strict";
import test from "node:test";
import {
  alias,
  and,
  createDatabase,
  eq,
  not,
  or,
  pgSchema,
  pgTable,
  bigint,
  integer,
  numeric,
  serial,
  text,
  timestamp,
  sql,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Q01 live suite (V07/V13 join legs): every query built through the typed
// join API is executed against real Postgres and compared row-for-row with
// independently authored hand-written SQL. Oracle statements are written
// here in plain SQL — never produced or helped by the compiler. Covers the
// five join forms, ON precedence with mixed AND/OR/fragments, self joins,
// same SQL table+column names in two schemas, expression projections over
// joined columns, outer-join nulls decoded through the codecs as null (not
// zero values), and parameter ordering across projection + on + where +
// limit. Both drivers run against a uniquely named q01_ throwaway database.

const DB_NAME = uniqueDbName("q01_joins");
const ALT_SCHEMA = "q01_alt";

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  name: text("name"),
});

const alt = pgSchema(ALT_SCHEMA);
const altUsers = alt.table("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  name: text("name"),
});

const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  note: timestamp("note"),
  total: numeric("total"),
  ref: bigint("ref"),
});

const employees = pgTable("employees", {
  id: serial("id").primaryKey(),
  managerId: integer("manager_id"),
  mentorId: integer("mentor_id"),
  name: text("name").notNull(),
});

const colors = pgTable("colors", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
});

type Tables = { users: typeof users; orders: typeof orders; employees: typeof employees; colors: typeof colors };
type TestDb = NeutronDatabase<Tables>;

// Hand-written fixture DDL and seed — never through the ORM.
async function seed(driver: { query: (s: string, p?: unknown[]) => Promise<unknown>; execute: (s: string, p?: unknown[]) => Promise<unknown> }): Promise<void> {
  await driver.execute(`create schema "${ALT_SCHEMA}"`);
  await driver.execute(`create table "users" (
    "id" serial primary key,
    "email" text not null,
    "name" text
  )`);
  await driver.execute(`create table "${ALT_SCHEMA}"."users" (
    "id" serial primary key,
    "email" text not null,
    "name" text
  )`);
  await driver.execute(`create table "orders" (
    "id" serial primary key,
    "user_id" integer not null,
    "note" timestamp,
    "total" numeric,
    "ref" bigint
  )`);
  await driver.execute(`create table "employees" (
    "id" serial primary key,
    "manager_id" integer,
    "mentor_id" integer,
    "name" text not null
  )`);
  await driver.execute(`create table "colors" (
    "id" serial primary key,
    "name" text not null
  )`);
  await driver.execute(`insert into "users" ("id", "email", "name") values
    (1, 'alice@x.com', 'Alice'),
    (2, 'bob@x.com', 'Bob'),
    (3, 'carol@x.com', null),
    (4, 'dave@x.com', 'Dave')`);
  await driver.execute(`insert into "${ALT_SCHEMA}"."users" ("id", "email", "name") values
    (1, 'alt-one@x.com', 'One'),
    (2, 'alt-two@x.com', 'Two'),
    (9, 'alt-nine@x.com', 'Nine')`);
  await driver.execute(`insert into "orders" ("id", "user_id", "note", "total", "ref") values
    (10, 1, '2026-01-02T03:04:05.678912', '19.50', 9007199254740993),
    (11, 1, null, '0.010', null),
    (12, 2, '2026-02-03T04:05:06.000001', '120.75', 42),
    (13, 3, null, null, null),
    (14, 99, '2026-03-04T05:06:07.000001', '7.00', 7)`);
  await driver.execute(`insert into "employees" ("id", "manager_id", "mentor_id", "name") values
    (1, null, null, 'Big Boss'),
    (2, 1, 3, 'Alice'),
    (3, 1, null, 'Bob'),
    (4, 2, 2, 'Carol')`);
  await driver.execute(`insert into "colors" ("id", "name") values (1, 'red'), (2, 'blue')`);
}

interface DriverLike {
  query<T = Record<string, unknown>>(s: string, p?: unknown[]): Promise<T[]>;
  execute(s: string, p?: unknown[]): Promise<unknown>;
}

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb, driver: DriverLike) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live joins (${driverKind})`))) {
    return;
  }
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
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
    if (/^q01_joins_[0-9_]+$/.test(DB_NAME)) {
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

/** Execute the hand-written oracle and deep-compare rows. Both sides carry
 *  explicit ORDER BY; `normalize` maps both sides onto comparable shapes
 *  when the oracle formats values independently (to_char / ::text). */
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
  assert.deepEqual(got.map(n), want.map(n), `${label}: compiled join results differ from hand-written SQL`);
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live joins (${driverKind}): V13 five join forms match hand-written SQL with exact rows`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const o = alias(orders, "o");

      // J1 — inner join, expression projection over joined columns,
      // parameter order: projection param ($1) before where param ($2);
      // limit inline. Oracle written by hand with the same intended shape.
      const j1 = await db.select({ email: users.email, bumped: sql`${o.total} * ${2}` })
        .from(users)
        .innerJoin(o, sql`${o.userId} = ${users.id}`)
        .where(eq(users.email, "alice@x.com"))
        .orderBy(sql`${o.id}`)
        .limit(2);
      assert.deepEqual([...j1], [
        { email: "alice@x.com", bumped: "39.00" },
        { email: "alice@x.com", bumped: "0.020" },
      ]);
      await compare(
        driver,
        "J1 inner + expression projection",
        j1 as Row[],
        `select "users"."email", ("o"."total" * 2)::text as "bumped" from "users" ` +
          `inner join "orders" as "o" on "o"."user_id" = "users"."id" ` +
          `where "users"."email" = $1 order by "o"."id" limit 2`,
        ["alice@x.com"],
      );

      // J2 — left join with a real unmatched from-row: every joined column
      // arrives as null and decodes as null (never zero values); matched
      // rows keep microsecond temporals and int8 precision.
      const j2 = await db.select({ email: users.email, total: o.total, note: o.note, ref: o.ref })
        .from(users)
        .leftJoin(o, sql`${o.userId} = ${users.id}`)
        .orderBy(sql`${users.id}`, sql`${o.id}`);
      assert.deepEqual([...j2], [
        { email: "alice@x.com", total: "19.50", note: "2026-01-02T03:04:05.678912", ref: 9007199254740993n },
        { email: "alice@x.com", total: "0.010", note: null, ref: null },
        { email: "bob@x.com", total: "120.75", note: "2026-02-03T04:05:06.000001", ref: 42n },
        { email: "carol@x.com", total: null, note: null, ref: null },
        { email: "dave@x.com", total: null, note: null, ref: null },
      ]);
      await compare(
        driver,
        "J2 left join nulls",
        j2 as Row[],
        `select "users"."email", "o"."total", to_char("o"."note", 'YYYY-MM-DD"T"HH24:MI:SS.US') as "note", "o"."ref"::text as "ref" ` +
          `from "users" left join "orders" as "o" on "o"."user_id" = "users"."id" order by "users"."id", "o"."id"`,
        [],
        (r) => ({ email: r.email, total: r.total, note: r.note, ref: r.ref === null ? null : BigInt(r.ref as string) }),
      );

      // J3 — right join: the from side carries the nulls (order 14 points
      // at user 99, which does not exist).
      const j3 = await db.select({ email: users.email, total: o.total })
        .from(users)
        .rightJoin(o, sql`${o.userId} = ${users.id}`)
        .orderBy(sql`${o.id}`);
      assert.equal(j3.length, 5);
      assert.deepEqual({ ...j3[4] }, { email: null, total: "7.00" });
      await compare(
        driver,
        "J3 right join",
        j3 as Row[],
        `select "users"."email", "o"."total" from "users" ` +
          `right join "orders" as "o" on "o"."user_id" = "users"."id" order by "o"."id"`,
        [],
      );

      // J4 — full join: both unmatched sides survive.
      const j4 = await db.select({ email: users.email, orderId: sql`${o.id}` })
        .from(users)
        .fullJoin(o, sql`${o.userId} = ${users.id}`)
        .orderBy(sql`${users.id}`, sql`${o.id}`);
      assert.equal(j4.length, 6);
      assert.deepEqual({ ...j4[4] }, { email: "dave@x.com", orderId: null });
      assert.deepEqual({ ...j4[5] }, { email: null, orderId: 14 });
      await compare(
        driver,
        "J4 full join",
        j4 as Row[],
        `select "users"."email", "o"."id" as "orderId" from "users" ` +
          `full join "orders" as "o" on "o"."user_id" = "users"."id" order by "users"."id", "o"."id"`,
        [],
      );

      // J5 — cross join: 4 users x 2 colors.
      const c = alias(colors, "c");
      const j5 = await db.select({ email: users.email, color: c.name })
        .from(users)
        .crossJoin(c)
        .orderBy(sql`${users.id}`, sql`${c.id}`);
      assert.equal(j5.length, 8);
      await compare(
        driver,
        "J5 cross join",
        j5 as Row[],
        `select "users"."email", "c"."name" as "color" from "users" ` +
          `cross join "colors" as "c" order by "users"."id", "c"."id"`,
        [],
      );
    });
  });

  test(`live joins (${driverKind}): V13 ON precedence with mixed and/or/fragments matches intended grouping`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const o = alias(orders, "o");

      // P1 — and(col-to-col, fragment-with-or): intended
      //   user_id = users.id AND (total > 100 OR note is null)
      // Bare splicing of the fragment would bind `or` tighter than `and`
      // and silently change the row set.
      const p1 = await db.select({ email: users.email, orderId: sql`${o.id}` })
        .from(users)
        .innerJoin(o, and(sql`${o.userId} = ${users.id}`, sql`${o.total} > ${100} or ${o.note} is null`))
        .orderBy(sql`${o.id}`);
      assert.deepEqual(p1.map((r) => r.orderId), [11, 12, 13]);
      await compare(
        driver,
        "P1 and(eq, frag-with-or)",
        p1 as Row[],
        `select "users"."email", "o"."id" as "orderId" from "users" ` +
          `inner join "orders" as "o" on ("o"."user_id" = "users"."id" and ("o"."total" > $1 or "o"."note" is null)) ` +
          `order by "o"."id"`,
        [100],
      );

      // P2 — not(or(eq, fragment)) on a left join: intended
      //   NOT (user_id = 1 OR total < 50)
      const p2 = await db.select({ orderId: sql`${o.id}` })
        .from(users)
        .leftJoin(o, not(or(eq(o.userId, 1), sql`${o.total} < ${50}`)))
        .orderBy(sql`${o.id}`);
      // The ON condition does not reference users, so it matches per pair:
      // order 12 alone satisfies NOT (…) for every user row (its total
      // 120.75 is not < 50 and its user_id is not 1); order 13's NULL total
      // makes the OR NULL → excluded; orders 10/11/14 fail positively.
      assert.deepEqual(p2.map((r) => r.orderId), [12, 12, 12, 12]);
      await compare(
        driver,
        "P2 not(or(eq, frag))",
        p2 as Row[],
        `select "o"."id" as "orderId" from "users" ` +
          `left join "orders" as "o" on (not (("o"."user_id" = $1) or ("o"."total" < $2))) ` +
          `order by "o"."id" nulls last`,
        [1, 50],
      );

      // P3 — bare fragment ON with authored mixed and/or: the author owns
      // the grouping in the ON position (no compiler operator applied).
      const p3 = await db.select({ orderId: sql`${o.id}` })
        .from(users)
        .innerJoin(o, sql`${o.userId} = ${users.id} and (${o.total} > ${100} or ${o.note} is null)`)
        .orderBy(sql`${o.id}`);
      assert.deepEqual(p3.map((r) => r.orderId), [11, 12, 13]);
      await compare(
        driver,
        "P3 authored fragment ON",
        p3 as Row[],
        `select "o"."id" as "orderId" from "users" ` +
          `inner join "orders" as "o" on "o"."user_id" = "users"."id" and ("o"."total" > $1 or "o"."note" is null) ` +
          `order by "o"."id"`,
        [100],
      );
    });
  });

  test(`live joins (${driverKind}): V13 self join — one table three times through aliases`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const mgr = alias(employees, "mgr");
      const mentor = alias(employees, "mentor");
      const rows = await db.select({ name: employees.name, manager: mgr.name, mentorName: mentor.name })
        .from(employees)
        .leftJoin(mgr, sql`${mgr.id} = ${employees.managerId}`)
        .leftJoin(mentor, sql`${mentor.id} = ${employees.mentorId}`)
        .orderBy(sql`${employees.id}`);
      assert.deepEqual([...rows], [
        { name: "Big Boss", manager: null, mentorName: null },
        { name: "Alice", manager: "Big Boss", mentorName: "Bob" },
        { name: "Bob", manager: "Big Boss", mentorName: null },
        { name: "Carol", manager: "Alice", mentorName: "Alice" },
      ]);
      await compare(
        driver,
        "self join twice via aliases",
        rows as Row[],
        `select "employees"."name", "mgr"."name" as "manager", "mentor"."name" as "mentorName" from "employees" ` +
          `left join "employees" as "mgr" on "mgr"."id" = "employees"."manager_id" ` +
          `left join "employees" as "mentor" on "mentor"."id" = "employees"."mentor_id" ` +
          `order by "employees"."id"`,
        [],
      );
    });
  });

  test(`live joins (${driverKind}): V13 same table and column names in two schemas join without collision`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const au = alias(altUsers, "au");

      // Cross-schema join of two tables that share the SQL name "users"
      // (and the column names id/email/name). Output keys are the
      // projection keys — unambiguous by construction.
      const rows = await db.select({ pub: users.email, altEmail: au.email })
        .from(users)
        .innerJoin(au, sql`${au.id} = ${users.id}`)
        .orderBy(sql`${users.id}`);
      assert.deepEqual([...rows], [
        { pub: "alice@x.com", altEmail: "alt-one@x.com" },
        { pub: "bob@x.com", altEmail: "alt-two@x.com" },
      ]);
      await compare(
        driver,
        "cross-schema same-name join",
        rows as Row[],
        `select "users"."email" as "pub", "au"."email" as "altEmail" from "users" ` +
          `inner join "${ALT_SCHEMA}"."users" as "au" on "au"."id" = "users"."id" ` +
          `order by "users"."id"`,
        [],
      );

      // Qualified reads address the RIGHT table: the alt schema's row 9 has
      // no public twin, and its emails are disjoint from public's.
      const altRows = await db.select().from(altUsers).orderBy(sql`${altUsers.id}`);
      assert.deepEqual(
        altRows.map((r) => r.email),
        ["alt-one@x.com", "alt-two@x.com", "alt-nine@x.com"],
      );
      await compare(
        driver,
        "qualified select from the alt schema",
        altRows as unknown as Row[],
        `select "id", "email", "name" from "${ALT_SCHEMA}"."users" order by "id"`,
        [],
      );

      // Qualified writes land in the alt schema only.
      const inserted = await db.insert(altUsers).values({ id: 10, email: "alt-ten@x.com", name: "Ten" }).returning();
      assert.deepEqual([...inserted].map((r) => r.email), ["alt-ten@x.com"]);
      const oracleCount = await driver.query<{ pub: number; alt: number }>(
        `select (select count(*)::int from "users") as "pub", (select count(*)::int from "${ALT_SCHEMA}"."users") as "alt"`,
      );
      assert.deepEqual(oracleCount[0], { pub: 4, alt: 4 });
      await db.delete(altUsers).where(eq(altUsers.id, 10));
      const after = await driver.query<{ alt: number }>(`select count(*)::int as "alt" from "${ALT_SCHEMA}"."users"`);
      assert.equal(after[0].alt, 3);
    });
  });

  test(`live joins (${driverKind}): V13 outer-join nullability with real NULL data through the codecs`, async () => {
    await withSuite(driverKind, async (db, driver) => {
      const o = alias(orders, "o");

      // The nullable side of a left join carries temporal, numeric and int8
      // columns with real NULLs (order 13 belongs to carol with all-NULL
      // payload; dave matches no order at all). Outer nulls decode as null —
      // not epoch strings, not 0n, not "" — and non-null values keep exact
      // microseconds and int8 digits.
      const rows = await db.select({ email: users.email, note: o.note, total: o.total, ref: o.ref })
        .from(users)
        .leftJoin(o, sql`${o.userId} = ${users.id}`)
        .orderBy(sql`${users.id}`, sql`${o.id}`);
      // rows: alice/order10, alice/order11, bob/order12, carol/order13, dave/—
      assert.equal(rows[0].note, "2026-01-02T03:04:05.678912");
      assert.equal(rows[0].ref, 9007199254740993n);
      assert.equal(rows[1].note, null);
      assert.equal(rows[1].total, "0.010");
      assert.equal(rows[1].ref, null);
      assert.equal(rows[2].note, "2026-02-03T04:05:06.000001");
      assert.equal(rows[3].note, null);
      assert.equal(rows[3].total, null);
      assert.equal(rows[3].ref, null);
      assert.equal(rows[4].note, null);
      assert.equal(rows[4].total, null);
      assert.equal(rows[4].ref, null);

      // Independent ground truth for the same intended rows: the oracle
      // formats through to_char/casts, never through the ORM's codecs.
      await compare(
        driver,
        "outer nulls vs independent oracle",
        rows as Row[],
        `select "users"."email", to_char("o"."note", 'YYYY-MM-DD"T"HH24:MI:SS.US') as "note", "o"."total", "o"."ref"::text as "ref" ` +
          `from "users" left join "orders" as "o" on "o"."user_id" = "users"."id" ` +
          `order by "users"."id", "o"."id"`,
        [],
        (r) => ({ email: r.email, note: r.note, total: r.total, ref: r.ref === null ? null : BigInt(r.ref as string) }),
      );

      // coalesce over an outer-joined column is an expression projection:
      // values compare against a hand-written expression oracle.
      const coalesced = await db.select({ email: users.email, totalOrZero: sql`coalesce(${o.total}, ${"0.00"})` })
        .from(users)
        .leftJoin(o, sql`${o.userId} = ${users.id}`)
        .orderBy(sql`${users.id}`, sql`${o.id}`);
      assert.deepEqual([...coalesced].map((r) => r.totalOrZero), ["19.50", "0.010", "120.75", "0.00", "0.00"]);
      await compare(
        driver,
        "coalesce over joined column",
        coalesced as Row[],
        `select "users"."email", coalesce("o"."total", '0.00')::text as "totalOrZero" ` +
          `from "users" left join "orders" as "o" on "o"."user_id" = "users"."id" ` +
          `order by "users"."id", "o"."id"`,
        [],
      );
    });
  });
}
