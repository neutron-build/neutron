import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  pgTable,
  serial,
  text,
  integer,
  boolean,
  varchar,
  schemaToDDL,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V01 suite: insert semantics must be independent of JavaScript object
// order. Connection selection and skip/fail semantics come from
// ./live-harness.ts; a uniquely named throwaway database (per process) is
// created and dropped by this file.

const DB_NAME = uniqueDbName("neutron_orm_b01");

// camelCase properties -> snake_case physical names. firstName/lastName are a
// same-type (text) column pair, so a value swap cannot hide behind a DB type
// error. nickname is nullable WITH a default, distinguishing omitted (DEFAULT)
// from explicit null (NULL). points is NOT NULL with a default.
const people = pgTable("people", {
  id: serial("id").primaryKey(),
  firstName: text("first_name").notNull(),
  lastName: text("last_name").notNull(),
  nickname: text("nickname").default("anon"),
  points: integer("points").notNull().default(10),
  email: varchar("email", 255).notNull().unique(),
});

// Every column has a default: default-only inserts are legal here.
const allDefaults = pgTable("all_defaults", {
  id: serial("id").primaryKey(),
  label: text("label").notNull().default("dflt"),
  flag: boolean("flag").notNull().default(false),
});

// Escaped/quoted names: embedded double quote in table and column names,
// reserved word as a physical column name.
const quotedNames = pgTable('we"ird', {
  id: serial("id").primaryKey(),
  reserved: text("group").notNull(),
  quoted: text('va"l').notNull(),
});

const PEOPLE_SELECT =
  'select id, first_name as "firstName", last_name as "lastName", nickname as "nickname", points as "points" from people order by id';

type PeopleRow = { id: number; firstName: string; lastName: string; nickname: string | null; points: number };

interface SuiteFixture {
  db: NeutronDatabase;
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live insert (${driverKind})`))) {
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
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { people, allDefaults, quotedNames },
  });
  try {
    for (const stmt of schemaToDDL([people, allDefaults, quotedNames])) {
      await db.driver.execute(stmt);
    }
    await fn({ db });
  } finally {
    await db.close();
    if (/^neutron_orm_b01_[0-9_]+$/.test(DB_NAME)) {
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

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live insert (${driverKind}): V01 object key order never permutes values`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // Canonical pair: second row reverses the first row's key order.
      await db.insert(people).values([
        { firstName: "Alice", lastName: "One", email: "a@x.com" },
        { lastName: "Two", firstName: "Bob", email: "b@x.com" },
      ]);
      const rows = (await db.driver.query(PEOPLE_SELECT)) as PeopleRow[];
      assert.equal(rows.length, 2);
      assert.equal(rows[0].firstName, "Alice");
      assert.equal(rows[0].lastName, "One");
      assert.equal(rows[1].firstName, "Bob");
      assert.equal(rows[1].lastName, "Two");

      // Independent oracle: same data inserted with a hand-written bound
      // INSERT, read back with a direct SELECT using explicit aliases.
      await db.driver.execute(
        "create table people_oracle (id serial primary key, first_name text not null, last_name text not null, nickname text default 'anon', points integer not null default 10)",
      );
      await db.driver.execute(
        "insert into people_oracle (first_name, last_name) values ($1, $2), ($3, $4)",
        ["Alice", "One", "Bob", "Two"],
      );
      const oracle = await db.driver.query<PeopleRow>(
        'select id, first_name as "firstName", last_name as "lastName", nickname as "nickname", points as "points" from people_oracle order by id',
      );
      assert.deepEqual(
        rows.map((r) => ({ firstName: r.firstName, lastName: r.lastName, nickname: r.nickname, points: r.points })),
        oracle.map((r) => ({ firstName: r.firstName, lastName: r.lastName, nickname: r.nickname, points: r.points })),
      );

      // Every permutation of a three-key row in one batch (rows 3..8).
      const perms = [
        { firstName: "F0", lastName: "L0", nickname: "N0", email: "p0@x.com" },
        { firstName: "F1", nickname: "N1", lastName: "L1", email: "p1@x.com" },
        { lastName: "L2", firstName: "F2", nickname: "N2", email: "p2@x.com" },
        { lastName: "L3", nickname: "N3", firstName: "F3", email: "p3@x.com" },
        { nickname: "N4", firstName: "F4", lastName: "L4", email: "p4@x.com" },
        { nickname: "N5", lastName: "L5", firstName: "F5", email: "p5@x.com" },
      ];
      await db.insert(people).values(perms);
      const permRows = ((await db.driver.query(PEOPLE_SELECT)) as PeopleRow[]).slice(2, 8);
      for (let i = 0; i < 6; i++) {
        assert.equal(permRows[i].firstName, `F${i}`, `perm ${i} firstName`);
        assert.equal(permRows[i].lastName, `L${i}`, `perm ${i} lastName`);
        assert.equal(permRows[i].nickname, `N${i}`, `perm ${i} nickname`);
        assert.equal(permRows[i].points, 10, `perm ${i} points default`);
      }
    });
  });

  test(`live insert (${driverKind}): V01 omitted/undefined request DEFAULT, explicit null is NULL`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(people).values([
        { firstName: "S0", lastName: "T0", nickname: "nick", points: 1, email: "s0@x.com" }, // full row
        { firstName: "S1", lastName: "T1", email: "s1@x.com" }, // omitted -> defaults
        { firstName: "S2", lastName: "T2", nickname: null, points: undefined, email: "s2@x.com" }, // null stays NULL
        { firstName: "S3", lastName: "T3", points: 3, email: "s3@x.com" }, // nickname absent
      ]);
      const rows = (await db.driver.query(PEOPLE_SELECT)) as PeopleRow[];
      assert.equal(rows[0].nickname, "nick");
      assert.equal(rows[0].points, 1);
      assert.equal(rows[1].nickname, "anon", "omitted nullable-with-default gets DEFAULT");
      assert.equal(rows[1].points, 10);
      assert.equal(rows[2].nickname, null, "explicit null is NULL, never DEFAULT");
      assert.equal(rows[2].points, 10, "undefined requests DEFAULT");
      assert.equal(rows[3].nickname, "anon");
      assert.equal(rows[3].points, 3);
    });
  });

  test(`live insert (${driverKind}): V01 default-only rows use valid syntax`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const single = db.insert(allDefaults).values([{}]);
      const singleSql = single.toSQL().sql;
      assert.ok(!/\(\s*\)\s*values\s*\(\s*\)/i.test(singleSql), `must not emit INSERT () VALUES (): ${singleSql}`);
      await single;
      let stored = await db.driver.query<{ label: string; flag: boolean }>("select label, flag from all_defaults order by id");
      assert.equal(stored.length, 1);
      assert.equal(stored[0].label, "dflt");
      assert.equal(stored[0].flag, false);

      const multi = db.insert(allDefaults).values([{}, {}]);
      const multiSql = multi.toSQL().sql;
      assert.ok(!/\(\s*\)\s*values\s*\(\s*\)/i.test(multiSql), `must not emit INSERT () VALUES (): ${multiSql}`);
      await multi;
      stored = await db.driver.query<{ label: string; flag: boolean }>("select label, flag from all_defaults order by id");
      assert.equal(stored.length, 3);
      assert.ok(stored.every((r) => r.label === "dflt" && r.flag === false));
    });
  });

  test(`live insert (${driverKind}): V01 escaped and quoted names survive insert and read-back`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(quotedNames).values([
        { reserved: "g1", quoted: "q1" },
        { quoted: "q2", reserved: "g2" }, // reordered keys
      ]);
      const rows = await db.driver.query<{ grp: string; val: string }>(
        'select "group" as "grp", "va""l" as "val" from "we""ird" order by id',
      );
      assert.equal(rows.length, 2);
      assert.equal(rows[0].grp, "g1");
      assert.equal(rows[0].val, "q1");
      assert.equal(rows[1].grp, "g2");
      assert.equal(rows[1].val, "q2");
    });
  });

  test(`live insert (${driverKind}): V01 returning reflects stored values on reordered batches`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // Returning keys are declared property names (B02); values must still
      // be correct regardless of each row's key order.
      const back = await db
        .insert(people)
        .values([
          { firstName: "R1", lastName: "Last1", email: "r1@x.com" },
          { lastName: "Last2", firstName: "R2", email: "r2@x.com" },
        ])
        .returning();
      assert.equal(back.length, 2);
      assert.equal(back[0].firstName, "R1");
      assert.equal(back[0].lastName, "Last1");
      assert.equal(back[0].nickname, "anon");
      assert.ok(!("first_name" in back[0]), "physical names must not appear in results");
      assert.equal(back[1].firstName, "R2");
      assert.equal(back[1].lastName, "Last2");
      assert.equal(back[1].nickname, "anon");

      const check = await db.driver.query<{ first_name: string; last_name: string }>(
        "select first_name, last_name from people order by id",
      );
      assert.equal(check[0].first_name, "R1");
      assert.equal(check[1].first_name, "R2");
    });
  });

  test(`live insert (${driverKind}): V01 unknown keys and empty arrays rejected before execution`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // Excess-key rows are a runtime contract; bypass the compile-time check.
      await assert.rejects(
        async () => {
          await db.insert(people).values([
            { firstName: "U1", lastName: "X", email: "u1@x.com" },
            { firstName: "U2", lastName: "X", email: "u2@x.com", bogus: "nope" } as never,
          ]);
        },
        /unknown column "bogus"/,
      );
      await assert.rejects(async () => {
        await db.insert(people).values([]);
      }, /empty array/);
      const cnt = await db.driver.query<{ n: number }>("select count(*)::int as n from people");
      assert.equal(cnt[0].n, 0, "rejected batches must not partially insert");
    });
  });

  test(`live insert (${driverKind}): V01 failure never partially inserts a batch`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(people).values([{ firstName: "K1", lastName: "X", email: "k@x.com" }]);
      const before = (await db.driver.query<{ n: number }>("select count(*)::int as n from people"))[0].n;

      await assert.rejects(
        async () => {
          await db.insert(people).values([
            { firstName: "K2", lastName: "X", email: "k2@x.com" },
            { firstName: "K3", lastName: "X", email: "k@x.com" }, // duplicate email
            { firstName: "K4", lastName: "X", email: "k4@x.com" },
          ]);
        },
        /duplicate key|unique/,
      );
      await assert.rejects(
        async () => {
          await db
            .insert(people)
            .values([
              { firstName: "K5", lastName: "X", email: "k5@x.com" },
              { firstName: "K6", lastName: "X", email: "k@x.com" }, // duplicate again
            ])
            .returning();
        },
        /duplicate key|unique/,
      );
      const after = (await db.driver.query<{ n: number }>("select count(*)::int as n from people"))[0].n;
      assert.equal(after, before, "no partial batch survives a failure");
    });
  });
}
