// ---------------------------------------------------------------------------
// Q07 live leg (V08/V10 family): enums and arrays through the real wire on
// both drivers, views read through the query layer, custom codecs over real
// values, and database-generated columns refusing writes against a live
// server. The schema DDL here is hand-written (the legacy TS DDL emitter
// fails closed on Q07 features by design; end-to-end DDL/diff lives in the
// Go round-trip suite).
// ---------------------------------------------------------------------------

import test from "node:test";
import assert from "node:assert/strict";
import {
  bigint,
  boolean,
  createDatabase,
  eq,
  integer,
  numeric,
  pgEnum,
  pgTable,
  pgView,
  serial,
  sql,
  text,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive } from "./live-harness.js";

const mood = pgEnum("q07_mood", ["sad", "ok", "glad"]);

const cards = pgTable("q07_cards", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  tone: mood("tone").notNull(),
  tags: text("tags").array().notNull(),
  scores: bigint("scores").array(),
  flags: boolean("flags").array(),
  toneList: mood("tone_list").array(),
});

const money = pgTable("q07_money", {
  id: serial("id").primaryKey(),
  amount: numeric("amount").codec({
    decode: (v: string): { cents: number } => ({ cents: Math.round(Number(v) * 100) }),
    encode: (v: { cents: number }): string => (v.cents / 100).toFixed(2),
  }),
});

const gen = pgTable("q07_gen", {
  id: integer("id").generatedAlwaysAsIdentity().primaryKey(),
  n: integer("n").notNull(),
  doubled: integer("doubled").generatedAlwaysAs(sql`n * 2`),
});

const members = pgTable("q07_members", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  active: boolean("active").notNull().default(true),
});

const activeMembers = pgView(
  "q07_active_members",
  { id: members.id, email: members.email },
  { definition: sql`select ${members.id}, ${members.email} from ${members} where ${members.active} = true` },
);

const SCHEMA_SQL = [
  `create type q07_mood as enum ('sad', 'ok', 'glad')`,
  `create table q07_cards (id serial primary key, title text not null, tone q07_mood not null, tags text[] not null, scores bigint[], flags boolean[], tone_list q07_mood[])`,
  `create table q07_money (id serial primary key, amount numeric not null)`,
  `create table q07_gen (id integer generated always as identity primary key, n integer not null, doubled integer generated always as (n * 2) stored)`,
  `create table q07_members (id serial primary key, email text not null, active boolean not null default true)`,
  `create view q07_active_members as select id, email from q07_members where active = true`,
];

type CardsDb = NeutronDatabase<{ cards: typeof cards }>;
type MoneyDb = NeutronDatabase<{ money: typeof money }>;
type GenDb = NeutronDatabase<{ gen: typeof gen }>;
type MembersDb = NeutronDatabase<{ members: typeof members }>;

async function withQ07Suite<T extends CardsDb | MoneyDb | GenDb | MembersDb>(driver: "postgres" | "pg", fn: (db: T) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live q07 (${driver})`))) return;
  const dbName = `q07_ts_${driver}_${process.pid}_${Date.now() % 100000}`;
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${dbName}"`);
  await admin.query(`create database "${dbName}"`);
  await admin.end();
  const url = new URL(TEST_URL);
  url.pathname = `/${dbName}`;
  const db = (await createDatabase({
    url: url.toString(),
    driverOptions: { driver },
    tables: { cards, money, gen, members },
  })) as unknown as T;
  for (const stmt of SCHEMA_SQL) {
    await db.driver.execute(stmt);
  }
  try {
    await fn(db);
  } finally {
    await db.close();
    const drop = new Pool({ connectionString: TEST_URL, max: 1 });
    await drop.query(`drop database if exists "${dbName}"`);
    await drop.end();
  }
}

for (const driver of ["postgres", "pg"] as const) {
  test(`q07 live (${driver}): enums and arrays round-trip losslessly`, async () => {
    await withQ07Suite(driver, async (db) => {
      await db.insert(cards).values({ title: "first", tone: "ok", tags: ["alpha", "beta", null], scores: [1n, 9007199254740993n, null], flags: [true, false, null], toneList: ["sad", "glad", null] });

      const row = (await db.select().from(cards).where(eq(cards.title, "first")))[0]!;
      assert.equal(row.tone, "ok");
      assert.deepEqual(row.tags, ["alpha", "beta", null]);
      assert.deepEqual(row.scores, [1n, 9007199254740993n, null]);
      assert.deepEqual(row.flags, [true, false, null]);
      assert.deepEqual(row.toneList, ["sad", "glad", null]);

      // Array predicates bind as array literals; unknown enum values reject.
      const filtered = await db.select().from(cards).where(eq(cards.tone, "glad" as never));
      assert.equal(filtered.length, 0);
      await assert.rejects(async () => db.select().from(cards).where(eq(cards.tone, "angry" as never)), /not a declared value/);

      // A database value outside the declared union fails the read loudly
      // instead of widening the type.
      await db.driver.execute(`alter type q07_mood add value 'meh'`);
      await db.driver.execute(`insert into q07_cards (title, tone, tags) values ('drifted', 'meh', '{}')`);
      await assert.rejects(async () => db.select().from(cards).where(eq(cards.title, "drifted")), /drifted from the schema declaration|meh/);
    });
  });

  test(`q07 live (${driver}): custom codecs map real numeric values both ways`, async () => {
    await withQ07Suite(driver, async (db) => {
      await db.insert(money).values({ amount: { cents: 12345 } });
      const row = (await db.select().from(money).where(eq(money.id, 1)))[0]!;
      assert.deepEqual(row.amount, { cents: 12345 });
      await db.update(money).set({ amount: { cents: 7 } }).where(eq(money.id, 1));
      const after = (await db.select().from(money).where(eq(money.amount, { cents: 7 })))[0]!;
      assert.equal(after.id, 1);
      const raw = await db.driver.query<{ amount: string }>("select amount::text as amount from q07_money where id = 1");
      assert.equal(raw[0].amount, "0.07");
    });
  });

  test(`q07 live (${driver}): identity and generated columns compute server-side and reject writes`, async () => {
    await withQ07Suite(driver, async (db) => {
      const inserted = await db.insert(gen).values({ n: 21 }).returning();
      assert.equal(inserted[0]!.id, 1);
      assert.equal(inserted[0]!.doubled, 42);
      await assert.rejects(
        async () => db.insert(gen).values({ id: 5, n: 1 } as never),
        /GENERATED ALWAYS AS IDENTITY|cannot insert into column/i,
      );
      await assert.rejects(
        async () => db.insert(gen).values({ n: 1, doubled: 3 } as never).returning(),
        /GENERATED ALWAYS AS \(stored\)|generated column/i,
      );
    });
  });

  test(`q07 live (${driver}): views read like tables and reject mutations`, async () => {
    await withQ07Suite(driver, async (db) => {
      await db.insert(members).values([{ email: "a@x.io" }, { email: "b@x.io" }, { email: "gone@x.io", active: false }]);
      const rows = await db.select().from(activeMembers).where(eq(activeMembers.email, "a@x.io"));
      assert.equal(rows.length, 1);
      assert.equal(rows[0]!.email, "a@x.io");
      const all = await db.select().from(activeMembers);
      assert.equal(all.length, 2);
      assert.throws(
        () => (db as unknown as { insert: (t: unknown) => { values: (v: unknown) => Promise<unknown> } }).insert(activeMembers).values({ email: "x" }),
        /is a view — views are read-only/,
      );
    });
  });
}
