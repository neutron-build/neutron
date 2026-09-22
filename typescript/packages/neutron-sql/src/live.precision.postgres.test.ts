import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  asc,
  pgTable,
  serial,
  integer,
  text,
  bigint,
  numeric,
  double,
  timestamp,
  bytea,
  relations,
  schemaToDDL,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// B05 rework live suite (F1/F2): precision and honesty of child leaves through
// the relation JSON path. int8/numeric leaves must arrive as exact strings
// (rendered ::text inside the aggregation), temporal/bytea leaves as their
// to_jsonb string forms, int4/float8 leaves as JS numbers. Composite int8
// keys must still correlate and order correctly. Connection selection and
// skip/fail semantics come from ./live-harness.ts; one unique disposable
// database per run, created and dropped.

const DB_NAME = uniqueDbName("neutron_orm_b05rw");

// wallets.id is int8 beyond Number.MAX_SAFE_INTEGER. moves carries a composite
// int8 primary key (both components marked primary so relation ordering uses
// the ordered tuple; the physical DDL is hand-written below because composite
// PK DDL is out of B05 scope), an int8 non-key leaf, a numeric leaf, int4 and
// float8 leaves that must stay numbers, and temporal/bytea leaves.
const wallets = pgTable("b05rw_wallets", {
  id: bigint("id").primaryKey(),
  label: text("label").notNull(),
});

const moves = pgTable("b05rw_moves", {
  walletId: bigint("wallet_id").notNull().references(() => wallets.id),
  dayKey: bigint("day_key").notNull().primaryKey(),
  seq: bigint("seq").notNull().primaryKey(),
  amount: numeric("amount").notNull(),
  delta: bigint("delta").notNull(),
  flag: integer("flag").notNull(),
  weight: double("weight").notNull(),
  postedAt: timestamp("posted_at").notNull(),
  payload: bytea("payload"),
  memo: text("memo"),
});

const MOVES_DDL =
  'create table "b05rw_moves" (\n' +
  '  "wallet_id" bigint not null references "b05rw_wallets" ("id"),\n' +
  '  "day_key" bigint not null,\n' +
  '  "seq" bigint not null,\n' +
  '  "amount" numeric not null,\n' +
  '  "delta" bigint not null,\n' +
  '  "flag" integer not null,\n' +
  '  "weight" double precision not null,\n' +
  '  "posted_at" timestamp not null,\n' +
  '  "payload" bytea,\n' +
  '  "memo" text,\n' +
  '  primary key ("day_key", "seq")\n' +
  ")";

const walletsRelations = relations(wallets, ({ many }) => ({ moves: many(moves) }));
const movesRelations = relations(moves, ({ one }) => ({
  wallet: one(wallets, { fields: [moves.walletId], references: [wallets.id] }),
}));

type Tables = { wallets: typeof wallets; moves: typeof moves };
type RelationsMap = { wallets: typeof walletsRelations; moves: typeof movesRelations };
type TestDb = NeutronDatabase<Tables, RelationsMap>;

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live precision (${driverKind})`))) {
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
    driver: { driver: driverKind },
    tables: { wallets, moves },
    relations: { wallets: walletsRelations, moves: movesRelations },
  });
  for (const stmt of schemaToDDL([wallets])) {
    await db.driver.execute(stmt);
  }
  await db.driver.execute(MOVES_DDL);
  try {
    await fn(db);
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

async function seed(db: TestDb): Promise<void> {
  await db.insert(wallets).values([
    { id: "9007199254740993", label: "W1" },
    { id: "9007199254740995", label: "W2" },
  ]);
  await db.insert(moves).values([
    // (1,11) and (11,1) collide under key string concat ("111" both); the
    // ordered tuple must keep both, in numeric order.
    { walletId: "9007199254740993", dayKey: "1", seq: "11", amount: "1234567890123456789012345678901.23", delta: "-9007199254740993", flag: 7, weight: 0.1, postedAt: new Date(2026, 0, 1, 19, 4, 5, 678), payload: Buffer.from([0x00, 0xff, 0x10]), memo: "m1" },
    { walletId: "9007199254740993", dayKey: "11", seq: "1", amount: "1.50", delta: "9007199254740995", flag: 8, weight: 1.5, postedAt: new Date(2026, 0, 2, 1, 2, 3, 0), memo: null },
    { walletId: "9007199254740993", dayKey: "1", seq: "9007199254740995", amount: "0.10", delta: "1", flag: 9, weight: -2.25, postedAt: new Date(2026, 0, 3, 4, 5, 6, 7) },
    { walletId: "9007199254740995", dayKey: "5", seq: "5", amount: "2.25", delta: "2", flag: 10, weight: 3.5, postedAt: new Date(2026, 0, 4, 7, 8, 9, 9) },
  ]);
  // Microsecond timestamp (a Date cannot carry 6 fraction digits): raw seed
  // with inlined literals — binding the timestamp string as a parameter lets
  // postgres.js route it through a Date (UTC shift + millisecond truncation).
  await db.driver.query(
    `insert into "b05rw_moves" ("wallet_id", "day_key", "seq", "amount", "delta", "flag", "weight", "posted_at", "payload", "memo") ` +
      `values ($1, $2, $3, $4, $5, $6, $7, '2026-01-01 19:04:05.678123'::timestamp, '\\x00ff10'::bytea, null)`,
    ["9007199254740993", "11", "9007199254740993", "99999999999999999999.99", "-1", 11, 0.5],
  );
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live precision (${driverKind}): int8/numeric/temporal/bytea child leaves arrive lossless and honestly typed`, async () => {
    await withSuite(driverKind, async (db) => {
      await seed(db);

      const w1 = await db.query.wallets.findFirst({ where: eq(wallets.label, "W1"), with: { moves: true } });
      assert.ok(w1);
      // Parent row (flat path): int8 is the default-mode bigint, exact.
      assert.equal(w1.id, 9007199254740993n);
      assert.equal(typeof w1.id, "bigint");

      const m = w1.moves;
      assert.equal(m.length, 4);

      // Composite int8 PK order is the numeric tuple, not string concat:
      // (1,11) < (1,9007199254740995) < (11,1) < (11,9007199254740993).
      assert.deepEqual(
        m.map((row) => [row.dayKey, row.seq]),
        [
          [1n, 11n],
          [1n, 9007199254740995n],
          [11n, 1n],
          [11n, 9007199254740993n],
        ],
      );

      // int8 leaves: exact strings, never doubles (9007199254740993 would
      // arrive as 9007199254740992 and 9007199254740995 as 9007199254740996
      // through an uncast jsonb number).
      assert.equal(m[0].delta, -9007199254740993n);
      assert.equal(m[1].seq, 9007199254740995n);
      assert.equal(m[2].delta, 9007199254740995n);
      assert.equal(m[3].seq, 9007199254740993n);
      for (const row of m) {
        assert.equal(typeof row.dayKey, "bigint");
        assert.equal(typeof row.seq, "bigint");
        assert.equal(typeof row.delta, "bigint");
      }

      // numeric leaves: 30+ digit values and trailing-zero scale survive exactly.
      assert.equal(m[0].amount, "1234567890123456789012345678901.23");
      assert.equal(m[1].amount, "0.10");
      assert.equal(m[2].amount, "1.50");
      assert.equal(m[3].amount, "99999999999999999999.99");
      for (const row of m) {
        assert.equal(typeof row.amount, "string");
      }

      // JS-safe leaves stay numbers (no blanket cast).
      assert.equal(m[0].flag, 7);
      assert.equal(typeof m[0].flag, "number");
      assert.equal(m[0].weight, 0.1);
      assert.equal(typeof m[0].weight, "number");
      assert.equal(m[3].weight, 0.5);

      // temporal leaves keep their canonical string form; bytea leaves
      // decode from the \x hex text form to Uint8Array (F03).
      assert.equal(typeof m[0].postedAt, "string");
      assert.equal(m[3].postedAt, "2026-01-01T19:04:05.678123", "microseconds intact");
      assert.ok(m[0].payload instanceof Uint8Array, "bytea child leaf decodes to Uint8Array");
      assert.equal(Buffer.from(m[0].payload).toString("hex"), "00ff10");
      assert.equal(Buffer.from(m[3].payload as Uint8Array).toString("hex"), "00ff10");
      assert.equal(m[1].payload, null, "SQL NULL becomes JSON null");
      assert.equal(m[1].memo, null);
      assert.equal(m[0].memo, "m1");

      // Independent oracle: hand-written correlated SQL for the same child set.
      const oracle = (await db.driver.query(
        `select w.id::text as id,
                (select coalesce(jsonb_agg(jsonb_build_object('dayKey', x.day_key::text, 'seq', x.seq::text) order by x.day_key, x.seq), '[]'::jsonb)
                   from b05rw_moves x where x.wallet_id = w.id) as moves
           from b05rw_wallets w where w.label = $1`,
        ["W1"],
      )) as Array<{ id: string; moves: Array<{ dayKey: string; seq: string }> }>;
      assert.equal(oracle[0].id, "9007199254740993");
      assert.deepEqual(
        oracle[0].moves.map((row) => [row.dayKey, row.seq]),
        m.map((row) => [String(row.dayKey), String(row.seq)]),
      );

      // Flat-path agreement: relation child leaves equal flat driver values.
      const flat = await db.select().from(moves);
      const flatBySeq = new Map(flat.map((row) => [row.seq, row]));
      for (const row of m) {
        const f = flatBySeq.get(row.seq);
        assert.ok(f);
        assert.equal(row.dayKey, f.dayKey);
        assert.equal(row.amount, f.amount);
        assert.equal(row.weight, f.weight);
      }

      // Timestamp agreement: PG's own ::text rendering, 'T' separator per to_jsonb.
      const tsOracle = (await db.driver.query(
        "select posted_at::text as s from b05rw_moves where day_key = $1 and seq = $2",
        ["1", "11"],
      )) as Array<{ s: string }>;
      assert.equal(m[0].postedAt, tsOracle[0].s.replace(" ", "T"));
    });
  });

  test(`live precision (${driverKind}): composite int8 correlation keys still match through the ::text casts`, async () => {
    await withSuite(driverKind, async (db) => {
      await seed(db);

      // Children collect per parent exactly — the JSON projection casts never
      // reach the correlation predicates, so W2 sees only its own move.
      const all = await db.query.wallets.findMany({ with: { moves: true } });
      assert.equal(all.length, 2);
      const byLabel = new Map(all.map((w) => [w.label, w]));
      assert.equal(byLabel.get("W1")!.moves.length, 4);
      const w2Moves = byLabel.get("W2")!.moves;
      assert.equal(w2Moves.length, 1);
      assert.deepEqual([w2Moves[0].dayKey, w2Moves[0].seq], [5n, 5n]);
      assert.equal(w2Moves[0].amount, "2.25");

      // To-one through an int8 key: the child finds its parent exactly.
      const move = await db.query.moves.findFirst({
        where: eq(moves.dayKey, "11"),
        orderBy: [asc(moves.seq)],
        with: { wallet: true },
      });
      assert.ok(move);
      assert.ok(move.wallet);
      assert.equal(move.wallet.id, 9007199254740993n);
      assert.equal(typeof move.wallet.id, "bigint");
      assert.equal(move.wallet.label, "W1");
    });
  });
}
