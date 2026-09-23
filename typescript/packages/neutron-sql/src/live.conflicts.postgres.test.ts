import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  excluded,
  getSqlState,
  integer,
  numeric,
  pgTable,
  bigint,
  schemaToDDL,
  sql,
  serial,
  text,
  timestamptz,
  bytea,
  uniqueIndex,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V13 suite (Q03): conflict handling against REAL uniqueness violations —
// plain unique, composite unique, partial unique index (WHERE), ON CONSTRAINT —
// conditional upserts, DO NOTHING / DO UPDATE returning arity per PostgreSQL
// semantics, precision-sensitive returning values, mapped keys, the same-batch
// "cannot affect row a second time" error, and key-order-invariant
// duplicate-assignment rejections. Every behavior case is checked against
// independently authored hand SQL executed on an identical oracle twin table.
// Live databases are q03_-prefixed throwaways; the shared admin DB's
// _neutron_migrations is never touched.

const DB_NAME = uniqueDbName("q03_conflicts");

const members = pgTable("q03_members", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  hits: integer("hits").notNull().default(0),
  score: numeric("score"),
});

const duo = pgTable(
  "q03_duo",
  {
    id: serial("id").primaryKey(),
    a: integer("a").notNull(),
    b: text("b").notNull(),
    tally: integer("tally").notNull().default(0),
  },
  (t) => [uniqueIndex("q03_duo_ab").on(t.a, t.b)],
);

const soft = pgTable("q03_soft", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  deletedAt: timestamptz("deleted_at"),
});

const exact = pgTable("q03_exact", {
  key: text("key").notNull().unique(),
  big: bigint("big"),
  amt: numeric("amt"),
  at: timestamptz("at"),
  payload: bytea("payload"),
});

const wide = pgTable("q03_wide", {
  id: serial("id").primaryKey(),
  userEmail: text("user_email").notNull().unique(),
  loginCount: integer("login_count").notNull().default(0),
});

const dupPhysical = pgTable("q03_dup", {
  id: serial("id").primaryKey(),
  first: text("shared_col").notNull(),
  second: text("shared_col").notNull(),
  email: text("email").notNull(),
});

interface SuiteFixture {
  db: NeutronDatabase;
  /** Statement counter fed by the database logger: proves pre-SQL rejections
   *  executed nothing. */
  statements: { count: number };
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live conflicts (${driverKind})`))) {
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
  const statements = { count: 0 };
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { members, duo, soft, exact, wide, dupPhysical },
    logger: () => {
      statements.count += 1;
    },
  });
  try {
    for (const stmt of schemaToDDL([members, duo, soft, exact, wide])) {
      await db.driver.execute(stmt);
    }
    // Partial unique index (predicate indexes are not expressible in the
    // current TableIndex surface — created with explicit DDL for this suite).
    await db.driver.execute("create unique index q03_soft_email_live on q03_soft (email) where deleted_at is null");
    // Oracle twins: hand-written DDL (deliberately NOT `create table like`,
    // which would share sequences and drop the unique constraints the
    // conflict behavior depends on).
    await db.driver.execute(
      "create table q03_members_oracle (id serial primary key, email text not null unique, hits integer not null default 0, score numeric)",
    );
    await db.driver.execute(
      "create table q03_duo_oracle (id serial primary key, a integer not null, b text not null, tally integer not null default 0)",
    );
    await db.driver.execute("create unique index q03_duo_oracle_ab on q03_duo_oracle (a, b)");
    await db.driver.execute("create table q03_soft_oracle (id serial primary key, email text not null, deleted_at timestamptz)");
    await db.driver.execute("create unique index q03_soft_oracle_email_live on q03_soft_oracle (email) where deleted_at is null");
    await db.driver.execute(
      "create table q03_wide_oracle (id serial primary key, user_email text not null unique, login_count integer not null default 0)",
    );
    await fn({ db, statements });
  } finally {
    await db.close();
    if (/^q03_conflicts_[0-9_]+$/.test(DB_NAME)) {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type MemberRow = { id: number; email: string; hits: number; score: string | null };

async function memberRows(db: NeutronDatabase, table = "q03_members"): Promise<MemberRow[]> {
  return (await db.driver.query(`select id, email, hits, score from ${table} order by id`)) as MemberRow[];
}

const INT8_A = 9007199254742241n; // 2^53 + 12345 (beyond JS safe range)
const INT8_B = 9223372036854775807n; // int8 max
const NUMERIC_40D = "-9998887776665554443332221110009998887776.5"; // 40 digits, signed, fractional
const TS_US = "2027-07-14T13:45:22.123456Z"; // microsecond precision
const BYTES = new Uint8Array([0x00, 0xff, 0x00, 0xff, 0xee, 0x00, 0xff, 0xff]);

for (const driverKind of ["postgres", "pg"] as const) {
  // -------------------------------------------------------------------------
  // DO NOTHING — real unique conflicts, hand-SQL oracle
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): DO NOTHING skips conflicting rows; returning shows the actual inserted set`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.driver.execute("insert into q03_members (email, hits) values ($1, 1)", ["seed@x.com"]);
      await db.driver.execute("insert into q03_members_oracle (email, hits) values ($1, 1)", ["seed@x.com"]);

      const back = await db
        .insert(members)
        .values([
          { email: "fresh@x.com", hits: 5, score: "1.50" },
          { email: "seed@x.com", hits: 9 }, // real unique conflict
        ])
        .onConflictDoNothing()
        .returning();

      // PostgreSQL semantics: DO NOTHING omits conflicted rows from RETURNING.
      assert.equal(back.length, 1);
      assert.equal(back[0].email, "fresh@x.com");
      assert.equal(back[0].hits, 5);
      assert.equal(back[0].score, "1.50");

      // Independent oracle: hand-written statement on the twin.
      await db.driver.execute(
        "insert into q03_members_oracle (email, hits, score) values ($1, $2, $3), ($4, $5, default) on conflict do nothing",
        ["fresh@x.com", 5, "1.50", "seed@x.com", 9],
      );
      const [ours, oracle] = await Promise.all([memberRows(db), memberRows(db, "q03_members_oracle")]);
      assert.deepEqual(ours.map(({ email, hits, score }) => ({ email, hits, score })), oracle.map(({ email, hits, score }) => ({ email, hits, score })));
      assert.equal(ours.length, 2);
      assert.equal(ours[1].email, "fresh@x.com", "conflicting row was not inserted");
    });
  });

  test(`live conflicts (${driverKind}): DO NOTHING non-returning variant reports zero affected rows on conflict`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(members).values({ email: "only@x.com" });
      const affected = await db.insert(members).values({ email: "only@x.com" }).onConflictDoNothing();
      assert.equal(affected, 0, "the conflicted row is not inserted, so no rows are affected");
      const inserted = await db.insert(members).values({ email: "other@x.com" }).onConflictDoNothing();
      assert.equal(inserted, 1);
    });
  });

  test(`live conflicts (${driverKind}): DO NOTHING with in-batch duplicates inserts distinct rows without error`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const back = await db
        .insert(members)
        .values([
          { email: "d1@x.com", hits: 1 },
          { email: "d1@x.com", hits: 2 }, // conflicts with the row above inside one batch
          { email: "d2@x.com", hits: 3 },
        ])
        .onConflictDoNothing()
        .returning();
      assert.equal(back.length, 2);
      const rows = await memberRows(db);
      assert.deepEqual(
        rows.map((r) => r.email),
        ["d1@x.com", "d2@x.com"],
      );
      assert.equal(rows[0].hits, 1, "first row of the pair wins; the second is skipped");
    });
  });

  test(`live conflicts (${driverKind}): DO NOTHING with an explicit target and ON CONSTRAINT`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(members).values({ email: "t@x.com" });
      const viaTarget = await db.insert(members).values({ email: "t@x.com" }).onConflictDoNothing({ target: members.email }).returning();
      assert.equal(viaTarget.length, 0);
      // PostgreSQL default name for an inline unique column constraint.
      const viaConstraint = await db
        .insert(members)
        .values({ email: "t@x.com" })
        .onConflictDoNothing({ target: { constraint: "q03_members_email_key" } })
        .returning();
      assert.equal(viaConstraint.length, 0);
      // A nonexistent constraint name surfaces the database error honestly.
      await assert.rejects(
        async () => {
          await db.insert(members).values({ email: "t@x.com" }).onConflictDoNothing({ target: { constraint: "no_such_uc" } });
        },
        /does not exist|no unique or exclusion constraint/,
      );
    });
  });

  // -------------------------------------------------------------------------
  // DO UPDATE — excluded values, returning arity, conditional predicates
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): DO UPDATE returns one row per input row (inserted or updated), excluded refs apply`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.driver.execute("insert into q03_members (email, hits, score) values ($1, $2, $3)", ["u@x.com", 10, "1.5"]);
      await db.driver.execute("insert into q03_members_oracle (email, hits, score) values ($1, $2, $3)", ["u@x.com", 10, "1.5"]);

      const back = await db
        .insert(members)
        .values([
          { email: "u@x.com", hits: 4, score: "2.5" }, // conflict -> update
          { email: "u2@x.com", hits: 7, score: "3.5" }, // fresh insert
        ])
        .onConflictUpdate({
          target: members.email,
          set: { hits: sql`${excluded(members.hits)} + 1`, score: sql`${excluded(members.score)}` },
        })
        .returning();

      assert.equal(back.length, 2, "one returning row per input row: inserted or updated");
      const byEmail = new Map(back.map((r) => [r.email, r]));
      assert.equal(byEmail.get("u@x.com")!.hits, 5, "4 (excluded) + 1");
      assert.equal(byEmail.get("u@x.com")!.score, "2.5");
      assert.equal(byEmail.get("u2@x.com")!.hits, 7);

      // Hand oracle: identical upsert written independently.
      await db.driver.execute(
        "insert into q03_members_oracle (email, hits, score) values ($1, $2, $3), ($4, $5, $6) " +
          'on conflict ("email") do update set "hits" = excluded."hits" + 1, "score" = excluded."score"',
        ["u@x.com", 4, "2.5", "u2@x.com", 7, "3.5"],
      );
      const [ours, oracle] = await Promise.all([memberRows(db), memberRows(db, "q03_members_oracle")]);
      assert.deepEqual(ours.map(({ email, hits, score }) => ({ email, hits, score })), oracle.map(({ email, hits, score }) => ({ email, hits, score })));
    });
  });

  test(`live conflicts (${driverKind}): conditional upsert — setWhere rejects the update and omits the row from returning`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // c1 carries a non-null score so the fragment's `or score is null` arm
      // cannot rescue it; c2 is eligible via hits < 10.
      await db.driver.execute("insert into q03_members (email, hits, score) values ($1, $2, $3)", ["c1@x.com", 100, "5.5"]);
      await db.driver.execute("insert into q03_members (email, hits) values ($1, $2)", ["c2@x.com", 1]);
      await db.driver.execute("insert into q03_members_oracle (email, hits, score) values ($1, $2, $3), ($4, $5, default)", ["c1@x.com", 100, "5.5", "c2@x.com", 1]);

      // setWhere true only when the existing row's hits are small; fragment
      // carries a top-level `or` to pin precedence inside the conflict clause.
      const setWhere = sql`${members.hits} < ${10} or ${members.score} is null`;
      const back = await db
        .insert(members)
        .values([
          { email: "c1@x.com", hits: 1 }, // existing hits=100 -> predicate false
          { email: "c2@x.com", hits: 2 }, // existing hits=1 -> predicate true
        ])
        .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)}` }, setWhere })
        .returning();

      assert.equal(back.length, 1, "the rejected conflict is neither updated nor returned");
      assert.equal(back[0].email, "c2@x.com");
      const rows = await memberRows(db);
      const c1 = rows.find((r) => r.email === "c1@x.com")!;
      assert.equal(c1.hits, 100, "setWhere-false row keeps its stored value");
      assert.equal(rows.find((r) => r.email === "c2@x.com")!.hits, 2);

      await db.driver.execute(
        "insert into q03_members_oracle (email, hits) values ($1, $2), ($3, $4) " +
          'on conflict ("email") do update set "hits" = excluded."hits" where q03_members_oracle."hits" < $5 or q03_members_oracle."score" is null',
        ["c1@x.com", 1, "c2@x.com", 2, 10],
      );
      const oracle = await memberRows(db, "q03_members_oracle");
      assert.deepEqual(
        rows.map(({ email, hits, score }) => ({ email, hits, score })),
        oracle.map(({ email, hits, score }) => ({ email, hits, score })),
      );
    });
  });

  // -------------------------------------------------------------------------
  // Partial unique index + composite target
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): partial unique index target infers via the index predicate`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const target = { columns: [soft.email] as const, where: sql`${soft.deletedAt} is null` };
      const first = await db.insert(soft).values({ email: "p@x.com" }).onConflictDoNothing({ target: [soft.email], where: sql`${soft.deletedAt} is null` }).returning();
      assert.equal(first.length, 1);
      const second = await db.insert(soft).values({ email: "p@x.com" }).onConflictDoNothing({ target }).returning();
      assert.equal(second.length, 0, "live row conflicts on the partial index");

      // Tombstone the row: the partial index no longer covers it.
      await db.driver.execute("update q03_soft set deleted_at = $1 where email = $2", ["2026-01-01T00:00:00Z", "p@x.com"]);
      await db.driver.execute("update q03_soft_oracle set deleted_at = $1 where email = $2", ["2026-01-01T00:00:00Z", "p@x.com"]);

      // Conditional upsert against the partial index: a fresh live row.
      const up = await db
        .insert(soft)
        .values({ email: "p@x.com" })
        .onConflictUpdate({ target, set: { deletedAt: null } })
        .returning();
      assert.equal(up.length, 1, "no conflict: the only matching row is tombstoned");

      // Hand oracle with the same shape on the twin.
      await db.driver.execute(
        "insert into q03_soft_oracle (email) values ($1), ($2) on conflict (email) where deleted_at is null do nothing",
        ["q@x.com", "q@x.com"],
      );
      const rows = (await db.driver.query(
        "select email, deleted_at is null as live from q03_soft order by id",
      )) as Array<{ email: string; live: boolean }>;
      assert.deepEqual(
        rows.map((r) => [r.email, r.live]),
        [
          ["p@x.com", false],
          ["p@x.com", true],
        ],
      );
    });
  });

  test(`live conflicts (${driverKind}): composite unique target conflicts and updates by the full key tuple`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(duo).values({ a: 1, b: "x", tally: 10 });
      const back = await db
        .insert(duo)
        .values([
          { a: 1, b: "x", tally: 5 }, // full composite key conflict
          { a: 1, b: "y", tally: 7 }, // same a, different b: no conflict
          { a: 2, b: "x", tally: 9 }, // same b, different a: no conflict
        ])
        .onConflictUpdate({ target: [duo.a, duo.b], set: { tally: sql`${excluded(duo.tally)} + ${100}` } })
        .returning();

      assert.equal(back.length, 3);
      const sorted = [...back].sort((r, z) => r.a - z.a || (r.b < z.b ? -1 : 1));
      assert.equal(sorted[0].tally, 105, "conflicted row updated: excluded 5 + 100");
      assert.equal(sorted[1].tally, 7);
      assert.equal(sorted[2].tally, 9);

      // Hand oracle on the twin with the identical batch.
      await db.driver.execute("insert into q03_duo_oracle (a, b, tally) values ($1, $2, $3)", [1, "x", 10]);
      await db.driver.execute(
        "insert into q03_duo_oracle (a, b, tally) values ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) " +
          'on conflict ("a", "b") do update set "tally" = excluded."tally" + $10',
        [1, "x", 5, 1, "y", 7, 2, "x", 9, 100],
      );
      const [ours, oracle] = (await Promise.all([
        db.driver.query("select a, b, tally from q03_duo order by a, b"),
        db.driver.query("select a, b, tally from q03_duo_oracle order by a, b"),
      ])) as [unknown, unknown];
      assert.deepEqual(ours, oracle);
    });
  });

  // -------------------------------------------------------------------------
  // Same-batch double affect — PostgreSQL's own cardinality error, surfaced
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): two rows in one batch conflicting with each other surface the database error and change nothing`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(members).values({ email: "keep@x.com", hits: 1 });
      await assert.rejects(
        async () => {
          await db
            .insert(members)
            .values([
              { email: "twice@x.com", hits: 1 },
              { email: "twice@x.com", hits: 2 },
            ])
            .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)}` } });
        },
        (err: unknown) => {
          assert.match(err instanceof Error ? err.message : String(err), /cannot affect row a second time/);
          assert.equal(getSqlState(err), "21000");
          return true;
        },
      );
      const rows = await memberRows(db);
      assert.equal(rows.length, 1, "the failed statement is atomic: none of its rows landed");
      assert.equal(rows[0].email, "keep@x.com");
    });
  });

  // -------------------------------------------------------------------------
  // Precision-sensitive returning — insert path and conflict-update path
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): upsert returning keeps int8/numeric/timestamptz/bytea byte-exact on both paths`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // Insert path.
      const inserted = await db
        .insert(exact)
        .values({ key: "k1", big: INT8_A, amt: NUMERIC_40D, at: TS_US, payload: BYTES })
        .returning(["key", "big", "amt", "at", "payload"]);
      assert.equal(inserted.length, 1);
      assert.equal(inserted[0].big, INT8_A);
      assert.equal(inserted[0].amt, NUMERIC_40D);
      assert.equal(inserted[0].at, TS_US);
      assert.ok(bytesEqual(inserted[0].payload, BYTES));

      // Hand oracle: read the stored truth back through lossless text forms.
      const truth = (await db.driver.query(
        "select big::text, amt::text, to_jsonb(at at time zone 'UTC')::text as at, encode(payload, 'hex') as payload from q03_exact where key = $1",
        ["k1"],
      )) as Array<{ big: string; amt: string; at: string; payload: string }>;
      assert.equal(truth[0].big, INT8_A.toString());
      assert.equal(truth[0].amt, NUMERIC_40D);
      assert.equal(JSON.parse(truth[0].at), TS_US.replace("Z", ""));
      assert.equal(truth[0].payload, Buffer.from(BYTES).toString("hex"));

      // Conflict-update path: excluded references carry the same precision,
      // including int8 max (excluded bound as an exact string) and a new
      // microsecond timestamp written through the update SET.
      const updated = await db
        .insert(exact)
        .values({ key: "k1", big: INT8_B, amt: "0.000001", at: "1999-12-31T23:59:59.999999Z", payload: new Uint8Array([0x00]) })
        .onConflictUpdate({
          target: exact.key,
          set: {
            big: sql`${excluded(exact.big)}`,
            amt: sql`${excluded(exact.amt)}`,
            at: sql`${excluded(exact.at)}`,
            payload: sql`${excluded(exact.payload)}`,
          },
        })
        .returning(["key", "big", "amt", "at", "payload"]);
      assert.equal(updated.length, 1);
      assert.equal(updated[0].big, INT8_B);
      assert.equal(updated[0].amt, "0.000001");
      assert.equal(updated[0].at, "1999-12-31T23:59:59.999999Z");
      assert.ok(bytesEqual(updated[0].payload, new Uint8Array([0x00])));

      const truth2 = (await db.driver.query(
        "select big::text, amt::text, to_jsonb(at at time zone 'UTC')::text as at, encode(payload, 'hex') as payload from q03_exact where key = $1",
        ["k1"],
      )) as Array<{ big: string; amt: string; at: string; payload: string }>;
      assert.equal(truth2[0].big, INT8_B.toString());
      assert.equal(truth2[0].amt, "0.000001");
      assert.equal(JSON.parse(truth2[0].at), "1999-12-31T23:59:59.999999");
      assert.equal(truth2[0].payload, "00");
    });
  });

  test(`live conflicts (${driverKind}): update ... returning subset stays byte-exact for precision-sensitive columns`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(exact).values({ key: "u1", big: 1n, amt: "1.5", at: "2020-01-01T00:00:00Z", payload: new Uint8Array([1]) });
      const back = await db
        .update(exact)
        .set({ big: INT8_A, amt: NUMERIC_40D, at: TS_US })
        .where(eq(exact.key, "u1"))
        .returning(["big", "amt", "at"]);
      assert.equal(back.length, 1);
      assert.equal(back[0].big, INT8_A);
      assert.equal(back[0].amt, NUMERIC_40D);
      assert.equal(back[0].at, TS_US);
      assert.ok(!("payload" in back[0]), "unselected columns are absent from subset rows");
      const truth = (await db.driver.query(
        "select big::text, amt::text from q03_exact where key = $1",
        ["u1"],
      )) as Array<{ big: string; amt: string }>;
      assert.equal(truth[0].big, INT8_A.toString());
      assert.equal(truth[0].amt, NUMERIC_40D);
    });
  });

  // -------------------------------------------------------------------------
  // Mapped keys
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): mapped property keys survive upsert returning and excluded references`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const first = await db.insert(wide).values({ userEmail: "mapped@x.com", loginCount: 3 }).returning();
      assert.equal(first[0].userEmail, "mapped@x.com");
      assert.ok(!("user_email" in first[0]), "physical names must not appear in results");

      const back = await db
        .insert(wide)
        .values({ userEmail: "mapped@x.com", loginCount: 5 })
        .onConflictUpdate({ target: wide.userEmail, set: { loginCount: sql`${excluded(wide.loginCount)} + ${1}` } })
        .returning(["userEmail", "loginCount"]);
      assert.equal(back.length, 1);
      assert.equal(back[0].userEmail, "mapped@x.com");
      assert.equal(back[0].loginCount, 6, "excluded mapped key: 5 + 1");
      assert.ok(!("id" in back[0]), "subset rows carry only the selected property keys");

      const stored = (await db.driver.query(
        'select login_count as c from q03_wide where user_email = $1',
        ["mapped@x.com"],
      )) as Array<{ c: number }>;
      assert.equal(stored[0].c, 6);
    });
  });

  // -------------------------------------------------------------------------
  // Q02 composition: excluded-adjacent set values from subqueries
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): DO UPDATE SET takes a scalar subquery over another table`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(members).values([
        { email: "s1@x.com", hits: 1 },
        { email: "s2@x.com", hits: 2 },
      ]);
      await db.insert(duo).values({ a: 7, b: "z", tally: 0 });
      // The set value composes a typed select builder (Q02) as a subquery and
      // an excluded reference in the same clause.
      const sub = db.select({ n: sql`count(*)::int` }).from(members);
      const back = await db
        .insert(duo)
        .values({ a: 7, b: "z", tally: 100 })
        .onConflictUpdate({ target: [duo.a, duo.b], set: { tally: sql`${excluded(duo.tally)} + (${sub.subquery()})` } })
        .returning(["a", "b", "tally"]);
      assert.equal(back.length, 1);
      assert.equal(back[0].tally, 102, "excluded 100 + (count of members = 2)");
    });
  });

  // -------------------------------------------------------------------------
  // Duplicate/conflicting assignments — order-independent, pre-SQL
  // -------------------------------------------------------------------------

  test(`live conflicts (${driverKind}): duplicate-physical assignments error before execution, invariant to key order`, async () => {
    // The dup-physical table maps two property keys onto one physical column;
    // such a table cannot even exist in SQL, which is exactly why assigning
    // both must die at the builder. The statement counter proves nothing
    // reached the driver.
    await withSuite(driverKind, async ({ db, statements }) => {
      const before = statements.count;

      const err1 = await captureError(() => db.insert(dupPhysical).values({ first: "a", second: "b", email: "x@dup.x" }).returning());
      const err2 = await captureError(() => db.insert(dupPhysical).values({ email: "x@dup.x", second: "b", first: "a" }).returning());
      assert.ok(err1 instanceof Error && err2 instanceof Error, "both key orders must error");
      assert.equal(err1.message, err2.message, "identical message regardless of key order");
      assert.match(err1.message, /physical column "shared_col" is assigned twice/);

      const setErr1 = await captureError(() => db.update(dupPhysical).set({ first: "a", second: "b" }).where(eq(dupPhysical.email, "seed@dup.x")));
      const setErr2 = await captureError(() => db.update(dupPhysical).set({ second: "b", first: "a" }).where(eq(dupPhysical.email, "seed@dup.x")));
      assert.ok(setErr1 instanceof Error && setErr2 instanceof Error);
      assert.equal(setErr1.message, setErr2.message);
      assert.match(setErr1.message, /physical column "shared_col" is assigned twice/);

      const chainErr1 = await captureError(() => db.update(dupPhysical).set({ first: "a" }).set({ second: "b" }).where(eq(dupPhysical.email, "seed@dup.x")));
      const chainErr2 = await captureError(() => db.update(dupPhysical).set({ second: "b" }).set({ first: "a" }).where(eq(dupPhysical.email, "seed@dup.x")));
      assert.ok(chainErr1 instanceof Error && chainErr2 instanceof Error);
      assert.equal(chainErr1.message, chainErr2.message, "chained .set() order does not change the error");

      const upErr1 = await captureError(() =>
        db.insert(dupPhysical).values({ first: "a", second: "b", email: "x@dup.x" }).onConflictUpdate({ target: dupPhysical.email, set: { first: "a", second: "b" } }).returning(),
      );
      const upErr2 = await captureError(() =>
        db.insert(dupPhysical).values({ second: "b", first: "a", email: "x@dup.x" }).onConflictUpdate({ target: dupPhysical.email, set: { second: "b", first: "a" } }).returning(),
      );
      assert.ok(upErr1 instanceof Error && upErr2 instanceof Error);
      assert.equal(upErr1.message, upErr2.message);

      assert.equal(statements.count, before, "nothing executed — every rejection happens before SQL");
    });
  });
}

/** Byte-wise comparison: drivers hand back Buffers (Uint8Array subclasses)
 *  and strict deepEqual compares prototypes. */
function bytesEqual(a: unknown, b: Uint8Array): boolean {
  if (!(a instanceof Uint8Array) || a.length !== b.length) return false;
  for (let i = 0; i < b.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

async function captureError(fn: () => unknown): Promise<unknown> {
  try {
    await fn();
    return null;
  } catch (err) {
    return err;
  }
}
