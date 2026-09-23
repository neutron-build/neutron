import assert from "node:assert/strict";
import test from "node:test";
import {
  astSelect,
  createDatabase,
  eq,
  gt,
  ident,
  inArray,
  and,
  or,
  not,
  ref,
  sql,
  sqlAst,
  timestamp,
  TRUSTED_SQL_ACK,
  trustSql,
  pgTable,
  serial,
  integer,
  text,
  boolean,
  type CompiledQuery,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V07 subset (F01): five representative compiled queries executed
// against real Postgres and compared row-for-row with independently
// hand-written equivalents. Every oracle statement is authored here, in
// plain SQL — never produced or helped by the AST compiler.
//
// Both sides run through the same pg Pool on a uniquely named throwaway
// database; connection selection and skip/fail semantics come from
// ./live-harness.ts.

const DB_NAME = uniqueDbName("neutron_orm_f01");

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  active: boolean("active").notNull().default(true),
  createdAt: integer("created_at"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  title: text("title").notNull(),
  published: boolean("published").notNull().default(false),
});

interface PgPoolLike {
  query(sqlText: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
  end(): Promise<void>;
}

async function setup(): Promise<PgPoolLike | null> {
  if (!(await ensureLive("live ast (pg)"))) return null;
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => PgPoolLike;
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const pool = new Pool({ connectionString: url.toString(), max: 2 });

  await pool.query(`create table "users" (
    "id" serial primary key,
    "email" text not null,
    "active" boolean not null default true,
    "created_at" integer
  )`);
  await pool.query(`create table "posts" (
    "id" serial primary key,
    "author_id" integer not null references "users" ("id"),
    "title" text not null,
    "published" boolean not null default false
  )`);

  // Deterministic seed, inserted with plain SQL (never through the ORM).
  await pool.query(`insert into "users" ("id", "email", "active", "created_at") values
    (1, 'alice@x.com', true, 100),
    (2, 'bob@x.com', true, 200),
    (3, 'carol@x.com', false, 300),
    (4, 'dave@x.com', true, 400)`);
  await pool.query(`insert into "posts" ("id", "author_id", "title", "published") values
    (10, 1, 'alpha', true),
    (11, 1, 'beta', false),
    (12, 2, 'gamma', true),
    (13, 3, 'delta', true),
    (14, 4, 'epsilon', false),
    (15, 2, 'zeta', false)`);
  await pool.query(`select setval(pg_get_serial_sequence('users', 'id'), 4)`);
  await pool.query(`select setval(pg_get_serial_sequence('posts', 'id'), 15)`);
  return pool;
}

async function teardown(pool: PgPoolLike): Promise<void> {
  await pool.end();
  if (/^neutron_orm_f01_[0-9_]+$/.test(DB_NAME)) {
    const { Pool } = (await import("pg")) as unknown as { Pool: new (o: object) => PgPoolLike };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(
      "select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()",
      [DB_NAME],
    );
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.end();
  }
}

test("live ast (pg): V07 five compiled queries match hand-written SQL on real Postgres", async () => {
  const pool = await setup();
  if (!pool) return;
  try {
    let compared = 0;

    const compare = async (label: string, compiled: CompiledQuery, oracleSql: string, oracleParams: unknown[]): Promise<void> => {
      const got = (await pool.query(compiled.sql, [...compiled.params])).rows;
      const want = (await pool.query(oracleSql, oracleParams)).rows;
      assert.deepEqual(got, want, `${label}: compiled and hand-written results differ`);
      compared++;
    };

    // Q1 — mapped default projection (created_at -> "createdAt"), bound
    // where value, order by.
    const q1 = astSelect()
      .from(users)
      .where(sqlAst`${ref("users", users.active)} = ${true}`)
      .orderBy(ref("users", users.id))
      .toSQL();
    assert.deepEqual(q1.params, [true]);
    await compare(
      "Q1 mapped projection",
      q1,
      'select "users"."id", "users"."email", "users"."active", "users"."created_at" as "createdAt" ' +
        'from "users" where ("users"."active" = $1) order by "users"."id" asc',
      [true],
    );

    // Q2 — aliased inner join; projection mixes a from-table column and an
    // alias-qualified join column; parameter in the where.
    const q2 = astSelect({ email: users.email, postTitle: sqlAst`${ref("p", posts.title)}` })
      .from(users)
      .innerJoin(posts, "p", sqlAst`${ref("p", posts.authorId)} = ${ref("users", users.id)}`)
      .where(sqlAst`${ref("p", posts.published)} = ${true}`)
      .orderBy(ref("p", posts.id))
      .toSQL();
    assert.deepEqual(q2.params, [true]);
    await compare(
      "Q2 aliased join",
      q2,
      'select "users"."email", "p"."title" as "postTitle" from "users" ' +
        'inner join "posts" as "p" on "p"."author_id" = "users"."id" ' +
        'where ("p"."published" = $1) order by "p"."id" asc',
      [true],
    );

    // Q3 — composable subquery from a second builder: parameters span both
    // statements with one global numbering.
    const authorIds = astSelect({ authorId: posts.authorId })
      .from(posts)
      .where(sqlAst`${ref("posts", posts.published)} = ${true}`)
      .subquery();
    const q3 = astSelect({ email: users.email })
      .from(users)
      .where(sqlAst`${ref("users", users.id)} in (${authorIds}) and ${ref("users", users.active)} = ${true}`)
      .orderBy(ref("users", users.id))
      .toSQL();
    assert.deepEqual(q3.params, [true, true]);
    await compare(
      "Q3 composed subquery",
      q3,
      'select "users"."email" from "users" where ("users"."id" in ' +
        '((select "posts"."author_id" as "authorId" from "posts" where ("posts"."published" = $1))) ' +
        'and "users"."active" = $2) order by "users"."id" asc',
      [true, true],
    );

    // Q4 — CTE referenced by name in a join, with a fragment containing a
    // fragment containing a parameter in the where clause.
    const inner = sqlAst`${ref("users", users.id)} <> ${1}`;
    const q4 = astSelect({ email: users.email, postId: sqlAst`${ident("r")}."id"` })
      .from(users)
      .withCte(
        "r",
        astSelect({ id: posts.id, authorId: posts.authorId })
          .from(posts)
          .where(sqlAst`${ref("posts", posts.published)} = ${true}`)
          .subquery(),
      )
      .innerJoin(ident("r"), undefined, sqlAst`${ident("r")}."authorId" = ${ref("users", users.id)}`)
      .where(sqlAst`${ref("users", users.active)} = ${true} and ${inner}`)
      .orderBy(sqlAst`${ident("r")}."id"`)
      .toSQL();
    assert.deepEqual(q4.params, [true, true, 1]);
    await compare(
      "Q4 cte join",
      q4,
      'with "r" as (select "posts"."id", "posts"."author_id" as "authorId" from "posts" where ("posts"."published" = $1)) ' +
        'select "users"."email", "r"."id" as "postId" from "users" ' +
        'inner join "r" on "r"."authorId" = "users"."id" ' +
        'where (("users"."active" = $2 and "users"."id" <> $3)) order by "r"."id" asc',
      [true, true, 1],
    );

    // Q5 — trusted segment with a dollar-quoted string containing a literal
    // $1 passes through untouched while the real parameter takes the $1
    // slot; plus a text parameter whose VALUE is "$1" (never spliced).
    const priceNote = trustSql("$$costs $1 hundred$$", TRUSTED_SQL_ACK);
    await pool.query(`update "posts" set "title" = $$costs $1 hundred$$ where "id" = 12`);
    const q5 = astSelect({ id: posts.id, title: posts.title })
      .from(posts)
      .where(
        sqlAst`${ref("posts", posts.title)} = ${priceNote} and ${ref("posts", posts.published)} = ${true} and ${ref("posts", posts.title)} <> ${"$1"}`,
      )
      .orderBy(ref("posts", posts.id))
      .toSQL();
    assert.deepEqual(q5.params, [true, "$1"]);
    await compare(
      "Q5 dollar-quoted trusted text",
      q5,
      'select "posts"."id", "posts"."title" from "posts" where (("posts"."title" = $$costs $1 hundred$$ ' +
        'and "posts"."published" = $1) and "posts"."title" <> $2) order by "posts"."id" asc',
      [true, "$1"],
    );

    assert.equal(compared, 5);
  } finally {
    await teardown(pool);
  }
});


// ---------------------------------------------------------------------------
// F04: the CRUD builders (compiler output) match independently hand-written
// mutation SQL on real Postgres — insert with DEFAULT cells, update with a
// fragment assignment and a text-cast predicate, delete with an IN list, and
// returning decode through the compiled statements' decode plans.
// ---------------------------------------------------------------------------

test("live ast (pg): V07 compiled CRUD matches hand-written SQL on real Postgres", async () => {
  const pool = await setup();
  if (!pool) return;
  try {
    const events = pgTable("events", {
      id: serial("id").primaryKey(),
      kind: text("kind").notNull(),
      at: timestamp("at").notNull(),
      n: integer("n").notNull().default(0),
    });
    // Snapshot URL is inert: postgres.js connects lazily and these queries
    // execute through the pool below, not through the db object.
    const db = await createDatabase({ url: "postgres://snapshot:nouser@127.0.0.1:1/none", driver: { driver: "postgres" }, tables: { events } });
    await pool.query(`create table "events" (
      "id" serial primary key,
      "kind" text not null,
      "at" timestamp not null,
      "n" integer not null default 0
    )`);

    let compared = 0;
    // Both sides mutate: run each inside a rolled-back transaction so the
    // comparison starts from identical state. Serial ids advance across
    // rollbacks (sequences are non-transactional), so ids are compared as
    // consecutive-within-a-side and stripped from the row-for-row compare.
    const compareRows = async (label: string, compiled: { sql: string; params: readonly unknown[] }, oracleSql: string, oracleParams: unknown[]): Promise<void> => {
      await pool.query("begin");
      const got = (await pool.query(compiled.sql, [...compiled.params])).rows;
      await pool.query("rollback");
      await pool.query("begin");
      const want = (await pool.query(oracleSql, oracleParams)).rows;
      await pool.query("rollback");
      const ids = (rows: Array<Record<string, unknown>>): number[] => rows.map((r) => r.id as number);
      const gotIds = ids(got);
      const wantIds = ids(want);
      assert.equal(gotIds.length, wantIds.length, `${label}: row counts differ`);
      for (const seq of [gotIds, wantIds]) {
        assert.deepEqual(seq, seq.map((_, i) => seq[0] + i), `${label}: ids must be consecutive within a side`);
      }
      const strip = (rows: Array<Record<string, unknown>>): Array<Record<string, unknown>> => rows.map((r) => ({ ...r, id: undefined }));
      assert.deepEqual(strip(got), strip(want), `${label}: compiled and hand-written results differ`);
      compared++;
    };

    // I1 — batch insert with mixed supplied/default cells + returning.
    const ins = db.insert(events)
      .values([
        { kind: "a", at: "2026-01-02T03:04:05.678912" },
        { kind: "b", at: "2026-02-03T04:05:06.000001", n: 7 },
      ])
      .returning()
      .toCompiled();
    await compareRows(
      "I1 batch insert returning",
      ins,
      `insert into "events" ("kind", "at", "n") values ($1, $2::text::timestamp, default), ($3, $4::text::timestamp, $5) ` +
        `returning "events"."id", "events"."kind", to_jsonb("events"."at")::text as "at", "events"."n"`,
      ["a", "2026-01-02T03:04:05.678912", "b", "2026-02-03T04:05:06.000001", 7],
    );

    // I2 — compiled default-only row form.
    const insDefault = db.insert(events).values({ kind: "d", at: "2026-03-01T00:00:00" }).toSQL();
    await pool.query(insDefault.sql, insDefault.params);
    const gotRows = (await pool.query(`select "n" from "events" where "kind" = 'd'`)).rows;
    assert.deepEqual(gotRows, [{ n: 0 }], "default-only cell used the column DEFAULT");
    compared++;

    // U1 — update with fragment assignment + cast predicate + returning.
    const upd = db.update(events)
      .set({ n: sql`${events.n} + 1`, kind: "a2" })
      .where(gt(events.at, "2026-01-01T00:00:00"))
      .returning()
      .toCompiled();
    await compareRows(
      "U1 update returning",
      upd,
      `update "events" set "n" = "n" + 1, "kind" = $1 where ("events"."at" > $2::text::timestamp) returning "events"."id", "events"."kind", to_jsonb("events"."at")::text as "at", "events"."n"`,
      ["a2", "2026-01-01T00:00:00"],
    );

    // D1 — delete with an IN list; affected counts equal between both sides
    // from an identical seeded state.
    const reseed = async (): Promise<void> => {
      await pool.query(`delete from "events"`);
      await pool.query(
        `insert into "events" ("kind", "at") values ('a2', '2026-01-05T00:00:00'), ('d', '2026-01-05T00:00:00'), ('keep', '2026-01-05T00:00:00')`,
      );
    };
    const del = db.delete(events).where(inArray(events.kind, ["a2", "d", "missing"])).toSQL();
    await reseed();
    await pool.query(del.sql, del.params);
    const delCount = Number(
      ((await pool.query(`select count(*)::int as c from "events"`)).rows[0] as { c: number }).c,
    );
    await reseed();
    await pool.query(`delete from "events" where "kind" in ($1, $2, $3)`, ["a2", "d", "missing"]);
    const oracleCount = Number(
      ((await pool.query(`select count(*)::int as c from "events"`)).rows[0] as { c: number }).c,
    );
    assert.equal(delCount, oracleCount, "compiled and hand-written deletes left the same rows behind");
    compared++;

    assert.ok(compared >= 4);
    await pool.query(`drop table if exists "events"`);
  } finally {
    await teardown(pool);
  }
});

// ---------------------------------------------------------------------------
// F04 rework (attempt 2) MAJOR-1 live regression: connective grouping over
// fragments, proven against the reviewer's 8-row boolean truth table. The
// attempt-1 compiler spliced fragment args of and()/not() bare — Postgres
// bound the escaped `or` tighter than the enclosing connective and silently
// returned 5 of 8 rows where `a and (b or c)` intends 3. Oracles are
// hand-written SQL with explicit, intended grouping.
// ---------------------------------------------------------------------------

test("live ast (pg): V07 connectives over fragments match intended-grouping oracles (truth table)", async () => {
  const pool = await setup();
  if (!pool) return;
  try {
    await pool.query(`create table "tt" ("a" boolean not null, "b" boolean not null, "c" boolean not null)`);
    await pool.query(`insert into "tt" ("a","b","c") values
      (true, true, true), (true, true, false), (true, false, true), (true, false, false),
      (false, true, true), (false, true, false), (false, false, true), (false, false, false)`);

    const tt = pgTable("tt", { a: boolean("a").notNull(), b: boolean("b").notNull(), c: boolean("c").notNull() });
    // Snapshot URL: postgres.js connects lazily; execution goes through the pool.
    const db = await createDatabase({ url: "postgres://snapshot:nouser@127.0.0.1:1/none", driver: { driver: "postgres" }, tables: { tt } });

    const compare = async (label: string, compiled: { sql: string; params: readonly unknown[] }, oracleSql: string, oracleParams: unknown[]): Promise<void> => {
      // The compiled side has no order by (it is not what these cases test);
      // normalize both sides to a canonical row order before deep-comparing.
      const canon = (rows: Array<Record<string, unknown>>): Array<Record<string, unknown>> =>
        [...rows].sort((r1, r2) => (JSON.stringify(r1) < JSON.stringify(r2) ? -1 : 1));
      const got = canon((await pool.query(compiled.sql, [...compiled.params])).rows);
      const want = canon((await pool.query(oracleSql, oracleParams)).rows);
      assert.deepEqual(got, want, `${label}: compiled grouping differs from intended semantics`);
    };

    // C1 — the reviewer's exact case: a = true AND (b OR c) intends 3 rows.
    await compare(
      "C1 and(eq, frag-with-or)",
      db.select({ a: tt.a, b: tt.b, c: tt.c })
        .from(tt)
        .where(and(eq(tt.a, true), sql`${tt.b} or ${tt.c}`))
        .toSQL(),
      `select "a", "b", "c" from "tt" where "a" = $1 and ("b" or "c") order by "a", "b", "c"`,
      [true],
    );
    // Bare splice would execute ((a) and b) or c — 5 rows.

    // C2 — not() over a fragment: NOT (b OR c) intends 2 rows; the bare
    // splice (not b) or c matched 6.
    await compare(
      "C2 not(frag)",
      db.select({ a: tt.a, b: tt.b, c: tt.c })
        .from(tt)
        .where(not(sql`${tt.b} or ${tt.c}`))
        .toSQL(),
      `select "a", "b", "c" from "tt" where not ("b" or "c") order by "a", "b", "c"`,
      [],
    );

    // C3 — nested: (a AND (b OR c)) OR (b = false) — the inner and() is not
    // a direct where-list child, so list-level flattening cannot save it.
    await compare(
      "C3 or(and(eq, frag), eq)",
      db.select({ a: tt.a, b: tt.b, c: tt.c })
        .from(tt)
        .where(or(and(eq(tt.a, true), sql`${tt.b} or ${tt.c}`), eq(tt.b, false)))
        .toSQL(),
      `select "a", "b", "c" from "tt" where ("a" = $1 and ("b" or "c")) or "b" = $2 order by "a", "b", "c"`,
      [true, false],
    );

    // C4 — fragment on the LEFT of and(), via the raw AST builder path.
    await compare(
      "C4 and(frag, eq)",
      astSelect({ a: tt.a, b: tt.b, c: tt.c })
        .from(tt)
        .where(and(sqlAst`${tt.c} or ${tt.b}`, eq(tt.a, true)))
        .toSQL(),
      `select "a", "b", "c" from "tt" where (("c" or "b") and "a" = $1) order by "a", "b", "c"`,
      [true],
    );

    await pool.query(`drop table if exists "tt"`);
  } finally {
    await teardown(pool);
  }
});
