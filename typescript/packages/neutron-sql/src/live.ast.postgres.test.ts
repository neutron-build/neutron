import assert from "node:assert/strict";
import test from "node:test";
import {
  astSelect,
  ident,
  ref,
  sqlAst,
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
