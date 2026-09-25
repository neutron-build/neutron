#!/usr/bin/env node
// @neutron-build/sql — relational read performance and integrity gate
// (orm-program R01, VERIFICATION V16).
//
//   node --expose-gc bench/orm-gate.mjs [--profile ci|full] [--driver pg|postgres]...
//        [--out results.json] [--gate] [--compare baseline.json]
//
// Needs a built package (pnpm build) and NEUTRON_TEST_DATABASE_URL pointing
// at a disposable PostgreSQL server; the gate creates and drops its own
// uniquely named database and never touches another one.
//
// What it measures, per driver, data scale, children-per-parent (0/2/20 on
// two independent to-many edges) and index state:
//   - result equality of a 100-parent relation page against two hand-written
//     SQL oracles (one correlated statement; parent + two IN-list queries
//     assembled in JS) and against the pinned reference tool (drizzle-orm,
//     exact devDependency pin), for both the two-edge page and a depth-3 page;
//   - statements per call, counted twice: client-side from the structured
//     logger and server-side from pg_stat_database transaction counters
//     around a closed pool (an N+1 regression shows up in both);
//   - the server plan (EXPLAIN ANALYZE of the exact compiled statement,
//     interleaved with the hand-written statement): node types, index use on
//     child edges, server execution time and its ratio to the hand-written
//     statement;
//   - client p50/p95 per call (cold first call reported separately) and the
//     JS compile overhead of toSQL() on its own;
//   - retained heap after GC, GC count/time and peak RSS;
//   - bounded streaming: time to first row and time until an early exit has
//     returned its pooled connection.
//
// Integrity failures (equality, statement counts, index use, stream release)
// always fail. --gate additionally enforces the ceilings in bench/budgets.json
// that are safe on shared CI runners: the same-run server-time ratio against
// the hand-written statement and generous absolute p50 ceilings. --compare
// checks p50 against a same-machine baseline and fails on a regression above
// the recorded tolerance — use it on the machine that produced the baseline,
// never across machines.

import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import { readFileSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance, PerformanceObserver } from "node:perf_hooks";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PKG = path.resolve(HERE, "..");
const require_ = createRequire(path.join(PKG, "package.json"));

// ---------------------------------------------------------------- arguments

// Parents per cohort. Every cohort holds the same number of parents; a
// cohort's children-per-parent is its k. The measured page is 100 parents
// from the middle of a cohort, so the index/seq-scan difference is visible.
const PROFILES = {
  ci: { scales: [{ name: "small", parents: 1000 }], iterations: 30, warmup: 5, unindexedIterations: 5, compileIterations: 500 },
  full: {
    scales: [
      { name: "small", parents: 1000 },
      { name: "large", parents: 10000 },
    ],
    iterations: 100,
    warmup: 10,
    unindexedIterations: 10,
    compileIterations: 2000,
  },
};
const COHORTS = [0, 2, 20];
const PAGE = 100;

function fail(message) {
  console.error(`orm-gate: ${message}`);
  process.exit(2);
}

function parseArgs(argv) {
  const out = { profile: "ci", drivers: [], out: null, gate: false, compare: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) fail(`${a} needs a value`);
      return argv[++i];
    };
    if (a === "--profile") out.profile = next();
    else if (a === "--driver") out.drivers.push(next());
    else if (a === "--out") out.out = next();
    else if (a === "--gate") out.gate = true;
    else if (a === "--compare") out.compare = next();
    else fail(`unknown argument ${a}`);
  }
  if (!(out.profile in PROFILES)) fail(`--profile must be one of ${Object.keys(PROFILES).join(", ")}`);
  if (out.drivers.length === 0) out.drivers = ["pg", "postgres"];
  for (const d of out.drivers) if (d !== "pg" && d !== "postgres") fail(`--driver must be pg or postgres, got ${d}`);
  return out;
}

// -------------------------------------------------------------- environment

const args = parseArgs(process.argv.slice(2));
const PROFILE = PROFILES[args.profile];
const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || "";
if (!TEST_URL) fail("NEUTRON_TEST_DATABASE_URL is not set (point it at a disposable PostgreSQL server)");

const sqlPkg = await import(pathToFileURL(path.join(PKG, "dist", "index.js")).href).catch(() =>
  fail("dist/index.js missing — run `pnpm build` in typescript/packages/neutron-sql first"),
);
const { createDatabase, pgTable, serial, integer, text, numeric, timestamptz, relations, eq, gt, and, asc, wrapPgPool } = sqlPkg;
const pg = require_("pg");
const postgresJs = require_("postgres");
const { relations: dRelations, gt: dGt, eq: dEq, and: dAnd, asc: dAsc } = require_("drizzle-orm");
const dCore = require_("drizzle-orm/pg-core");
const { drizzle: drizzlePg } = require_("drizzle-orm/node-postgres");
const { drizzle: drizzlePostgresJs } = require_("drizzle-orm/postgres-js");

// Installed version of a dependency, found from its resolved entry point
// (drizzle-orm does not export ./package.json).
function installedVersion(name) {
  let dir = path.dirname(require_.resolve(name));
  for (;;) {
    try {
      const pkg = JSON.parse(readFileSync(path.join(dir, "package.json"), "utf8"));
      if (pkg.name === name) return pkg.version;
    } catch {}
    const up = path.dirname(dir);
    if (up === dir) throw new Error(`cannot find package.json for ${name}`);
    dir = up;
  }
}
const drizzleVersion = installedVersion("drizzle-orm");

const budgets = JSON.parse(readFileSync(path.join(HERE, "budgets.json"), "utf8"));
if (budgets.referenceTools?.["drizzle-orm"] && budgets.referenceTools["drizzle-orm"] !== drizzleVersion) {
  fail(`drizzle-orm ${drizzleVersion} is installed but budgets.json pins ${budgets.referenceTools["drizzle-orm"]} — the reference tool is pinned, re-record deliberately`);
}

const DB_NAME = `nsql_bench_${process.pid}_${Math.floor(Date.now() / 1000)}`;
function dbUrl(name) {
  const u = new URL(TEST_URL);
  u.pathname = `/${name}`;
  return u.toString();
}
const BENCH_URL = dbUrl(DB_NAME);

// ---------------------------------------------------------------- schemas

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  cohort: integer("cohort").notNull(),
  name: text("name").notNull(),
  createdAt: timestamptz("created_at").notNull(),
});
const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  title: text("title").notNull(),
  views: integer("views").notNull(),
});
const audits = pgTable("audits", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  kind: text("kind").notNull(),
  amount: numeric("amount").notNull(),
});
const comments = pgTable("comments", {
  id: serial("id").primaryKey(),
  postId: integer("post_id").notNull(),
  commenterId: integer("commenter_id").notNull(),
  body: text("body").notNull(),
});
const usersRelations = relations(users, ({ many }) => ({
  posts: many(posts),
  audits: many(audits),
}));
const postsRelations = relations(posts, ({ one, many }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id] }),
  comments: many(comments),
}));
const auditsRelations = relations(audits, ({ one }) => ({
  user: one(users, { fields: [audits.userId], references: [users.id] }),
}));
const commentsRelations = relations(comments, ({ one }) => ({
  post: one(posts, { fields: [comments.postId], references: [posts.id] }),
  commenter: one(users, { fields: [comments.commenterId], references: [users.id] }),
}));
const TABLES = { users, posts, audits, comments };
const RELATIONS = { users: usersRelations, posts: postsRelations, audits: auditsRelations, comments: commentsRelations };

// drizzle-orm reference schema (same physical tables).
const dUsers = dCore.pgTable("users", {
  id: dCore.serial("id").primaryKey(),
  cohort: dCore.integer("cohort").notNull(),
  name: dCore.text("name").notNull(),
  createdAt: dCore.timestamp("created_at", { withTimezone: true, mode: "string" }).notNull(),
});
const dPosts = dCore.pgTable("posts", {
  id: dCore.serial("id").primaryKey(),
  authorId: dCore.integer("author_id").notNull(),
  title: dCore.text("title").notNull(),
  views: dCore.integer("views").notNull(),
});
const dAudits = dCore.pgTable("audits", {
  id: dCore.serial("id").primaryKey(),
  userId: dCore.integer("user_id").notNull(),
  kind: dCore.text("kind").notNull(),
  amount: dCore.numeric("amount").notNull(),
});
const dComments = dCore.pgTable("comments", {
  id: dCore.serial("id").primaryKey(),
  postId: dCore.integer("post_id").notNull(),
  commenterId: dCore.integer("commenter_id").notNull(),
  body: dCore.text("body").notNull(),
});
const DRIZZLE_SCHEMA = {
  users: dUsers,
  posts: dPosts,
  audits: dAudits,
  comments: dComments,
  usersRelations: dRelations(dUsers, ({ many }) => ({ posts: many(dPosts), audits: many(dAudits) })),
  postsRelations: dRelations(dPosts, ({ one, many }) => ({
    author: one(dUsers, { fields: [dPosts.authorId], references: [dUsers.id] }),
    comments: many(dComments),
  })),
  auditsRelations: dRelations(dAudits, ({ one }) => ({ user: one(dUsers, { fields: [dAudits.userId], references: [dUsers.id] }) })),
  commentsRelations: dRelations(dComments, ({ one }) => ({
    post: one(dPosts, { fields: [dComments.postId], references: [dPosts.id] }),
    commenter: one(dUsers, { fields: [dComments.commenterId], references: [dUsers.id] }),
  })),
};

// ------------------------------------------------------------------ helpers

const admin = new pg.Pool({ connectionString: TEST_URL, max: 1 });
const loader = () => new pg.Pool({ connectionString: BENCH_URL, max: 1 });

function quantile(sorted, q) {
  if (sorted.length === 0) return NaN;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
  return sorted[idx];
}
function r3(x) {
  return Math.round(x * 1000) / 1000;
}
function summarize(samples) {
  const s = [...samples].sort((a, b) => a - b);
  const mean = s.reduce((a, b) => a + b, 0) / s.length;
  return { n: s.length, min: r3(s[0]), p50: r3(quantile(s, 0.5)), p95: r3(quantile(s, 0.95)), max: r3(s[s.length - 1]), mean: r3(mean) };
}
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

async function serverXacts() {
  const c = await admin.connect();
  try {
    await c.query("select pg_stat_clear_snapshot()");
    const r = await c.query("select xact_commit::bigint as c, xact_rollback::bigint as r from pg_stat_database where datname = $1", [DB_NAME]);
    return Number(r.rows[0].c) + Number(r.rows[0].r);
  } finally {
    c.release();
  }
}
async function settledXacts() {
  // Wait until no backend is connected to the bench database (exiting
  // backends flush their counters) and three consecutive reads agree.
  for (let i = 0; i < 100; i++) {
    const r = await admin.query("select count(*)::int as n from pg_stat_activity where datname = $1", [DB_NAME]);
    if (r.rows[0].n === 0) break;
    await sleep(50);
  }
  let prev = await serverXacts();
  let stable = 0;
  for (let i = 0; i < 60 && stable < 2; i++) {
    await sleep(100);
    const cur = await serverXacts();
    stable = cur === prev ? stable + 1 : 0;
    prev = cur;
  }
  return prev;
}

const gcStats = { count: 0, ms: 0 };
new PerformanceObserver((list) => {
  for (const e of list.getEntries()) {
    gcStats.count++;
    gcStats.ms += e.duration;
  }
}).observe({ entryTypes: ["gc"] });
function heapAfterGc() {
  if (typeof global.gc === "function") {
    global.gc();
    global.gc();
  }
  return process.memoryUsage().heapUsed;
}

// ----------------------------------------------------------------- fixture

async function createFixture(parents) {
  const c = loader();
  try {
    await c.query(`drop table if exists comments, audits, posts, users`);
    await c.query(`create table users (id serial primary key, cohort integer not null, name text not null, created_at timestamptz not null) with (autovacuum_enabled = false)`);
    await c.query(`create table posts (id serial primary key, author_id integer not null references users(id), title text not null, views integer not null) with (autovacuum_enabled = false)`);
    await c.query(`create table audits (id serial primary key, user_id integer not null references users(id), kind text not null, amount numeric(12,2) not null) with (autovacuum_enabled = false)`);
    await c.query(`create table comments (id serial primary key, post_id integer not null references posts(id), commenter_id integer not null references users(id), body text not null) with (autovacuum_enabled = false)`);
    await c.query(
      `insert into users (cohort, name, created_at)
       select v.c, 'user ' || v.c || '-' || g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 second' + (g % 1000) * interval '1 microsecond'
       from (values (0), (2), (20)) v(c), generate_series(1, $1::int) g
       order by v.c, g`,
      [parents],
    );
    await c.query(
      `insert into posts (author_id, title, views)
       select u.id, 'post ' || u.id || '-' || j, (u.id * 7 + j) % 1000
       from users u, generate_series(1, u.cohort) j
       order by u.id, j`,
    );
    await c.query(
      `insert into audits (user_id, kind, amount)
       select u.id, case when j % 2 = 0 then 'debit' else 'credit' end, ((u.id * 13 + j) % 1000000) / 100.0
       from users u, generate_series(1, u.cohort) j
       order by u.id, j`,
    );
    // Depth-3 data: two comments per post of the k=2 cohort, each by another
    // user (the to-one edge at depth 3).
    await c.query(
      `insert into comments (post_id, commenter_id, body)
       select p.id, ((p.author_id + j) % (select max(id) from users)) + 1, 'comment ' || p.id || '-' || j
       from posts p join users u on u.id = p.author_id and u.cohort = 2, generate_series(1, 2) j
       order by p.id, j`,
    );
    const ranges = await c.query(`select cohort, min(id)::int as lo, max(id)::int as hi from users group by cohort order by cohort`);
    const counts = await c.query(
      `select (select count(*) from users)::int as users, (select count(*) from posts)::int as posts,
              (select count(*) from audits)::int as audits, (select count(*) from comments)::int as comments`,
    );
    return { ranges: Object.fromEntries(ranges.rows.map((r) => [r.cohort, { lo: r.lo, hi: r.hi }])), counts: counts.rows[0] };
  } finally {
    await c.end();
  }
}

async function setIndexes(on) {
  const c = loader();
  try {
    if (on) {
      await c.query(`create index if not exists posts_author_id_idx on posts (author_id)`);
      await c.query(`create index if not exists audits_user_id_idx on audits (user_id)`);
      await c.query(`create index if not exists comments_post_id_idx on comments (post_id)`);
    } else {
      await c.query(`drop index if exists posts_author_id_idx, audits_user_id_idx, comments_post_id_idx`);
    }
    // VACUUM sets hint bits and the visibility map after the bulk load, so
    // the first measured driver does not pay for them and later ones not.
    await c.query(`vacuum (analyze)`);
  } finally {
    await c.end();
  }
}

// ------------------------------------------------------------- the queries

function neutronPageArgs(cohort, start) {
  return {
    where: and(eq(users.cohort, cohort), gt(users.id, start)),
    orderBy: [asc(users.id)],
    limit: PAGE,
    with: {
      posts: { orderBy: [asc(posts.id)] },
      audits: { orderBy: [asc(audits.id)] },
    },
  };
}
function neutronDeepArgs(cohort, start) {
  return {
    where: and(eq(users.cohort, cohort), gt(users.id, start)),
    orderBy: [asc(users.id)],
    limit: PAGE,
    with: { posts: { orderBy: [asc(posts.id)], with: { comments: { orderBy: [asc(comments.id)], with: { commenter: true } } } } },
  };
}
function drizzlePage(ddb, cohort, start) {
  return ddb.query.users.findMany({
    where: dAnd(dEq(dUsers.cohort, cohort), dGt(dUsers.id, start)),
    orderBy: [dAsc(dUsers.id)],
    limit: PAGE,
    with: { posts: { orderBy: [dAsc(dPosts.id)] }, audits: { orderBy: [dAsc(dAudits.id)] } },
  });
}
function drizzleDeep(ddb, cohort, start) {
  return ddb.query.users.findMany({
    where: dAnd(dEq(dUsers.cohort, cohort), dGt(dUsers.id, start)),
    orderBy: [dAsc(dUsers.id)],
    limit: PAGE,
    with: { posts: { orderBy: [dAsc(dPosts.id)], with: { comments: { orderBy: [dAsc(dComments.id)], with: { commenter: true } } } } },
  });
}

// Hand-written oracle A: one correlated statement, authored independently of
// the compiler (json_agg per edge, explicit ORDER BY, text forms chosen here).
const ORACLE_PAGE_SQL = `
select u.id, u.cohort, u.name, (extract(epoch from u.created_at) * 1000000)::bigint::text as created_us,
  coalesce((select json_agg(json_build_object('id', p.id, 'authorId', p.author_id, 'title', p.title, 'views', p.views) order by p.id)
            from posts p where p.author_id = u.id), '[]'::json) as posts,
  coalesce((select json_agg(json_build_object('id', a.id, 'userId', a.user_id, 'kind', a.kind, 'amount', a.amount::text) order by a.id)
            from audits a where a.user_id = u.id), '[]'::json) as audits
from users u
where u.cohort = $1 and u.id > $2
order by u.id
limit ${PAGE}`;
const ORACLE_DEEP_SQL = `
select u.id, u.cohort, u.name, (extract(epoch from u.created_at) * 1000000)::bigint::text as created_us,
  coalesce((select json_agg(json_build_object('id', p.id, 'authorId', p.author_id, 'title', p.title, 'views', p.views,
            'comments', coalesce((select json_agg(json_build_object('id', c.id, 'postId', c.post_id, 'commenterId', c.commenter_id, 'body', c.body,
                          'commenter', (select json_build_object('id', cu.id, 'cohort', cu.cohort, 'name', cu.name,
                                               'created_us', (extract(epoch from cu.created_at) * 1000000)::bigint::text)
                                        from users cu where cu.id = c.commenter_id)) order by c.id)
                                  from comments c where c.post_id = p.id), '[]'::json)) order by p.id)
            from posts p where p.author_id = u.id), '[]'::json) as posts
from users u
where u.cohort = $1 and u.id > $2
order by u.id
limit ${PAGE}`;

// Hand-written oracle B: the classic multi-statement plan — parents, then
// one IN-list query per edge — assembled in JS. Three statements by design.
async function oracleBatched(client, cohort, start) {
  const parents = (
    await client.query(
      `select id, cohort, name, (extract(epoch from created_at) * 1000000)::bigint::text as created_us
       from users where cohort = $1 and id > $2 order by id limit ${PAGE}`,
      [cohort, start],
    )
  ).rows;
  const ids = parents.map((p) => p.id);
  const ps = (await client.query(`select id, author_id, title, views from posts where author_id = any($1::int[]) order by id`, [ids])).rows;
  const as = (await client.query(`select id, user_id, kind, amount::text as amount from audits where user_id = any($1::int[]) order by id`, [ids])).rows;
  const byUser = new Map(parents.map((p) => [p.id, { ...p, posts: [], audits: [] }]));
  for (const p of ps) byUser.get(p.author_id).posts.push({ id: p.id, authorId: p.author_id, title: p.title, views: p.views });
  for (const a of as) byUser.get(a.user_id).audits.push({ id: a.id, userId: a.user_id, kind: a.kind, amount: a.amount });
  return [...byUser.values()];
}

// ----------------------------------------------------------- normalization
//
// Every source is reduced to one plain shape before deepStrictEqual:
// timestamps as epoch microseconds (string), numerics as exact decimal
// strings, integers as numbers.

function usFromCanonical(s) {
  // '2026-01-01T00:00:01.000123Z' or '2026-01-01 00:00:01.000123+00'.
  const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}(?::?\d{2})?)$/.exec(s);
  if (!m) throw new Error(`unparseable timestamp ${JSON.stringify(s)}`);
  const [, y, mo, d, h, mi, se, frac = "", off] = m;
  let ms = Date.UTC(+y, +mo - 1, +d, +h, +mi, +se);
  if (off !== "Z") {
    const sign = off[0] === "-" ? -1 : 1;
    const oh = +off.slice(1, 3);
    const om = off.length > 3 ? +off.slice(-2) : 0;
    ms -= sign * (oh * 60 + om) * 60000;
  }
  return (BigInt(ms) * 1000n + BigInt(frac.padEnd(6, "0"))).toString();
}
const normUser = (u, created) => ({ id: u.id, cohort: u.cohort, name: u.name, created_us: created });
const normPost = (p) => ({ id: p.id, authorId: p.authorId, title: p.title, views: p.views });
const normAudit = (a) => ({ id: a.id, userId: a.userId, kind: a.kind, amount: a.amount });
const normNeutronPage = (rows) =>
  rows.map((u) => ({ ...normUser(u, usFromCanonical(u.createdAt)), posts: u.posts.map(normPost), audits: u.audits.map(normAudit) }));
const normOraclePage = (rows) => rows.map((u) => ({ ...normUser(u, u.created_us), posts: u.posts.map(normPost), audits: u.audits.map(normAudit) }));
function normDeep(rows, createdOf) {
  return rows.map((u) => ({
    ...normUser(u, createdOf(u)),
    posts: u.posts.map((p) => ({
      ...normPost(p),
      comments: p.comments.map((c) => ({
        id: c.id,
        postId: c.postId,
        commenterId: c.commenterId,
        body: c.body,
        commenter: c.commenter === null ? null : normUser(c.commenter, createdOf(c.commenter)),
      })),
    })),
  }));
}
const neutronCreated = (u) => usFromCanonical(u.createdAt);
const oracleCreated = (u) => u.created_us;
function canonDecimal(x) {
  return x.includes(".") ? x.replace(/0+$/, "").replace(/\.$/, "") : x;
}
const canonAmounts = (rows) => rows.map((u) => (u.audits ? { ...u, audits: u.audits.map((a) => ({ ...a, amount: canonDecimal(a.amount) })) } : u));

// ------------------------------------------------------------------ plans

function walkPlan(node, out = []) {
  out.push({ type: node["Node Type"], relation: node["Relation Name"] ?? null, index: node["Index Name"] ?? null });
  for (const child of node.Plans ?? []) walkPlan(child, out);
  return out;
}
const nodeLabel = (x) => `${x.type}${x.relation ? `(${x.relation}${x.index ? `/${x.index}` : ""})` : ""}`;

// EXPLAIN ANALYZE the compiled statement and the hand-written oracle,
// alternating runs so machine drift hits both sides equally. The ratio of
// server execution medians is a same-run comparison, stable enough to gate
// on shared CI runners where absolute times are not.
async function explainPair(neutronSql, neutronParams, handSql, handParams, runs) {
  const c = loader();
  try {
    const n = [];
    const h = [];
    let plan;
    let handPlan;
    for (let i = 0; i < runs; i++) {
      plan = (await c.query(`explain (analyze, buffers, format json) ${neutronSql}`, neutronParams)).rows[0]["QUERY PLAN"][0];
      n.push(plan["Execution Time"]);
      handPlan = (await c.query(`explain (analyze, buffers, format json) ${handSql}`, handParams)).rows[0]["QUERY PLAN"][0];
      h.push(handPlan["Execution Time"]);
    }
    const nodes = walkPlan(plan.Plan);
    const neutron = summarize(n);
    const hand = summarize(h);
    return {
      executionMs: neutron,
      handExecutionMs: hand,
      serverRatioVsHand: r3(neutron.p50 / hand.p50),
      planningMs: r3(plan["Planning Time"]),
      nodes: [...new Set(nodes.map(nodeLabel))],
      handNodes: [...new Set(walkPlan(handPlan.Plan).map(nodeLabel))],
      childSeqScans: nodes.filter((x) => x.type === "Seq Scan" && x.relation !== "users").map((x) => x.relation),
      sharedHit: plan.Plan["Shared Hit Blocks"] ?? null,
      sharedRead: plan.Plan["Shared Read Blocks"] ?? null,
    };
  } finally {
    await c.end();
  }
}

// ---------------------------------------------------------------- drivers

async function openNeutron(driver, statements) {
  return createDatabase({
    url: BENCH_URL,
    driverOptions: { driver, max: 2 },
    tables: TABLES,
    relations: RELATIONS,
    logger: (e) => {
      if (e.kind === "query-end" || e.kind === "query-error") statements.count++;
    },
  });
}
function openDrizzle(driver) {
  if (driver === "pg") {
    const pool = new pg.Pool({ connectionString: BENCH_URL, max: 2 });
    let count = 0;
    const orig = pool.query.bind(pool);
    pool.query = (...a) => {
      count++;
      return orig(...a);
    };
    return { db: drizzlePg(pool, { schema: DRIZZLE_SCHEMA }), close: () => pool.end(), statements: () => count };
  }
  const client = postgresJs(BENCH_URL, { max: 2, onnotice: () => {} });
  return { db: drizzlePostgresJs(client, { schema: DRIZZLE_SCHEMA }), close: () => client.end({ timeout: 5 }), statements: () => null };
}

async function timed(fn, iterations, warmup) {
  for (let i = 0; i < warmup; i++) await fn();
  const gc0 = { ...gcStats };
  const heap0 = heapAfterGc();
  const samples = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await fn();
    samples.push(performance.now() - t0);
  }
  const heap1 = heapAfterGc();
  return {
    ...summarize(samples),
    retainedHeapKiB: Math.round((heap1 - heap0) / 1024),
    gcCount: gcStats.count - gc0.count,
    gcMs: r3(gcStats.ms - gc0.ms),
  };
}

// Server-side statement count per call: (xacts(1 + M calls) - xacts(1 call)) / M,
// each measured around a pool that is closed before reading, so the backend
// flushed its counters at exit. Setup statements cancel out. Foreign
// transactions in the database (an autovacuum worker's visit) can only ADD
// to a window, so the minimum over repetitions is the robust estimate.
async function serverStatementsPerCall(open, call, m = 10) {
  async function run(n) {
    const before = await settledXacts();
    const h = await open();
    for (let i = 0; i < n; i++) await call(h);
    await h.close();
    return (await settledXacts()) - before;
  }
  let best = Infinity;
  for (let rep = 0; rep < 3; rep++) {
    const one = await run(1);
    const many = await run(1 + m);
    best = Math.min(best, (many - one) / m);
    if (Number.isInteger(best) && best <= 1) break;
  }
  return best;
}

// ----------------------------------------------------------------- scenario

const results = {
  tool: "neutron-sql orm-gate",
  profile: args.profile,
  startedAt: new Date().toISOString(),
  environment: null,
  scales: [],
  stream: [],
  compile: null,
  failures: [],
};

function check(label, fn, kind = "integrity") {
  try {
    fn();
  } catch (err) {
    results.failures.push({ kind, label, message: err instanceof Error ? err.message.split("\n").slice(0, 6).join(" ") : String(err) });
  }
}

async function environment() {
  const c = loader();
  try {
    const v = (await c.query("select version() as v, current_setting('server_version_num') as n, current_setting('shared_buffers') as sb, current_setting('work_mem') as wm")).rows[0];
    return {
      node: process.version,
      platform: `${process.platform}-${process.arch}`,
      cpu: os.cpus()[0]?.model ?? "unknown",
      cpus: os.cpus().length,
      memGiB: Math.round(os.totalmem() / 2 ** 30),
      loadavg: os.loadavg().map(r3),
      server: v.v,
      serverVersionNum: Number(v.n),
      sharedBuffers: v.sb,
      workMem: v.wm,
      drivers: { pg: installedVersion("pg"), postgres: installedVersion("postgres") },
      referenceTools: { "drizzle-orm": drizzleVersion },
      exposeGc: typeof global.gc === "function",
    };
  } finally {
    await c.end();
  }
}

async function runScale(scale) {
  const t0 = performance.now();
  const fixture = await createFixture(scale.parents);
  const out = { scale: scale.name, parents: scale.parents, rows: fixture.counts, loadMs: Math.round(performance.now() - t0), scenarios: [] };
  for (const indexed of [true, false]) {
    await setIndexes(indexed);
    const iterations = indexed ? PROFILE.iterations : PROFILE.unindexedIterations;
    const warmup = indexed ? PROFILE.warmup : 1;
    for (const shape of ["page", "deep"]) {
      for (const cohort of shape === "deep" ? [2] : COHORTS) {
        const range = fixture.ranges[cohort];
        const start = range.lo + Math.floor(scale.parents / 2) - 1;
        const label = `${scale.name}/${indexed ? "indexed" : "unindexed"}/${shape}/k=${cohort}`;
        const scenario = { label, scale: scale.name, indexed, shape, childrenPerParent: cohort, plan: undefined, drivers: {} };
        out.scenarios.push(scenario);

        // Oracle results (independent SQL).
        const oc = loader();
        let oracle;
        try {
          if (shape === "page") {
            oracle = normOraclePage((await oc.query(ORACLE_PAGE_SQL, [cohort, start])).rows);
            const batched = normOraclePage(await oracleBatched(oc, cohort, start));
            check(`${label}: hand oracles A and B agree`, () => assert.deepStrictEqual(batched, oracle));
          } else {
            oracle = normDeep((await oc.query(ORACLE_DEEP_SQL, [cohort, start])).rows, oracleCreated);
          }
        } finally {
          await oc.end();
        }
        check(`${label}: oracle page is full`, () => assert.equal(oracle.length, PAGE));
        check(`${label}: oracle child count`, () => assert.equal(oracle.reduce((n, u) => n + u.posts.length, 0), cohort * PAGE));

        for (const driver of args.drivers) {
          const d = {};
          scenario.drivers[driver] = d;
          const statements = { count: 0 };
          const db = await openNeutron(driver, statements);
          const nArgs = shape === "page" ? neutronPageArgs(cohort, start) : neutronDeepArgs(cohort, start);
          const call = () => db.query.users.findMany(nArgs);
          try {
            // Cold: first relational call on a fresh pool (connection
            // establishment and the capability probe included).
            const tc = performance.now();
            const first = await call();
            d.coldMs = r3(performance.now() - tc);
            const norm = shape === "page" ? normNeutronPage(first) : normDeep(first, neutronCreated);
            check(`${label} ${driver}: neutron equals hand SQL oracle`, () => assert.deepStrictEqual(norm, oracle));

            statements.count = 0;
            await call();
            d.clientStatementsPerCall = statements.count;
            check(`${label} ${driver}: one statement per relational read (client count)`, () => assert.equal(statements.count, 1));

            d.neutron = await timed(call, iterations, warmup);
            if (scenario.plan === undefined) {
              // The statement and its plan are driver-independent: explain once.
              const compiled = db.query.users.toSQL(nArgs);
              scenario.plan = await explainPair(compiled.sql, compiled.params, shape === "page" ? ORACLE_PAGE_SQL : ORACLE_DEEP_SQL, [cohort, start], indexed ? 9 : 3);
              if (indexed) {
                check(`${label}: indexed child edges never seq-scan`, () => assert.deepEqual(scenario.plan.childSeqScans, []));
              }
              // The compiled statement has the same correlated-subquery shape
              // as the hand oracle; its server time must stay close to it.
              // Judged on indexed scenarios where the oracle takes >= 1 ms:
              // sub-ms medians are noise, and unindexed plans are the same
              // per-parent seq scans on both sides (3 runs of ~0.1-1 s each,
              // recorded but too noisy to gate).
              const maxRatio = budgets.serverRatioVsHandMax;
              if (maxRatio !== undefined && indexed && scenario.plan.handExecutionMs.p50 >= 1) {
                check(
                  `${label}: server time ${scenario.plan.serverRatioVsHand}x the hand-written statement (max ${maxRatio}x)`,
                  () => assert.ok(scenario.plan.serverRatioVsHand <= maxRatio),
                  "budget",
                );
              }
            }
          } finally {
            await db.close();
          }
          d.serverStatementsPerCall = await serverStatementsPerCall(
            async () => {
              const h = await openNeutron(driver, { count: 0 });
              return { db: h, close: () => h.close() };
            },
            (h) => h.db.query.users.findMany(nArgs),
          );
          check(`${label} ${driver}: one statement per relational read (server count)`, () => assert.equal(d.serverStatementsPerCall, 1));

          // Pinned reference tool, same page.
          const dz = openDrizzle(driver);
          try {
            const dcall = () => (shape === "page" ? drizzlePage(dz.db, cohort, start) : drizzleDeep(dz.db, cohort, start));
            const first = await dcall();
            const norm = shape === "page" ? normNeutronPage(first) : normDeep(first, neutronCreated);
            // drizzle-orm builds relation JSON with json_build_array over raw
            // numeric columns, so JSON.parse turns 1.50 into 1.5 (and would
            // round beyond 2^53): its numeric TEXT is not exact. Compare it on
            // decimal value, and record whether the exact text survived.
            check(`${label} ${driver}: drizzle-orm ${drizzleVersion} equals hand SQL oracle (decimal value)`, () =>
              assert.deepStrictEqual(canonAmounts(norm), canonAmounts(oracle)),
            );
            d.drizzleExactNumericText = isDeepStrictEqual(norm, oracle);
            const before = dz.statements();
            await dcall();
            d.drizzleClientStatementsPerCall = before === null ? null : dz.statements() - before;
            d.drizzle = await timed(dcall, iterations, warmup);
          } finally {
            await dz.close();
          }

          // Hand-written timings (pg): the multi-statement plan and oracle A.
          if (shape === "page" && driver === "pg") {
            const bc = new pg.Pool({ connectionString: BENCH_URL, max: 2 });
            try {
              await oracleBatched(bc, cohort, start);
              d.handBatched = await timed(() => oracleBatched(bc, cohort, start), iterations, warmup);
              d.handBatchedStatementsPerCall = 3;
            } finally {
              await bc.end();
            }
            const ac = new pg.Pool({ connectionString: BENCH_URL, max: 2 });
            try {
              await ac.query(ORACLE_PAGE_SQL, [cohort, start]);
              d.handSingle = await timed(() => ac.query(ORACLE_PAGE_SQL, [cohort, start]), iterations, warmup);
            } finally {
              await ac.end();
            }
          }
          const budgetKey = `${shape}/k=${cohort}/${indexed ? "indexed" : "unindexed"}`;
          // The absolute ceiling judges the median: at 30 samples the p95 is
          // the second-slowest call, which one scheduler stall on a loaded
          // runner decides (R01 measured p50 7.9 ms / p95 86 ms on a machine
          // at load 9-26). A regression the ceiling exists for — N+1, a lost
          // index, a cartesian product — moves the median by orders of
          // magnitude. p95 is recorded and compared same-machine.
          const ceiling = budgets.ciCeilings?.p50Ms?.[scale.name]?.[budgetKey];
          if (ceiling !== undefined) {
            d.ceilingP50Ms = ceiling;
            check(`${label} ${driver}: p50 ${d.neutron.p50} ms within the ${ceiling} ms ceiling`, () => assert.ok(d.neutron.p50 <= ceiling), "budget");
          }
          process.stderr.write(
            `  ${label} ${driver}: neutron p50 ${d.neutron.p50} p95 ${d.neutron.p95} ms | drizzle p50 ${d.drizzle.p50} ms | server ${scenario.plan.executionMs.p50} ms (hand ${scenario.plan.handExecutionMs.p50} ms)\n`,
          );
        }
      }
    }
  }
  return out;
}

async function streamGate() {
  // Early exit from a server-side cursor over the posts table must roll back
  // and hand the pooled connection back promptly. Each driver runs with a
  // single pooled connection, so the follow-up query only completes if the
  // stream released it; for pg the pool counters are asserted directly.
  const out = [];
  for (const driver of args.drivers) {
    const entry = { driver };
    const pool = driver === "pg" ? new pg.Pool({ connectionString: BENCH_URL, max: 1 }) : null;
    const db = pool
      ? await createDatabase({ driver: wrapPgPool(pool), tables: TABLES, relations: RELATIONS })
      : await createDatabase({ url: BENCH_URL, driverOptions: { driver: "postgres", max: 1 }, tables: TABLES, relations: RELATIONS });
    try {
      const firstRow = [];
      const release = [];
      for (let i = 0; i < 10; i++) {
        const t0 = performance.now();
        let tBreak = 0;
        let n = 0;
        for await (const _row of db.select().from(posts).orderBy(asc(posts.id)).stream({ batchSize: 100 })) {
          if (n === 0) firstRow.push(performance.now() - t0);
          if (++n === 10) {
            tBreak = performance.now();
            break;
          }
        }
        release.push(performance.now() - tBreak);
        if (pool) {
          check(`stream ${driver}: connection back in the pool after early exit`, () => assert.equal(pool.totalCount - pool.idleCount, 0));
        }
        await db.driver.query("select 1");
      }
      entry.firstRowMs = summarize(firstRow);
      entry.earlyExitReleaseMs = summarize(release);
    } finally {
      await db.close();
      if (pool) await pool.end().catch(() => {});
    }
    const ceiling = budgets.ciCeilings?.streamEarlyExitReleaseP95Ms;
    if (ceiling !== undefined) {
      check(`stream ${driver}: early-exit release p95 ${entry.earlyExitReleaseMs.p95} ms within the ${ceiling} ms ceiling`, () =>
        assert.ok(entry.earlyExitReleaseMs.p95 <= ceiling),
      );
    }
    out.push(entry);
  }
  return out;
}

async function compileOverhead() {
  // Pure JS compile cost of the relational compiler (no driver round trip),
  // reported separately from server time as V16 requires.
  const db = await createDatabase({ url: BENCH_URL, driverOptions: { driver: "pg", max: 1 }, tables: TABLES, relations: RELATIONS });
  const dz = openDrizzle("pg");
  try {
    const page = neutronPageArgs(20, 1);
    const deep = neutronDeepArgs(2, 1);
    const measure = (fn) => {
      for (let i = 0; i < 100; i++) fn();
      const s = [];
      for (let i = 0; i < PROFILE.compileIterations; i++) {
        const t0 = performance.now();
        fn();
        s.push((performance.now() - t0) * 1000);
      }
      return summarize(s);
    };
    return {
      unit: "microseconds per compile",
      neutronPage: measure(() => db.query.users.toSQL(page)),
      neutronDeep: measure(() => db.query.users.toSQL(deep)),
      drizzlePage: measure(() => drizzlePage(dz.db, 20, 1).toSQL()),
      drizzleDeep: measure(() => drizzleDeep(dz.db, 2, 1).toSQL()),
    };
  } finally {
    await db.close();
    await dz.close();
  }
}

// --------------------------------------------------------------------- main

try {
  await admin.query(`create database "${DB_NAME}"`);
  results.environment = await environment();
  process.stderr.write(`orm-gate: ${results.environment.server}\n`);
  for (const scale of PROFILE.scales) {
    process.stderr.write(`orm-gate: scale ${scale.name} (${scale.parents} parents per cohort)\n`);
    results.scales.push(await runScale(scale));
    if (scale === PROFILE.scales[0]) {
      await setIndexes(true);
      results.stream = await streamGate();
      results.compile = await compileOverhead();
    }
  }
  results.peakRssMiB = Math.round(process.resourceUsage().maxRSS / 1024);
} catch (err) {
  results.failures.push({ kind: "integrity", label: "harness", message: err instanceof Error ? `${err.message}\n${err.stack}` : String(err) });
} finally {
  try {
    await admin.query(`drop database if exists "${DB_NAME}" with (force)`);
  } catch (err) {
    results.failures.push({ kind: "integrity", label: "teardown", message: String(err) });
  }
  await admin.end();
}
results.finishedAt = new Date().toISOString();

if (args.compare) {
  const base = JSON.parse(readFileSync(args.compare, "utf8"));
  const tol = budgets.sameMachine?.regressionPct ?? 20;
  const floor = budgets.sameMachine?.absoluteFloorMs ?? 0.5;
  results.comparison = [];
  const index = new Map();
  for (const s of base.scales ?? []) for (const sc of s.scenarios) for (const [drv, d] of Object.entries(sc.drivers)) index.set(`${sc.label} ${drv}`, d);
  for (const s of results.scales) {
    for (const sc of s.scenarios) {
      for (const [drv, d] of Object.entries(sc.drivers)) {
        const b = index.get(`${sc.label} ${drv}`);
        if (!b) continue;
        const p50Pct = r3(((d.neutron.p50 - b.neutron.p50) / b.neutron.p50) * 100);
        const p95Pct = r3(((d.neutron.p95 - b.neutron.p95) / b.neutron.p95) * 100);
        // Sub-millisecond medians are dominated by scheduler noise; a
        // regression must also exceed an absolute floor to count.
        const regressed = p50Pct > tol && d.neutron.p50 - b.neutron.p50 > floor;
        results.comparison.push({ key: `${sc.label} ${drv}`, baseP50: b.neutron.p50, p50: d.neutron.p50, p50Pct, baseP95: b.neutron.p95, p95: d.neutron.p95, p95Pct, regressed });
        if (regressed) {
          results.failures.push({ kind: "compare", label: `${sc.label} ${drv}: p50 regressed ${p50Pct}% vs same-machine baseline (tolerance ${tol}%)`, message: "investigate before adjusting the baseline" });
        }
      }
    }
  }
}

const json = JSON.stringify(results, null, 2);
if (args.out) writeFileSync(args.out, json + "\n");
else process.stdout.write(json + "\n");

// Integrity and --compare failures always fail; budget ceilings fail with --gate.
const blocking = results.failures.filter((f) => f.kind !== "budget" || args.gate);
for (const f of results.failures) console.error(`orm-gate ${blocking.includes(f) ? "FAIL" : "WARN"} ${f.label}: ${f.message}`);
console.error(`orm-gate: ${blocking.length === 0 ? "PASS" : `${blocking.length} failure(s)`}`);
process.exit(blocking.length === 0 ? 0 : 1);
