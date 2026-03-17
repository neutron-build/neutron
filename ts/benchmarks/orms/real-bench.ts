/**
 * REAL ORM Benchmark — Neutron raw SQL vs Drizzle ORM
 *
 * This benchmark actually uses Drizzle's query builder API.
 * "Neutron" side uses raw better-sqlite3 prepared statements,
 * which is what Neutron's TypeScript client does under the hood
 * (parameterized SQL over pgwire/HTTP transport).
 *
 * What this measures:
 *   - Query builder overhead (Drizzle's JS object → SQL compilation)
 *   - Result mapping overhead (Drizzle's row → typed object mapping)
 *   - NOT transport/network (both hit same in-memory SQLite)
 *
 * What this does NOT measure:
 *   - Neutron's multi-model advantage (KV, Vector, etc.)
 *   - Network transport overhead (pgwire vs HTTP)
 *   - Prisma (requires codegen step; omitted for now)
 */

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { eq, and, sql, count, countDistinct } from "drizzle-orm";
import * as schema from "./drizzle-schema.js";

// ============================================================================
// Harness
// ============================================================================

interface Result {
  scenario: string;
  rawMs: number;
  drizzleMs: number;
  overheadPct: number;
  rawOpsPerSec: number;
  drizzleOpsPerSec: number;
}

function bench(fn: () => void, iterations: number): number {
  // warm up — let V8 JIT compile
  for (let i = 0; i < Math.min(iterations, 500); i++) fn();

  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  return (performance.now() - t0) / iterations;
}

const results: Result[] = [];

function record(scenario: string, rawMs: number, drizzleMs: number) {
  const overheadPct = ((drizzleMs - rawMs) / rawMs) * 100;
  results.push({
    scenario,
    rawMs,
    drizzleMs,
    overheadPct,
    rawOpsPerSec: 1000 / rawMs,
    drizzleOpsPerSec: 1000 / drizzleMs,
  });
}

// ============================================================================
// Setup
// ============================================================================

const sqlite = new Database(":memory:");
sqlite.pragma("journal_mode = WAL");

sqlite.exec(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id),
    user_id INTEGER NOT NULL REFERENCES users(id),
    body TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
  );
  CREATE TABLE post_tags (
    post_id INTEGER NOT NULL REFERENCES posts(id),
    tag_id INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (post_id, tag_id)
  );
  CREATE INDEX idx_posts_user ON posts(user_id);
  CREATE INDEX idx_comments_post ON comments(post_id);
  CREATE INDEX idx_comments_user ON comments(user_id);
`);

// Seed
const insertU = sqlite.prepare("INSERT INTO users (email,name,age) VALUES (?,?,?)");
for (let i = 1; i <= 200; i++) insertU.run(`u${i}@x.com`, `User ${i}`, 20 + (i % 50));

const insertP = sqlite.prepare("INSERT INTO posts (user_id,title,content,published) VALUES (?,?,?,?)");
for (let u = 1; u <= 200; u++)
  for (let p = 1; p <= 5; p++)
    insertP.run(u, `Post ${p} by ${u}`, "Lorem ipsum dolor sit amet...", p % 2);

const insertC = sqlite.prepare("INSERT INTO comments (post_id,user_id,body) VALUES (?,?,?)");
for (let p = 1; p <= 1000; p++)
  for (let c = 1; c <= 3; c++)
    insertC.run(p, ((p + c) % 200) + 1, `Comment ${c} on post ${p}`);

for (const tag of ["rust", "typescript", "database", "performance", "zig"])
  sqlite.prepare("INSERT INTO tags (name) VALUES (?)").run(tag);

const insertPT = sqlite.prepare("INSERT INTO post_tags (post_id, tag_id) VALUES (?,?)");
for (let p = 1; p <= 1000; p++) insertPT.run(p, (p % 5) + 1);

const db = drizzle(sqlite);

console.log("Database: in-memory SQLite | 200 users, 1000 posts, 3000 comments, 5 tags\n");

// ============================================================================
// Scenarios
// ============================================================================

// --- 1. SELECT by primary key ---
{
  const stmt = sqlite.prepare("SELECT * FROM users WHERE id = ?");
  const rawMs = bench(() => stmt.get(42), 20_000);
  const drzMs = bench(() => db.select().from(schema.users).where(eq(schema.users.id, 42)).get(), 20_000);
  record("SELECT by PK", rawMs, drzMs);
}

// --- 2. SELECT with WHERE + AND ---
{
  const stmt = sqlite.prepare("SELECT * FROM posts WHERE user_id = ? AND published = 1");
  const rawMs = bench(() => stmt.all(10), 10_000);
  const drzMs = bench(
    () => db.select().from(schema.posts).where(and(eq(schema.posts.userId, 10), eq(schema.posts.published, true))).all(),
    10_000,
  );
  record("SELECT WHERE+AND", rawMs, drzMs);
}

// --- 3. SELECT with LIMIT ---
{
  const stmt = sqlite.prepare("SELECT * FROM users ORDER BY age DESC LIMIT 20");
  const rawMs = bench(() => stmt.all(), 10_000);
  const drzMs = bench(() => db.select().from(schema.users).orderBy(schema.users.age).limit(20).all(), 10_000);
  record("SELECT ORDER+LIMIT", rawMs, drzMs);
}

// --- 4. COUNT ---
{
  const stmt = sqlite.prepare("SELECT count(*) as c FROM posts WHERE user_id = ?");
  const rawMs = bench(() => stmt.get(5), 10_000);
  const drzMs = bench(
    () => db.select({ c: count() }).from(schema.posts).where(eq(schema.posts.userId, 5)).get(),
    10_000,
  );
  record("COUNT", rawMs, drzMs);
}

// --- 5. JOIN (posts + authors) ---
{
  const stmt = sqlite.prepare(`
    SELECT p.id, p.title, u.name as author
    FROM posts p JOIN users u ON u.id = p.user_id
    WHERE p.published = 1 LIMIT 20
  `);
  const rawMs = bench(() => stmt.all(), 5_000);
  const drzMs = bench(
    () =>
      db
        .select({ id: schema.posts.id, title: schema.posts.title, author: schema.users.name })
        .from(schema.posts)
        .innerJoin(schema.users, eq(schema.users.id, schema.posts.userId))
        .where(eq(schema.posts.published, true))
        .limit(20)
        .all(),
    5_000,
  );
  record("INNER JOIN", rawMs, drzMs);
}

// --- 6. LEFT JOIN + GROUP BY + aggregate ---
{
  const stmt = sqlite.prepare(`
    SELECT p.id, p.title, COUNT(c.id) as cc
    FROM posts p LEFT JOIN comments c ON c.post_id = p.id
    WHERE p.user_id = ?
    GROUP BY p.id
  `);
  const rawMs = bench(() => stmt.all(3), 2_000);
  const drzMs = bench(
    () =>
      db
        .select({ id: schema.posts.id, title: schema.posts.title, cc: count(schema.comments.id) })
        .from(schema.posts)
        .leftJoin(schema.comments, eq(schema.comments.postId, schema.posts.id))
        .where(eq(schema.posts.userId, 3))
        .groupBy(schema.posts.id)
        .all(),
    2_000,
  );
  record("LEFT JOIN+GROUP BY", rawMs, drzMs);
}

// --- 7. Complex 4-table JOIN ---
{
  const stmt = sqlite.prepare(`
    SELECT p.id, p.title, u.name, COUNT(DISTINCT c.id) as cc
    FROM posts p
    JOIN users u ON u.id = p.user_id
    LEFT JOIN comments c ON c.post_id = p.id
    LEFT JOIN post_tags pt ON pt.post_id = p.id
    WHERE p.published = 1
    GROUP BY p.id
    LIMIT 10
  `);
  const rawMs = bench(() => stmt.all(), 1_000);
  // Drizzle can build this, but it's awkward—fall back to sql`` for fairness
  const drzMs = bench(
    () =>
      db
        .select({
          id: schema.posts.id,
          title: schema.posts.title,
          author: schema.users.name,
          cc: countDistinct(schema.comments.id),
        })
        .from(schema.posts)
        .innerJoin(schema.users, eq(schema.users.id, schema.posts.userId))
        .leftJoin(schema.comments, eq(schema.comments.postId, schema.posts.id))
        .leftJoin(schema.postTags, eq(schema.postTags.postId, schema.posts.id))
        .where(eq(schema.posts.published, true))
        .groupBy(schema.posts.id)
        .limit(10)
        .all(),
    1_000,
  );
  record("4-table JOIN", rawMs, drzMs);
}

// --- 8. INSERT ---
{
  let n = 0;
  const stmt = sqlite.prepare("INSERT INTO users (email,name,age) VALUES (?,?,?)");
  const rawMs = bench(() => { n++; stmt.run(`bench-raw-${n}@x.com`, "Bench", 30); }, 2_000);
  n = 0;
  const drzMs = bench(
    () => { n++; db.insert(schema.users).values({ email: `bench-drz-${n}@x.com`, name: "Bench", age: 30 }).run(); },
    2_000,
  );
  record("INSERT", rawMs, drzMs);
}

// --- 9. UPDATE ---
{
  const stmt = sqlite.prepare("UPDATE users SET age = age + 1 WHERE id = ?");
  const rawMs = bench(() => stmt.run(1), 5_000);
  const drzMs = bench(
    () => db.update(schema.users).set({ age: sql`age + 1` }).where(eq(schema.users.id, 1)).run(),
    5_000,
  );
  record("UPDATE", rawMs, drzMs);
}

// --- 10. DELETE ---
{
  // Seed throwaway rows
  for (let i = 0; i < 5000; i++) sqlite.prepare("INSERT INTO tags (name) VALUES (?)").run(`del-raw-${i}`);
  for (let i = 0; i < 5000; i++) sqlite.prepare("INSERT INTO tags (name) VALUES (?)").run(`del-drz-${i}`);

  let rawI = 0;
  const rawMs = bench(() => {
    sqlite.prepare("DELETE FROM tags WHERE name = ?").run(`del-raw-${rawI++}`);
  }, 2_000);
  let drzI = 0;
  const drzMs = bench(() => {
    db.delete(schema.tags).where(eq(schema.tags.name, `del-drz-${drzI++}`)).run();
  }, 2_000);
  record("DELETE", rawMs, drzMs);
}

sqlite.close();

// ============================================================================
// Report
// ============================================================================

const W = 96;
console.log("=".repeat(W));
console.log("  NEUTRON RAW SQL  vs  DRIZZLE ORM  —  Real Benchmark");
console.log("=".repeat(W));
console.log("");

const header =
  "Scenario".padEnd(24) +
  "Raw SQL".padStart(12) +
  "Drizzle".padStart(12) +
  "Overhead".padStart(12) +
  "Raw ops/s".padStart(14) +
  "Drz ops/s".padStart(14);
console.log(header);
console.log("-".repeat(W));

for (const r of results) {
  const oh = r.overheadPct >= 0 ? `+${r.overheadPct.toFixed(1)}%` : `${r.overheadPct.toFixed(1)}%`;
  console.log(
    r.scenario.padEnd(24) +
      `${r.rawMs.toFixed(4)}ms`.padStart(12) +
      `${r.drizzleMs.toFixed(4)}ms`.padStart(12) +
      oh.padStart(12) +
      Math.round(r.rawOpsPerSec).toLocaleString().padStart(14) +
      Math.round(r.drizzleOpsPerSec).toLocaleString().padStart(14),
  );
}

console.log("-".repeat(W));

const avgRaw = results.reduce((s, r) => s + r.rawMs, 0) / results.length;
const avgDrz = results.reduce((s, r) => s + r.drizzleMs, 0) / results.length;
const avgOh = ((avgDrz - avgRaw) / avgRaw) * 100;

console.log(
  "AVERAGE".padEnd(24) +
    `${avgRaw.toFixed(4)}ms`.padStart(12) +
    `${avgDrz.toFixed(4)}ms`.padStart(12) +
    `+${avgOh.toFixed(1)}%`.padStart(12) +
    Math.round(1000 / avgRaw).toLocaleString().padStart(14) +
    Math.round(1000 / avgDrz).toLocaleString().padStart(14),
);

console.log("");
console.log("=".repeat(W));
console.log("  ANALYSIS");
console.log("=".repeat(W));
console.log("");

// Categorize
const reads = results.filter((r) => !["INSERT", "UPDATE", "DELETE"].includes(r.scenario));
const writes = results.filter((r) => ["INSERT", "UPDATE", "DELETE"].includes(r.scenario));

const avgReadRaw = reads.reduce((s, r) => s + r.rawMs, 0) / reads.length;
const avgReadDrz = reads.reduce((s, r) => s + r.drizzleMs, 0) / reads.length;
const avgWriteRaw = writes.reduce((s, r) => s + r.rawMs, 0) / writes.length;
const avgWriteDrz = writes.reduce((s, r) => s + r.drizzleMs, 0) / writes.length;

console.log(`  READ overhead:   ${((avgReadDrz - avgReadRaw) / avgReadRaw * 100).toFixed(1)}%  (${avgReadRaw.toFixed(4)}ms -> ${avgReadDrz.toFixed(4)}ms)`);
console.log(`  WRITE overhead:  ${((avgWriteDrz - avgWriteRaw) / avgWriteRaw * 100).toFixed(1)}%  (${avgWriteRaw.toFixed(4)}ms -> ${avgWriteDrz.toFixed(4)}ms)`);
console.log("");

const biggest = results.reduce((a, b) => (a.overheadPct > b.overheadPct ? a : b));
const smallest = results.reduce((a, b) => (a.overheadPct < b.overheadPct ? a : b));

console.log(`  Highest overhead:  ${biggest.scenario} (+${biggest.overheadPct.toFixed(1)}%)`);
console.log(`  Lowest overhead:   ${smallest.scenario} (${smallest.overheadPct.toFixed(1)}%)`);
console.log("");

// Context
console.log("=".repeat(W));
console.log("  CONTEXT");
console.log("=".repeat(W));
console.log("");
console.log("  These numbers measure QUERY BUILDER overhead only.");
console.log("  In a real HTTP request the total latency stack is roughly:");
console.log("");
console.log("    Network round-trip:     ~50-200ms");
console.log("    TLS handshake:          ~10-50ms");
console.log("    HTTP framework:         ~0.1-0.5ms");
console.log("    Database I/O (PG/disk): ~0.5-50ms");
console.log("    ORM query builder:      ~0.005-0.05ms  <-- what this measures");
console.log("    Result serialization:   ~0.01-0.1ms");
console.log("");
console.log("  ORM overhead is <0.1% of a typical web request.");
console.log("");
console.log("  What Neutron offers that Drizzle/Prisma cannot:");
console.log("    - 14 data models (KV, Vector, Graph, TimeSeries, Streams, ...)");
console.log("    - Type-safe clients in Rust, Go, Python, TypeScript, Zig");
console.log("    - Feature detection across Nucleus and PostgreSQL");
console.log("    - Cross-model transactions");
console.log("");
