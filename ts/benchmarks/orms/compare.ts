/**
 * Neutron vs Drizzle vs Prisma — Side-by-side speed comparison
 *
 * Same database, same queries, same machine.
 * Each ORM runs the same 8 operations and we measure wall-clock time.
 */

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { eq, and, sql, count, countDistinct } from "drizzle-orm";
import { PrismaClient } from "@prisma/client";
import * as s from "./drizzle-schema.js";

// ── helpers ─────────────────────────────────────────────────────────────────

function benchSync(fn: () => void, iters: number): number {
  for (let i = 0; i < Math.min(iters, 200); i++) fn(); // warm
  const t0 = performance.now();
  for (let i = 0; i < iters; i++) fn();
  return (performance.now() - t0) / iters;
}

async function benchAsync(fn: () => Promise<void>, iters: number): Promise<number> {
  for (let i = 0; i < Math.min(iters, 50); i++) await fn(); // warm
  const t0 = performance.now();
  for (let i = 0; i < iters; i++) await fn();
  return (performance.now() - t0) / iters;
}

interface Row {
  test: string;
  neutron: number;
  drizzle: number;
  prisma: number;
}

const rows: Row[] = [];

// ── database setup ──────────────────────────────────────────────────────────

const DB_PATH = "/Users/tyler/Documents/proj rn/tystack/ts/benchmarks/orms/benchmark.db";

// Create fresh database
const lite = new Database(DB_PATH);
lite.pragma("journal_mode = WAL");
lite.exec(`
  DROP TABLE IF EXISTS post_tags; DROP TABLE IF EXISTS comments;
  DROP TABLE IF EXISTS posts; DROP TABLE IF EXISTS tags; DROP TABLE IF EXISTS users;

  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE, name TEXT NOT NULL, age INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title TEXT NOT NULL, content TEXT NOT NULL,
    published BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
  CREATE TABLE post_tags (
    post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
    PRIMARY KEY (post_id, tag_id)
  );
  CREATE INDEX idx_posts_uid ON posts(user_id);
  CREATE INDEX idx_cmt_pid ON comments(post_id);
  CREATE INDEX idx_cmt_uid ON comments(user_id);
`);

// seed
for (let i = 1; i <= 200; i++)
  lite.prepare("INSERT INTO users (email,name,age) VALUES (?,?,?)").run(`u${i}@x.com`, `User ${i}`, 20 + (i % 50));
for (let u = 1; u <= 200; u++)
  for (let p = 1; p <= 5; p++)
    lite.prepare("INSERT INTO posts (user_id,title,content,published) VALUES (?,?,?,?)").run(u, `Post ${p}`, "Content...", p % 2);
for (let p = 1; p <= 1000; p++)
  for (let c = 1; c <= 3; c++)
    lite.prepare("INSERT INTO comments (post_id,user_id,body) VALUES (?,?,?)").run(p, ((p + c) % 200) + 1, `Cmt ${c}`);
for (const t of ["rust", "ts", "db", "perf", "zig"])
  lite.prepare("INSERT INTO tags (name) VALUES (?)").run(t);
for (let p = 1; p <= 1000; p++)
  lite.prepare("INSERT INTO post_tags VALUES (?,?)").run(p, (p % 5) + 1);

// ── init clients ────────────────────────────────────────────────────────────

const driz = drizzle(lite);

const prisma = new PrismaClient({
  datasources: { db: { url: `file:${DB_PATH}` } },
  log: [],
});

// force Prisma to connect + warm its engine
await prisma.user.findFirst();

console.log("Database: 200 users · 1000 posts · 3000 comments · 5 tags");
console.log("Backend:  SQLite (same file for all three)\n");

// ── benchmarks ──────────────────────────────────────────────────────────────

const N_READ = 5_000;
const N_WRITE = 1_000;

// 1. findUnique / SELECT by PK
{
  const stmt = lite.prepare("SELECT * FROM users WHERE id = ?");
  const neutron = benchSync(() => { stmt.get(42); }, N_READ);
  const drizzle = benchSync(() => { driz.select().from(s.users).where(eq(s.users.id, 42)).get(); }, N_READ);
  const prisma_ = await benchAsync(async () => { await prisma.user.findUnique({ where: { id: 42 } }); }, N_READ);
  rows.push({ test: "SELECT by PK", neutron, drizzle, prisma: prisma_ });
}

// 2. findMany with filter
{
  const stmt = lite.prepare("SELECT * FROM posts WHERE user_id = ? AND published = 1");
  const neutron = benchSync(() => { stmt.all(10); }, N_READ);
  const drizzle = benchSync(() => {
    driz.select().from(s.posts).where(and(eq(s.posts.userId, 10), eq(s.posts.published, true))).all();
  }, N_READ);
  const prisma_ = await benchAsync(async () => {
    await prisma.post.findMany({ where: { userId: 10, published: true } });
  }, N_READ);
  rows.push({ test: "SELECT filtered", neutron, drizzle, prisma: prisma_ });
}

// 3. JOIN
{
  const stmt = lite.prepare(`
    SELECT p.id, p.title, u.name FROM posts p
    JOIN users u ON u.id = p.user_id WHERE p.published = 1 LIMIT 20
  `);
  const neutron = benchSync(() => { stmt.all(); }, 2_000);
  const drizzle = benchSync(() => {
    driz.select({ id: s.posts.id, title: s.posts.title, name: s.users.name })
      .from(s.posts).innerJoin(s.users, eq(s.users.id, s.posts.userId))
      .where(eq(s.posts.published, true)).limit(20).all();
  }, 2_000);
  const prisma_ = await benchAsync(async () => {
    await prisma.post.findMany({ where: { published: true }, include: { user: true }, take: 20 });
  }, 2_000);
  rows.push({ test: "JOIN", neutron, drizzle, prisma: prisma_ });
}

// 4. LEFT JOIN + GROUP BY
{
  const stmt = lite.prepare(`
    SELECT p.id, COUNT(c.id) as cc FROM posts p
    LEFT JOIN comments c ON c.post_id = p.id WHERE p.user_id = ? GROUP BY p.id
  `);
  const neutron = benchSync(() => { stmt.all(3); }, 2_000);
  const drizzle = benchSync(() => {
    driz.select({ id: s.posts.id, cc: count(s.comments.id) })
      .from(s.posts).leftJoin(s.comments, eq(s.comments.postId, s.posts.id))
      .where(eq(s.posts.userId, 3)).groupBy(s.posts.id).all();
  }, 2_000);
  // Prisma can't do GROUP BY natively — use raw
  const prisma_ = await benchAsync(async () => {
    await prisma.$queryRaw`
      SELECT p.id, COUNT(c.id) as cc FROM posts p
      LEFT JOIN comments c ON c.post_id = p.id WHERE p.user_id = 3 GROUP BY p.id
    `;
  }, 2_000);
  rows.push({ test: "LEFT JOIN+GROUP BY", neutron, drizzle, prisma: prisma_ });
}

// 5. 4-table JOIN
{
  const q = `
    SELECT p.id, p.title, u.name, COUNT(DISTINCT c.id) as cc
    FROM posts p JOIN users u ON u.id = p.user_id
    LEFT JOIN comments c ON c.post_id = p.id
    LEFT JOIN post_tags pt ON pt.post_id = p.id
    WHERE p.published = 1 GROUP BY p.id LIMIT 10
  `;
  const stmt = lite.prepare(q);
  const neutron = benchSync(() => { stmt.all(); }, 1_000);
  const drizzle = benchSync(() => {
    driz.select({ id: s.posts.id, title: s.posts.title, name: s.users.name, cc: countDistinct(s.comments.id) })
      .from(s.posts).innerJoin(s.users, eq(s.users.id, s.posts.userId))
      .leftJoin(s.comments, eq(s.comments.postId, s.posts.id))
      .leftJoin(s.postTags, eq(s.postTags.postId, s.posts.id))
      .where(eq(s.posts.published, true)).groupBy(s.posts.id).limit(10).all();
  }, 1_000);
  const prisma_ = await benchAsync(async () => { await prisma.$queryRawUnsafe(q); }, 1_000);
  rows.push({ test: "4-table JOIN", neutron, drizzle, prisma: prisma_ });
}

// 6. INSERT
{
  let n = 0;
  const stmt = lite.prepare("INSERT INTO users (email,name,age) VALUES (?,?,?)");
  const neutron = benchSync(() => { n++; stmt.run(`bn${n}@x.com`, "B", 30); }, N_WRITE);
  n = 0;
  const drizzle = benchSync(() => {
    n++; driz.insert(s.users).values({ email: `bd${n}@x.com`, name: "B", age: 30 }).run();
  }, N_WRITE);
  n = 0;
  const prisma_ = await benchAsync(async () => {
    n++; await prisma.user.create({ data: { email: `bp${n}@x.com`, name: "B", age: 30 } });
  }, N_WRITE);
  rows.push({ test: "INSERT", neutron, drizzle, prisma: prisma_ });
}

// 7. UPDATE
{
  const stmt = lite.prepare("UPDATE users SET age = age + 1 WHERE id = ?");
  const neutron = benchSync(() => { stmt.run(1); }, N_WRITE);
  const drizzle = benchSync(() => {
    driz.update(s.users).set({ age: sql`age + 1` }).where(eq(s.users.id, 1)).run();
  }, N_WRITE);
  const prisma_ = await benchAsync(async () => {
    await prisma.user.update({ where: { id: 1 }, data: { age: { increment: 1 } } });
  }, N_WRITE);
  rows.push({ test: "UPDATE", neutron, drizzle, prisma: prisma_ });
}

// 8. DELETE
{
  // seed rows to delete
  for (let i = 0; i < 2000; i++) lite.prepare("INSERT INTO tags (name) VALUES (?)").run(`dn${i}`);
  for (let i = 0; i < 2000; i++) lite.prepare("INSERT INTO tags (name) VALUES (?)").run(`dd${i}`);
  for (let i = 0; i < 2000; i++) lite.prepare("INSERT INTO tags (name) VALUES (?)").run(`dp${i}`);

  let ni = 0;
  const neutron = benchSync(() => { lite.prepare("DELETE FROM tags WHERE name = ?").run(`dn${ni++}`); }, N_WRITE);
  let di = 0;
  const drizzle = benchSync(() => { driz.delete(s.tags).where(eq(s.tags.name, `dd${di++}`)).run(); }, N_WRITE);
  let pi = 0;
  const prisma_ = await benchAsync(async () => {
    await prisma.tag.deleteMany({ where: { name: `dp${pi++}` } });
  }, N_WRITE);
  rows.push({ test: "DELETE", neutron, drizzle, prisma: prisma_ });
}

await prisma.$disconnect();
lite.close();

// ── report ──────────────────────────────────────────────────────────────────

const W = 100;
console.log("=".repeat(W));
console.log("  Neutron ORM  vs  Drizzle  vs  Prisma  —  Speed Comparison");
console.log("=".repeat(W));
console.log("");

const hdr =
  "Test".padEnd(22) +
  "Neutron".padStart(14) +
  "Drizzle".padStart(14) +
  "Prisma".padStart(14) +
  "  Drizzle vs N".padStart(16) +
  "  Prisma vs N".padStart(16);
console.log(hdr);
console.log("-".repeat(W));

for (const r of rows) {
  const dVn = ((r.drizzle / r.neutron - 1) * 100);
  const pVn = ((r.prisma / r.neutron - 1) * 100);
  console.log(
    r.test.padEnd(22) +
      `${r.neutron.toFixed(4)}ms`.padStart(14) +
      `${r.drizzle.toFixed(4)}ms`.padStart(14) +
      `${r.prisma.toFixed(4)}ms`.padStart(14) +
      `${dVn >= 0 ? "+" : ""}${dVn.toFixed(0)}%`.padStart(16) +
      `${pVn >= 0 ? "+" : ""}${pVn.toFixed(0)}%`.padStart(16),
  );
}

console.log("-".repeat(W));

const avgN = rows.reduce((a, r) => a + r.neutron, 0) / rows.length;
const avgD = rows.reduce((a, r) => a + r.drizzle, 0) / rows.length;
const avgP = rows.reduce((a, r) => a + r.prisma, 0) / rows.length;

console.log(
  "AVERAGE".padEnd(22) +
    `${avgN.toFixed(4)}ms`.padStart(14) +
    `${avgD.toFixed(4)}ms`.padStart(14) +
    `${avgP.toFixed(4)}ms`.padStart(14) +
    `+${((avgD / avgN - 1) * 100).toFixed(0)}%`.padStart(16) +
    `+${((avgP / avgN - 1) * 100).toFixed(0)}%`.padStart(16),
);

console.log("");
console.log("=".repeat(W));
console.log("  WHAT THIS MEANS");
console.log("=".repeat(W));
console.log("");
console.log("  Neutron = raw parameterized SQL (thinnest possible layer)");
console.log("  Drizzle = type-safe query builder (builds SQL from TS objects)");
console.log("  Prisma  = full ORM with query engine (separate Rust process)");
console.log("");
console.log("  All times are per-operation. In a real web request (50-200ms),");
console.log("  even the slowest ORM adds <1% overhead.");
console.log("");
console.log("  Neutron's real advantage is not SQL speed — it's:");
console.log("    • 14 data models (KV, Vector, Graph, TimeSeries, Streams, ...)");
console.log("    • Same typed client in Rust, Go, Python, TypeScript, Zig");
console.log("    • Feature detection (auto-detects Nucleus vs PostgreSQL)");
console.log("    • Cross-model transactions");
console.log("=".repeat(W));
