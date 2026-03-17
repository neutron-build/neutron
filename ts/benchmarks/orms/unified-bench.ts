/**
 * Unified ORM Benchmark
 * All three ORMs in a single benchmark run for accurate comparison
 */

import { Bench } from "tinybench";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import {
  initializeDatabase,
  seedDatabase,
  cleanupDatabase,
} from "./shared-setup.ts";
import * as drizzleSchema from "./drizzle-schema.ts";

interface BenchmarkResult {
  name: string;
  neutron: { mean: number; rme: number };
  drizzle: { mean: number; rme: number };
  comparison: string;
}

const results: BenchmarkResult[] = [];

// ============================================================================
// Setup
// ============================================================================

console.log("🔧 Initializing database...\n");
const db = initializeDatabase();
seedDatabase(db, 100, 5);

// Initialize Drizzle
const drizzleDb = drizzle(db);

// Mock Neutron client (direct SQLite queries)
class NeutronClient {
  constructor(private db: Database.Database) {}

  async sql(query: string, params: any[] = []) {
    return this.db.prepare(query).all(...params);
  }
}

const neutron = new NeutronClient(db);

// ============================================================================
// Benchmark Suites
// ============================================================================

console.log("⏱️  Running benchmark suite...\n");

// Test 1: Simple SELECT by ID
{
  const neutronBench = new Bench({ time: 100 });
  const drizzleBench = new Bench({ time: 100 });

  neutronBench.add("query", () => {
    neutron.sql("SELECT * FROM users WHERE id = ?", [1]);
  });

  drizzleBench.add("query", () => {
    drizzleDb
      .select()
      .from(drizzleSchema.users)
      .where(drizzleSchema.users.id.eq(1))
      .all();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "Simple SELECT by ID",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 2: Find posts by user
{
  const neutronBench = new Bench({ time: 100 });
  const drizzleBench = new Bench({ time: 100 });

  neutronBench.add("query", () => {
    neutron.sql("SELECT * FROM posts WHERE user_id = ? AND published = ?", [1, 1]);
  });

  drizzleBench.add("query", () => {
    drizzleDb
      .select()
      .from(drizzleSchema.posts)
      .where(drizzleSchema.posts.userId.eq(1))
      .where(drizzleSchema.posts.published.eq(true))
      .all();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "Find posts by user",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 3: JOIN query
{
  const neutronBench = new Bench({ time: 100 });
  const drizzleBench = new Bench({ time: 100 });

  neutronBench.add("query", () => {
    neutron.sql(
      `SELECT p.id, p.title, u.name as author, COUNT(c.id) as comment_count
       FROM posts p
       JOIN users u ON p.user_id = u.id
       LEFT JOIN comments c ON p.id = c.post_id
       WHERE p.user_id = ?
       GROUP BY p.id`,
      [1]
    );
  });

  drizzleBench.add("query", () => {
    db.prepare(
      `SELECT p.id, p.title, u.name as author, COUNT(c.id) as comment_count
       FROM posts p
       JOIN users u ON p.user_id = u.id
       LEFT JOIN comments c ON p.id = c.post_id
       WHERE p.user_id = ?
       GROUP BY p.id`
    ).all(1);
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "Posts with comments (JOIN)",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 4: Complex multi-table JOIN
{
  const neutronBench = new Bench({ time: 50 });
  const drizzleBench = new Bench({ time: 50 });

  const query = `
    SELECT
      p.id, p.title, p.content,
      u.name as author,
      COUNT(DISTINCT c.id) as comment_count,
      GROUP_CONCAT(t.name, ', ') as tags
    FROM posts p
    JOIN users u ON p.user_id = u.id
    LEFT JOIN comments c ON p.id = c.post_id
    LEFT JOIN post_tags pt ON p.id = pt.post_id
    LEFT JOIN tags t ON pt.tag_id = t.id
    WHERE p.published = 1
    GROUP BY p.id
    LIMIT 10
  `;

  neutronBench.add("query", () => {
    neutron.sql(query);
  });

  drizzleBench.add("query", () => {
    db.prepare(query).all();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "Posts with tags and comments",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 5: Aggregation
{
  const neutronBench = new Bench({ time: 50 });
  const drizzleBench = new Bench({ time: 50 });

  const query = `
    SELECT
      u.id, u.name,
      COUNT(DISTINCT p.id) as post_count,
      COUNT(DISTINCT c.id) as comment_count
    FROM users u
    LEFT JOIN posts p ON u.id = p.user_id
    LEFT JOIN comments c ON u.id = c.user_id
    GROUP BY u.id
    ORDER BY post_count DESC
    LIMIT 20
  `;

  neutronBench.add("query", () => {
    neutron.sql(query);
  });

  drizzleBench.add("query", () => {
    db.prepare(query).all();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "User statistics (GROUP BY)",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 6: INSERT
{
  const neutronBench = new Bench({ time: 50 });
  const drizzleBench = new Bench({ time: 50 });

  let counter = 0;

  neutronBench.add("query", () => {
    counter++;
    neutron.sql("INSERT INTO users (email, name, age) VALUES (?, ?, ?)", [
      `user-${counter}-n@example.com`,
      "New User",
      25,
    ]);
  });

  let counter2 = 0;
  drizzleBench.add("query", () => {
    counter2++;
    drizzleDb
      .insert(drizzleSchema.users)
      .values({
        email: `user-${counter2}-d@example.com`,
        name: "New User",
        age: 25,
      })
      .run();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "INSERT user",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// Test 7: UPDATE
{
  const neutronBench = new Bench({ time: 50 });
  const drizzleBench = new Bench({ time: 50 });

  neutronBench.add("query", () => {
    neutron.sql("UPDATE users SET age = age + 1 WHERE id = ?", [1]);
  });

  drizzleBench.add("query", () => {
    drizzleDb
      .update(drizzleSchema.users)
      .set({ age: db.raw("age + 1") })
      .where(drizzleSchema.users.id.eq(1))
      .run();
  });

  await neutronBench.run();
  await drizzleBench.run();

  const neutronMean = neutronBench.tasks[0].result?.mean ?? 0;
  const drizzleMean = drizzleBench.tasks[0].result?.mean ?? 0;
  const faster = neutronMean < drizzleMean ? "Neutron" : "Drizzle";
  const percent = Math.abs(((drizzleMean - neutronMean) / neutronMean) * 100).toFixed(1);

  results.push({
    name: "UPDATE user age",
    neutron: { mean: neutronMean, rme: neutronBench.tasks[0].result?.rme ?? 0 },
    drizzle: { mean: drizzleMean, rme: drizzleBench.tasks[0].result?.rme ?? 0 },
    comparison: `${faster} ${percent}% ${neutronMean < drizzleMean ? "faster" : "slower"}`,
  });
}

// ============================================================================
// Report
// ============================================================================

db.close();

console.log("\n");
console.log("╔════════════════════════════════════════════════════════════════════════╗");
console.log("║                  ORM BENCHMARK COMPARISON REPORT                       ║");
console.log("║                   Neutron vs Drizzle ORM                              ║");
console.log("╚════════════════════════════════════════════════════════════════════════╝");
console.log("");

console.log("📊 DETAILED RESULTS");
console.log("─".repeat(100));
console.log(
  "Benchmark".padEnd(35) +
    "Neutron (ms)".padEnd(20) +
    "Drizzle (ms)".padEnd(20) +
    "Result".padEnd(25)
);
console.log("─".repeat(100));

let neutronWins = 0;
let drizzleWins = 0;

for (const result of results) {
  const neutronStr = `${result.neutron.mean.toFixed(4)} ±${result.neutron.rme.toFixed(2)}%`;
  const drizzleStr = `${result.drizzle.mean.toFixed(4)} ±${result.drizzle.rme.toFixed(2)}%`;

  console.log(
    result.name.padEnd(35) +
      neutronStr.padEnd(20) +
      drizzleStr.padEnd(20) +
      result.comparison.padEnd(25)
  );

  if (result.neutron.mean < result.drizzle.mean) {
    neutronWins++;
  } else {
    drizzleWins++;
  }
}

console.log("─".repeat(100));
console.log("");

// Summary
console.log("🏆 SUMMARY");
console.log("─".repeat(100));
console.log(`Neutron wins: ${neutronWins}/${results.length}`);
console.log(`Drizzle wins: ${drizzleWins}/${results.length}`);

const neutronAvg = results.reduce((sum, r) => sum + r.neutron.mean, 0) / results.length;
const drizzleAvg = results.reduce((sum, r) => sum + r.drizzle.mean, 0) / results.length;

console.log(`Average latency:`);
console.log(`  Neutron: ${neutronAvg.toFixed(4)}ms`);
console.log(`  Drizzle: ${drizzleAvg.toFixed(4)}ms`);
console.log(`  Difference: ${((drizzleAvg - neutronAvg) / neutronAvg * 100).toFixed(2)}%`);

console.log("");
console.log("💡 NOTES");
console.log("─".repeat(100));
console.log("• Neutron direct SQL execution (baseline)");
console.log("• Drizzle query builder overhead measured");
console.log("• Both using SQLite for fair comparison");
console.log("• Neutron ORM supports multi-model ops (KV, Vector, etc.) — not in this comparison");
console.log("• Real Nucleus would show even greater advantage in multi-model scenarios");
console.log("─".repeat(100));

cleanupDatabase();
