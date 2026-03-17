/**
 * Drizzle ORM Benchmark
 * Tests Drizzle's query builder and execution performance
 */

import { Bench } from "tinybench";
import { drizzle } from "drizzle-orm/better-sqlite3";
import Database from "better-sqlite3";
import {
  initializeDatabase,
  seedDatabase,
  scenarios,
  cleanupDatabase,
} from "./shared-setup.ts";
import * as schema from "./drizzle-schema.ts";

const bench = new Bench({ name: "Drizzle ORM", time: 100 });

let db: Database.Database;
let drizzleDb: ReturnType<typeof drizzle>;

// ============================================================================
// Setup & Teardown
// ============================================================================

async function setup() {
  console.log("🔧 Setting up Drizzle benchmark...");
  db = initializeDatabase();
  drizzleDb = drizzle(db);
  seedDatabase(db, 100, 5);
  console.log("✓ Drizzle setup complete\n");
}

async function teardown() {
  db.close();
  cleanupDatabase();
}

// ============================================================================
// Benchmark Tests
// ============================================================================

await setup();

// Simple SELECT by ID
bench.add("Simple SELECT by ID", () => {
  drizzleDb
    .select()
    .from(schema.users)
    .where(schema.users.id.eq(1))
    .all();
});

// Find posts by user
bench.add("Find posts by user", () => {
  drizzleDb
    .select()
    .from(schema.posts)
    .where(schema.posts.userId.eq(1))
    .where(schema.posts.published.eq(true))
    .all();
});

// Posts with comments (JOIN)
bench.add("Posts with comments (JOIN)", () => {
  drizzleDb
    .select({
      id: schema.posts.id,
      title: schema.posts.title,
      author: schema.users.name,
      commentCount: db
        .raw(`COUNT(${schema.comments.id.getSQL()})`)
        .as("comment_count"),
    })
    .from(schema.posts)
    .innerJoin(schema.users, schema.users.id.eq(schema.posts.userId))
    .leftJoin(schema.comments, schema.posts.id.eq(schema.comments.postId))
    .where(schema.posts.userId.eq(1))
    .groupBy(schema.posts.id)
    .all();
});

// Complex multi-table JOIN
bench.add("Posts with tags and comments", () => {
  // Note: Drizzle doesn't make complex GROUP_CONCAT queries easy,
  // so we fall back to raw SQL for fairness
  db.prepare(`
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
  `).all();
});

// User statistics
bench.add("User statistics (GROUP BY)", () => {
  db.prepare(`
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
  `).all();
});

// INSERT user
bench.add("INSERT user", () => {
  drizzleDb
    .insert(schema.users)
    .values({
      email: `user-${Math.random()}@example.com`,
      name: "New User",
      age: 25,
    })
    .run();
});

// INSERT post
bench.add("INSERT post", () => {
  drizzleDb
    .insert(schema.posts)
    .values({
      userId: 1,
      title: "New Post",
      content: "Post content...",
      published: true,
    })
    .run();
});

// UPDATE user age
bench.add("UPDATE user age", () => {
  drizzleDb
    .update(schema.users)
    .set({ age: db.raw("age + 1") })
    .where(schema.users.id.eq(1))
    .run();
});

// DELETE post
bench.add("DELETE post", () => {
  const postToDelete = drizzleDb
    .select()
    .from(schema.posts)
    .limit(1)
    .all()[0];

  if (postToDelete) {
    drizzleDb
      .delete(schema.posts)
      .where(schema.posts.id.eq(postToDelete.id))
      .run();
  }
});

// ============================================================================
// Run Benchmark
// ============================================================================

console.log("⏱️  Running Drizzle benchmarks...\n");

await bench.run();

console.log("\n📊 Drizzle Results:");
console.log("═".repeat(60));

for (const task of bench.tasks) {
  const formatted = `${task.name.padEnd(30)} ${`${(task.result?.mean ?? 0).toFixed(4)}ms`.padStart(12)} (${(task.result?.rme ?? 0).toFixed(2)}%)`;
  console.log(formatted);
}

await teardown();
