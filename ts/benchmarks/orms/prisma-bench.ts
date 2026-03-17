/**
 * Prisma ORM Benchmark
 * Tests Prisma's query execution performance
 */

import { Bench } from "tinybench";
import { PrismaClient } from "@prisma/client";
import Database from "better-sqlite3";
import {
  initializeDatabase,
  seedDatabase,
  cleanupDatabase,
} from "./shared-setup.ts";

const bench = new Bench({ name: "Prisma ORM", time: 100 });

let db: Database.Database;
let prisma: PrismaClient;

// ============================================================================
// Setup & Teardown
// ============================================================================

async function setup() {
  console.log("🔧 Setting up Prisma benchmark...");
  db = initializeDatabase();
  seedDatabase(db, 100, 5);
  db.close();

  // Initialize Prisma
  prisma = new PrismaClient({
    datasources: {
      db: {
        url: "file:./benchmark.db",
      },
    },
  });

  console.log("✓ Prisma setup complete\n");
}

async function teardown() {
  await prisma.$disconnect();
  cleanupDatabase();
}

// ============================================================================
// Benchmark Tests
// ============================================================================

await setup();

// Simple SELECT by ID
bench.add("Simple SELECT by ID", async () => {
  await prisma.user.findUnique({
    where: { id: 1 },
  });
});

// Find posts by user
bench.add("Find posts by user", async () => {
  await prisma.post.findMany({
    where: {
      userId: 1,
      published: true,
    },
  });
});

// Posts with comments (JOIN)
bench.add("Posts with comments (JOIN)", async () => {
  await prisma.post.findMany({
    where: { userId: 1 },
    include: {
      user: true,
      comments: true,
    },
  });
});

// Complex multi-table JOIN (via raw query, as Prisma's query builder struggles)
bench.add("Posts with tags and comments", async () => {
  await prisma.$queryRaw`
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
});

// User statistics (via raw query)
bench.add("User statistics (GROUP BY)", async () => {
  await prisma.$queryRaw`
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
});

// INSERT user
bench.add("INSERT user", async () => {
  await prisma.user.create({
    data: {
      email: `user-${Math.random()}@example.com`,
      name: "New User",
      age: 25,
    },
  });
});

// INSERT post
bench.add("INSERT post", async () => {
  await prisma.post.create({
    data: {
      userId: 1,
      title: "New Post",
      content: "Post content...",
      published: true,
    },
  });
});

// UPDATE user age
bench.add("UPDATE user age", async () => {
  await prisma.user.update({
    where: { id: 1 },
    data: {
      age: {
        increment: 1,
      },
    },
  });
});

// DELETE post
bench.add("DELETE post", async () => {
  const postToDelete = await prisma.post.findFirst();
  if (postToDelete) {
    await prisma.post.delete({
      where: { id: postToDelete.id },
    });
  }
});

// ============================================================================
// Run Benchmark
// ============================================================================

console.log("⏱️  Running Prisma benchmarks...\n");

await bench.run();

console.log("\n📊 Prisma Results:");
console.log("═".repeat(60));

for (const task of bench.tasks) {
  const formatted = `${task.name.padEnd(30)} ${`${(task.result?.mean ?? 0).toFixed(4)}ms`.padStart(12)} (${(task.result?.rme ?? 0).toFixed(2)}%)`;
  console.log(formatted);
}

await teardown();
