/**
 * Shared database setup and schema for ORM benchmarks.
 * All three ORMs (Neutron, Drizzle, Prisma) use the same SQLite database.
 */

import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, "benchmark.db");

// ============================================================================
// Schema
// ============================================================================

export interface User {
  id: number;
  email: string;
  name: string;
  age: number;
  created_at: string;
}

export interface Post {
  id: number;
  user_id: number;
  title: string;
  content: string;
  published: boolean;
  created_at: string;
}

export interface Comment {
  id: number;
  post_id: number;
  user_id: number;
  body: string;
  created_at: string;
}

export interface Tag {
  id: number;
  name: string;
}

export interface PostTag {
  post_id: number;
  tag_id: number;
}

// ============================================================================
// Database Setup
// ============================================================================

export function initializeDatabase(): Database.Database {
  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");

  // Drop existing tables (clean slate for benchmarks)
  db.exec(`
    DROP TABLE IF EXISTS post_tags;
    DROP TABLE IF EXISTS comments;
    DROP TABLE IF EXISTS posts;
    DROP TABLE IF EXISTS tags;
    DROP TABLE IF EXISTS users;
  `);

  // Create schema
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      email TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      age INTEGER NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      published BOOLEAN NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE comments (
      id INTEGER PRIMARY KEY,
      post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE tags (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE post_tags (
      post_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL,
      PRIMARY KEY (post_id, tag_id),
      FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
      FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );

    CREATE INDEX idx_posts_user_id ON posts(user_id);
    CREATE INDEX idx_comments_post_id ON comments(post_id);
    CREATE INDEX idx_comments_user_id ON comments(user_id);
    CREATE INDEX idx_post_tags_tag_id ON post_tags(tag_id);
  `);

  return db;
}

// ============================================================================
// Seed Data
// ============================================================================

export function seedDatabase(db: Database.Database, userCount = 100, postsPerUser = 5) {
  // Seed users
  const insertUser = db.prepare(`
    INSERT INTO users (email, name, age)
    VALUES (?, ?, ?)
  `);

  for (let i = 1; i <= userCount; i++) {
    insertUser.run(`user${i}@example.com`, `User ${i}`, 20 + (i % 60));
  }

  // Seed posts
  const insertPost = db.prepare(`
    INSERT INTO posts (user_id, title, content, published)
    VALUES (?, ?, ?, ?)
  `);

  for (let userId = 1; userId <= userCount; userId++) {
    for (let p = 1; p <= postsPerUser; p++) {
      insertPost.run(
        userId,
        `Post ${p} by User ${userId}`,
        `This is the content of post ${p}...`,
        p % 2 === 0 ? 1 : 0
      );
    }
  }

  // Seed tags
  const tags = ["rust", "typescript", "database", "performance", "web"];
  const insertTag = db.prepare("INSERT INTO tags (name) VALUES (?)");

  for (const tag of tags) {
    insertTag.run(tag);
  }

  // Seed post-tag relationships
  const insertPostTag = db.prepare("INSERT INTO post_tags (post_id, tag_id) VALUES (?, ?)");
  const allPostIds = db.prepare("SELECT id FROM posts").all() as Array<{ id: number }>;

  for (let i = 0; i < allPostIds.length; i++) {
    const postId = allPostIds[i].id;
    const tagId = (i % tags.length) + 1;
    insertPostTag.run(postId, tagId);
  }

  // Seed comments
  const insertComment = db.prepare(`
    INSERT INTO comments (post_id, user_id, body)
    VALUES (?, ?, ?)
  `);

  const commentsPerPost = 3;
  for (let postId = 1; postId <= userCount * postsPerUser; postId++) {
    for (let c = 1; c <= commentsPerPost; c++) {
      const userId = Math.floor(Math.random() * userCount) + 1;
      insertComment.run(postId, userId, `Comment ${c} on post ${postId}`);
    }
  }
}

// ============================================================================
// Test Scenarios (Queries & Operations)
// ============================================================================

export const scenarios = {
  /**
   * Simple lookup: Find user by ID
   * Expected: Very fast, minimal overhead
   */
  simple_select_by_id: {
    name: "Simple SELECT by ID",
    description: "Find a user by ID",
    sql: "SELECT * FROM users WHERE id = ?",
    params: [1],
    iterations: 10000,
  },

  /**
   * Simple filter: Find all posts by a user
   * Expected: Indexed query, fast
   */
  find_posts_by_user: {
    name: "Find posts by user",
    description: "SELECT * FROM posts WHERE user_id = ? AND published = ?",
    sql: "SELECT * FROM posts WHERE user_id = ? AND published = ?",
    params: [1, 1],
    iterations: 5000,
  },

  /**
   * JOIN: Find posts with comments and author
   * Expected: More complex, shows JOIN performance
   */
  posts_with_comments: {
    name: "Posts with comments (JOIN)",
    description: "Select posts with comment count and author name",
    sql: `
      SELECT p.id, p.title, u.name as author, COUNT(c.id) as comment_count
      FROM posts p
      JOIN users u ON p.user_id = u.id
      LEFT JOIN comments c ON p.id = c.post_id
      WHERE p.user_id = ?
      GROUP BY p.id
    `,
    params: [1],
    iterations: 1000,
  },

  /**
   * Complex JOIN: Posts with tags and comments
   * Expected: More complex query, shows multi-table JOIN performance
   */
  posts_with_tags_and_comments: {
    name: "Posts with tags and comments",
    description: "Complex multi-table JOIN",
    sql: `
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
    `,
    params: [],
    iterations: 500,
  },

  /**
   * Aggregation: User statistics
   * Expected: GROUP BY performance
   */
  user_stats: {
    name: "User statistics (GROUP BY)",
    description: "Count posts and comments per user",
    sql: `
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
    `,
    params: [],
    iterations: 500,
  },

  /**
   * INSERT: Single user
   * Expected: Shows write overhead
   */
  insert_user: {
    name: "INSERT user",
    description: "Insert a new user",
    sql: "INSERT INTO users (email, name, age) VALUES (?, ?, ?)",
    params: [`user-${Math.random()}@example.com`, "New User", 25],
    iterations: 1000,
    is_write: true,
  },

  /**
   * INSERT with RETURNING: Single post
   * Expected: Shows insert + return overhead
   */
  insert_post: {
    name: "INSERT post",
    description: "Insert a new post and get the ID",
    sql: "INSERT INTO posts (user_id, title, content, published) VALUES (?, ?, ?, ?)",
    params: [1, "New Post", "Post content...", 1],
    iterations: 1000,
    is_write: true,
  },

  /**
   * UPDATE: Update user age
   * Expected: Shows update overhead
   */
  update_user_age: {
    name: "UPDATE user age",
    description: "Update a user's age",
    sql: "UPDATE users SET age = age + 1 WHERE id = ?",
    params: [1],
    iterations: 1000,
    is_write: true,
  },

  /**
   * DELETE: Remove a post
   * Expected: Shows delete overhead and cascade
   */
  delete_post: {
    name: "DELETE post",
    description: "Delete a post (cascades to comments)",
    sql: "DELETE FROM posts WHERE id = ?",
    params: [1],
    iterations: 100,
    is_write: true,
  },
};

export function getDatabase(): Database.Database {
  return new Database(DB_PATH);
}

export function cleanupDatabase() {
  try {
    const db = new Database(DB_PATH);
    db.close();
  } catch {
    // Ignore
  }
}
