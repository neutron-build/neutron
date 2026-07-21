// Postgres-dialect port of typescript/benchmarks/orms/drizzle-schema.ts
// (that file targets sqlite-core; this harness speaks pgwire).
import {
  pgTable,
  text,
  integer,
  boolean,
  timestamp,
  primaryKey,
} from "drizzle-orm/pg-core";

export const users = pgTable("users", {
  id: integer("id").primaryKey(),
  email: text("email").notNull().unique(),
  name: text("name").notNull(),
  age: integer("age").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export const posts = pgTable("posts", {
  id: integer("id").primaryKey(),
  userId: integer("user_id")
    .notNull()
    .references(() => users.id),
  title: text("title").notNull(),
  content: text("content").notNull(),
  published: boolean("published").default(false),
  createdAt: timestamp("created_at").defaultNow(),
});

export const tags = pgTable("tags", {
  id: integer("id").primaryKey(),
  name: text("name").notNull().unique(),
});

export const postTags = pgTable(
  "post_tags",
  {
    postId: integer("post_id")
      .notNull()
      .references(() => posts.id),
    tagId: integer("tag_id")
      .notNull()
      .references(() => tags.id),
  },
  (t) => [primaryKey({ columns: [t.postId, t.tagId] })]
);
