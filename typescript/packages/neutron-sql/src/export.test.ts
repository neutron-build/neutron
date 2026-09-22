import assert from "node:assert/strict";
import test from "node:test";
import { exportSchema, pgTable, serial, integer, text, varchar, boolean, timestamp, index, relations, vector } from "./index.js";

const users = pgTable(
  "users",
  {
    id: serial("id").primaryKey(),
    email: varchar("email", 255).notNull().unique(),
    active: boolean("active").notNull().default(true),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [index("users_email_idx").on(t.email)],
);

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
});

test("exportSchema: deterministic, sorted, complete", () => {
  const exported = exportSchema({ posts, users });
  assert.equal(exported.version, 1);
  assert.deepEqual(exported.tables.map((t) => t.name), ["posts", "users"]);

  const usersTable = exported.tables[1];
  assert.deepEqual(
    usersTable.columns.map((c) => c.name),
    ["id", "email", "active", "created_at"],
  );
  assert.equal(usersTable.columns[1].varcharLength, 255);
  assert.equal(usersTable.columns[1].notNull, true);
  assert.equal(usersTable.columns[1].unique, true);
  assert.equal(usersTable.columns[2].default, "true");
  assert.equal(usersTable.columns[3].defaultNow, true);
  assert.deepEqual(usersTable.indexes, [{ name: "users_email_idx", unique: false, columns: ["email"] }]);

  const postsTable = exported.tables[0];
  assert.deepEqual(postsTable.columns[1].foreignKey, { table: "users", column: "id", onDelete: "cascade" });
});

test("exportSchema: JSON round-trip is stable", () => {
  const a = JSON.stringify(exportSchema({ posts, users }));
  const b = JSON.stringify(exportSchema({ users, posts }));
  assert.equal(a, b);
  const parsed = JSON.parse(a);
  assert.equal(parsed.tables.length, 2);
});

test("exportSchema: relations entries are ignored (tables only)", () => {
  const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
  const exported = exportSchema({ users, posts, usersRelations } as never);
  assert.equal(exported.tables.length, 2);
});

test("exportSchema: vector columns marked nucleusOnly with dimensions", () => {
  const docs = pgTable("docs", {
    id: serial("id").primaryKey(),
    embedding: vector("embedding", 1536),
  });
  const exported = exportSchema({ docs });
  const emb = exported.tables[0].columns[1];
  assert.equal(emb.type, "vector");
  assert.equal(emb.nucleusOnly, true);
  assert.equal(emb.vectorDimensions, 1536);
});
