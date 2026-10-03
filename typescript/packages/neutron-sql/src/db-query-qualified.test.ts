import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  integer,
  pgSchema,
  pgTable,
  relations,
  serial,
  text,
  type Driver,
  type PinnedExecutor,
} from "./index.js";

// Gap 1.20 regression: relational reads (db.query) refuse schema-qualified
// tables BY DESIGN — CRUD select/insert/update/delete and alias joins render
// qualified references, nested reads do not (documented on pgSchema and the
// TableMetadata schema field). This pins the two documented runtime refusal
// paths (db.ts tables/relations registration) through the public setup API
// so they cannot silently break: registering a pgSchema-declared table, or a
// relation set owned by one, must fail closed inside createDatabase BEFORE
// any statement reaches the driver — zero queries, zero mutation. The fake
// driver below is the db-scope.test.ts recording pattern: it only proves no
// statement was issued; no database behavior is claimed here.

function recordingDriver(): Driver & { statements: string[] } {
  const statements: string[] = [];
  const record = (sqlText: string): void => {
    statements.push(sqlText);
  };
  const pin: PinnedExecutor = {
    async query<T>(sqlText: string): Promise<T[]> {
      record(sqlText);
      return [] as T[];
    },
    async execute(sqlText: string): Promise<number> {
      record(sqlText);
      return 0;
    },
    release(): void {},
  };
  const driver: Driver = {
    async query<T>(sqlText: string): Promise<T[]> {
      record(sqlText);
      return [] as T[];
    },
    async execute(sqlText: string): Promise<number> {
      record(sqlText);
      return 0;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return fn(driver);
    },
    async close(): Promise<void> {},
    lifecycle: { ownership: "borrowed", terminated: false, terminate: () => Promise.resolve() },
    pin: () => Promise.resolve(pin),
  };
  return Object.assign(driver, { statements });
}

const alt = pgSchema("alt");
const altUsers = alt.table("users", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
});
const altPosts = alt.table("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
});
const users = pgTable("users", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
});
const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  title: text("title").notNull(),
});
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id] }),
}));
const altPostsRelations = relations(altPosts, ({ one }) => ({
  author: one(users, { fields: [altPosts.authorId], references: [users.id] }),
}));

test("db.query setup refuses a schema-qualified table before any driver statement", async () => {
  const driver = recordingDriver();
  await assert.rejects(
    createDatabase({ driver, tables: { altUsers } }),
    /tables\.altUsers: "alt"\."users" declares a schema — relational reads \(db\.query\) on schema-qualified tables/,
  );
  assert.equal(driver.statements.length, 0);
});

test("db.query setup refuses a relation set owned by a schema-qualified table before any driver statement", async () => {
  const driver = recordingDriver();
  await assert.rejects(
    createDatabase({ driver, tables: { users }, relations: { altPosts: altPostsRelations } }),
    /relations\.altPosts: "alt"\."posts" declares a schema — relational reads on schema-qualified tables/,
  );
  assert.equal(driver.statements.length, 0);
});

test("db.query setup control: the same driver accepts plain tables and compiles nested reads", async () => {
  // Positive control proving the refusals above are the schema boundary,
  // not the setup path generally: plain tables + a plain relation register
  // fine, db.query compiles (toSQL never touches the driver), and still no
  // statement has been issued.
  const driver = recordingDriver();
  const db = await createDatabase({ driver, tables: { users, posts }, relations: { posts: postsRelations } });
  const compiled = db.query.posts.toSQL({ with: { author: true } });
  assert.ok(compiled.sql.includes('as "__rel_author"'), compiled.sql);
  assert.equal(driver.statements.length, 0);
  await db.close();
});
