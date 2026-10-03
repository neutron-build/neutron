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

// SQL_RELATION_TARGET_GUARD regression: the Gap 1.20 registration guards check
// each relation set's OWN table only, so a relation whose TARGET table is
// schema-qualified (declared via pgSchema) passed both guards and compiled an
// unqualified `from` against the search-path twin — silently reading the wrong
// table. Registration must fail closed (same refusal family as the OWN-table
// guards in db.ts) before any statement reaches the driver. The fake driver is
// the db-scope.test.ts recording pattern: it proves zero statements, no
// database behavior.

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

const alt = pgSchema("altrx");
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

test("db.query setup refuses a one() relation targeting a schema-qualified table before any driver statement", async () => {
  const postsWithAltAuthor = relations(posts, ({ one }) => ({
    author: one(altUsers, { fields: [posts.authorId], references: [altUsers.id] }),
  }));
  const driver = recordingDriver();
  await assert.rejects(
    createDatabase({ driver, tables: { posts }, relations: { posts: postsWithAltAuthor } }),
    /relations\.posts\.author: target "altrx"\."users" declares a schema — relational reads on schema-qualified tables/,
  );
  assert.equal(driver.statements.length, 0);
});

test("db.query setup refuses a many() relation targeting a schema-qualified table before any driver statement", async () => {
  // Adversarial shape: a plain table with the same bare name carries the
  // target's relation set, so at base resolution succeeds by bare name and
  // the compiled read targets the plain twin instead of "altrx"."posts".
  const postsRelations = relations(posts, ({ one }) => ({
    author: one(users, { fields: [posts.authorId], references: [users.id] }),
  }));
  const usersWithAltPosts = relations(users, ({ many }) => ({
    posts: many(altPosts),
  }));
  const driver = recordingDriver();
  await assert.rejects(
    createDatabase({
      driver,
      tables: { users, posts },
      relations: { users: usersWithAltPosts, posts: postsRelations },
    }),
    /relations\.users\.posts: target "altrx"\."posts" declares a schema — relational reads on schema-qualified tables/,
  );
  assert.equal(driver.statements.length, 0);
});

test("db.query setup control: unqualified relation targets still register and compile", async () => {
  // Positive control proving the refusals above are the schema boundary, not
  // the setup path generally: plain one() and many() edges register fine,
  // db.query compiles both (toSQL never touches the driver), and still no
  // statement has been issued.
  const postsRelations = relations(posts, ({ one }) => ({
    author: one(users, { fields: [posts.authorId], references: [users.id] }),
  }));
  const usersRelations = relations(users, ({ many }) => ({
    posts: many(posts),
  }));
  const driver = recordingDriver();
  const db = await createDatabase({
    driver,
    tables: { users, posts },
    relations: { users: usersRelations, posts: postsRelations },
  });
  const withAuthor = db.query.posts.toSQL({ with: { author: true } });
  assert.ok(withAuthor.sql.includes('as "__rel_author"'), withAuthor.sql);
  const withPosts = db.query.users.toSQL({ with: { posts: true } });
  assert.ok(withPosts.sql.includes('as "__rel_posts"'), withPosts.sql);
  assert.equal(driver.statements.length, 0);
  await db.close();
});
