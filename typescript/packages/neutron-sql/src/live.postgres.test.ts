import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  desc,
  pgTable,
  serial,
  integer,
  text,
  boolean,
  timestamp,
  varchar,
  relations,
  schemaToDDL,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

const DB_NAME = uniqueDbName("neutron_sql_test");

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: varchar("email", 255).notNull().unique(),
  name: text("name"),
  active: boolean("active").notNull().default(true),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
  published: boolean("published").notNull().default(false),
});

const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

type TestDb = NeutronDatabase<{ users: typeof users; posts: typeof posts }, { users: typeof usersRelations; posts: typeof postsRelations }>;

interface SuiteFixture {
  db: TestDb;
  cleanup: () => Promise<void>;
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live (${driverKind})`))) {
    return;
  }
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { users, posts },
    relations: { users: usersRelations, posts: postsRelations },
  });
  for (const stmt of schemaToDDL([users, posts])) {
    await db.driver.execute(stmt);
  }
  try {
    await fn({ db, cleanup: async () => {} });
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live (${driverKind}): CRUD round-trip + typed relation read`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      // insert (returning gives rows)
      const inserted = await db.insert(users).values({ email: "a@x.com", name: "Alice" }).returning();
      assert.equal(inserted.length, 1);
      assert.equal(inserted[0].email, "a@x.com");
      assert.ok(typeof inserted[0].id === "number");
      assert.equal(inserted[0].active, true); // column default applied
      const aliceId = inserted[0].id!;

      const bobby = await db.insert(users).values({ email: "b@x.com", name: "Bob" }).returning();
      const bobId = bobby[0].id!;

      await db.insert(posts).values([
        { userId: aliceId, title: "A1" },
        { userId: aliceId, title: "A2" },
        { userId: bobId, title: "B1" },
      ]);

      // plain select
      const all = await db.select().from(users);
      assert.equal(all.length, 2);

      // relational read: one statement, nested arrays
      const alice = await db.query.users.findFirst({ where: eq(users.email, "a@x.com"), with: { posts: true } });
      assert.ok(alice);
      assert.equal(alice.name, "Alice");
      assert.ok(Array.isArray(alice.posts));
      assert.equal(alice.posts.length, 2);
      assert.deepEqual(
        alice.posts.map((p: { title: string }) => p.title).sort(),
        ["A1", "A2"],
      );

      // empty relation -> [] not [null]
      const lonely = await db.query.users.findFirst({
        where: eq(users.email, "nope@x.com"),
        with: { posts: true },
      });
      assert.equal(lonely, undefined);

      // one() side: posts with author, null FK impossible here (not null) but shape check
      const b1 = await db.query.posts.findFirst({ where: eq(posts.title, "B1"), with: { author: true } });
      assert.ok(b1);
      assert.ok(b1.author);
      assert.equal(b1.author!.email, "b@x.com");

      // findMany + orderBy + where
      const withPosts = await db.query.users.findMany({ with: { posts: true }, orderBy: [desc(users.id)] });
      assert.equal(withPosts.length, 2);
      assert.equal(withPosts[0].name, "Bob");

      // update ... returning
      const updated = await db.update(users).set({ name: "Alice 2" }).where(eq(users.id, aliceId)).returning();
      assert.equal(updated[0].name, "Alice 2");

      // plain update -> count
      const count = await db.update(posts).set({ published: true }).where(eq(posts.userId, aliceId));
      assert.equal(count, 2);

      // transaction: rollback discards
      await assert.rejects(
        () =>
          db.transaction(async (tx) => {
            await tx.insert(users).values({ email: "tx@x.com", name: "Tx" });
            throw new Error("boom");
          }),
        /boom/,
      );
      const txUser = await db.select().from(users).where(eq(users.email, "tx@x.com"));
      assert.equal(txUser.length, 0);

      // transaction: commit keeps
      await db.transaction(async (tx) => {
        await tx.insert(users).values({ email: "tx2@x.com", name: "Tx2" });
      });
      const tx2 = await db.select().from(users).where(eq(users.email, "tx2@x.com"));
      assert.equal(tx2.length, 1);

      // delete with cascade
      const deleted = await db.delete(users).where(eq(users.id, aliceId));
      assert.equal(deleted, 1);
      const remainingPosts = await db.select().from(posts);
      assert.equal(remainingPosts.length, 1); // B1 survives; A1/A2 cascaded

      // toSQL on live db object without executing
      const probeQuery = db.select().from(users);
      assert.ok(probeQuery.toSQL().sql.startsWith("select"));
    });
  });

  test(`live (${driverKind}): logger observes executed SQL`, async () => {
    await withSuite(driverKind, async () => {
      const url = new URL(TEST_URL);
      url.pathname = `/${DB_NAME}`;
      const events: Array<{ sql: string }> = [];
      const db = await createDatabase({
        url: url.toString(),
        driverOptions: { driver: driverKind },
        tables: { users, posts },
        relations: { users: usersRelations, posts: postsRelations },
        logger: (e) => events.push({ sql: e.sql }),
      });
      try {
        await db.select().from(users);
        assert.ok(events.length >= 1);
        assert.ok(events[0].sql.startsWith("select"));
      } finally {
        await db.close();
      }
    });
  });
}
