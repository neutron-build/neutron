import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  asc,
  pgTable,
  serial,
  integer,
  text,
  relations,
  schemaToDDL,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// B05 live suite (V03 one-level subset + runtime rejections). Connection
// selection and skip/fail semantics come from ./live-harness.ts; a unique
// disposable database per run is created and dropped; the shared dev
// instance is never a target.

const DB_NAME = uniqueDbName("neutron_orm_b05");

// Shared fixture: users (self manager FK, two inbound FKs from posts),
// posts (author + reviewer both -> users, named pair), comments (one FK),
// invoices/invoice_lines (composite tenant-local identity).
const users = pgTable("users", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  managerId: integer("manager_id"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull().references(() => users.id),
  reviewerId: integer("reviewer_id").references(() => users.id),
  title: text("title").notNull(),
});

const comments = pgTable("comments", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id),
  body: text("body").notNull(),
});

const invoices = pgTable("invoices", {
  tenantId: integer("tenant_id").notNull(),
  id: integer("id").notNull(),
  amount: integer("amount").notNull(),
});

const invoiceLines = pgTable("invoice_lines", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").notNull(),
  invoiceId: integer("invoice_id").notNull(),
  memo: text("memo").notNull(),
});

const usersRelations = relations(users, ({ one, many }) => ({
  posts: many(posts, { relationName: "author" }),
  reviewedPosts: many(posts, { relationName: "reviewer" }),
  comments: many(comments),
  manager: one(users, { fields: [users.managerId], references: [users.id] }),
}));

const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id], relationName: "author" }),
  reviewer: one(users, { fields: [posts.reviewerId], references: [users.id], relationName: "reviewer" }),
}));

const commentsRelations = relations(comments, ({ one }) => ({
  owner: one(users, { fields: [comments.userId], references: [users.id] }),
}));

const invoicesRelations = relations(invoices, ({ many }) => ({
  lines: many(invoiceLines),
}));

const invoiceLinesRelations = relations(invoiceLines, ({ one }) => ({
  invoice: one(invoices, {
    fields: [invoiceLines.tenantId, invoiceLines.invoiceId],
    references: [invoices.tenantId, invoices.id],
  }),
}));

type Tables = {
  users: typeof users;
  posts: typeof posts;
  comments: typeof comments;
  invoices: typeof invoices;
  invoiceLines: typeof invoiceLines;
};
type RelationsMap = {
  users: typeof usersRelations;
  posts: typeof postsRelations;
  comments: typeof commentsRelations;
  invoices: typeof invoicesRelations;
  invoiceLines: typeof invoiceLinesRelations;
};
type TestDb = NeutronDatabase<Tables, RelationsMap>;

async function withSuite(
  driverKind: "postgres" | "pg",
  fn: (db: TestDb) => Promise<void>,
): Promise<void> {
  if (!(await ensureLive(`live relations (${driverKind})`))) {
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
    tables: { users, posts, comments, invoices, invoiceLines },
    relations: {
      users: usersRelations,
      posts: postsRelations,
      comments: commentsRelations,
      invoices: invoicesRelations,
      invoiceLines: invoiceLinesRelations,
    },
  });
  for (const stmt of schemaToDDL([users, posts, comments, invoices, invoiceLines])) {
    await db.driver.execute(stmt);
  }
  try {
    await fn(db);
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

interface Seed {
  aliceId: number;
  bobId: number;
  carolId: number;
  daveId: number;
  p1Id: number;
  p2Id: number;
}

async function seed(db: TestDb): Promise<Seed> {
  const alice = (await db.insert(users).values({ name: "Alice" }).returning())[0];
  const bob = (await db.insert(users).values({ name: "Bob" }).returning())[0];
  const carol = (await db.insert(users).values({ name: "Carol", managerId: alice.id }).returning())[0];
  const dave = (await db.insert(users).values({ name: "Dave", managerId: bob.id }).returning())[0];

  const insertedPosts = await db
    .insert(posts)
    .values([
      { authorId: alice.id, reviewerId: bob.id, title: "P1" },
      { authorId: alice.id, title: "P2" },
    ])
    .returning();

  await db.insert(comments).values([
    { userId: alice.id, body: "C1" },
    { userId: alice.id, body: "C2" },
    { userId: alice.id, body: "C3" },
    { userId: bob.id, body: "dup" },
    { userId: bob.id, body: "dup" },
  ]);

  await db.insert(invoices).values([
    { tenantId: 1, id: 100, amount: 10 },
    { tenantId: 2, id: 100, amount: 20 },
  ]);
  await db.insert(invoiceLines).values([
    { tenantId: 1, invoiceId: 100, memo: "L1" },
    { tenantId: 1, invoiceId: 100, memo: "L2" },
    { tenantId: 2, invoiceId: 100, memo: "L3" },
  ]);

  return { aliceId: alice.id, bobId: bob.id, carolId: carol.id, daveId: dave.id, p1Id: insertedPosts[0].id, p2Id: insertedPosts[1].id };
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live relations (${driverKind}): two to-many children aggregate independently (V03 core)`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const alice = await db.query.users.findFirst({
        where: eq(users.id, ids.aliceId),
        with: { posts: true, comments: true },
      });
      assert.ok(alice);
      // Exact types: posts/comments are required keys of the row.
      assert.equal(alice.posts.length, 2);
      assert.equal(alice.comments.length, 3);
      assert.deepEqual(
        alice.posts.map((p) => ({ id: p.id, title: p.title })),
        [
          { id: ids.p1Id, title: "P1" },
          { id: ids.p2Id, title: "P2" },
        ],
      );
      assert.deepEqual(
        alice.comments.map((c) => c.body),
        ["C1", "C2", "C3"],
      );

      // Independent oracle: hand-written correlated SQL with explicit ORDER BY.
      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', p.id, 'title', p.title) order by p.id), '[]'::jsonb)
                   from posts p where p.author_id = u.id) as posts,
                (select coalesce(jsonb_agg(jsonb_build_object('id', c.id, 'body', c.body) order by c.id), '[]'::jsonb)
                   from comments c where c.user_id = u.id) as comments
         from users u where u.id = $1`,
        [ids.aliceId],
      )) as Array<{ id: number; posts: Array<{ id: number; title: string }>; comments: Array<{ id: number; body: string }> }>;
      assert.equal(oracle.length, 1);
      assert.deepEqual(
        alice.posts.map((p) => ({ id: p.id, title: p.title })),
        oracle[0].posts,
      );
      assert.deepEqual(
        alice.comments.map((c) => ({ id: c.id, body: c.body })),
        oracle[0].comments,
      );

      // Parent limit applies to parent rows, never to children.
      const limited = await db.query.users.findMany({ with: { posts: true }, orderBy: [asc(users.id)], limit: 2 });
      assert.equal(limited.length, 2);
      assert.equal(limited[0].posts.length, 2);
    });
  });

  test(`live relations (${driverKind}): no-child user -> [] for to-many, null for missing to-one`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const carol = await db.query.users.findFirst({
        where: eq(users.id, ids.carolId),
        with: { posts: true, comments: true },
      });
      assert.ok(carol);
      assert.deepEqual(carol.posts, []);
      assert.deepEqual(carol.comments, []);

      // Nullable to-one (reviewer) missing -> null, not undefined/[]. P2 has no reviewer.
      const p2 = await db.query.posts.findFirst({ where: eq(posts.id, ids.p2Id), with: { reviewer: true } });
      assert.ok(p2);
      assert.equal(p2.reviewer, null);

      // Self-relation to-one missing -> null (Alice has no manager).
      const alice = await db.query.users.findFirst({ where: eq(users.id, ids.aliceId), with: { manager: true } });
      assert.ok(alice);
      assert.equal(alice.manager, null);
    });
  });

  test(`live relations (${driverKind}): two FKs to one target resolve via named relations`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const p1 = await db.query.posts.findFirst({ where: eq(posts.id, ids.p1Id), with: { author: true } });
      assert.ok(p1);
      assert.equal(p1.author?.name, "Alice");

      const p1r = await db.query.posts.findFirst({ where: eq(posts.id, ids.p1Id), with: { reviewer: true } });
      assert.ok(p1r);
      assert.equal(p1r.reviewer?.name, "Bob");

      // Named inverse: Alice authored both posts; Bob reviewed only P1.
      const alice = await db.query.users.findFirst({ where: eq(users.id, ids.aliceId), with: { posts: true } });
      assert.ok(alice);
      assert.deepEqual(alice.posts.map((p) => p.title).sort(), ["P1", "P2"]);
      const bob = await db.query.users.findFirst({ where: eq(users.id, ids.bobId), with: { reviewedPosts: true } });
      assert.ok(bob);
      assert.deepEqual(
        bob.reviewedPosts.map((p) => p.title),
        ["P1"],
      );
    });
  });

  test(`live relations (${driverKind}): self-relation (manager) works at one level`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const carol = await db.query.users.findFirst({ where: eq(users.id, ids.carolId), with: { manager: true } });
      assert.ok(carol);
      assert.ok(carol.manager);
      assert.equal(carol.manager.name, "Alice");
      assert.equal(carol.manager.id, ids.aliceId);

      const dave = await db.query.users.findFirst({ where: eq(users.id, ids.daveId), with: { manager: true } });
      assert.ok(dave);
      assert.equal(dave.manager?.name, "Bob");
    });
  });

  test(`live relations (${driverKind}): composite tenant keys stay tenant-local`, async () => {
    await withSuite(driverKind, async (db) => {
      await seed(db);

      const tenantOne = await db.query.invoices.findMany({ where: eq(invoices.tenantId, 1), with: { lines: true } });
      assert.equal(tenantOne.length, 1);
      assert.equal(tenantOne[0].id, 100);
      assert.deepEqual(
        tenantOne[0].lines.map((l) => l.memo),
        ["L1", "L2"],
      );

      const oracle = (await db.driver.query(
        `select i.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', l.id, 'memo', l.memo) order by l.id), '[]'::jsonb)
                   from invoice_lines l where l.tenant_id = i.tenant_id and l.invoice_id = i.id) as lines
         from invoices i where i.tenant_id = $1 order by i.id`,
        [1],
      )) as Array<{ id: number; lines: Array<{ id: number; memo: string }> }>;
      assert.equal(oracle.length, 1);
      assert.deepEqual(
        tenantOne[0].lines.map((l) => ({ id: l.id, memo: l.memo })),
        oracle[0].lines,
      );

      // Reverse composite to-one: the tenant-2 line finds only its own invoice.
      const lineByMemo = await db.driver.query(
        "select id from invoice_lines where memo = $1 order by id",
        ["L3"],
      );
      const lineId = (lineByMemo as Array<{ id: number }>)[0].id;
      const line = await db.query.invoiceLines.findFirst({ where: eq(invoiceLines.id, lineId), with: { invoice: true } });
      assert.ok(line);
      assert.ok(line.invoice);
      assert.equal(line.invoice.tenantId, 2);
      assert.equal(line.invoice.amount, 20);
    });
  });

  test(`live relations (${driverKind}): duplicate-valued distinct children both survive`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const bob = await db.query.users.findFirst({ where: eq(users.id, ids.bobId), with: { comments: true } });
      assert.ok(bob);
      assert.equal(bob.comments.length, 2);
      const [first, second] = bob.comments;
      assert.ok(first && second && first.id !== second.id);
      assert.equal(first.body, "dup");
      assert.equal(second.body, "dup");

      const oracle = (await db.driver.query(
        "select count(*)::int as n from comments where user_id = $1",
        [ids.bobId],
      )) as Array<{ n: number }>;
      assert.equal(oracle[0].n, 2);
    });
  });

  test(`live relations (${driverKind}): unsupported shapes are rejected explicitly`, async () => {
    await withSuite(driverKind, async (db) => {
      await assert.rejects(
        () => db.query.users.findMany({ with: { bogus: true } as never }),
        /unknown relation "bogus" on users/,
      );
      await assert.rejects(
        () => db.query.users.findMany({ with: { posts: 42 as never } }),
        /relation "posts" in with on users: expected true or a per-relation options object, got a number/,
      );
      await assert.rejects(
        () => db.query.users.findMany({ with: { posts: { columns: ["bogus"] } as never } }),
        /unknown column "bogus" in args.columns on users\.posts/,
      );
      await assert.rejects(
        () => db.query.users.findMany({ columns: ["bogus"] as never }),
        /unknown column "bogus" in args.columns on users/,
      );
      await assert.rejects(
        () => db.query.users.findMany({ columns: [] }),
        /args.columns on users is empty/,
      );
      await assert.rejects(
        () => db.query.users.findMany({ columns: "id" as never }),
        /args.columns on users must be an array of property keys/,
      );
      // Two references to one target and nested with are SUPPORTED since Q05 —
      // covered positively in live.q05.postgres.test.ts.
      const bothRefs = await db.query.posts.findMany({ with: { author: true, reviewer: true } });
      assert.ok(Array.isArray(bothRefs));
      const nested = await db.query.users.findMany({ with: { posts: { with: { author: true } } }, limit: 1 });
      assert.ok(Array.isArray(nested));
    });
  });

  test(`live relations (${driverKind}): runtime rows carry exactly the typed keys`, async () => {
    await withSuite(driverKind, async (db) => {
      const ids = await seed(db);

      const alice = await db.query.users.findFirst({
        where: eq(users.id, ids.aliceId),
        with: { posts: true, comments: true },
      });
      assert.ok(alice);
      assert.deepEqual(Object.keys(alice).sort(), ["comments", "id", "managerId", "name", "posts"]);

      const selected = await db.query.users.findMany({ columns: ["id", "name"], with: { posts: true } });
      for (const row of selected) {
        assert.deepEqual(Object.keys(row).sort(), ["id", "name", "posts"]);
      }
    });
  });
}
