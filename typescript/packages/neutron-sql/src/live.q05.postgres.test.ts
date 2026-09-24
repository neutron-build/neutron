import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  gt,
  and,
  asc,
  desc,
  pgTable,
  serial,
  integer,
  text,
  bigint,
  numeric,
  timestamptz,
  timestamp,
  bytea,
  relations,
  wrapPgPool,
  wrapPostgresJs,
  CapabilityRequirementError,
  type Driver,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Q05 live battery (V03/V04/V08 at depth 3): nested relational reads with
// per-child filter/order/limit, two child collections, self relationships
// (incl. data cycles), two references to one target, named inverse pairs,
// composite tenant keys, lossless child values through the JSON projection,
// statement-count assertions (N+1 catcher), pure compile/explain inspection,
// and capability fail-before-running on an engine that reports jsonb
// functions unsupported. Every shape is checked against hand-written SQL
// oracles authored here (independent of the implementation).

const DB_NAME = uniqueDbName("q05_nested");

// --- fixture ----------------------------------------------------------------

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  managerId: integer("manager_id"),
  createdBy: integer("created_by"),
  updatedBy: integer("updated_by"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  reviewerId: integer("reviewer_id"),
  title: text("title").notNull(),
  views: integer("views").notNull().default(0),
});

const comments = pgTable("comments", {
  id: serial("id").primaryKey(),
  postId: integer("post_id").notNull(),
  commenterId: integer("commenter_id").notNull(),
  body: text("body").notNull(),
  score: integer("score").notNull().default(0),
});

const invoices = pgTable("q05_invoices", {
  tenantId: integer("tenant_id").notNull(),
  id: integer("id").notNull(),
  amount: numeric("amount").notNull(),
});

const invoiceLines = pgTable("q05_invoice_lines", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").notNull(),
  invoiceId: integer("invoice_id").notNull(),
  memo: text("memo").notNull(),
});

const wallets = pgTable("q05_wallets", {
  id: bigint("id").primaryKey(),
  name: text("name").notNull(),
});

const moves = pgTable("q05_moves", {
  id: serial("id").primaryKey(),
  walletId: bigint("wallet_id").notNull(),
  delta: bigint("delta"),
  amount: numeric("amount"),
  postedAt: timestamptz("posted_at"),
});

const audits = pgTable("q05_audits", {
  id: serial("id").primaryKey(),
  moveId: integer("move_id").notNull(),
  big: bigint("big"),
  amt: numeric("amt"),
  at: timestamp("at"),
  payload: bytea("payload"),
  note: text("note"),
});

const usersRelations = relations(users, ({ one, many }) => ({
  posts: many(posts, { relationName: "author" }),
  reviewedPosts: many(posts, { relationName: "reviewer" }),
  comments: many(comments),
  manager: one(users, { fields: [users.managerId], references: [users.id], relationName: "reports" }),
  reports: many(users, { relationName: "reports" }),
  createdByUser: one(users, { fields: [users.createdBy], references: [users.id], relationName: "created" }),
  updatedByUser: one(users, { fields: [users.updatedBy], references: [users.id], relationName: "updated" }),
}));

const postsRelations = relations(posts, ({ one, many }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id], relationName: "author" }),
  reviewer: one(users, { fields: [posts.reviewerId], references: [users.id], relationName: "reviewer" }),
  comments: many(comments),
}));

const commentsRelations = relations(comments, ({ one }) => ({
  post: one(posts, { fields: [comments.postId], references: [posts.id] }),
  commenter: one(users, { fields: [comments.commenterId], references: [users.id] }),
}));

const invoicesRelations = relations(invoices, ({ many }) => ({ lines: many(invoiceLines) }));

// Deliberately REVERSED field/reference order: the pair must zip by POSITION.
const invoiceLinesRelations = relations(invoiceLines, ({ one }) => ({
  invoice: one(invoices, {
    fields: [invoiceLines.invoiceId, invoiceLines.tenantId],
    references: [invoices.id, invoices.tenantId],
  }),
}));

const walletsRelations = relations(wallets, ({ many }) => ({ moves: many(moves) }));
const movesRelations = relations(moves, ({ one, many }) => ({
  wallet: one(wallets, { fields: [moves.walletId], references: [wallets.id] }),
  audits: many(audits),
}));
const auditsRelations = relations(audits, ({ one }) => ({
  move: one(moves, { fields: [audits.moveId], references: [moves.id] }),
}));

type Tables = {
  users: typeof users;
  posts: typeof posts;
  comments: typeof comments;
  q05_invoices: typeof invoices;
  q05_invoice_lines: typeof invoiceLines;
  q05_wallets: typeof wallets;
  q05_moves: typeof moves;
  q05_audits: typeof audits;
};
type RelationsMap = {
  users: typeof usersRelations;
  posts: typeof postsRelations;
  comments: typeof commentsRelations;
  q05_invoices: typeof invoicesRelations;
  q05_invoice_lines: typeof invoiceLinesRelations;
  q05_wallets: typeof walletsRelations;
  q05_moves: typeof movesRelations;
  q05_audits: typeof auditsRelations;
};
type TestDb = NeutronDatabase<Tables, RelationsMap>;

interface SuiteFixture {
  db: TestDb;
  /** Statement counter fed by the database logger: the N+1 catcher. */
  statements: { count: number };
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live q05 (${driverKind})`))) {
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
  const statements = { count: 0 };
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { users, posts, comments, q05_invoices: invoices, q05_invoice_lines: invoiceLines, q05_wallets: wallets, q05_moves: moves, q05_audits: audits },
    relations: {
      users: usersRelations,
      posts: postsRelations,
      comments: commentsRelations,
      q05_invoices: invoicesRelations,
      q05_invoice_lines: invoiceLinesRelations,
      q05_wallets: walletsRelations,
      q05_moves: movesRelations,
      q05_audits: auditsRelations,
    } as RelationsMap,
    logger: (e) => {
      if (e.kind === "query-end") statements.count += 1;
    },
  });
  try {
    await db.driver.execute(
      `create table "users" ("id" serial primary key, "name" text not null, "manager_id" integer, "created_by" integer, "updated_by" integer)`,
    );
    await db.driver.execute(
      `create table "posts" ("id" serial primary key, "author_id" integer not null references "users"("id"), "reviewer_id" integer references "users"("id"), "title" text not null, "views" integer not null default 0)`,
    );
    await db.driver.execute(
      `create table "comments" ("id" serial primary key, "post_id" integer not null references "posts"("id"), "commenter_id" integer not null references "users"("id"), "body" text not null, "score" integer not null default 0)`,
    );
    // Composite PKs are not expressible in the current DDL surface — DDL is
    // hand-written (both columns marked primary in schema metadata so
    // relation ordering uses the ordered tuple).
    await db.driver.execute(
      `create table "q05_invoices" ("tenant_id" integer not null, "id" integer not null, "amount" numeric not null, primary key ("tenant_id", "id"))`,
    );
    await db.driver.execute(
      `create table "q05_invoice_lines" ("id" serial primary key, "tenant_id" integer not null, "invoice_id" integer not null)`,
    );
    await db.driver.execute(`alter table "q05_invoice_lines" add column "memo" text not null default 'm'`);
    await db.driver.execute(
      `create table "q05_wallets" ("id" bigint primary key, "name" text not null)`,
    );
    await db.driver.execute(
      `create table "q05_moves" ("id" serial primary key, "wallet_id" bigint not null references "q05_wallets"("id"), "delta" bigint, "amount" numeric, "posted_at" timestamptz)`,
    );
    await db.driver.execute(
      `create table "q05_audits" ("id" serial primary key, "move_id" integer not null references "q05_moves"("id"), "big" bigint, "amt" numeric, "at" timestamp, "payload" bytea, "note" text)`,
    );
    await fn({ db, statements });
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
  eveId: number;
  zedId: number;
  yazId: number;
  p1: number;
  p2: number;
  p3: number;
  p4: number;
}

async function seed(db: TestDb): Promise<Seed> {
  // Each test seeds its own deterministic dataset: reset sequences and rows.
  await db.driver.execute(
    `truncate users, posts, comments, q05_invoices, q05_invoice_lines, q05_wallets, q05_moves, q05_audits restart identity cascade`,
  );
  const [alice, bob] = await db.insert(users).values([{ name: "Alice" }, { name: "Bob" }]).returning();
  const [carol, dave, zed, yaz] = await db.insert(users).values([
    { name: "Carol", managerId: alice.id },
    { name: "Dave", managerId: bob.id },
    { name: "Zed" },
    { name: "Yaz" },
  ]).returning();
  const [eve] = await db.insert(users).values([
    { name: "Eve", managerId: carol.id, createdBy: bob.id, updatedBy: alice.id },
  ]).returning();
  // The cycle: Zed -> Yaz -> Zed.
  await db.update(users).set({ managerId: yaz.id }).where(eq(users.id, zed.id));
  await db.update(users).set({ managerId: zed.id }).where(eq(users.id, yaz.id));

  const insertedPosts = await db.insert(posts).values([
    { authorId: alice.id, reviewerId: bob.id, title: "P1", views: 5 },
    { authorId: alice.id, title: "P2", views: 9 },
    { authorId: alice.id, title: "P3", views: 1 },
    { authorId: bob.id, reviewerId: alice.id, title: "P4", views: 7 },
  ]).returning();
  const [p1, p2, p3, p4] = insertedPosts;

  await db.insert(comments).values([
    { postId: p1.id, commenterId: carol.id, body: "c1", score: 9 },
    { postId: p1.id, commenterId: dave.id, body: "dup", score: 1 },
    { postId: p1.id, commenterId: eve.id, body: "dup", score: 5 },
    { postId: p2.id, commenterId: carol.id, body: "c4", score: 7 },
    { postId: p3.id, commenterId: dave.id, body: "c5", score: 3 },
    { postId: p3.id, commenterId: eve.id, body: "c6", score: 8 },
    { postId: p4.id, commenterId: alice.id, body: "c7", score: 2 },
  ]);

  await db.insert(invoices).values([
    { tenantId: 1, id: 100, amount: "10.50" },
    { tenantId: 2, id: 100, amount: "20.25" },
    { tenantId: 1, id: 200, amount: "30.75" },
  ]);
  await db.insert(invoiceLines).values([
    { tenantId: 1, invoiceId: 100, memo: "L1" },
    { tenantId: 1, invoiceId: 100, memo: "L2" },
    { tenantId: 2, invoiceId: 100, memo: "L3" },
  ]);

  await db.insert(wallets).values([
    { id: "9007199254740993", name: "W1" },
    { id: "9007199254740995", name: "W2" },
  ]);
  const insertedMoves = await db.insert(moves).values([
    { walletId: "9007199254740993", delta: "-9007199254740993", amount: "1234567890123456789012345678901.23", postedAt: "2026-03-04T05:06:07.123456Z" },
    { walletId: "9007199254740993", delta: "9007199254740995", amount: "1.50", postedAt: "2027-12-31T23:59:59.999999Z" },
  ]).returning();
  await db.insert(audits).values([
    {
      moveId: insertedMoves[0].id,
      big: "9007199254740993",
      amt: "99999999999999999999.99",
      at: "2026-01-02T03:04:05.678123",
      payload: new Uint8Array([0x00, 0xff, 0x10]),
      note: null,
    },
  ]);

  return {
    aliceId: alice.id, bobId: bob.id, carolId: carol.id, daveId: dave.id, eveId: eve.id,
    zedId: zed.id, yazId: yaz.id, p1: p1.id, p2: p2.id, p3: p3.id, p4: p4.id,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q05 (${driverKind}): depth-3 many->many->one with column subsets matches a hand-SQL oracle`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        columns: ["id", "name"],
        with: { posts: { columns: ["id", "title"], with: { comments: { columns: ["id", "score"], with: { commenter: { columns: ["name"] } } } } } },
      });
      assert.equal(rows.length, 1);
      const alice = rows[0];
      assert.deepEqual(Object.keys(alice).sort(), ["id", "name", "posts"]);
      // Alice authored P1, P2, P3 (PK order).
      assert.deepEqual(alice.posts.map((p) => p.title), ["P1", "P2", "P3"]);
      for (const p of alice.posts) {
        assert.deepEqual(Object.keys(p).sort(), ["comments", "id", "title"]);
      }
      // P1 carries 3 comments in PK order with exact scores; commenters resolve.
      const p1 = alice.posts.find((p) => p.title === "P1")!;
      assert.deepEqual(p1.comments.map((c) => c.score), [9, 1, 5]);
      assert.deepEqual(p1.comments.map((c) => c.commenter!.name), ["Carol", "Dave", "Eve"]);
      for (const c of p1.comments) {
        assert.deepEqual(Object.keys(c).sort(), ["commenter", "id", "score"]);
        assert.deepEqual(Object.keys(c.commenter!), ["name"]);
      }

      // Independent oracle: hand-written nested correlated SQL.
      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', p.id, 'title', p.title, 'comments',
                   (select coalesce(jsonb_agg(jsonb_build_object('id', cm.id, 'score', cm.score, 'commenter',
                      (select jsonb_build_object('name', cu.name) from users cu where cu.id = cm.commenter_id)
                    ) order by cm.id), '[]'::jsonb)
                      from comments cm where cm.post_id = p.id)
                 ) order by p.id), '[]'::jsonb)
                   from posts p where p.author_id = u.id) as posts
         from users u where u.id = $1`,
        [s.aliceId],
      )) as Array<{ id: number; posts: Array<{ id: number; title: string; comments: Array<{ id: number; score: number; commenter: { name: string } }> }> }>;
      assert.equal(oracle.length, 1);
      assert.deepEqual(alice.posts, oracle[0].posts);
    });
  });

  test(`live q05 (${driverKind}): per-child filter/order/limit applies WITHIN each parent's children (the classic trap)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { where: gt(posts.views, 2), orderBy: [desc(posts.views)], limit: 2, columns: ["id", "views"] } },
      });
      // Alice's posts have views 5, 9, 1. Filtered to >2: {9,5}; top-2 by
      // views desc: [9 (P2), 5 (P1)]. A GLOBAL limit over all users' posts
      // would give a different, wrong answer (checked below).
      assert.deepEqual(rows[0].posts.map((p) => p.views), [9, 5]);

      // Global-limit wrongness proof: Bob's only qualifying post (7) is NOT
      // in Alice's list; a global top-2 by views would be [9, 7] and would
      // give Alice [9] only or leak Bob's post in — the per-parent result
      // keeps each parent's own top-2.
      const bob = await db.query.users.findFirst({
        where: eq(users.id, s.bobId),
        with: { posts: { where: gt(posts.views, 2), orderBy: [desc(posts.views)], limit: 2 } },
      });
      assert.deepEqual(bob!.posts.map((p) => p.views), [7]);

      // Per-parent offset: skip Alice's most-viewed, keep the next one.
      const offsetRows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { orderBy: [desc(posts.views)], offset: 1, columns: ["views"] } },
      });
      assert.deepEqual(offsetRows[0].posts.map((p) => p.views), [5, 1]);

      // Hand-SQL oracle twin for the limit case.
      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', t.id, 'views', t.views) order by t.views desc, t.id), '[]'::jsonb)
                   from (select p.id, p.views from posts p where p.author_id = u.id and p.views > $2 order by p.views desc, p.id asc limit 2) t) as posts
         from users u where u.id = $1`,
        [s.aliceId, 2],
      )) as Array<{ posts: Array<{ id: number; views: number }> }>;
      assert.deepEqual(
        rows[0].posts.map((p) => ({ id: p.id, views: p.views })),
        oracle[0].posts,
      );
    });
  });

  test(`live q05 (${driverKind}): columns subset + limit + orderBy on an UNSELECTED column (rework M1)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      // review-1 M1 probe verbatim: the order key (views) is outside the
      // columns subset — this 42703'd ("__rel_posts_lim.views does not
      // exist") before the rework on both drivers under both TZs.
      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { columns: ["id"], orderBy: [desc(posts.views)], limit: 2 } },
      });
      // Alice's posts by views desc: P2 (9), P1 (5) — top-2 of her 3.
      assert.deepEqual(rows[0].posts.map((p) => p.id), [s.p2, s.p1]);
      // Child rows carry ONLY the selected column.
      for (const p of rows[0].posts) {
        assert.deepEqual(Object.keys(p), ["id"]);
      }

      // Hand-SQL oracle twin (derived table carries the order key).
      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', t.id) order by t.views desc, t.id), '[]'::jsonb)
                   from (select p.id, p.views from posts p where p.author_id = u.id order by p.views desc, p.id asc limit 2) t) as posts
         from users u where u.id = $1`,
        [s.aliceId],
      )) as Array<{ posts: Array<{ id: number }> }>;
      assert.deepEqual(rows[0].posts, oracle[0].posts);
    });
  });

  test(`live q05 (${driverKind}): columns subset + limit + nested with on an UNSELECTED FK (rework M2)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      // review-1 M2 probe verbatim: the nested author edge's FK correlation
      // reads the derived alias — this 42703'd ("__rel_posts_lim.author_id
      // does not exist") before the rework on both drivers under both TZs.
      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { columns: ["id", "title"], limit: 2, with: { author: { columns: ["name"] } } } },
      });
      // PK order (no user order): P1, P2 of Alice's 3, each with its author.
      assert.deepEqual(rows[0].posts.map((p) => p.title), ["P1", "P2"]);
      assert.deepEqual(rows[0].posts.map((p) => p.author!.name), ["Alice", "Alice"]);

      // Hand-SQL oracle twin (derived table carries the FK column).
      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', t.id, 'title', t.title, 'author',
                   (select jsonb_build_object('name', a.name) from users a where a.id = t.author_id)
                 ) order by t.id), '[]'::jsonb)
                   from (select p.id, p.title, p.author_id from posts p where p.author_id = u.id order by p.id asc limit 2) t) as posts
         from users u where u.id = $1`,
        [s.aliceId],
      )) as Array<{ posts: Array<{ id: number; title: string; author: { name: string } }> }>;
      assert.deepEqual(rows[0].posts, oracle[0].posts);
    });
  });

  test(`live q05 (${driverKind}): combined unselected order key + unselected FK under limit`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      // Both rework defects in one query: orderBy on an unselected column
      // AND a nested with whose FK is unselected, under limit.
      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { columns: ["id"], orderBy: [desc(posts.views)], limit: 2, with: { author: { columns: ["name"] } } } },
      });
      assert.deepEqual(rows[0].posts.map((p) => p.id), [s.p2, s.p1]);
      assert.deepEqual(rows[0].posts.map((p) => p.author!.name), ["Alice", "Alice"]);

      const oracle = (await db.driver.query(
        `select u.id,
                (select coalesce(jsonb_agg(jsonb_build_object('id', t.id, 'author',
                   (select jsonb_build_object('name', a.name) from users a where a.id = t.author_id)
                 ) order by t.views desc, t.id), '[]'::jsonb)
                   from (select p.id, p.views, p.author_id from posts p where p.author_id = u.id order by p.views desc, p.id asc limit 2) t) as posts
         from users u where u.id = $1`,
        [s.aliceId],
      )) as Array<{ posts: Array<{ id: number; author: { name: string } }> }>;
      assert.deepEqual(rows[0].posts, oracle[0].posts);
    });
  });

  test(`live q05 (${driverKind}): two references to one target resolve independently (created_by + updated_by -> users)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      const eve = await db.query.users.findFirst({
        where: eq(users.id, s.eveId),
        columns: ["id", "name"],
        with: { createdByUser: { columns: ["name"] }, updatedByUser: { columns: ["name"] } },
      });
      assert.ok(eve);
      assert.equal(eve.createdByUser!.name, "Bob");
      assert.equal(eve.updatedByUser!.name, "Alice");
      // No collision: the two subqueries returned different rows.
      assert.notEqual(eve.createdByUser!.name, eve.updatedByUser!.name);

      // The same shape through the posts pair (author + reviewer).
      const p4 = await db.query.posts.findFirst({
        where: eq(posts.id, s.p4),
        with: { author: { columns: ["name"] }, reviewer: { columns: ["name"] } },
      });
      assert.ok(p4);
      assert.equal(p4.author!.name, "Bob");
      assert.equal(p4.reviewer!.name, "Alice");

      // Nested under the pair: both references carry their own nested edges.
      const deep = await db.query.posts.findFirst({
        where: eq(posts.id, s.p4),
        with: {
          author: { columns: ["name"], with: { manager: { columns: ["name"] } } },
          reviewer: { columns: ["name"], with: { posts: { columns: ["title"], limit: 1 } } },
        },
      });
      assert.ok(deep);
      assert.equal(deep.author!.name, "Bob");
      assert.equal(deep.author!.manager, null); // Bob has no manager.
      assert.equal(deep.reviewer!.name, "Alice");
      assert.deepEqual(deep.reviewer!.posts.map((p) => p.title), ["P1"]); // top-1 by PK among Alice's 3.

      const oracle = (await db.driver.query(
        `select p.id,
                (select jsonb_build_object('name', a.name, 'manager', (select jsonb_build_object('name', m.name) from users m where m.id = a.manager_id)) from users a where a.id = p.author_id) as author,
                (select jsonb_build_object('name', r.name, 'posts', (select coalesce(jsonb_agg(jsonb_build_object('title', t.title) order by t.id) , '[]'::jsonb) from (select q.id, q.title from posts q where q.author_id = r.id order by q.id limit 1) t)) from users r where r.id = p.reviewer_id) as reviewer
         from posts p where p.id = $1`,
        [s.p4],
      )) as Array<{ author: { name: string; manager: { name: string } | null }; reviewer: { name: string; posts: Array<{ title: string }> } }>;
      assert.equal(oracle[0].author.name, "Bob");
      assert.equal(oracle[0].author.manager, null);
      assert.deepEqual(oracle[0].reviewer.posts, deep.reviewer!.posts);
    });
  });

  test(`live q05 (${driverKind}): named inverse pairs stay independent (posts vs reviewedPosts)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: {
          posts: { columns: ["title"] },
          reviewedPosts: { columns: ["title"] },
          comments: true,
        },
      });
      // Alice authored P1..P3, reviewed only P4, commented once (c7 on P4).
      assert.deepEqual(rows[0].posts.map((p) => p.title), ["P1", "P2", "P3"]);
      assert.deepEqual(rows[0].reviewedPosts.map((p) => p.title), ["P4"]);
      assert.equal(rows[0].comments.length, 1);
      // Three child collections on one parent, no cross-contamination.

      const oracle = (await db.driver.query(
        `select
           (select count(*)::int from posts where author_id = $1) as authored,
           (select count(*)::int from posts where reviewer_id = $1) as reviewed,
           (select count(*)::int from comments where commenter_id = $1) as commented`,
        [s.aliceId],
      )) as Array<{ authored: number; reviewed: number; commented: number }>;
      assert.equal(rows[0].posts.length, oracle[0].authored);
      assert.equal(rows[0].reviewedPosts.length, oracle[0].reviewed);
      assert.equal(rows[0].comments.length, oracle[0].commented);
    });
  });

  test(`live q05 (${driverKind}): self-relation tree to depth 3, including a data cycle, terminates exactly`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      // Chain: Alice -> Carol (report) -> Eve -> (Eve's reports: none).
      const alice = await db.query.users.findFirst({
        where: eq(users.id, s.aliceId),
        columns: ["name"],
        with: { reports: { columns: ["name"], with: { reports: { columns: ["name"], with: { reports: { columns: ["name"] } } } } } },
      });
      assert.ok(alice);
      assert.deepEqual(alice.reports.map((r) => r.name), ["Carol"]);
      assert.deepEqual(alice.reports[0].reports.map((r) => r.name), ["Eve"]);
      assert.deepEqual(alice.reports[0].reports[0].reports, []); // Eve manages nobody.

      // To-one chain: Eve -> Carol -> Alice -> null.
      const eve = await db.query.users.findFirst({
        where: eq(users.id, s.eveId),
        columns: ["name"],
        with: { manager: { columns: ["name"], with: { manager: { columns: ["name"], with: { manager: { columns: ["name"] } } } } } },
      });
      assert.ok(eve);
      assert.equal(eve.manager!.name, "Carol");
      assert.equal(eve.manager!.manager!.name, "Alice");
      assert.equal(eve.manager!.manager!.manager, null);

      // The CYCLE: Zed -> Yaz -> Zed -> Yaz terminates at depth 3.
      const zed = await db.query.users.findFirst({
        where: eq(users.id, s.zedId),
        columns: ["name"],
        with: { reports: { columns: ["name"], with: { reports: { columns: ["name"], with: { reports: { columns: ["name"] } } } } } },
      });
      assert.ok(zed);
      assert.deepEqual(zed.reports.map((r) => r.name), ["Yaz"]);
      assert.deepEqual(zed.reports[0].reports.map((r) => r.name), ["Zed"]);
      assert.deepEqual(zed.reports[0].reports[0].reports.map((r) => r.name), ["Yaz"]);

      // Oracle twin for the cycle (hand-written, depth-bounded by structure).
      const oracle = (await db.driver.query(
        `select u.name,
                (select coalesce(jsonb_agg(jsonb_build_object('name', r1.name, 'reports',
                   (select coalesce(jsonb_agg(jsonb_build_object('name', r2.name, 'reports',
                      (select coalesce(jsonb_agg(jsonb_build_object('name', r3.name) order by r3.id), '[]'::jsonb)
                        from users r3 where r3.manager_id = r2.id)
                   ) order by r2.id), '[]'::jsonb)
                     from users r2 where r2.manager_id = r1.id)
                ) order by r1.id), '[]'::jsonb)
                  from users r1 where r1.manager_id = u.id) as reports
         from users u where u.id = $1`,
        [s.zedId],
      )) as Array<{ reports: Array<{ name: string; reports: Array<{ name: string; reports: Array<{ name: string }> }> }> }>;
      assert.deepEqual(zed.reports, oracle[0].reports);
    });
  });

  test(`live q05 (${driverKind}): childless parents -> [] and missing to-ones -> null at depth`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      // Dave authored no posts; his manager Bob has posts — the depth-2 empty
      // array must not swallow the deeper structure. (Dave commented twice.)
      const dave = await db.query.users.findFirst({
        where: eq(users.id, s.daveId),
        with: { posts: { with: { comments: true } }, comments: true, manager: { with: { posts: true } } },
      });
      assert.ok(dave);
      assert.deepEqual(dave.posts, []);
      assert.equal(dave.comments.length, 2);
      // Zed is empty everywhere: no posts, no comments, cycle-safe.
      const zed = await db.query.users.findFirst({
        where: eq(users.id, s.zedId),
        with: { posts: { with: { comments: true } }, comments: true },
      });
      assert.ok(zed);
      assert.deepEqual(zed.posts, []);
      assert.deepEqual(zed.comments, []);
      assert.ok(dave.manager);
      assert.equal(dave.manager.posts.length, 1);
      assert.equal(dave.manager.posts[0].title, "P4");

      // Missing to-one nested inside a child object: P2 has no reviewer.
      const p2 = await db.query.posts.findFirst({
        where: eq(posts.id, s.p2),
        with: { comments: { with: { post: { with: { reviewer: { columns: ["name"] } } } } } },
      });
      assert.ok(p2);
      assert.equal(p2.comments.length, 1);
      assert.ok(p2.comments[0].post);
      assert.equal(p2.comments[0].post.reviewer, null); // null, NOT undefined/[]/missing key.
      assert.ok("reviewer" in p2.comments[0].post);

      // Invoice (1,200) has no lines: childless composite parent -> [].
      const emptyInvoice = await db.query.q05_invoices.findMany({
        where: and(eq(invoices.tenantId, 1), eq(invoices.id, 200)),
        with: { lines: true },
      });
      assert.equal(emptyInvoice.length, 1);
      assert.deepEqual(emptyInvoice[0].lines, []);
    });
  });

  test(`live q05 (${driverKind}): composite tenant keys stay tenant-local through nesting`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await seed(db);

      const tenantOne = await db.query.q05_invoices.findMany({
        where: eq(invoices.tenantId, 1),
        with: { lines: { with: { invoice: { columns: ["amount"] } } } },
      });
      assert.equal(tenantOne.length, 2); // (1,100) and (1,200)
      const inv100 = tenantOne.find((i) => i.id === 100)!;
      assert.equal(inv100.lines.length, 2); // only tenant-1 lines
      assert.deepEqual(inv100.lines.map((l) => l.memo), ["L1", "L2"]);
      // Depth-3 bounce back: each line's invoice is the tenant-1 amount.
      for (const line of inv100.lines) {
        assert.ok(line.invoice);
        assert.equal(line.invoice.amount, "10.50");
      }
      // The childless composite parent still resolves at depth.
      const inv200 = tenantOne.find((i) => i.id === 200)!;
      assert.deepEqual(inv200.lines, []);

      // Reversed-order zip: the line->invoice one() declares fields/references
      // in the OPPOSITE order; correlation is positional, so tenant-2's line
      // resolves the tenant-2 invoice.
      const tenantTwoLines = await db.query.q05_invoice_lines.findMany({
        where: eq(invoiceLines.tenantId, 2),
        with: { invoice: { columns: ["amount", "tenantId"] } },
      });
      assert.equal(tenantTwoLines.length, 1);
      assert.ok(tenantTwoLines[0].invoice);
      assert.equal(tenantTwoLines[0].invoice.amount, "20.25");
      assert.equal(tenantTwoLines[0].invoice.tenantId, 2);

      // Oracle: hand SQL with the reversed pairing.
      const oracle = (await db.driver.query(
        `select l.memo, (select jsonb_build_object('amount', i.amount::text, 'tenantId', i.tenant_id)
                          from q05_invoices i where i.id = l.invoice_id and i.tenant_id = l.tenant_id) as invoice
         from q05_invoice_lines l where l.tenant_id = 2 order by l.id`,
      )) as Array<{ memo: string; invoice: { amount: string; tenantId: number } }>;
      assert.equal(oracle.length, 1);
      assert.equal(oracle[0].invoice.amount, tenantTwoLines[0].invoice!.amount);
      assert.equal(oracle[0].invoice.tenantId, tenantTwoLines[0].invoice!.tenantId);
    });
  });

  test(`live q05 (${driverKind}): lossless values through depth-3 JSON projection (${driverKind})`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await seed(db);

      // wallets (int8 PK) -> moves (int8/numeric/timestamptz) -> audits
      // (int8/numeric/timestamp/bytea): every leaf must be byte-exact.
      const rows = await db.query.q05_wallets.findMany({
        where: eq(wallets.id, "9007199254740993"),
        with: {
          moves: {
            orderBy: [asc(moves.id)],
            with: { audits: { columns: ["big", "amt", "at", "payload", "note"] } },
          },
        },
      });
      assert.equal(rows.length, 1);
      const w = rows[0];
      assert.equal(w.id, 9007199254740993n); // int8 PK, default bigint mode.
      assert.equal(w.moves.length, 2);
      const [m0, m1] = w.moves;
      assert.equal(m0.delta, -9007199254740993n);
      assert.equal(m0.amount, "1234567890123456789012345678901.23"); // 31-digit exact, scale intact.
      assert.equal(m0.postedAt, "2026-03-04T05:06:07.123456Z"); // microseconds intact.
      assert.equal(m1.delta, 9007199254740995n);
      assert.equal(m1.amount, "1.50"); // trailing zero preserved.
      assert.equal(m1.postedAt, "2027-12-31T23:59:59.999999Z");

      assert.equal(m0.audits.length, 1);
      const a = m0.audits[0];
      assert.equal(a.big, 9007199254740993n);
      assert.equal(a.amt, "99999999999999999999.99");
      assert.equal(a.at, "2026-01-02T03:04:05.678123"); // naive timestamp, microseconds.
      assert.deepEqual(a.payload, new Uint8Array([0x00, 0xff, 0x10]));
      assert.equal(a.note, null);

      // Depth-3 to-one chain back up: audits -> move -> wallet (int8 exact).
      const back = await db.query.q05_audits.findMany({
        with: { move: { columns: ["delta"], with: { wallet: { columns: ["id", "name"] } } } },
      });
      assert.equal(back.length, 1);
      assert.ok(back[0].move);
      assert.equal(back[0].move.delta, -9007199254740993n);
      assert.ok(back[0].move.wallet);
      assert.equal(back[0].move.wallet.id, 9007199254740993n);
      assert.equal(back[0].move.wallet.name, "W1");

      // Flat-path agreement (independent of the JSON projection).
      // ::text renders timestamptz in the SESSION timezone — compare through
      // the UTC wall clock so the oracle is session-tz independent.
      const flat = (await db.driver.query(
        `select delta::text, amount::text, (posted_at at time zone 'UTC')::text as posted_at from q05_moves order by id`,
      )) as Array<{ delta: string; amount: string; posted_at: string }>;
      assert.equal(m0.delta, BigInt(flat[0].delta));
      assert.equal(m0.amount, flat[0].amount);
      assert.equal(m0.postedAt, flat[0].posted_at.replace(" ", "T") + "Z");
    });
  });

  test(`live q05 (${driverKind}): one statement per query — no N+1 at any depth`, async () => {
    await withSuite(driverKind, async ({ db, statements }) => {
      await seed(db);
      statements.count = 0;
      await db.query.users.findMany({
        with: {
          posts: { with: { comments: { with: { commenter: { with: { reports: true } } } } } },
          reviewedPosts: true,
          comments: true,
        },
        limit: 3,
      });
      assert.equal(statements.count, 1, "a depth-3 two-collection query is ONE statement");

      statements.count = 0;
      await db.query.users.findFirst({ with: { posts: { with: { comments: true } } } });
      assert.equal(statements.count, 1);

      statements.count = 0;
      await db.query.q05_wallets.findMany({ with: { moves: { with: { audits: true } } } });
      assert.equal(statements.count, 1);
    });
  });

  test(`live q05 (${driverKind}): parent pagination applies before child expansion`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await seed(db);

      const page1 = await db.query.users.findMany({
        orderBy: [asc(users.id)],
        limit: 2,
        columns: ["id"],
        with: { posts: true },
      });
      const page2 = await db.query.users.findMany({
        orderBy: [asc(users.id)],
        limit: 2,
        offset: 2,
        columns: ["id"],
        with: { posts: true },
      });
      // Page 1 = Alice, Bob; each keeps their FULL child set (2-3 posts and
      // 1 post respectively) — the parent limit never truncates children.
      assert.equal(page1.length, 2);
      assert.equal(page2.length, 2);
      assert.notEqual(page1[0].id, page2[0].id);
      const aliceRow = page1[0];
      assert.equal(aliceRow.posts.length, 3);
      assert.equal(page1[1].posts.length, 1);

      // Oracle: the exact parent set with full children per parent.
      const oracle = (await db.driver.query(
        `select u.id,
                (select count(*)::int from posts p where p.author_id = u.id) as n
         from users u order by u.id limit 2`,
      )) as Array<{ id: number; n: number }>;
      assert.deepEqual(page1.map((u) => ({ id: u.id, n: u.posts.length })), [...oracle]);
    });
  });

  test(`live q05 (${driverKind}): duplicate-valued distinct children both survive at depth`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const s = await seed(db);

      const rows = await db.query.users.findMany({
        where: eq(users.id, s.aliceId),
        with: { posts: { columns: ["id"], with: { comments: { columns: ["id", "body"] } } } },
      });
      const p1 = rows[0].posts.find((p) => p.id === s.p1)!;
      const dupes = p1.comments.filter((c) => c.body === "dup");
      assert.equal(dupes.length, 2, "equal-valued distinct children must BOTH survive (no DISTINCT collapse)");
      assert.notEqual(dupes[0].id, dupes[1].id);

      const oracle = (await db.driver.query(
        `select count(*)::int as n from comments where post_id = $1 and body = 'dup'`,
        [s.p1],
      )) as Array<{ n: number }>;
      assert.equal(dupes.length, oracle[0].n);
    });
  });

  test(`live q05 (${driverKind}): pure toSQL/explainQuery inspect without executing`, async () => {
    await withSuite(driverKind, async ({ db, statements }) => {
      await seed(db);
      statements.count = 0;
      const compiled = db.query.users.toSQL({ with: { posts: { with: { comments: true } } } });
      assert.ok(compiled.sql.startsWith('select '));
      assert.ok(compiled.sql.includes('as "__rel_posts"'));
      assert.ok(compiled.sql.includes('as "__rel_posts__comments"'));
      const again = db.query.users.toSQL({ with: { posts: { with: { comments: true } } } });
      assert.equal(again.sql, compiled.sql);

      const plan = db.query.users.explainQuery({ with: { posts: { with: { comments: true } } } });
      assert.equal(plan.table, "users");
      assert.equal(plan.statementCount, 1);
      assert.equal(plan.depth, 2);
      assert.deepEqual(plan.capabilities, ["jsonb-functions"]);
      const postsEdge = plan.edges.find((e) => e.key === "posts");
      assert.ok(postsEdge);
      assert.equal(postsEdge.nested.length, 1);
      assert.equal(postsEdge.nested[0].key, "comments");

      assert.equal(statements.count, 0, "pure inspection executed nothing");
    });
  });
}

// ---------------------------------------------------------------------------
// Capability fail-before-running: an adapter whose engine reports PostgreSQL
// 9.3 (before jsonb, 9.4) must reject relational statements BEFORE any
// statement executes. The decorator is a real adapter over a real connection
// pool — every query executes against the server; only the version probe
// (`select version() as version`, the capability contract's probe) is
// rewritten, which is precisely what a downlevel engine reports.
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live q05 (${driverKind}): jsonb-functions-unsupported engine rejects before running partial work`, async () => {
    if (!(await ensureLive(`live q05 capability (${driverKind})`))) return;
    const { Pool } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
    };
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${DB_NAME}"`);
    await admin.query(`create database "${DB_NAME}"`);
    await admin.end();

    const url = new URL(TEST_URL);
    url.pathname = `/${DB_NAME}`;
    const statements: string[] = [];
    const base: Driver = driverKind === "pg"
      ? wrapPgPool(new (await import("pg")).Pool({ connectionString: url.toString(), max: 1 }) as never)
      : wrapPostgresJs(((await import("postgres")).default)(url.toString(), { max: 1 }) as never);
    const downlevel: Driver = {
      query: <T = Record<string, unknown>>(sqlText: string, params?: unknown[]): Promise<T[]> => {
        statements.push(sqlText);
        if (sqlText === "select version() as version") {
          return Promise.resolve([{ version: "PostgreSQL 9.3.26 (downlevel probe)" }] as T[]);
        }
        return base.query<T>(sqlText, params);
      },
      execute: (sqlText, params) => {
        statements.push(sqlText);
        return base.execute(sqlText, params);
      },
      begin: (fn) => base.begin(fn),
      close: () => base.close(),
      lifecycle: base.lifecycle,
    };

    const db = await createDatabase({
      driver: downlevel,
      tables: { users, posts, comments } as Tables,
      relations: { users: usersRelations, posts: postsRelations, comments: commentsRelations } as RelationsMap,
    });
    try {
      // A relational query requires jsonb-functions; the 9.3 version fact
      // proves it unsupported -> CapabilityRequirementError BEFORE SQL.
      const err = await db.query.users.findMany({ with: { posts: true } }).then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof CapabilityRequirementError, `expected CapabilityRequirementError, got ${String(err)}`);
      assert.match(err.message, /jsonb-functions/);
      assert.match(err.message, /unsupported/);
      // Zero partial work: the ONLY statement the adapter saw is the version
      // probe itself (memoized once per gate).
      assert.equal(statements.length, 1);
      assert.equal(statements[0], "select version() as version");

      // The gate is honest about the evidence trail.
      const status = await db.capability("jsonb-functions");
      assert.equal(status.status, "unsupported");
      assert.match(status.evidence, /9\.3\.26/);
      assert.equal(status.engine.version, "9.3.26");
    } finally {
      await db.close();
      const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
      await admin2.query(`drop database if exists "${DB_NAME}"`);
      await admin2.end();
    }
  });
}
