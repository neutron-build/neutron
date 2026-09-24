import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  pgTable,
  serial,
  integer,
  text,
  numeric,
  bigint,
  relations,
  uniqueIndex,
  ServerSqlError,
  NestedWriteError,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Q06 live battery (V13 nested-write cases). Every database-facing claim is
// checked against hand-written SQL oracles authored here (raw driver
// queries, independent of the ORM's read path). Covers: create over two
// child collections with to-one connects; adopt through many-edge connects;
// update mixing child create/update/delete with edge ops; mid-graph
// late-FK failure rollback proven by row counts; unique-key connects
// (missing / non-unique key set / ambiguous against a non-enforcing
// database / duplicate selectors); composite keys end to end; graph cycles
// with preallocated keys (success through a deferred link) and a NOT NULL
// cycle rejected before any SQL; the two-ref created_by/updated_by shape;
// delete cascades per declared ownership (undeclared dependents abort,
// nested dispositions, disconnect, mutual-manager data cycles); savepoint
// behavior inside db.transaction; and statement-count assertions against
// the logger (the dry plan names every statement that runs).

const DB_NAME = uniqueDbName("q06_writes");

// --- fixture ----------------------------------------------------------------

const users = pgTable(
  "q06_users",
  {
    id: serial("id").primaryKey(),
    email: text("email").notNull(),
    name: text("name"),
    managerId: integer("manager_id"),
    createdBy: integer("created_by"),
    updatedBy: integer("updated_by"),
  },
  (t) => [uniqueIndex("q06_users_email_uq").on(t.email)],
);

const posts = pgTable(
  "q06_posts",
  {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    reviewerId: integer("reviewer_id"),
    slug: text("slug").notNull(),
    title: text("title").notNull(),
    views: integer("views").notNull().default(0),
    pinnedAt: text("pinned_at"),
  },
  (t) => [uniqueIndex("q06_posts_slug_uq").on(t.slug)],
);

const comments = pgTable("q06_comments", {
  id: serial("id").primaryKey(),
  postId: integer("post_id").notNull(),
  commenterId: integer("commenter_id").notNull(),
  body: text("body").notNull(),
});

const invoices = pgTable("q06_invoices", {
  tenantId: integer("tenant_id").primaryKey(),
  id: integer("id").primaryKey(),
  amount: numeric("amount").notNull(),
});

const lines = pgTable("q06_lines", {
  id: serial("id").primaryKey(),
  tenantId: integer("tenant_id").notNull(),
  invoiceId: integer("invoice_id").notNull(),
  memo: text("memo").notNull(),
});

const wallets = pgTable("q06_wallets", {
  id: bigint("id").primaryKey(),
  label: text("label").notNull(),
});

const moves = pgTable("q06_moves", {
  id: serial("id").primaryKey(),
  walletId: bigint("wallet_id").notNull(),
  note: text("note"),
});

// NOT NULL self-reference: the unbreakable cycle shape.
const partners = pgTable("q06_partners", {
  id: integer("id").primaryKey(),
  partnerId: integer("partner_id").notNull(),
  name: text("name").notNull(),
});

const usersRelations = relations(users, ({ one, many }) => ({
  manager: one(users, { fields: [users.managerId], references: [users.id], relationName: "reports" }),
  reports: many(users, { relationName: "reports" }),
  createdByUser: one(users, { fields: [users.createdBy], references: [users.id], relationName: "created" }),
  updatedByUser: one(users, { fields: [users.updatedBy], references: [users.id], relationName: "updated" }),
  posts: many(posts, { relationName: "author" }),
  reviewedPosts: many(posts, { relationName: "reviewer" }),
}));
const postsRelations = relations(posts, ({ one, many }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id], relationName: "author" }),
  reviewer: one(users, { fields: [posts.reviewerId], references: [users.id], relationName: "reviewer" }),
  comments: many(comments),
}));
const commentsRelations = relations(comments, ({ one }) => ({
  post: one(posts, { fields: [comments.postId], references: [posts.id] }),
}));
const invoicesRelations = relations(invoices, ({ many }) => ({ lines: many(lines) }));
const linesRelations = relations(lines, ({ one }) => ({
  invoice: one(invoices, { fields: [lines.invoiceId, lines.tenantId], references: [invoices.id, invoices.tenantId] }),
}));
const walletsRelations = relations(wallets, ({ many }) => ({ moves: many(moves) }));
const movesRelations = relations(moves, ({ one }) => ({
  wallet: one(wallets, { fields: [moves.walletId], references: [wallets.id] }),
}));
const partnersRelations = relations(partners, ({ one }) => ({
  partner: one(partners, { fields: [partners.partnerId], references: [partners.id] }),
}));

type Tables = {
  q06_users: typeof users;
  q06_posts: typeof posts;
  q06_comments: typeof comments;
  q06_invoices: typeof invoices;
  q06_lines: typeof lines;
  q06_wallets: typeof wallets;
  q06_moves: typeof moves;
  q06_partners: typeof partners;
};
type RelationsMap = {
  q06_users: typeof usersRelations;
  q06_posts: typeof postsRelations;
  q06_comments: typeof commentsRelations;
  q06_invoices: typeof invoicesRelations;
  q06_lines: typeof linesRelations;
  q06_wallets: typeof walletsRelations;
  q06_moves: typeof movesRelations;
  q06_partners: typeof partnersRelations;
};
type TestDb = NeutronDatabase<Tables, RelationsMap>;

interface SuiteFixture {
  db: TestDb;
  /** Statement counter fed by the database logger: the N+1 catcher. */
  statements: { count: number };
}

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live q06 (${driverKind})`))) {
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
    tables: {
      q06_users: users,
      q06_posts: posts,
      q06_comments: comments,
      q06_invoices: invoices,
      q06_lines: lines,
      q06_wallets: wallets,
      q06_moves: moves,
      q06_partners: partners,
    },
    relations: {
      q06_users: usersRelations,
      q06_posts: postsRelations,
      q06_comments: commentsRelations,
      q06_invoices: invoicesRelations,
      q06_lines: linesRelations,
      q06_wallets: walletsRelations,
      q06_moves: movesRelations,
      q06_partners: partnersRelations,
    } as RelationsMap,
    logger: (e) => {
      if (e.kind === "query-end") statements.count += 1;
    },
  });
  try {
    // The users email unique index is deliberately NOT created: the
    // ambiguity case reuses this state (declared-unique metadata over a
    // non-enforcing database).
    await db.driver.execute(
      `create table "q06_users" ("id" serial primary key, "email" text not null, "name" text, "manager_id" integer references "q06_users"("id"), "created_by" integer references "q06_users"("id"), "updated_by" integer references "q06_users"("id"))`,
    );
    await db.driver.execute(
      `create table "q06_posts" ("id" serial primary key, "author_id" integer not null references "q06_users"("id"), "reviewer_id" integer references "q06_users"("id"), "slug" text not null, "title" text not null, "views" integer not null default 0, "pinned_at" text)`,
    );
    await db.driver.execute(`create unique index "q06_posts_slug_uq" on "q06_posts" ("slug")`);
    await db.driver.execute(
      `create table "q06_comments" ("id" serial primary key, "post_id" integer not null references "q06_posts"("id"), "commenter_id" integer not null references "q06_users"("id"), "body" text not null)`,
    );
    await db.driver.execute(
      `create table "q06_invoices" ("tenant_id" integer not null, "id" integer not null, "amount" numeric not null, primary key ("tenant_id", "id"))`,
    );
    await db.driver.execute(
      `create table "q06_lines" ("id" serial primary key, "tenant_id" integer not null, "invoice_id" integer not null, foreign key ("invoice_id", "tenant_id") references "q06_invoices"("id", "tenant_id"), "memo" text not null)`,
    );
    await db.driver.execute(`create table "q06_wallets" ("id" bigint primary key, "label" text not null)`);
    await db.driver.execute(
      `create table "q06_moves" ("id" serial primary key, "wallet_id" bigint not null references "q06_wallets"("id"), "note" text)`,
    );
    await db.driver.execute(
      `create table "q06_partners" ("id" integer primary key, "partner_id" integer not null references "q06_partners"("id"), "name" text not null)`,
    );
    await fn({ db, statements });
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

/** Raw-SQL oracle: rows straight from the driver (never the ORM read path). */
async function oracle(db: TestDb, sql: string, params?: unknown[]): Promise<Record<string, unknown>[]> {
  return (await db.driver.query(sql, params ?? [])) as Record<string, unknown>[];
}

async function count(db: TestDb, table: string, where = "true", params?: unknown[]): Promise<number> {
  const rows = await oracle(db, `select count(*)::int as n from "${table}" where ${where}`, params);
  return rows[0].n as number;
}

async function seedBase(db: TestDb): Promise<void> {
  await db.driver.execute(`insert into "q06_users" ("email", "name") values ('boss@x.com', 'Boss'), ('rev@x.com', 'Rev')`);
  await db.driver.execute(
    `insert into "q06_posts" ("author_id", "reviewer_id", "slug", "title") values (1, 2, 'orphan-1', 'Orphan One'), (1, null, 'orphan-2', 'Orphan Two')`,
  );
}

const BOTH = ["postgres", "pg"] as const;

test("live q06: create over two child collections with to-one connects; oracle twins; statement count", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        const before = statements.count;
        const row = await db.query.q06_users.create({
          data: {
            email: "alice@x.com",
            name: "Alice",
            manager: { connect: { email: "boss@x.com" } },
            posts: {
              create: [
                { slug: "p1", title: "First", reviewer: { connect: { email: "rev@x.com" } } },
                { slug: "p2", title: "Second" },
                { slug: "p3", title: "Third" },
              ],
            },
            reviewedPosts: { create: { slug: "r1", title: "Reviewed", authorId: 1 } },
          },
        });
        const used = statements.count - before;
        // 2 connect lookups + 1 user insert + 4 post inserts.
        assert.equal(used, 7, `statement count (got ${used})`);

        // Oracle twins: the created graph, read by hand-written SQL.
        const alice = await oracle(db, `select "id", "manager_id" from "q06_users" where "email" = 'alice@x.com'`);
        assert.equal(alice.length, 1);
        assert.equal(alice[0].manager_id, 1, "manager FK from the connect");
        const aliceId = alice[0].id as number;
        assert.equal(row.managerId, 1, "created row returns its linked key");
        assert.equal(row.email, "alice@x.com");
        assert.ok(typeof row.id === "number" && row.id > 0, "serial id returned");

        const createdPosts = await oracle(
          db,
          `select "slug", "author_id", "reviewer_id" from "q06_posts" where "slug" in ('p1','p2','p3','r1') order by "slug"`,
        );
        assert.deepEqual(
          createdPosts.map((p) => [p.slug, p.author_id, p.reviewer_id]),
          [
            ["p1", aliceId, 2],
            ["p2", aliceId, null],
            ["p3", aliceId, null],
            ["r1", 1, aliceId],
          ],
          "author children bind the parent; the reviewedPosts child binds the parent as reviewer (its explicit authorId passes through)",
        );
      });
    });
  }
});

test("live q06: adopt existing rows through many-edge connects (one UPDATE per row, keys exact)", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        const before = statements.count;
        await db.query.q06_users.create({
          data: { email: "adopt@x.com", reviewedPosts: { connect: [{ slug: "orphan-1" }, { slug: "orphan-2" }] } },
        });
        const used = statements.count - before;
        // 1 user insert + 2 adopt updates. A to-many connect IS the update:
        // its affected-row count enforces exactly-one (no separate lookup).
        assert.equal(used, 3, `statement count (got ${used})`);
        const adopted = await oracle(
          db,
          `select "slug", "reviewer_id", "author_id" from "q06_posts" where "slug" in ('orphan-1','orphan-2') order by "slug"`,
        );
        const adopter = (await oracle(db, `select "id" from "q06_users" where "email" = 'adopt@x.com'`))[0].id as number;
        assert.deepEqual(adopted.map((p) => [p.slug, p.reviewer_id]), [
          ["orphan-1", adopter],
          ["orphan-2", adopter],
        ]);
        assert.deepEqual(adopted.map((p) => p.author_id), [1, 1], "authors untouched");
      });
    });
  }
});

test("live q06: update mixes child create/update/delete, edge connect and to-one disconnect", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db }) => {
        await seedBase(db);
        const made = await db.query.q06_users.create({
          data: {
            email: "upd@x.com",
            manager: { connect: { email: "boss@x.com" } },
            posts: { create: [{ slug: "u1", title: "Old1" }, { slug: "u2", title: "Old2" }, { slug: "u3", title: "Old3" }] },
          },
        });
        const userId = made.id as number;
        const updated = await db.query.q06_users.update({
          where: { email: "upd@x.com" },
          data: {
            name: "Upd II",
            manager: { disconnect: true },
            posts: {
              create: [{ slug: "u4", title: "New" }],
              update: [{ where: { slug: "u1" }, data: { title: "Old1!", views: 7 } }],
              delete: [{ slug: "u2" }],
            },
            reviewedPosts: { connect: [{ slug: "orphan-1" }] },
          },
        });
        assert.equal(updated.name, "Upd II");
        assert.equal(updated.managerId, null, "disconnect nulls the parent FK");

        const user = await oracle(db, `select "name", "manager_id" from "q06_users" where "email" = 'upd@x.com'`);
        assert.deepEqual(user.map((r) => ({ name: r.name, manager_id: r.manager_id })), [{ name: "Upd II", manager_id: null }]);
        const postsNow = await oracle(
          db,
          `select "slug", "title", "views", "author_id" from "q06_posts" where "slug" like 'u%' order by "slug"`,
        );
        assert.deepEqual(
          postsNow.map((p) => [p.slug, p.title, p.views]),
          [
            ["u1", "Old1!", 7],
            ["u3", "Old3", 0],
            ["u4", "New", 0],
          ],
        );
        assert.ok(postsNow.every((p) => p.author_id === userId), "scoped: only this parent's children touched");
        assert.equal(await count(db, "q06_posts", `"slug" = 'u2'`), 0, "deleted child is gone");
        const orphan = await oracle(db, `select "reviewer_id" from "q06_posts" where "slug" = 'orphan-1'`);
        assert.equal(orphan[0].reviewer_id, userId, "reviewer connect adopted the orphan");

        // to-one update through the edge, then delete-and-disconnect.
        await db.query.q06_users.update({ where: { email: "upd@x.com" }, data: { manager: { connect: { email: "boss@x.com" } } } });
        await db.query.q06_users.update({ where: { email: "upd@x.com" }, data: { manager: { update: { name: "Big Boss" } } } });
        const boss = await oracle(db, `select "name" from "q06_users" where "email" = 'boss@x.com'`);
        assert.deepEqual(boss.map((r) => ({ name: r.name })), [{ name: "Big Boss" }]);
        await db.query.q06_users.update({ where: { email: "upd@x.com" }, data: { reviewedPosts: { delete: [{ slug: "orphan-1" }] } } });
        assert.equal(await count(db, "q06_posts", `"slug" = 'orphan-1'`), 0, "delete through the edge removed the row");
      });
    });
  }
});

test("live q06: mid-graph late FK failure rolls back everything (row-count proofs)", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        const before = statements.count;
        // The graph: user -> posts[0] -> comments. The LAST statement (the
        // second comment insert) carries a plain FK value pointing at a
        // nonexistent commenter: a real late constraint violation after
        // earlier statements already ran.
        await assert.rejects(
          db.query.q06_users.create({
            data: {
              email: "doomed@x.com",
              posts: {
                create: [
                  { slug: "d1", title: "D1", comments: { create: [{ commenterId: 1, body: "ok" }] } },
                  { slug: "d2", title: "D2", comments: { create: [{ commenterId: 424242, body: "bad fk" }] } },
                ],
              },
            },
          }),
          (err: unknown) => {
            assert.ok(err instanceof ServerSqlError, `ServerSqlError, got ${String(err)}`);
            assert.equal((err as ServerSqlError).sqlstate, "23503");
            return true;
          },
        );
        assert.ok(statements.count - before >= 4, "the failure happened mid-graph, after earlier statements ran");
        // All-or-nothing: nothing survived.
        assert.equal(await count(db, "q06_users", `"email" = 'doomed@x.com'`), 0);
        assert.equal(await count(db, "q06_posts", `"slug" in ('d1','d2')`), 0);
        assert.equal(await count(db, "q06_comments", `"body" in ('ok','bad fk')`), 0);
        // The write system is usable afterwards (connection clean).
        const ok = await db.query.q06_users.create({ data: { email: "after@x.com" } });
        assert.equal(ok.email, "after@x.com");
      });
    });
  }
});

test("live q06: unique-key connect — missing, non-unique key set, ambiguous, duplicate selectors", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db }) => {
        await seedBase(db);
        // unique: by the posts slug unique index.
        const u = await db.query.q06_users.create({ data: { email: "c1@x.com", reviewedPosts: { connect: [{ slug: "orphan-1" }] } } });
        assert.equal(await count(db, "q06_posts", `"slug" = 'orphan-1' and "reviewer_id" = ${u.id}`), 1);
        // missing: precise error.
        await assert.rejects(
          db.query.q06_users.create({ data: { email: "c2@x.com", manager: { connect: { email: "nope@x.com" } } } }),
          (err: unknown) => {
            assert.ok(err instanceof NestedWriteError);
            assert.equal(err.reason, "not-found");
            assert.match((err as Error).message, /connect.*matched no q06_users row/);
            return true;
          },
        );
        // key set that is not a declared unique set.
        await assert.rejects(
          db.query.q06_users.update({ where: { id: 1 }, data: { manager: { connect: { name: "x" } } } }),
          /must name exactly one unique key/,
        );
        // ambiguous: the schema declares email unique but the DATABASE has
        // no unique index on it — two rows match. Actual cardinality
        // enforcement, not metadata trust.
        await db.driver.execute(`insert into "q06_users" ("email") values ('dup@x.com'), ('dup@x.com')`);
        await assert.rejects(
          db.query.q06_users.create({ data: { email: "c3@x.com", manager: { connect: { email: "dup@x.com" } } } }),
          (err: unknown) => {
            assert.ok(err instanceof NestedWriteError);
            assert.equal(err.reason, "cardinality");
            assert.equal(err.rowCount, 2);
            return true;
          },
        );
        assert.equal(await count(db, "q06_users", `"email" = 'c3@x.com'`), 0, "rolled back");
        // duplicate selectors in one edge op target one row twice.
        await assert.rejects(
          db.query.q06_users.create({ data: { email: "c4@x.com", reviewedPosts: { connect: [{ slug: "orphan-2" }, { slug: "orphan-2" }] } } }),
          /the same q06_posts row is targeted by both/,
        );
      });
    });
  }
});

test("live q06: composite keys — PK connect disambiguates, FK pairs propagate, misses are precise", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        await db.driver.execute(`insert into "q06_invoices" ("tenant_id", "id", "amount") values (3, 1, '10.00'), (4, 1, '20.00'), (3, 2, '30.00')`);
        await db.driver.execute(`insert into "q06_lines" ("tenant_id", "invoice_id", "memo") values (3, 2, 'seed')`);
        const before = statements.count;
        // Same local id in two tenants: composite identification is exact.
        await db.query.q06_lines.update({ where: { id: 1 }, data: { invoice: { connect: { tenantId: 4, id: 1 } } } });
        const line1 = await oracle(db, `select "tenant_id", "invoice_id" from "q06_lines" where "id" = 1`);
        assert.deepEqual(line1.map((r) => ({ tenant_id: r.tenant_id, invoice_id: r.invoice_id })), [{ tenant_id: 4, invoice_id: 1 }], "composite connect picked tenant 4's invoice 1");
        // Composite FK propagation through a nested create.
        const created = await db.query.q06_invoices.create({
          data: { tenantId: 7, id: 9, amount: "99.50", lines: { create: [{ memo: "a" }, { memo: "b" }] } },
        });
        assert.equal(created.tenantId, 7);
        assert.equal(created.amount, "99.50");
        const newLines = await oracle(db, `select "tenant_id", "invoice_id", "memo" from "q06_lines" where "memo" in ('a','b') order by "memo"`);
        assert.deepEqual(newLines.map((r) => ({ tenant_id: r.tenant_id, invoice_id: r.invoice_id, memo: r.memo })), [
          { tenant_id: 7, invoice_id: 9, memo: "a" },
          { tenant_id: 7, invoice_id: 9, memo: "b" },
        ]);
        assert.ok(statements.count - before <= 5, "bounded statement count");
        // Composite unique lookup that misses.
        await assert.rejects(
          db.query.q06_lines.update({ where: { id: 1 }, data: { invoice: { connect: { tenantId: 4, id: 999 } } } }),
          (err: unknown) => err instanceof NestedWriteError && err.reason === "not-found",
        );
        // Partial composite key is not a unique key.
        await assert.rejects(
          db.query.q06_lines.update({ where: { id: 1 }, data: { invoice: { connect: { id: 1 } } } }),
          /must name exactly one unique key \(got \{id\}; unique keys: \{tenantId, id\}\)/,
        );
      });
    });
  }
});

test("live q06: cycle with preallocated keys succeeds through a deferred link; unbreakable cycles reject before SQL", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        const before = statements.count;
        // manager_id is nullable: the a->b->c->a manager loop closes through
        // email preallocation (every created row carries its unique key).
        const made = await db.query.q06_users.create({
          data: {
            email: "cyc-a@x.com",
            manager: { create: { email: "cyc-b@x.com", manager: { create: { email: "cyc-c@x.com", manager: { connect: { email: "cyc-a@x.com" } } } } } },
          },
        });
        assert.ok(statements.count - before >= 4, "inserts + at least one deferred link");
        const cycle = await oracle(
          db,
          `select u."email", m."email" as manager from "q06_users" u left join "q06_users" m on m."id" = u."manager_id" where u."email" like 'cyc-%' order by u."email"`,
        );
        assert.deepEqual(cycle.map((r) => ({ email: r.email, manager: r.manager })), [
          { email: "cyc-a@x.com", manager: "cyc-b@x.com" },
          { email: "cyc-b@x.com", manager: "cyc-c@x.com" },
          { email: "cyc-c@x.com", manager: "cyc-a@x.com" },
        ]);
        void made;
        // Without preallocated identity the connect is a plain lookup that
        // finds nothing (serial ids are unknowable in advance).
        await assert.rejects(
          db.query.q06_users.create({
            data: { email: "nc-a@x.com", manager: { create: { email: "nc-b@x.com", manager: { connect: { id: 424242 } } } } },
          }),
          (err: unknown) => err instanceof NestedWriteError && err.reason === "not-found",
        );
        assert.equal(await count(db, "q06_users", `"email" like 'nc-%'`), 0, "nothing half-applied");
        // NOT NULL cycle: rejected before any statement executes.
        const stmtBefore = statements.count;
        await assert.rejects(
          db.query.q06_partners.create({ data: { id: 1, name: "A", partner: { create: { id: 2, name: "B", partner: { connect: { id: 1 } } } } } }),
          /reference cycle that cannot be ordered/,
        );
        assert.equal(statements.count - stmtBefore, 0, "rejected before any statement executed");
        assert.equal(await count(db, "q06_partners"), 0);
      });
    });
  }
});

test("live q06: two-ref shape — created_by and updated_by both to users", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db }) => {
        await seedBase(db);
        const made = await db.query.q06_users.create({
          data: {
            email: "two-ref@x.com",
            createdByUser: { connect: { email: "boss@x.com" } },
            updatedByUser: { create: { email: "system@x.com" } },
          },
        });
        const bossId = (await oracle(db, `select "id" from "q06_users" where "email" = 'boss@x.com'`))[0].id as number;
        const systemId = (await oracle(db, `select "id" from "q06_users" where "email" = 'system@x.com'`))[0].id as number;
        const row = await oracle(db, `select "created_by", "updated_by" from "q06_users" where "email" = 'two-ref@x.com'`);
        assert.deepEqual(row.map((r) => ({ created_by: r.created_by, updated_by: r.updated_by })), [{ created_by: bossId, updated_by: systemId }]);
        assert.equal(made.createdBy, bossId);
        assert.equal(made.updatedBy, systemId);
      });
    });
  }
});

test("live q06: bigint keys connect losslessly (beyond 2^53); dry plans execute nothing", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        const beyond = "9007199254740993"; // 2^53 + 1
        await db.driver.execute(`insert into "q06_wallets" ("id", "label") values (${beyond}, 'w')`);
        const before = statements.count;
        const plan = db.query.q06_moves.explainCreate({ data: { note: "n", wallet: { connect: { id: beyond } } } });
        assert.equal(statements.count - before, 0, "explain executed nothing");
        assert.equal(plan.statementCount, 2);
        await db.query.q06_moves.create({ data: { note: "n", wallet: { connect: { id: beyond } } } });
        const rows = await oracle(db, `select "wallet_id" from "q06_moves" where "note" = 'n'`);
        // The raw driver hands int8 back as its exact decimal text — a JS
        // Number round-trip would have produced 9007199254740992.
        assert.equal(BigInt(String(rows[0].wallet_id)), BigInt(beyond), "int8 key bound exactly, never through JS Number");
      });
    });
  }
});

test("live q06: delete cascades per declared ownership; undeclared dependents abort", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db }) => {
        await seedBase(db);
        const made = await db.query.q06_users.create({
          data: {
            email: "gone@x.com",
            posts: {
              create: [{ slug: "g1", title: "G1", comments: { create: [{ commenterId: 1, body: "x" }] } }, { slug: "g2", title: "G2" }],
            },
          },
        });
        void made;
        // Undeclared dependents abort the whole write (rolled back).
        await assert.rejects(
          db.query.q06_users.delete({ where: { email: "gone@x.com" } }),
          (err: unknown) => {
            assert.ok(err instanceof NestedWriteError);
            assert.equal(err.reason, "undeclared-dependents");
            assert.match((err as Error).message, /no disposition was declared/);
            return true;
          },
        );
        assert.equal(await count(db, "q06_users", `"email" = 'gone@x.com'`), 1, "rollback kept the row");
        assert.equal(await count(db, "q06_posts", `"slug" like 'g%'`), 2);
        // Nested dispositions: posts deleted, their comments deleted too.
        const deleted = await db.query.q06_users.delete({
          where: { email: "gone@x.com" },
          cascade: { posts: { delete: { comments: "delete" } } },
        });
        assert.equal(deleted.email, "gone@x.com", "deleted row returned");
        assert.equal(await count(db, "q06_users", `"email" = 'gone@x.com'`), 0);
        assert.equal(await count(db, "q06_posts", `"slug" like 'g%'`), 0);
        assert.equal(await count(db, "q06_comments", `"body" = 'x'`), 0);
        // Disconnect disposition on a nullable edge keeps the rows.
        await db.query.q06_users.delete({ where: { email: "rev@x.com" }, cascade: { reviewedPosts: "disconnect", posts: "delete" } });
        const orphans = await oracle(db, `select "reviewer_id", "author_id" from "q06_posts" where "slug" = 'orphan-1'`);
        assert.deepEqual(orphans.map((r) => ({ reviewer_id: r.reviewer_id, author_id: r.author_id })), [{ reviewer_id: null, author_id: 1 }], "disconnect kept the row, dropped the edge");
        assert.equal(await count(db, "q06_users", `"email" = 'rev@x.com'`), 0);
      });
    });
  }
});

test("live q06: delete through a mutual-manager data cycle disconnects the survivor, not the root", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db }) => {
        await seedBase(db);
        await db.driver.execute(`insert into "q06_users" ("email", "manager_id") values ('m1@x.com', null), ('m2@x.com', null)`);
        const ids = await oracle(db, `select "id", "email" from "q06_users" where "email" in ('m1@x.com','m2@x.com')`);
        const m1 = ids.find((r) => r.email === "m1@x.com")!.id as number;
        const m2 = ids.find((r) => r.email === "m2@x.com")!.id as number;
        await db.driver.execute(`update "q06_users" set "manager_id" = ${m2} where "id" = ${m1}`);
        await db.driver.execute(`update "q06_users" set "manager_id" = ${m1} where "id" = ${m2}`);
        await db.query.q06_users.delete({ where: { email: "m1@x.com" }, cascade: { posts: "delete", reviewedPosts: "disconnect", reports: "disconnect" } });
        const survivor = await oracle(db, `select "email", "manager_id" from "q06_users" where "email" = 'm2@x.com'`);
        assert.deepEqual(survivor.map((r) => ({ email: r.email, manager_id: r.manager_id })), [{ email: "m2@x.com", manager_id: null }], "the mutual manager edge was disconnected");
        assert.equal(await count(db, "q06_users", `"email" = 'm1@x.com'`), 0);
      });
    });
  }
});

test("live q06: one transaction per write — savepoints inside db.transaction; no implicit retry", async (t) => {
  for (const kind of BOTH) {
    await t.test(kind, async () => {
      await withSuite(kind, async ({ db, statements }) => {
        await seedBase(db);
        // A tx-scoped write joins the caller's transaction.
        const result = await db.transaction(async (tx) => {
          const row = await tx.query.q06_users.create({ data: { email: "tx@x.com" } });
          await tx.insert(posts).values({ authorId: row.id as number, slug: "txp", title: "TxPost" });
          return row.id as number;
        });
        assert.equal(await count(db, "q06_users", `"email" = 'tx@x.com'`), 1);
        assert.equal(await count(db, "q06_posts", `"slug" = 'txp' and "author_id" = ${result}`), 1);
        // A failed write inside a transaction rolls back to its savepoint;
        // the outer transaction continues and commits.
        const outcome = await db.transaction(async (tx) => {
          await tx.insert(comments).values({ postId: 1, commenterId: 1, body: "outer" });
          const failed = await tx.query.q06_users
            .update({ where: { email: "tx@x.com" }, data: { posts: { connect: [{ slug: "no-such" }] } } })
            .then(
              () => "ok",
              (e: unknown) => (e instanceof NestedWriteError ? "rolled-back" : `other:${String(e)}`),
            );
          const stillThere = (await tx.select().from(users)).some((u) => u.email === "tx@x.com");
          return { failed, stillThere };
        });
        assert.equal(outcome.failed, "rolled-back");
        assert.equal(outcome.stillThere, true, "the outer transaction stayed usable after the savepoint rollback");
        assert.equal(await count(db, "q06_comments", `"body" = 'outer'`), 1, "outer work committed");
        // An outer abort rolls the joined write back with it.
        await assert.rejects(
          db.transaction(async (tx) => {
            await tx.query.q06_users.create({ data: { email: "txfail@x.com" } });
            throw new Error("outer abort");
          }),
          /outer abort/,
        );
        assert.equal(await count(db, "q06_users", `"email" = 'txfail@x.com'`), 0);
        // Root-level late unique violation: one transaction, never retried.
        const before = statements.count;
        await assert.rejects(
          db.query.q06_users.create({ data: { email: "dup2@x.com", posts: { create: [{ slug: "orphan-2", title: "dup" }] } } }),
          (err: unknown) => {
            assert.ok(err instanceof ServerSqlError);
            assert.equal((err as ServerSqlError).sqlstate, "23505", "late unique violation");
            return true;
          },
        );
        assert.equal(await count(db, "q06_users", `"email" = 'dup2@x.com'`), 0);
        // Single attempt: exactly one statement succeeded before the late
        // failure (the failing insert emits query-error, not query-end); a
        // retry would have re-run the user insert and counted 2.
        const used = statements.count - before;
        assert.equal(used, 1, `single attempt, one statement in before the failure (got ${used})`);
      });
    });
  }
});
