import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  pgTable,
  serial,
  integer,
  bigint,
  text,
  relations,
  uniqueIndex,
  sql,
  NestedWriteError,
  NeutronSqlError,
  MAX_RELATION_DEPTH,
  type Driver,
  type PinnedExecutor,
  type NestedWritePlan,
  type NeutronDatabase,
} from "./index.js";

// Q06 unit battery: pure dry compilation of nested writes (every planned
// statement, parameter, dependency and edge owner), pre-SQL rejections, and
// the transaction control flow over an in-memory pinned-driver double. The
// double is a CONTROL for runner semantics only (statement order, binding of
// step outputs, rollback/savepoint on failure, no retry); every
// database-facing behavior (constraints, cardinality, rollback of real rows)
// is proven in live.q06.postgres.test.ts.

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  name: text("name").notNull(),
  bestPostId: integer("best_post_id"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  editorId: integer("editor_id"),
  title: text("title").notNull(),
});

const comments = pgTable("comments", {
  id: serial("id").primaryKey(),
  postId: integer("post_id"),
  body: text("body").notNull(),
});

// Composite tenant-local identities; the line's FK is declared in REVERSED
// order relative to the invoice PK (positional zip is the contract).
const invoices = pgTable("invoices", {
  tenantId: integer("tenant_id").primaryKey(),
  id: integer("id").primaryKey(),
  memo: text("memo"),
});

const invoiceLines = pgTable(
  "invoice_lines",
  {
    id: serial("id").primaryKey(),
    tenantId: integer("tenant_id").notNull(),
    invoiceId: integer("invoice_id").notNull(),
    sku: text("sku").notNull(),
    lineNo: integer("line_no").notNull(),
  },
  (t) => [uniqueIndex("invoice_lines_sku_line").on(t.sku, t.lineNo)],
);

const wallets = pgTable("wallets", {
  id: bigint("id").primaryKey(),
  name: text("name").notNull(),
});

const moves = pgTable("moves", {
  id: serial("id").primaryKey(),
  walletId: bigint("wallet_id").notNull(),
  note: text("note"),
});

// NOT NULL self-referencing foreign key: a genuine unbreakable cycle shape
// (every edge of the cycle is required, so no edge can be deferred).
const partners = pgTable("partners", {
  id: integer("id").primaryKey(),
  partnerId: integer("partner_id").notNull(),
  name: text("name").notNull(),
});

const usersRelations = relations(users, ({ one, many }) => ({
  posts: many(posts, { relationName: "author" }),
  edited: many(posts, { relationName: "editor" }),
  bestPost: one(posts, { fields: [users.bestPostId], references: [posts.id], relationName: "best" }),
}));
const postsRelations = relations(posts, ({ one, many }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id], relationName: "author" }),
  editor: one(users, { fields: [posts.editorId], references: [users.id], relationName: "editor" }),
  bestOf: many(users, { relationName: "best" }),
  comments: many(comments),
}));
const commentsRelations = relations(comments, ({ one }) => ({
  post: one(posts, { fields: [comments.postId], references: [posts.id] }),
}));
const invoicesRelations = relations(invoices, ({ many }) => ({ lines: many(invoiceLines) }));
const invoiceLinesRelations = relations(invoiceLines, ({ one }) => ({
  invoice: one(invoices, { fields: [invoiceLines.invoiceId, invoiceLines.tenantId], references: [invoices.id, invoices.tenantId] }),
}));
const walletsRelations = relations(wallets, ({ many }) => ({ moves: many(moves) }));
const movesRelations = relations(moves, ({ one }) => ({
  wallet: one(wallets, { fields: [moves.walletId], references: [wallets.id] }),
}));
const partnersRelations = relations(partners, ({ one }) => ({
  partner: one(partners, { fields: [partners.partnerId], references: [partners.id] }),
}));

const TABLES = { users, posts, comments, invoices, invoice_lines: invoiceLines, wallets, moves, partners };
const RELATIONS = {
  users: usersRelations,
  posts: postsRelations,
  comments: commentsRelations,
  invoices: invoicesRelations,
  invoice_lines: invoiceLinesRelations,
  wallets: walletsRelations,
  moves: movesRelations,
  partners: partnersRelations,
};
type Db = NeutronDatabase<typeof TABLES, typeof RELATIONS>;

// ---------------------------------------------------------------------------
// In-memory pinned-driver double (runner control only)
// ---------------------------------------------------------------------------

interface Executed {
  sql: string;
  params: unknown[];
}

type Responder = (sqlText: string, params: unknown[]) => Array<Record<string, unknown>> | number;

function fakeDriver(respond: Responder): Driver & { log: Executed[]; released: unknown[] } {
  const log: Executed[] = [];
  const released: unknown[] = [];
  const exec = async (sqlText: string, params: unknown[] = []): Promise<Array<Record<string, unknown>> | number> => {
    log.push({ sql: sqlText, params });
    const r = respond(sqlText, params);
    return r;
  };
  const pin: PinnedExecutor = {
    async query<T>(sqlText: string, params?: unknown[]): Promise<T[]> {
      const r = await exec(sqlText, params);
      return (typeof r === "number" ? [] : r) as T[];
    },
    async execute(sqlText: string, params?: unknown[]): Promise<number> {
      const r = await exec(sqlText, params);
      return typeof r === "number" ? r : r.length;
    },
    release(err?: unknown): void {
      released.push(err);
    },
  };
  const driver: Driver = {
    query: pin.query,
    execute: pin.execute,
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return fn(driver);
    },
    async close(): Promise<void> {},
    lifecycle: { ownership: "borrowed", terminated: false, terminate: () => Promise.resolve() },
    pin: () => Promise.resolve(pin),
  };
  return Object.assign(driver, { log, released });
}

/** Default responder: control statements answer 0; inserts/updates with
 *  RETURNING answer one row with sequential ids; plain DML answers 1. */
function defaultResponder(): Responder {
  let nextId = 100;
  return (sqlText) => {
    if (/^(begin|commit|rollback|savepoint|release|rollback to)/.test(sqlText)) return 0;
    if (/ returning /.test(sqlText) || sqlText.startsWith("select")) {
      const id = nextId++;
      return [{ id, email: `u${id}`, name: "n", bestPostId: null, authorId: 1, editorId: null, title: "t", postId: null, body: "b" }];
    }
    return 1;
  };
}

async function makeDb(respond: Responder = defaultResponder()): Promise<{ db: Db; driver: ReturnType<typeof fakeDriver> }> {
  const driver = fakeDriver(respond);
  const db = (await createDatabase({ driver, tables: TABLES, relations: RELATIONS })) as unknown as Db;
  return { db, driver };
}

function shape(plan: NestedWritePlan): Array<[string, string, string, string, readonly number[]]> {
  return plan.steps.map((s) => [s.op, s.action, s.path, s.sql, s.dependsOn]);
}

const ref = (step: number, key: string) => ({ kind: "step-output", step, key });

// ---------------------------------------------------------------------------
// Dry compilation — exact statements (hand-written oracle strings)
// ---------------------------------------------------------------------------

test("explainCreate: root insert, to-many creates at depth 2, to-one connect lookup — every statement exposed in order", async () => {
  const { db, driver } = await makeDb();
  const plan = db.query.users.explainCreate({
    data: {
      email: "a@x",
      name: "A",
      posts: {
        create: [
          { title: "P1", comments: { create: { body: "c1" } } },
          { title: "P2", editor: { connect: { email: "e@x" } } },
        ],
      },
    },
  });
  assert.equal(plan.operation, "create");
  assert.equal(plan.atomic, true);
  assert.equal(plan.statementCount, 5);
  assert.equal(plan.deferredLinks, 0);
  assert.deepEqual(shape(plan), [
    [
      "create", "insert", "users",
      `insert into "users" ("email", "name") values ($1, $2) returning "users"."id", "users"."email", "users"."name", "users"."best_post_id" as "bestPostId"`,
      [],
    ],
    ["create", "insert", "users.posts[0]", `insert into "posts" ("author_id", "title") values ($1, $2) returning "posts"."id"`, [0]],
    ["create", "insert", "users.posts[0].comments[0]", `insert into "comments" ("post_id", "body") values ($1, $2)`, [1]],
    ["connect", "select", "users.posts[1].editor.connect", `select "users"."id" from "users" where ("users"."email" = $1)`, []],
    ["create", "insert", "users.posts[1]", `insert into "posts" ("author_id", "editor_id", "title") values ($1, $2, $3)`, [0, 3]],
  ]);
  assert.deepEqual(plan.steps[0].params, ["a@x", "A"]);
  assert.deepEqual(plan.steps[1].params, [ref(0, "id"), "P1"]);
  assert.deepEqual(plan.steps[2].params, [ref(1, "id"), "c1"]);
  assert.deepEqual(plan.steps[3].params, ["e@x"]);
  assert.deepEqual(plan.steps[4].params, [ref(0, "id"), ref(3, "id"), "P2"]);
  assert.ok(plan.steps.every((s) => s.expect === "exactly-one-row"));
  assert.deepEqual(plan.steps.map((s) => s.returnsResult), [true, false, false, false, false]);
  // Pure: nothing touched the driver.
  assert.equal(driver.log.length, 0);
});

test("explainCreate: edge ownership is explicit — one() owned by the declaring table, many() by the target", async () => {
  const { db } = await makeDb();
  const plan = db.query.posts.explainCreate({
    data: { title: "T", author: { create: { email: "n@x", name: "N" } }, comments: { create: { body: "b" } } },
  });
  assert.deepEqual(shape(plan), [
    ["create", "insert", "posts.author", `insert into "users" ("email", "name") values ($1, $2) returning "users"."id"`, []],
    [
      "create", "insert", "posts",
      `insert into "posts" ("author_id", "title") values ($1, $2) returning "posts"."id", "posts"."author_id" as "authorId", "posts"."editor_id" as "editorId", "posts"."title"`,
      [0],
    ],
    ["create", "insert", "posts.comments[0]", `insert into "comments" ("post_id", "body") values ($1, $2)`, [1]],
  ]);
  assert.deepEqual(plan.steps[0].edge, {
    relation: "author", source: "posts", target: "users", cardinality: "one", owner: "posts", foreignKey: ["author_id"], references: ["id"],
  });
  assert.deepEqual(plan.steps[2].edge, {
    relation: "comments", source: "posts", target: "comments", cardinality: "many", owner: "comments", foreignKey: ["post_id"], references: ["id"],
  });
});

test("explainCreate is independent of data key order (columns schema-ordered, relations declaration-ordered)", async () => {
  const { db } = await makeDb();
  const a = db.query.users.explainCreate({
    data: { email: "a@x", name: "A", bestPost: { connect: { id: 9 } }, posts: { create: { title: "T", editorId: 3 } }, edited: { connect: { id: 4 } } },
  });
  const b = db.query.users.explainCreate({
    data: { edited: { connect: { id: 4 } }, posts: { create: { editorId: 3, title: "T" } }, bestPost: { connect: { id: 9 } }, name: "A", email: "a@x" },
  });
  assert.deepEqual(shape(a), shape(b));
  assert.deepEqual(a.steps.map((s) => s.params), b.steps.map((s) => s.params));
});

test("explainUpdate: lookup captures old FK values, update nulls before delete, to-many ops scoped to the parent", async () => {
  const { db } = await makeDb();
  const plan = db.query.users.explainUpdate({
    where: { email: "a@x" },
    data: {
      name: "B",
      bestPost: { delete: true },
      posts: { connect: [{ id: 3 }], update: { where: { id: 4 }, data: { title: "x" } }, delete: { id: 5 } },
      edited: { disconnect: { id: 7 } },
    },
  });
  assert.deepEqual(shape(plan), [
    ["lookup", "select", "users", `select "users"."best_post_id" as "bestPostId" from "users" where ("users"."email" = $1)`, []],
    [
      "update", "update", "users",
      `update "users" set "name" = $1, "best_post_id" = $2 where ("users"."email" = $3) returning "users"."id", "users"."email", "users"."name", "users"."best_post_id" as "bestPostId"`,
      [0],
    ],
    ["connect", "update", "users.posts.connect[0]", `update "posts" set "author_id" = $1 where ("posts"."id" = $2)`, [1]],
    ["update", "update", "users.posts.update[0]", `update "posts" set "title" = $1 where ("posts"."id" = $2) and ("posts"."author_id" = $3)`, [1]],
    ["delete", "delete", "users.posts.delete[0]", `delete from "posts" where ("posts"."id" = $1) and ("posts"."author_id" = $2)`, [1]],
    ["disconnect", "update", "users.edited.disconnect[0]", `update "posts" set "editor_id" = $1 where ("posts"."id" = $2) and ("posts"."editor_id" = $3)`, [1]],
    ["delete", "delete", "users.bestPost.delete", `delete from "posts" where ("posts"."id" = $1)`, [0, 1]],
  ]);
  assert.deepEqual(plan.steps[1].params, ["B", null, "a@x"]);
  assert.deepEqual(plan.steps[6].params, [ref(0, "bestPostId")]);
  assert.deepEqual(plan.steps[5].params, [null, 7, ref(1, "id")]);
  assert.equal(plan.steps[1].returnsResult, true);
});

test("explainUpdate with only relation operations reads the row through a lookup that returns it", async () => {
  const { db } = await makeDb();
  const plan = db.query.users.explainUpdate({ where: { id: 1 }, data: { posts: { create: { title: "N" } } } });
  assert.deepEqual(shape(plan), [
    [
      "lookup", "select", "users",
      `select "users"."id", "users"."email", "users"."name", "users"."best_post_id" as "bestPostId" from "users" where ("users"."id" = $1)`,
      [],
    ],
    ["create", "insert", "users.posts[0]", `insert into "posts" ("author_id", "title") values ($1, $2)`, [0]],
  ]);
  assert.equal(plan.steps[0].returnsResult, true);
});

test("explainUpdate: to-one update targets the currently connected row through the owner's FK", async () => {
  const { db } = await makeDb();
  const plan = db.query.posts.explainUpdate({ where: { id: 8 }, data: { title: "T2", author: { update: { name: "Renamed" } } } });
  assert.deepEqual(shape(plan), [
    [
      "update", "update", "posts",
      `update "posts" set "title" = $1 where ("posts"."id" = $2) returning "posts"."id", "posts"."author_id" as "authorId", "posts"."editor_id" as "editorId", "posts"."title"`,
      [],
    ],
    ["update", "update", "posts.author.update", `update "users" set "name" = $1 where ("users"."id" = $2)`, [0]],
  ]);
  assert.deepEqual(plan.steps[1].params, ["Renamed", ref(0, "authorId")]);
});

test("composite keys: connect by composite PK, FK columns zip positionally even when declared in reversed order", async () => {
  const { db } = await makeDb();
  const plan = db.query.invoice_lines.explainCreate({ data: { sku: "S", lineNo: 1, invoice: { connect: { tenantId: 2, id: 7 } } } });
  assert.deepEqual(shape(plan), [
    ["connect", "select", "invoice_lines.invoice.connect", `select "invoices"."tenant_id" as "tenantId", "invoices"."id" from "invoices" where ("invoices"."tenant_id" = $1) and ("invoices"."id" = $2)`, []],
    [
      "create", "insert", "invoice_lines",
      `insert into "invoice_lines" ("tenant_id", "invoice_id", "sku", "line_no") values ($1, $2, $3, $4) returning "invoice_lines"."id", "invoice_lines"."tenant_id" as "tenantId", "invoice_lines"."invoice_id" as "invoiceId", "invoice_lines"."sku", "invoice_lines"."line_no" as "lineNo"`,
      [0],
    ],
  ]);
  // tenant_id <- invoices.tenantId, invoice_id <- invoices.id (positional).
  assert.deepEqual(plan.steps[1].params, [ref(0, "tenantId"), ref(0, "id"), "S", 1]);

  const parent = db.query.invoices.explainCreate({ data: { tenantId: 2, id: 7, lines: { create: [{ sku: "A", lineNo: 1 }] } } });
  assert.deepEqual(parent.steps[1].params, [ref(0, "tenantId"), ref(0, "id"), "A", 1]);
  assert.equal(parent.steps[1].sql, `insert into "invoice_lines" ("tenant_id", "invoice_id", "sku", "line_no") values ($1, $2, $3, $4)`);

  // Composite unique index as a selector.
  const upd = db.query.invoices.explainUpdate({
    where: { tenantId: 2, id: 7 },
    data: { lines: { delete: { sku: "A", lineNo: 1 } } },
  });
  assert.equal(upd.steps[1].sql, `delete from "invoice_lines" where ("invoice_lines"."sku" = $1) and ("invoice_lines"."line_no" = $2) and ("invoice_lines"."tenant_id" = $3) and ("invoice_lines"."invoice_id" = $4)`);
  assert.deepEqual(upd.steps[1].params, ["A", 1, ref(0, "tenantId"), ref(0, "id")]);

  assert.throws(
    () => db.query.invoice_lines.explainCreate({ data: { sku: "S", lineNo: 1, invoice: { connect: { id: 7 } } } }),
    /connect on invoices must name exactly one unique key \(got \{id\}; unique keys: \{tenantId, id\}\)/,
  );
});

// ---------------------------------------------------------------------------
// Same-graph references and cycles (preallocated keys)
// ---------------------------------------------------------------------------

test("connect to a row created in the same graph (preallocated key) needs no lookup and orders after the create", async () => {
  const { db } = await makeDb();
  // posts.connect names post 50, which the `edited` edge creates LATER in
  // declaration order: the connect update must be reordered after it.
  const plan = db.query.users.explainCreate({
    data: {
      id: 1,
      email: "u@x",
      name: "U",
      posts: { connect: { id: 50 } },
      edited: { create: { id: 50, title: "Shared", author: { connect: { id: 1 } } } },
    },
  });
  assert.deepEqual(
    plan.steps.map((s) => [s.op, s.path, s.dependsOn]),
    [
      ["create", "users", []],
      ["create", "users.edited[0]", [0]],
      ["connect", "users.posts.connect[0]", [0, 1]],
    ],
  );
  // author.connect {id: 1} resolved to the root insert — no select statement.
  assert.ok(plan.steps.every((s) => s.action !== "select"));
  assert.deepEqual(plan.steps[1].params, [50, ref(0, "id"), ref(0, "id"), "Shared"]);
});

test("a preallocated-key cycle through a NULLABLE owned FK is broken by a link step; the link returns the final row", async () => {
  const { db } = await makeDb();
  const plan = db.query.users.explainCreate({
    data: { id: 10, email: "c@x", name: "C", bestPost: { create: { id: 5, title: "Best", author: { connect: { id: 10 } } } } },
  });
  assert.equal(plan.deferredLinks, 1);
  assert.deepEqual(shape(plan), [
    ["create", "insert", "users", `insert into "users" ("id", "email", "name", "best_post_id") values ($1, $2, $3, $4) returning "users"."id"`, []],
    ["create", "insert", "users.bestPost", `insert into "posts" ("id", "author_id", "title") values ($1, $2, $3) returning "posts"."id"`, [0]],
    [
      "link", "update", "users.bestPost.link",
      `update "users" set "best_post_id" = $1 where ("users"."id" = $2) returning "users"."id", "users"."email", "users"."name", "users"."best_post_id" as "bestPostId"`,
      [0, 1],
    ],
  ]);
  assert.deepEqual(plan.steps[0].params, [10, "c@x", "C", null]);
  assert.deepEqual(plan.steps.map((s) => s.returnsResult), [false, false, true]);
});

test("a cycle whose every edge is NOT NULL is rejected before any statement, naming the cycle", async () => {
  const { db, driver } = await makeDb();
  // partners.partner_id is NOT NULL on BOTH rows: partner 1 needs partner 2's
  // key at insert time and vice versa (the connect matches the same-graph
  // create through its explicit primary key). No edge is nullable, so no
  // deferred link can break the loop.
  const attempt = () =>
    db.query.partners.explainCreate({
      data: { id: 1, name: "A", partner: { create: { id: 2, name: "B", partner: { connect: { id: 1 } } } } },
    });
  assert.throws(attempt, (err: unknown) => {
    assert.ok(err instanceof NeutronSqlError);
    assert.match((err as Error).message, /reference cycle that cannot be ordered/);
    assert.match((err as Error).message, /partners\.partner/);
    return true;
  });
  await assert.rejects(
    db.query.partners.create({
      data: { id: 1, name: "A", partner: { create: { id: 2, name: "B", partner: { connect: { id: 1 } } } } },
    }),
    /reference cycle/,
  );
  assert.equal(driver.log.length, 0, "rejected cycles never open a transaction");
});

test("a same-graph connect under a created target is orderable (not a cycle) when only one path is required", async () => {
  const { db } = await makeDb();
  // posts.author creates user 10 (post 5 needs user 10's key); user 10's
  // posts.connect targets post 5 as a follow-up UPDATE — the loop closes
  // through a separate statement, not through the insert, so the graph has
  // a valid order: user insert, post insert, connect update.
  const plan = db.query.posts.explainCreate({
    data: {
      id: 5,
      title: "T",
      author: { create: { id: 10, email: "z@x", name: "Z", posts: { connect: { id: 5 } } } },
    },
  });
  assert.deepEqual(
    plan.steps.map((s) => [s.op, s.path, s.dependsOn]),
    [
      ["create", "posts.author", []],
      ["create", "posts", [0]],
      ["connect", "posts.author.posts.connect[0]", [0, 1]],
    ],
  );
});

// ---------------------------------------------------------------------------
// Pre-SQL rejections
// ---------------------------------------------------------------------------

test("rejections: unknown keys, disallowed/combined ops, required edges, double assignment, non-unique and null selectors", async () => {
  const { db, driver } = await makeDb();
  const cases: Array<[() => unknown, RegExp]> = [
    [() => db.query.users.explainCreate({ data: { email: "a", name: "A", nope: 1 } as never }), /unknown key "nope" on users/],
    [() => db.query.users.explainCreate({ data: { email: "a", name: "A", posts: { delete: { id: 1 } } } as never }), /disallowed relation operation "delete".*use update/],
    [() => db.query.posts.explainUpdate({ where: { id: 1 }, data: { author: { connect: { id: 1 }, create: { email: "e", name: "n" } } } as never }), /exactly one operation/],
    [() => db.query.users.explainUpdate({ where: { id: 1 }, data: { posts: { disconnect: { id: 3 } } } }), /disconnect on relation "posts" would set NOT NULL foreign-key column\(s\) "author_id"/],
    [() => db.query.posts.explainUpdate({ where: { id: 1 }, data: { author: { delete: true } } }), /delete on relation "author" would set NOT NULL/],
    [() => db.query.posts.explainUpdate({ where: { id: 1 }, data: { author: { disconnect: true } } }), /disconnect on relation "author" would set NOT NULL/],
    [() => db.query.posts.explainCreate({ data: { title: "T", authorId: 1, author: { connect: { id: 2 } } } }), /"authorId" \("author_id"\) on posts is assigned by both data.authorId and relation author/],
    [
      () => db.query.users.explainCreate({ data: { email: "a", name: "A", posts: { create: { title: "T", author: { connect: { id: 2 } } } } } }),
      /assigned by both the parent edge users\.posts and relation author/,
    ],
    [() => db.query.posts.explainCreate({ data: { title: "T" } }), /missing required column\(s\) "authorId"/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: { connect: { name: "x" } } } as never }), /must name exactly one unique key \(got \{name\}; unique keys: \{id\}, \{email\}\)/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: { connect: { id: null } } } as never }), /"id" is null — NULL never identifies a row/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: { connect: { id: 1, email: "e" } } } }), /must name exactly one unique key/],
    [() => db.query.users.explainCreate({ data: { email: "a", name: "A", posts: { create: [] } } }), /create received an empty array/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: { create: [{ email: "e", name: "n" }] } } as never }), /creates exactly one row — pass an object/],
    [() => db.query.users.explainUpdate({ where: { id: 1 }, data: {} }), /update data for users is empty/],
    [() => db.query.users.explainUpdate({ where: { name: "x" } as never, data: { name: "y" } }), /update where on users must name exactly one unique key/],
    [
      () => db.query.users.explainUpdate({ where: { id: 1 }, data: { posts: { update: { where: { id: 3 }, data: { title: "a" } }, delete: { id: 3 } } } }),
      /the same posts row is targeted by both update\[0\] and delete\[0\]/,
    ],
    [
      () => db.query.users.explainCreate({ data: { email: "a", name: "A", posts: { create: [{ id: 3, title: "a" }, { id: 3, title: "b" }] } } }),
      /two creates in one write supply the same primary key on posts/,
    ],
    [() => db.query.users.explainUpdate({ where: { email: "a" }, data: { id: 9, posts: { create: { title: "t" } } } }), /data assigns "id", which relation "posts" references/],
    [() => db.query.posts.explainUpdate({ where: { id: 1 }, data: { authorId: 3, author: { update: { name: "n" } } } }), /"authorId", the foreign key of relation "author"/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: null } } as never), /expects an operations object/],
    [() => db.query.posts.explainCreate({ data: { title: "T", author: {} } } as never), /has no operation/],
    [() => db.query.posts.explainUpdate({ where: { id: 1 }, data: { editor: { disconnect: false } } } as never), /disconnect on a to-one relation takes `true`/],
    [() => db.query.users.explainCreate({ data: { email: 1, name: "A" } } as never), /column "email" \("email"\) on users: text columns accept strings/],
    [() => db.query.users.explainCreate({ data: { email: null, name: "A" } } as never), /null is not allowed for NOT NULL column "email"/],
    [() => db.query.users.explainCreate({ data: { email: "a", name: "A" }, extra: 1 } as never), /unknown argument\(s\) extra/],
  ];
  for (const [fn, re] of cases) assert.throws(fn, re);
  assert.equal(driver.log.length, 0);
});

test("nesting deeper than MAX_RELATION_DEPTH is rejected before SQL", async () => {
  const { db } = await makeDb();
  // posts -> comments -> post -> comments -> ... alternates edges.
  type Nest = Record<string, unknown>;
  let data: Nest = { body: "leaf" };
  for (let i = 0; i < MAX_RELATION_DEPTH + 1; i++) {
    const postData: Nest = { title: `p${i}`, author: { connect: { id: 1 } }, comments: { create: data } };
    data = { body: `c${i}`, post: { create: postData } };
  }
  assert.throws(() => db.query.comments.explainCreate({ data: data as never }), /nesting depth \d+ exceeds the maximum of 5/);
});

// ---------------------------------------------------------------------------
// Execution control flow (in-memory double)
// ---------------------------------------------------------------------------

test("create executes exactly the planned statements, in order, inside one BEGIN/COMMIT, binding step outputs", async () => {
  const { db, driver } = await makeDb();
  const args = {
    data: { email: "a@x", name: "A", posts: { create: [{ title: "P1", comments: { create: { body: "c1" } } }] } },
  } as const;
  const plan = db.query.users.explainCreate(args);
  const row = await db.query.users.create(args);
  assert.deepEqual(
    driver.log.map((e) => e.sql),
    ["begin", ...plan.steps.map((s) => s.sql), "commit"],
  );
  // Root insert answered id 100, post insert 101: bound into the children.
  assert.deepEqual(driver.log[2].params, [100, "P1"]);
  assert.deepEqual(driver.log[3].params, [101, "c1"]);
  assert.equal(row.id, 100);
  assert.deepEqual(driver.released, [undefined]);
});

test("a step matching zero rows throws NestedWriteError, rolls back, never commits and is not retried", async () => {
  const base = defaultResponder();
  const { db, driver } = await makeDb((sqlText, params) => (sqlText.startsWith(`update "posts"`) ? 0 : base(sqlText, params)));
  await assert.rejects(
    db.query.users.update({ where: { id: 1 }, data: { name: "B", posts: { connect: { id: 42 } } } }),
    (err: unknown) => {
      assert.ok(err instanceof NestedWriteError);
      assert.equal(err.reason, "not-found");
      assert.equal(err.op, "connect");
      assert.equal(err.path, "users.posts.connect[0]");
      assert.equal(err.rowCount, 0);
      return true;
    },
  );
  const sqls = driver.log.map((e) => e.sql);
  assert.equal(sqls[0], "begin");
  assert.equal(sqls.at(-1), "rollback");
  assert.ok(!sqls.includes("commit"));
  assert.equal(sqls.filter((s) => s === "begin").length, 1, "no automatic retry");
});

test("a step matching several rows (schema claims unique, database does not enforce it) rolls back with reason cardinality", async () => {
  const base = defaultResponder();
  const { db, driver } = await makeDb((sqlText, params) => (sqlText.startsWith(`update "posts"`) ? 2 : base(sqlText, params)));
  await assert.rejects(
    db.query.users.update({ where: { id: 1 }, data: { posts: { connect: { id: 42 } } } }),
    (err: unknown) => err instanceof NestedWriteError && err.reason === "cardinality" && err.rowCount === 2,
  );
  assert.equal(driver.log.at(-1)?.sql, "rollback");
});

test("to-one update through a NULL foreign key fails as no-connected-row before running the statement", async () => {
  const base = defaultResponder();
  const { db, driver } = await makeDb((sqlText, params) => {
    if (sqlText.startsWith(`update "posts"`)) return [{ id: 8, authorId: 1, editorId: null, title: "t" }];
    return base(sqlText, params);
  });
  await assert.rejects(
    db.query.posts.update({ where: { id: 8 }, data: { title: "x", editor: { update: { name: "n" } } } }),
    (err: unknown) => err instanceof NestedWriteError && err.reason === "no-connected-row",
  );
  assert.ok(!driver.log.some((e) => e.sql.startsWith(`update "users"`)));
  assert.equal(driver.log.at(-1)?.sql, "rollback");
});

test("inside db.transaction a nested write runs in a savepoint: a failure rolls back to it and the outer transaction continues", async () => {
  const base = defaultResponder();
  const { db, driver } = await makeDb((sqlText, params) => (sqlText.startsWith(`update "posts"`) ? 0 : base(sqlText, params)));
  const outcome = await db.transaction(async (tx) => {
    await tx.insert(comments).values({ body: "outer" });
    const failed = await tx.query.users.update({ where: { id: 1 }, data: { posts: { connect: { id: 42 } } } }).then(
      () => "ok",
      (e: unknown) => (e instanceof NestedWriteError ? "rolled-back" : "other"),
    );
    return failed;
  });
  assert.equal(outcome, "rolled-back");
  const sqls = driver.log.map((e) => e.sql);
  assert.equal(sqls[0], "begin");
  assert.ok(sqls.includes(`savepoint "neutron_sp_1"`));
  assert.ok(sqls.includes(`rollback to savepoint "neutron_sp_1"`));
  assert.equal(sqls.at(-1), "commit");
  await assert.rejects(
    db.transaction((tx) => tx.query.users.create({ data: { email: "a", name: "b" } }, { isolation: "serializable" })),
    /isolation is a property of the outer BEGIN/,
  );
});

test("validation errors never touch the connection; isolation renders into BEGIN", async () => {
  const { db, driver } = await makeDb();
  await assert.rejects(db.query.posts.create({ data: { title: "T" } }), /missing required column/);
  assert.equal(driver.log.length, 0);
  await db.query.users.create({ data: { email: "a", name: "b" } }, { isolation: "serializable" });
  assert.equal(driver.log[0].sql, "begin isolation level serializable");
});

test("int8 keys are captured losslessly and re-bound exactly (never through JS Number)", async () => {
  const { db, driver } = await makeDb((sqlText) => {
    if (/^(begin|commit|rollback)/.test(sqlText)) return 0;
    if (sqlText.startsWith(`insert into "wallets"`)) return [{ id: "9007199254740993", name: "W" }];
    return 1;
  });
  const plan = db.query.wallets.explainCreate({ data: { id: "9007199254740993", name: "W", moves: { create: [{ note: "a" }] } } });
  assert.deepEqual(plan.steps[1].params, [ref(0, "id"), "a"]);
  const row = await db.query.wallets.create({ data: { id: "9007199254740993", name: "W", moves: { create: [{ note: "a" }] } } });
  assert.equal(row.id, 9007199254740993n);
  const moveInsert = driver.log.find((e) => e.sql.startsWith(`insert into "moves"`));
  assert.deepEqual(moveInsert?.params, [9007199254740993n, "a"]);
});

test("update data accepts structural expressions like update().set()", async () => {
  const { db } = await makeDb();
  const plan = db.query.posts.explainUpdate({ where: { id: 1 }, data: { title: sql`upper(${posts.title})` } });
  assert.equal(plan.steps[0].sql.startsWith(`update "posts" set "title" = upper("posts"."title") where ("posts"."id" = $1)`), true);
});

// ---------------------------------------------------------------------------
// Delete (root) — predicate-driven dependent handling
// ---------------------------------------------------------------------------

test("explainDelete: lookup, scans per declared many edge in declaration order, mutations deepest first, root delete last", async () => {
  const { db, driver } = await makeDb();
  const plan = db.query.users.explainDelete({
    where: { email: "a@x" },
    cascade: { posts: { delete: { comments: "delete" } } },
  });
  assert.equal(plan.operation, "delete");
  assert.equal(plan.atomic, true);
  assert.equal(plan.statementCount, plan.steps.length);
  assert.deepEqual(
    plan.steps.map((s) => [s.op, s.path, s.table]),
    [
      ["lookup", "users", "users"],
      ["scan", "users.posts.scan", "posts"],
      ["scan", "posts.bestOf.scan", "users"],
      ["scan", "posts.comments.scan", "comments"],
      ["delete", "posts.comments.delete", "comments"],
      ["delete", "users.posts.delete", "posts"],
      ["scan", "users.edited.scan", "posts"],
      ["delete", "users", "users"],
    ],
  );
  assert.deepEqual(plan.steps[0].sql, `select "users"."id" from "users" where ("users"."email" = $1)`);
  assert.deepEqual(plan.steps[0].params, ["a@x"]);
  // Level-2 predicates correlate through a DISTINCTLY ALIASED exists; a
  // child table matching the deletion root excludes the root row (data
  // cycles through the root never double-count it).
  assert.equal(
    plan.steps[3].sql,
    `select 1 as "matched" from "comments" where exists((select 1 from "posts" as "__np2" where ("__np2"."author_id" = $1) and ("__np2"."id" = "comments"."post_id")))`,
  );
  assert.deepEqual(plan.steps[3].params, [ref(0, "id")]);
  assert.equal(
    plan.steps[4].sql,
    `delete from "comments" where exists((select 1 from "posts" as "__np2" where ("__np2"."author_id" = $1) and ("__np2"."id" = "comments"."post_id")))`,
  );
  assert.deepEqual(plan.steps[4].expect, { affectedMatchesStep: 3 });
  assert.equal(
    plan.steps[2].sql,
    `select 1 as "matched" from "users" where exists((select 1 from "posts" as "__np1" where ("__np1"."author_id" = $1) and ("__np1"."id" = "users"."best_post_id"))) and (not ("users"."id" = $2))`,
  );
  assert.deepEqual(plan.steps[2].params, [ref(0, "id"), ref(0, "id")]);
  assert.deepEqual(plan.steps[1].expect, "zero-or-more-rows");
  assert.equal(
    plan.steps[7].sql,
    `delete from "users" where ("users"."id" = $1) returning "users"."id", "users"."email", "users"."name", "users"."best_post_id" as "bestPostId"`,
  );
  assert.deepEqual(plan.steps[7].expect, "exactly-one-row");
  assert.equal(plan.steps.at(-1)?.returnsResult, true);
  assert.deepEqual(plan.steps[7].dependsOn, [0, 1, 2, 3, 4, 5, 6]);
  assert.equal(driver.log.length, 0, "pure — nothing executed");
});

test("explainDelete: disconnect nulls nullable dependents after its scan", async () => {
  const { db } = await makeDb();
  const plan = db.query.users.explainDelete({ where: { id: 4 }, cascade: { edited: "disconnect" } });
  assert.deepEqual(
    plan.steps.map((s) => [s.op, s.path]),
    [
      ["lookup", "users"],
      ["scan", "users.posts.scan"],
      ["scan", "users.edited.scan"],
      ["disconnect", "users.edited.disconnect"],
      ["delete", "users"],
    ],
  );
  assert.equal(plan.steps[3].sql, `update "posts" set "editor_id" = $1 where ("posts"."editor_id" = $2)`);
  assert.deepEqual(plan.steps[3].params, [null, ref(0, "id")]);
  assert.deepEqual(plan.steps[3].expect, { affectedMatchesStep: 2 });
});

test("explainDelete: a self many() edge excludes the deletion root from its own dependents scan", async () => {
  const crews = pgTable("crews", {
    id: serial("id").primaryKey(),
    leadId: integer("lead_id"),
  });
  const crewsRelations = relations(crews, ({ one, many }) => ({
    lead: one(crews, { fields: [crews.leadId], references: [crews.id], relationName: "crew" }),
    members: many(crews, { relationName: "crew" }),
  }));
  const db2 = (await createDatabase({
    driver: fakeDriver(defaultResponder()),
    tables: { crews },
    relations: { crews: crewsRelations },
  })) as unknown as NeutronDatabase<{ crews: typeof crews }, { crews: typeof crewsRelations }>;
  const plan = db2.query.crews.explainDelete({ where: { id: 1 }, cascade: { members: "disconnect" } });
  assert.equal(
    plan.steps[1].sql,
    `select 1 as "matched" from "crews" where ("crews"."lead_id" = $1) and (not ("crews"."id" = $2))`,
  );
  assert.equal(plan.steps[2].sql, `update "crews" set "lead_id" = $1 where ("crews"."lead_id" = $2) and (not ("crews"."id" = $3))`);
});

test("delete rejections: unknown relation, to-one key, bad disposition, disconnect on a required FK, non-unique where, unknown args", async () => {
  const { db, driver } = await makeDb();
  const cases: Array<[() => unknown, RegExp]> = [
    [() => db.query.users.explainDelete({ where: { id: 1 }, cascade: { nope: "delete" } as never }), /unknown relation "nope"/],
    [() => db.query.users.explainDelete({ where: { id: 1 }, cascade: { bestPost: "delete" } as never }), /dispositions apply to dependent \(to-many\) edges — "bestPost" is a to-one relation/],
    [() => db.query.users.explainDelete({ where: { id: 1 }, cascade: { posts: "nuke" } as never }), /must be "disconnect", "delete" or \{ delete: \{\.\.\.\} \}/],
    [() => db.query.users.explainDelete({ where: { id: 1 }, cascade: { posts: { delete: 5 } } as never }), /delete takes a nested dispositions object/],
    [() => db.query.users.explainDelete({ where: { id: 1 }, cascade: { posts: "disconnect" } }), /disconnect on relation "posts" would set NOT NULL foreign-key column\(s\) "author_id"/],
    [() => db.query.users.explainDelete({ where: { name: "x" } as never }), /delete where on users must name exactly one unique key/],
    [() => db.query.users.explainDelete({ where: { id: 1 }, extra: true } as never), /unknown argument\(s\) extra/],
  ];
  for (const [fn, re] of cases) assert.throws(fn, re);
  assert.equal(driver.log.length, 0);
});

const orchards = pgTable("orchards", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
});
const trees = pgTable("trees", {
  id: serial("id").primaryKey(),
  orchardId: integer("orchard_id").notNull(),
  kind: text("kind").notNull(),
});
const apples = pgTable("apples", {
  id: serial("id").primaryKey(),
  treeId: integer("tree_id").notNull(),
});
const orchardsRelations = relations(orchards, ({ many }) => ({ trees: many(trees) }));
const treesRelations = relations(trees, ({ one, many }) => ({
  orchard: one(orchards, { fields: [trees.orchardId], references: [orchards.id] }),
  apples: many(apples),
}));
const applesRelations = relations(apples, ({ one }) => ({ tree: one(trees, { fields: [apples.treeId], references: [trees.id] }) }));

test("delete executes its plan inside one transaction; undeclared dependents and diverging scans roll back", async () => {
  const makeOrchardDb = async (respond: Responder) => {
    const driver = fakeDriver(respond);
    const db = (await createDatabase({
      driver,
      tables: { orchards, trees, apples },
      relations: { orchards: orchardsRelations, trees: treesRelations, apples: applesRelations },
    })) as unknown as NeutronDatabase<
      { orchards: typeof orchards; trees: typeof trees; apples: typeof apples },
      { orchards: typeof orchardsRelations; trees: typeof treesRelations; apples: typeof applesRelations }
    >;
    return { db, driver };
  };
  const base = defaultResponder();
  const graph = (scanRows: number, mutateCount?: number): Responder => (sqlText, params) => {
    if (sqlText.startsWith("select 1")) return Array.from({ length: scanRows }, () => ({ matched: 1 }));
    if (mutateCount !== undefined && (sqlText.startsWith("delete from") || sqlText.startsWith(`update`)) && !/returning/.test(sqlText)) {
      return mutateCount;
    }
    return base(sqlText, params);
  };
  {
    const { db, driver } = await makeOrchardDb(graph(0, 0));
    const plan = db.query.orchards.explainDelete({ where: { id: 9 }, cascade: { trees: { delete: { apples: "delete" } } } });
    const row = await db.query.orchards.delete({ where: { id: 9 }, cascade: { trees: { delete: { apples: "delete" } } } });
    assert.deepEqual(
      driver.log.map((e) => e.sql),
      ["begin", ...plan.steps.map((s) => s.sql), "commit"],
    );
    // The lookup select consumed id 100; the root DELETE ... RETURNING row
    // (the result) is the responder's next id.
    assert.equal(row.id, 101);
  }
  {
    // No dispositions declared: the trees scan finds rows and aborts.
    const { db, driver } = await makeOrchardDb(graph(2));
    await assert.rejects(
      db.query.orchards.delete({ where: { id: 9 } }),
      (err: unknown) => {
        assert.ok(err instanceof NestedWriteError);
        assert.equal(err.reason, "undeclared-dependents");
        assert.equal(err.rowCount, 2);
        return true;
      },
    );
    const sqls = driver.log.map((e) => e.sql);
    assert.equal(sqls.at(-1), "rollback");
    assert.ok(!sqls.includes("commit"));
  }
  {
    const { db, driver } = await makeOrchardDb(graph(2, 3));
    await assert.rejects(
      db.query.orchards.delete({ where: { id: 9 }, cascade: { trees: { delete: { apples: "delete" } } } }),
      (err: unknown) => err instanceof NestedWriteError && err.reason === "row-set-changed",
    );
    assert.equal(driver.log.at(-1)?.sql, "rollback");
  }
});

// ---------------------------------------------------------------------------
// Compile-time surface (checked by tsc over this file)
// ---------------------------------------------------------------------------

test("typed surface: relation operations, selectors and values are checked at compile time", async () => {
  const { db } = await makeDb();
  const typed = async (): Promise<void> => {
    const created = await db.query.users.create({ data: { email: "e", name: "n", posts: { create: [{ title: "t" }] } } });
    const id: number = created.id;
    const best: number | null = created.bestPostId;
    void id;
    void best;
    const gone = await db.query.users.delete({ where: { email: "e" }, cascade: { posts: { delete: { comments: "delete" } } } });
    const goneName: string = gone.name;
    void goneName;
    // @ts-expect-error cascade keys are declared relation names
    await db.query.users.delete({ where: { id: 1 }, cascade: { nope: "delete" } });
    // @ts-expect-error to-one relations take no disposition
    await db.query.users.delete({ where: { id: 1 }, cascade: { bestPost: "delete" } });
    // @ts-expect-error dispositions are the declared vocabulary
    await db.query.users.delete({ where: { id: 1 }, cascade: { posts: "nuke" } });
    // @ts-expect-error delete requires where
    await db.query.users.delete({});
    // @ts-expect-error unknown relation operation
    await db.query.users.create({ data: { email: "e", name: "n", posts: { upsert: {} } } });
    // @ts-expect-error update-only operations are not offered on create
    await db.query.users.create({ data: { email: "e", name: "n", posts: { delete: { id: 1 } } } });
    // @ts-expect-error unknown column in data
    await db.query.users.create({ data: { email: "e", name: "n", nickname: "x" } });
    // @ts-expect-error wrong value type
    await db.query.users.create({ data: { email: 1, name: "n" } });
    // @ts-expect-error a to-one create takes one object
    await db.query.posts.create({ data: { title: "t", author: { create: [{ email: "e", name: "n" }] } } });
    // @ts-expect-error to-one disconnect takes `true`
    await db.query.posts.update({ where: { id: 1 }, data: { editor: { disconnect: false } } });
    // @ts-expect-error selector keys are the target's columns
    await db.query.posts.create({ data: { title: "t", author: { connect: { handle: "x" } } } });
    // @ts-expect-error update requires where
    await db.query.users.update({ data: { name: "x" } });
  };
  void typed;
});
