import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  integer,
  text,
  varchar,
  boolean,
  timestamp,
  timestamptz,
  date,
  bigint,
  numeric,
  double,
  bytea,
  jsonb,
  uuid,
  index,
  uniqueIndex,
  relations,
  and,
  or,
  not,
  eq,
  gt,
  lt,
  inArray,
  asc,
  desc,
  sql,
  raw,
  astSelect,
  trustSql,
  TRUSTED_SQL_ACK,
  schemaToDDL,
  createTableSQL,
  getTableName,
  getTableColumns,
  getTableIndexes,
  isPgTable,
  exportTable,
  compileStatement,
  selectStatement,
  ident,
  TABLE_SYMBOL,
  type AnyColumnBuilder,
  type ColumnBuilder,
  type Condition,
  type Relation,
} from "./index.js";
import { buildRelationalSQL, resolveRelations } from "./relations.js";
import type { RQBArgs } from "./relations.js";
import type { PgTableCore, TableRelations } from "./schema.js";
import { createDatabase, type RelationChildModelOf } from "./db.js";

// Schema used across the snapshot suite — mirrors the acceptance brief's
// users + posts example.
const users = pgTable(
  "users",
  {
    id: serial("id").primaryKey(),
    email: varchar("email", 255).notNull().unique(),
    name: text("name"),
    active: boolean("active").notNull().default(true),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [index("users_email_idx").on(t.email), uniqueIndex("users_active_idx").on(t.active)],
);

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
  body: text("body"),
  published: boolean("published").notNull().default(false),
});

const usersRelations = relations(users, ({ many }) => ({
  posts: many(posts),
}));

const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

// Snapshot tests never execute; postgres.js connects lazily so the URL is inert.
const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { users, posts },
  relations: { users: usersRelations, posts: postsRelations },
});

test("DDL: users table snapshot", () => {
  assert.equal(
    createTableSQL(users),
    [
      'create table "users" (',
      '  "id" serial primary key,',
      '  "email" varchar(255) not null unique,',
      '  "name" text,',
      '  "active" boolean not null default true,',
      '  "created_at" timestamp not null default now()',
      ")",
    ].join("\n"),
  );
});

test("DDL: schemaToDDL orders posts after users and emits indexes", () => {
  const ddl = schemaToDDL([posts, users]);
  assert.ok(ddl[0].startsWith('create table "users"'));
  assert.equal(ddl[1], 'create index "users_email_idx" on "users" ("email")');
  assert.equal(ddl[2], 'create unique index "users_active_idx" on "users" ("active")');
  assert.ok(ddl[3].startsWith('create table "posts"'));
  assert.ok(ddl[3].includes('references "users" ("id") on delete cascade'));
});

test("select: plain, where, order, limit, offset", () => {
  const plain = db.select().from(users).toSQL();
  assert.equal(
    plain.sql,
    'select "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt" from "users"',
  );
  assert.deepEqual(plain.params, []);

  const filtered = db.select().from(users)
    .where(and(eq(users.id, 1, "users"), gt(users.id, 0, "users")))
    .orderBy(desc(users.createdAt, "users"), asc(users.id, "users"))
    .limit(10)
    .offset(20)
    .toSQL();
  assert.equal(
    filtered.sql,
    'select "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt" from "users" where ("users"."id" = $1) and ("users"."id" > $2) order by "users"."created_at" desc, "users"."id" asc limit 10 offset 20',
  );
  assert.deepEqual(filtered.params, [1, 0]);
});

test("select: projection with raw fragment", () => {
  const q = db.select({ email: users.email, n: sql`count(*)` }).from(users).toSQL();
  assert.equal(q.sql, 'select "users"."email", count(*) as "n" from "users"');
});

test("select: or + inArray param renumbering", () => {
  const q = db.select().from(users)
    .where(or(eq(users.id, 5, "users"), inArray(users.email, ["a@x.com", "b@x.com"], "users")))
    .toSQL();
  assert.equal(q.sql, 'select "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt" from "users" where (("users"."id" = $1) or ("users"."email" in ($2, $3)))');
  assert.deepEqual(q.params, [5, "a@x.com", "b@x.com"]);
});

test("select: aliased projection labels columns with the requested key", () => {
  const q = db.select({ displayName: users.email, createdAt: users.createdAt }).from(users).toSQL();
  assert.equal(q.sql, 'select "users"."email" as "displayName", to_jsonb("users"."created_at")::text as "createdAt" from "users"');
});

test("select: property keys differing from physical names alias by metadata", () => {
  const mapped = pgTable("mapped", {
    firstName: text("first_name").notNull(),
    first_name: text("firstName").notNull(),
  });
  const q = db.select().from(mapped).toSQL();
  assert.equal(q.sql, 'select "mapped"."first_name" as "firstName", "mapped"."firstName" as "first_name" from "mapped"');
});

test("insert: single and multi-row with returning", () => {
  const single = db.insert(users).values({ email: "a@x.com", name: "A" }).returning().toSQL();
  assert.equal(
    single.sql,
    'insert into "users" ("email", "name") values ($1, $2) returning "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt"',
  );
  assert.deepEqual(single.params, ["a@x.com", "A"]);

  const multi = db.insert(posts)
    .values([
      { userId: 1, title: "p1" },
      { userId: 1, title: "p2" },
    ])
    .toSQL();
  assert.equal(multi.sql, 'insert into "posts" ("user_id", "title") values ($1, $2), ($3, $4)');
  assert.deepEqual(multi.params, [1, "p1", 1, "p2"]);
});

test("insert without values throws", () => {
  assert.throws(() => db.insert(users).toSQL(), /requires \.values/);
});

test("insert: empty array is rejected distinctly from missing values", async () => {
  assert.throws(() => db.insert(users).values([]).toSQL(), /empty array/);
  await assert.rejects(() => Promise.resolve(db.insert(users).values([])), /empty array/);
});

test("insert: reordered keys bind by key against schema-ordered columns", () => {
  const q = db.insert(users)
    .values([
      { email: "a@x.com", name: "A" },
      { name: "B", email: "b@x.com" },
    ])
    .toSQL();
  assert.equal(q.sql, 'insert into "users" ("email", "name") values ($1, $2), ($3, $4)');
  assert.deepEqual(q.params, ["a@x.com", "A", "b@x.com", "B"]);
});

test("insert: sparse rows use default for missing and undefined keys", () => {
  const q = db.insert(users)
    .values([
      { email: "a@x.com", name: "A" },
      { email: "b@x.com" },
      { email: "c@x.com", name: undefined },
    ])
    .toSQL();
  assert.equal(q.sql, 'insert into "users" ("email", "name") values ($1, $2), ($3, default), ($4, default)');
  assert.deepEqual(q.params, ["a@x.com", "A", "b@x.com", "c@x.com"]);
});

test("insert: explicit null is a bound NULL, never default", () => {
  const q = db.insert(users).values({ email: "a@x.com", name: null }).toSQL();
  assert.equal(q.sql, 'insert into "users" ("email", "name") values ($1, $2)');
  assert.deepEqual(q.params, ["a@x.com", null]);
});

test("insert: column list is schema-ordered regardless of first-row key order", () => {
  const q = db.insert(users).values({ name: "A", email: "a@x.com" }).toSQL();
  assert.equal(q.sql, 'insert into "users" ("email", "name") values ($1, $2)');
  assert.deepEqual(q.params, ["a@x.com", "A"]);
});

test("insert: default-only rows compile to valid syntax", () => {
  const allDefaults = pgTable("all_defaults", {
    id: serial("id").primaryKey(),
    label: text("label").notNull().default("dflt"),
  });
  const single = db.insert(allDefaults).values({}).toSQL();
  assert.equal(single.sql, 'insert into "all_defaults" default values');
  assert.deepEqual(single.params, []);

  const multi = db.insert(allDefaults).values([{}, {}]).toSQL();
  assert.equal(multi.sql, 'insert into "all_defaults" ("id") values (default), (default)');
  assert.deepEqual(multi.params, []);
});

test("insert: unknown key rejected in any row before execution", () => {
  // Excess keys are caught at compile time on literals; these runtime checks
  // cover values that arrive without static types.
  assert.throws(() => db.insert(users).values({ email: "a@x.com", nope: 1 } as never).toSQL(), /unknown column "nope"/);
  assert.throws(
    () => db.insert(users).values([{ email: "a@x.com" }, { email: "b@x.com", nope: 1 } as never]).toSQL(),
    /unknown column "nope"/,
  );
  assert.throws(() => db.insert(users).values([{ email: "a@x.com" }, { toString: 1 } as never]).toSQL(), /unknown column "toString"/);
});

test("insert: unbindable values are rejected before execution", () => {
  assert.throws(() => db.insert(users).values({ email: "a@x.com", name: Symbol("s") as never }).toSQL(), /symbol/);
  assert.throws(() => db.insert(users).values({ email: "a@x.com", name: (() => 1) as never }).toSQL(), /function/);
  assert.throws(() => db.insert(users).values("not an object" as never).toSQL(), /must be objects/);
});

test("insert: nested object values rejected before execution with column context", () => {
  assert.throws(
    () => db.insert(users).values({ email: "a@x.com", name: { nested: true } as never }).toSQL(),
    /column "name" \("name"\) on users: text columns accept strings/,
  );
  // B01 review minor 2: pg silently JSON-stringifies and postgres.js stores
  // "[object Object]" — both must now fail before any driver call.
  const b = db.insert(users).values({ email: "a@x.com", active: { nested: true } as never });
  assert.throws(() => b.toSQL(), /column "active" \("active"\) on users: boolean columns accept booleans/);
  assert.throws(() => db.insert(users).values({ email: "a@x.com", name: [1, 2] as never }).toSQL(), /column "name" \("name"\) on users: text columns accept strings/);
});

test("insert: inherited (prototype-chain) values bind DEFAULT, not the inherited value", () => {
  // B01 review minor 1: cells must read own properties only.
  const row2 = Object.create({ name: "inherited" }) as Record<string, unknown>;
  row2.email = "b@x.com";
  const q = db.insert(users).values([{ email: "a@x.com", name: "own" }, row2 as never]).toSQL();
  assert.equal(q.sql, 'insert into "users" ("email", "name") values ($1, $2), ($3, default)');
  assert.deepEqual(q.params, ["a@x.com", "own", "b@x.com"]);
});

test("insert: missing required keys rejected before execution", () => {
  // email is NOT NULL without default; id is serial (server default); the
  // rest have defaults or are nullable.
  assert.throws(() => db.insert(users).values({} as never).toSQL(), /missing required column\(s\) "email" \("email"\)/);
  assert.throws(() => db.insert(users).values({ name: "A" } as never).toSQL(), /missing required column/);
  assert.throws(
    () => db.insert(users).values([{ email: "a@x.com" }, { name: "B" } as never]).toSQL(),
    /row 1 is missing required column\(s\) "email"/,
  );
  // Explicit undefined counts as missing (it requests DEFAULT).
  assert.throws(() => db.insert(users).values({ email: undefined } as never).toSQL(), /NOT NULL without a default/);
  // NOT NULL WITH a default (active) is not required.
  const ok = db.insert(users).values({ email: "a@x.com" }).toSQL();
  assert.equal(ok.sql, 'insert into "users" ("email") values ($1)');
});

test("insert: integer primary key without default is required; serial is not", () => {
  const tenant = pgTable("tenants", {
    id: integer("id").primaryKey(),
    label: text("label").notNull().default("l"),
  });
  assert.throws(() => db.insert(tenant).values({ label: "x" } as never).toSQL(), /missing required column\(s\) "id" \("id"\)/);
  assert.doesNotThrow(() => db.insert(tenant).values({ id: 1 } as never).toSQL());
});

test("insert: null for NOT NULL columns rejected before execution", () => {
  assert.throws(
    () => db.insert(users).values({ email: "a@x.com", active: null } as never).toSQL(),
    /null is not allowed for NOT NULL column "active" \("active"\)/,
  );
  // A primary key column (integer, no default) rejects null too.
  const tenant = pgTable("tenants2", {
    id: integer("id").primaryKey(),
    label: text("label").notNull().default("l"),
  });
  assert.throws(
    () => db.insert(tenant).values({ id: null, label: "x" } as never).toSQL(),
    /null is not allowed for NOT NULL column "id" \("id"\)/,
  );
  // Nullable columns still accept null.
  assert.doesNotThrow(() => db.insert(users).values({ email: "a@x.com", name: null }).toSQL());
});

test("update: set, where, fragment set, returning", () => {
  const q = db.update(users).set({ name: "B", active: sql`not ${true}` }).where(eq(users.id, 1)).returning().toSQL();
  assert.equal(
    q.sql,
    'update "users" set "name" = $1, "active" = not $2 where ("users"."id" = $3) returning "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt"',
  );
  assert.deepEqual(q.params, ["B", true, 1]);
});

test("update: property keys map to physical column names in assignments", () => {
  const people = pgTable("people_map", {
    id: serial("id").primaryKey(),
    firstName: text("first_name").notNull(),
    nick: text("nick"),
  });
  const q = db.update(people).set({ firstName: "Ada", nick: null }).where(eq(people.id, 1)).toSQL();
  assert.equal(q.sql, 'update "people_map" set "first_name" = $1, "nick" = $2 where ("people_map"."id" = $3)');
  assert.deepEqual(q.params, ["Ada", null, 1]);
});

test("update: unknown set key rejected; null/undefined/fragment order", () => {
  const people = pgTable("people_map2", {
    id: serial("id").primaryKey(),
    firstName: text("first_name").notNull(),
    nick: text("nick"),
  });
  assert.throws(() => db.update(people).set({ firstName: "x", nope: 1 } as never), /unknown column "nope" on people_map2/);
  // null for NOT NULL rejected before execution, nullable null binds.
  assert.throws(() => db.update(people).set({ firstName: null } as never), /null is not allowed for NOT NULL column "firstName"/);
  assert.doesNotThrow(() => db.update(people).set({ nick: null }));
  // undefined keys are ignored entirely (never a bound value).
  const q = db.update(people).set({ nick: undefined, firstName: "X" }).where(eq(people.id, 1)).toSQL();
  assert.equal(q.sql, 'update "people_map2" set "first_name" = $1 where ("people_map2"."id" = $2)');
  // An update whose .set() leaves no assignments is an error.
  assert.throws(() => db.update(people).set({ nick: undefined }).where(eq(people.id, 1)).toSQL(), /no assignments/);
  // Fragments keep working in assignments (B01-supported API).
  const frag = db.update(people).set({ firstName: sql`upper(${"bob"})` }).where(eq(people.id, 1)).toSQL();
  assert.equal(frag.sql, 'update "people_map2" set "first_name" = upper($1) where ("people_map2"."id" = $2)');
});

test("update: invalid set values rejected before execution with column context", () => {
  const people = pgTable("people_map3", {
    id: serial("id").primaryKey(),
    firstName: text("first_name").notNull(),
    meta: jsonb("meta"),
  });
  assert.throws(() => db.update(people).set({ firstName: { nested: true } as never }), /column "firstName" \("first_name"\) on people_map3: text columns accept strings/);
  assert.throws(() => db.update(people).set({ firstName: new Map() as never }), /text columns accept strings/);
  // json/jsonb columns keep accepting JSON values (objects bind as values).
  assert.doesNotThrow(() => db.update(people).set({ meta: { a: 1, b: [null, "x"] } }));
  assert.throws(() => db.update(people).set({ meta: { a: 1n } as never }), /JSON-representable/);
});

test("update without where throws", () => {
  assert.throws(() => db.update(users).set({ name: "x" }).toSQL(), /where/);
});

test("delete: where + returning", () => {
  const q = db.delete(posts).where(eq(posts.userId, 7)).returning().toSQL();
  assert.equal(q.sql, 'delete from "posts" where ("posts"."user_id" = $1) returning "posts"."id", "posts"."user_id" as "userId", "posts"."title", "posts"."body", "posts"."published"');
  assert.deepEqual(q.params, [7]);
});

test("relational: findMany with posts (many) aggregates one independent subquery", () => {
  const { sqlText } = relational(usersRelations, { with: { posts: true } });
  assert.equal(
    sqlText,
    'select "users"."id", "users"."email", "users"."name", "users"."active", to_jsonb("users"."created_at")::text as "createdAt", ' +
      '(select coalesce(jsonb_agg(jsonb_build_object(\'id\', "__rel_posts"."id", \'userId\', "__rel_posts"."user_id", ' +
      '\'title\', "__rel_posts"."title", \'body\', "__rel_posts"."body", \'published\', "__rel_posts"."published") ' +
      'order by "__rel_posts"."id"), \'[]\'::jsonb) from "posts" as "__rel_posts" where ("__rel_posts"."user_id" = "users"."id")) as "posts" ' +
      'from "users"',
  );
});

test("relational: two to-many children are independent correlated subqueries (no join, no group by)", () => {
  const comments = pgTable("comments", {
    id: serial("id").primaryKey(),
    userId: integer("user_id").notNull().references(() => users.id),
    body: text("body").notNull(),
  });
  const commentsRelations = relations(comments, ({ one }) => ({
    owner: one(users, { fields: [comments.userId], references: [users.id] }),
  }));
  const usersWithComments = relations(users, ({ many }) => ({
    posts: many(posts),
    comments: many(comments),
  }));
  resolveRelations([usersWithComments, postsRelations, commentsRelations]);
  const { sqlText } = relational(usersWithComments, { with: { posts: true, comments: true } });
  assert.ok(sqlText.includes('from "users"'));
  assert.ok(!sqlText.includes(" join "), "no joins between sibling relations");
  assert.ok(!sqlText.includes("group by"), "no group by");
  const postsSub = sqlText.indexOf('as "posts"');
  const commentsSub = sqlText.indexOf('as "comments"');
  assert.ok(postsSub > 0 && commentsSub > postsSub);
  assert.ok(sqlText.includes('from "posts" as "__rel_posts"'));
  assert.ok(sqlText.includes('from "comments" as "__rel_comments"'));
});

test("relational: findFirst with author (one) nulls on missing FK", () => {
  const { sqlText } = relational(postsRelations, { with: { author: true } });
  assert.equal(
    sqlText,
    'select "posts"."id", "posts"."user_id" as "userId", "posts"."title", "posts"."body", "posts"."published", ' +
      '(select jsonb_build_object(\'id\', "__rel_author"."id", \'email\', "__rel_author"."email", \'name\', "__rel_author"."name", ' +
      '\'active\', "__rel_author"."active", \'createdAt\', "__rel_author"."created_at") ' +
      'from "users" as "__rel_author" where ("__rel_author"."id" = "posts"."user_id")) as "author" ' +
      'from "posts"',
  );
});

test("relational: self-relation (manager) aliases the correlated scope", () => {
  const selfUsers = pgTable("self_users", {
    id: serial("id").primaryKey(),
    managerId: integer("manager_id").references((): AnyColumnBuilder => selfUsers.id),
  });
  const selfRelations = relations(selfUsers, ({ one }) => ({
    manager: one(selfUsers, { fields: [selfUsers.managerId], references: [selfUsers.id] }),
  }));
  const { sqlText } = relational(selfRelations, { with: { manager: true } });
  assert.ok(sqlText.includes('from "self_users" as "__rel_manager"'));
  assert.ok(sqlText.includes('where ("__rel_manager"."id" = "self_users"."manager_id")'));
});

test("relational: where + limit + orderBy apply to the parent row", () => {
  const { sqlText, params } = relationalWithParams(usersRelations, {
    where: eq(users.email, "a@x.com", "users"),
    with: { posts: true },
    orderBy: [desc(users.id, "users")],
    limit: 5,
  });
  assert.ok(sqlText.includes('where ("users"."email" = $1)'));
  assert.ok(sqlText.endsWith('order by "users"."id" desc limit 5'));
  assert.deepEqual(params, ["a@x.com"]);
});

test("relational: unknown relation throws", () => {
  assert.throws(
    () => relational(usersRelations, { with: { nope: true } }),
    /unknown relation "nope"/,
  );
});

test("relational: two references to one target in one with get distinct aliases (Q05)", () => {
  const reviewerPosts = pgTable("reviewer_posts", {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    reviewerId: integer("reviewer_id"),
  });
  const revRelations = relations(reviewerPosts, ({ one }) => ({
    author: one(users, { fields: [reviewerPosts.authorId], references: [users.id], relationName: "author" }),
    reviewer: one(users, { fields: [reviewerPosts.reviewerId], references: [users.id], relationName: "reviewer" }),
  }));
  // Both edges in ONE with clause: independent subqueries, distinct aliases,
  // each correlated through its own FK pair.
  const { sqlText } = relational(revRelations, { with: { author: true, reviewer: true } });
  assert.ok(sqlText.includes('as "author"'));
  assert.ok(sqlText.includes('as "reviewer"'));
  assert.ok(sqlText.includes('from "users" as "__rel_author" where ("__rel_author"."id" = "reviewer_posts"."author_id")'));
  assert.ok(sqlText.includes('from "users" as "__rel_reviewer" where ("__rel_reviewer"."id" = "reviewer_posts"."reviewer_id")'));
  // Each relation on its own still builds.
  assert.ok(relational(revRelations, { with: { author: true } }).sqlText.includes('as "author"'));
  assert.ok(relational(revRelations, { with: { reviewer: true } }).sqlText.includes('as "reviewer"'));
});

test("relational: non-true/non-object with values are rejected", () => {
  assert.throws(
    () => relational(usersRelations, { with: { posts: 42 as never } }),
    /relation "posts" in with on users: expected true or a per-relation options object, got a number/,
  );
  assert.throws(
    () => relational(usersRelations, { with: { posts: false as never } }),
    /relation "posts" in with on users: expected true or a per-relation options object, got a boolean/,
  );
  assert.throws(
    () => relational(usersRelations, { with: { posts: ["id"] as never } }),
    /relation "posts" in with on users: expected true or a per-relation options object, got an array/,
  );
});

test("relational: args.columns rejects unknown and empty selections", () => {
  assert.throws(
    () => relational(usersRelations, { columns: ["bogus"] }),
    /unknown column "bogus" in args.columns on users/,
  );
  assert.throws(
    () => relational(usersRelations, { columns: [] }),
    /args.columns on users is empty/,
  );
  const { sqlText } = relational(usersRelations, { columns: ["email", "name"] });
  assert.equal(sqlText, 'select "users"."email", "users"."name" from "users"');
});

test("relational: args.columns rejects non-array input", () => {
  assert.throws(
    () => relational(usersRelations, { columns: "id" as never }),
    /args.columns on users must be an array of property keys/,
  );
});

test("relational: int8/numeric child leaves render ::text in the JSON projection only", () => {
  const castAccounts = pgTable("cast_accounts", {
    id: bigint("id").primaryKey(),
    label: text("label").notNull(),
  });
  const castTxs = pgTable("cast_txs", {
    id: serial("id").primaryKey(),
    accountId: integer("account_id").notNull().references(() => castAccounts.id),
    amount: numeric("amount").notNull(),
    note: text("note"),
  });
  const accountsSide = relations(castAccounts, ({ many }) => ({ txs: many(castTxs) }));
  const txsSide = relations(castTxs, ({ one }) => ({
    account: one(castAccounts, { fields: [castTxs.accountId], references: [castAccounts.id] }),
  }));
  resolveRelations([accountsSide, txsSide]);

  const many = relational(accountsSide, { with: { txs: true } });
  assert.ok(many.sqlText.includes("'amount', \"__rel_txs\".\"amount\"::text"), "numeric leaf casts to text");
  assert.ok(many.sqlText.includes("'id', \"__rel_txs\".\"id\""), "serial leaf stays raw");
  assert.ok(!many.sqlText.includes('"__rel_txs"."id"::text'), "serial (int4) leaves are not blanket-cast");
  assert.ok(many.sqlText.includes("'accountId', \"__rel_txs\".\"account_id\""), "int4 leaf stays raw");
  assert.ok(!many.sqlText.includes('"__rel_txs"."account_id"::text'), "no blanket cast over int4 leaves");
  assert.ok(!many.sqlText.includes("::text ="), "correlation predicates never cast");
  assert.ok(
    many.sqlText.includes('where ("__rel_txs"."account_id" = "cast_accounts"."id")'),
    "correlation keys stay raw columns",
  );
  assert.ok(many.sqlText.includes('order by "__rel_txs"."id"'), "order key stays a raw column");

  const one = relational(txsSide, { with: { account: true } });
  assert.ok(one.sqlText.includes("'id', \"__rel_account\".\"id\"::text"), "to-one int8 leaf casts to text too");
});

test("resolveRelations: many() targeting a PK-less table is an explicit error", () => {
  const pklessTarget = pgTable("pkless_target", { label: text("label").notNull() });
  const pklessOwners = pgTable("pkless_owners", { id: serial("id").primaryKey() });
  const ownerSide = relations(pklessOwners, ({ many }) => ({ items: many(pklessTarget) }));
  const targetSide = relations(pklessTarget, ({ one }) => ({
    owner: one(pklessOwners, { fields: [pklessTarget.label], references: [pklessOwners.id] }),
  }));
  assert.throws(
    () => resolveRelations([ownerSide, targetSide]),
    /relation "items" on pkless_owners: target table pkless_target has no primary key; relation ordering undefined/,
  );
});

test("relational: PK-less many() targets never fall back to a column for ordering", async () => {
  const pklessTarget = pgTable("pkless_target2", { label: text("label").notNull() });
  const pklessOwners = pgTable("pkless_owners2", { id: serial("id").primaryKey() });
  const handWired: Relation = {
    kind: "many",
    targetTable: pklessTarget,
    source: { kind: "one", targetTable: pklessOwners, fields: [pklessTarget.label], references: [pklessOwners.id] },
  };
  assert.throws(
    () => buildRelationalSQL(pklessOwners, { items: handWired }, { with: { items: true } }),
    /target table pkless_target2 has no primary key; relation ordering undefined/,
  );
  await assert.rejects(
    () =>
      createDatabase({
        url: "postgres://snapshot:nouser@127.0.0.1:1/none",
        driverOptions: { driver: "postgres" },
        relations: {
          owners: relations(pklessOwners, ({ many }) => ({ items: many(pklessTarget) })),
          items: relations(pklessTarget, ({ one }) => ({
            owner: one(pklessOwners, { fields: [pklessTarget.label], references: [pklessOwners.id] }),
          })),
        },
      }),
    /target table pkless_target2 has no primary key; relation ordering undefined/,
  );
});

test("resolveRelations: ambiguous reverse (two FKs, no relationName) is an error", () => {
  const ambiguousPosts = pgTable("ambiguous_posts", {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    reviewerId: integer("reviewer_id"),
  });
  const usersSide = relations(users, ({ many }) => ({ posts: many(ambiguousPosts) }));
  const postsSide = relations(ambiguousPosts, ({ one }) => ({
    author: one(users, { fields: [ambiguousPosts.authorId], references: [users.id] }),
    reviewer: one(users, { fields: [ambiguousPosts.reviewerId], references: [users.id] }),
  }));
  assert.throws(
    () => resolveRelations([usersSide, postsSide]),
    /relation "posts" on users is ambiguous: ambiguous_posts declares multiple one\(\) relations to users \("author", "reviewer"\) — give the pair an explicit relationName/,
  );
});

test("resolveRelations: relationName pairs match across tables", () => {
  const namedPosts = pgTable("named_posts", {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    reviewerId: integer("reviewer_id"),
  });
  const usersSide = relations(users, ({ many }) => ({
    authored: many(namedPosts, { relationName: "author" }),
    reviewed: many(namedPosts, { relationName: "reviewer" }),
  }));
  const postsSide = relations(namedPosts, ({ one }) => ({
    author: one(users, { fields: [namedPosts.authorId], references: [users.id], relationName: "author" }),
    reviewer: one(users, { fields: [namedPosts.reviewerId], references: [users.id], relationName: "reviewer" }),
  }));
  const resolved = resolveRelations([usersSide, postsSide]);
  const authored = resolved.byTable.get("users")?.authored;
  const reviewed = resolved.byTable.get("users")?.reviewed;
  assert.equal(authored?.kind === "many" ? authored.source?.relationName : undefined, "author");
  assert.equal(reviewed?.kind === "many" ? reviewed.source?.relationName : undefined, "reviewer");
});

test("resolveRelations: relationName without a matching pair is an error", () => {
  const lonelyPosts = pgTable("lonely_posts", {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
  });
  const usersSide = relations(users, ({ many }) => ({ posts: many(lonelyPosts, { relationName: "editor" }) }));
  const postsSide = relations(lonelyPosts, ({ one }) => ({
    author: one(users, { fields: [lonelyPosts.authorId], references: [users.id], relationName: "author" }),
  }));
  assert.throws(
    () => resolveRelations([usersSide, postsSide]),
    /declares relationName "editor" but no one\(\) on lonely_posts targeting users declares the same relationName/,
  );
});

test("resolveRelations: duplicate relationName within one table is an error", () => {
  const dupPosts = pgTable("dup_posts", {
    id: serial("id").primaryKey(),
    aId: integer("a_id").notNull(),
    bId: integer("b_id").notNull(),
  });
  const postsSide = relations(dupPosts, ({ one }) => ({
    a: one(users, { fields: [dupPosts.aId], references: [users.id], relationName: "same" }),
    b: one(users, { fields: [dupPosts.bId], references: [users.id], relationName: "same" }),
  }));
  assert.throws(
    () => resolveRelations([postsSide]),
    /duplicate relationName "same" on dup_posts: one\(\) relations "a" and "b" declare it/,
  );
});

function relational(rel: TableRelations, args: RQBArgs): { sqlText: string } {
  const built = buildRelationalSQL(rel.table, rel.entries, args);
  return { sqlText: built.sql };
}

function relationalWithParams(
  rel: TableRelations,
  args: RQBArgs,
): { sqlText: string; params: unknown[] } {
  const built = buildRelationalSQL(rel.table, rel.entries, args);
  return { sqlText: built.sql, params: built.params };
}

// ---------------------------------------------------------------------------
// F02: metadata-name collisions. Authoritative table metadata lives in the
// symbol-keyed internal record; user columns named columns/tableName/indexes
// are ordinary columns and must not clobber it (B02 review M1).
// ---------------------------------------------------------------------------

const metaNames = pgTable(
  "meta_names",
  {
    id: serial("id").primaryKey(),
    columns: text("columns").notNull(),
    tableName: text("table_name"),
    indexes: integer("indexes"),
  },
  (t) => [index("meta_names_columns_idx").on(t.columns)],
);

const metaChildren = pgTable("meta_children", {
  id: serial("id").primaryKey(),
  parentId: integer("parent_id").notNull().references(() => metaNames.id),
  label: text("label").notNull(),
});

const metaNamesRelations = relations(metaNames, ({ many }) => ({
  children: many(metaChildren),
}));
const metaChildrenRelations = relations(metaChildren, ({ one }) => ({
  parent: one(metaNames, { fields: [metaChildren.parentId], references: [metaNames.id] }),
}));

test("metadata: user columns named columns/tableName/indexes cannot clobber table metadata", () => {
  assert.ok(isPgTable(metaNames));
  assert.equal(getTableName(metaNames), "meta_names");
  assert.deepEqual(Object.keys(getTableColumns(metaNames)), ["id", "columns", "tableName", "indexes"]);
  assert.equal(getTableColumns(metaNames).tableName.columnName, "table_name");
  assert.deepEqual(
    getTableIndexes(metaNames).map((i) => i.indexName),
    ["meta_names_columns_idx"],
  );
  // The user-facing surface still exposes those names as plain columns.
  assert.equal(metaNames.columns.columnName, "columns");
  assert.equal(metaNames.columns.dataType, "text");
  assert.equal(metaNames.tableName.columnName, "table_name");
  assert.equal(metaNames.indexes.columnName, "indexes");
  // The metadata record itself is frozen; the caller's column map is not.
  const metaRecord = (metaNames as { [TABLE_SYMBOL]: unknown })[TABLE_SYMBOL];
  assert.ok(Object.isFrozen(metaRecord));
  assert.equal(Object.isFrozen(getTableColumns(metaNames)), false);
});

test("metadata: accessors fail closed on non-tables", () => {
  assert.throws(() => getTableName({} as never), /not a neutron-sql table/);
  assert.throws(() => getTableColumns({ tableName: "fake" } as never), /not a neutron-sql table/);
  assert.throws(() => getTableIndexes(null as never), /not a neutron-sql table/);
  // Pre-F02 shape (symbol === true) must not pass as a table.
  const legacyShape = { [Symbol.for("@neutron-build/sql.table")]: true } as never;
  assert.equal(isPgTable(legacyShape), false);
  assert.throws(() => getTableName(legacyShape), /not a neutron-sql table/);
});

test("metadata: full CRUD compiles correctly on the collision table", () => {
  const sel = db.select().from(metaNames).toSQL();
  assert.equal(
    sel.sql,
    'select "meta_names"."id", "meta_names"."columns", "meta_names"."table_name" as "tableName", "meta_names"."indexes" from "meta_names"',
  );

  const ins = db.insert(metaNames).values({ columns: "c1", tableName: null, indexes: 3 }).toSQL();
  assert.equal(ins.sql, 'insert into "meta_names" ("columns", "table_name", "indexes") values ($1, $2, $3)');
  assert.deepEqual(ins.params, ["c1", null, 3]);

  const ret = db.insert(metaNames).values({ columns: "c1" }).returning().toSQL();
  assert.equal(
    ret.sql,
    'insert into "meta_names" ("columns") values ($1) returning "meta_names"."id", "meta_names"."columns", "meta_names"."table_name" as "tableName", "meta_names"."indexes"',
  );

  const upd = db
    .update(metaNames)
    .set({ columns: "c2", tableName: "t" })
    .where(eq(metaNames.columns, "c1"))
    .toSQL();
  assert.equal(upd.sql, 'update "meta_names" set "columns" = $1, "table_name" = $2 where ("meta_names"."columns" = $3)');
  assert.deepEqual(upd.params, ["c2", "t", "c1"]);

  const del = db.delete(metaNames).where(eq(metaNames.tableName, "t")).returning().toSQL();
  assert.equal(
    del.sql,
    'delete from "meta_names" where ("meta_names"."table_name" = $1) returning "meta_names"."id", "meta_names"."columns", "meta_names"."table_name" as "tableName", "meta_names"."indexes"',
  );
});

test("metadata: DDL and export use authoritative metadata on the collision table", () => {
  assert.equal(
    createTableSQL(metaNames),
    [
      'create table "meta_names" (',
      '  "id" serial primary key,',
      '  "columns" text not null,',
      '  "table_name" text,',
      '  "indexes" integer',
      ")",
    ].join("\n"),
  );
  const ddl = schemaToDDL([metaNames]);
  assert.ok(ddl.some((s) => s === 'create index "meta_names_columns_idx" on "meta_names" ("columns")'));

  const exported = exportTable(metaNames);
  assert.equal(exported.name, "meta_names");
  assert.deepEqual(
    exported.columns.map((c) => c.name),
    ["id", "columns", "table_name", "indexes"],
  );
  assert.deepEqual(exported.indexes, [{ name: "meta_names_columns_idx", unique: false, columns: ["columns"] }]);
});

test("metadata: relational reads project collision-named columns through metadata", () => {
  resolveRelations([metaNamesRelations, metaChildrenRelations]);
  const { sqlText } = relational(metaNamesRelations, { with: { children: true } });
  assert.ok(sqlText.startsWith('select "meta_names"."id", "meta_names"."columns", "meta_names"."table_name" as "tableName", "meta_names"."indexes", '));
  assert.ok(sqlText.includes('\'id\', "__rel_children"."id"'));
  assert.ok(sqlText.includes('\'parentId\', "__rel_children"."parent_id"'));
  assert.ok(sqlText.includes('from "meta_children" as "__rel_children" where ("__rel_children"."parent_id" = "meta_names"."id")'));

  const one = relational(metaChildrenRelations, { with: { parent: true } });
  assert.ok(one.sqlText.includes('\'columns\', "__rel_parent"."columns"'));
  assert.ok(one.sqlText.includes('\'tableName\', "__rel_parent"."table_name"'));
  assert.ok(one.sqlText.includes('\'indexes\', "__rel_parent"."indexes"'));
});

// keep imports referenced for type-only uses
void jsonb;
void uuid;

// ---------------------------------------------------------------------------
// V04 compile-time fixtures.
// tsc (pnpm run lint and the build inside pnpm test) fails when any expected
// error disappears: an unused @ts-expect-error directive is a compile error.
// ---------------------------------------------------------------------------

// @ts-expect-error missing required insert key "email" (NOT NULL, no default)
void (() => db.insert(users).values({}));
// @ts-expect-error null is not assignable to a NOT NULL column on insert
void (() => db.insert(users).values({ email: "x@x.com", active: null }));
// @ts-expect-error null is not assignable to a NOT NULL column on update
void (() => db.update(users).set({ active: null }));
// @ts-expect-error invalid update value type: text column rejects a number
void (() => db.update(users).set({ name: 123 }));
// @ts-expect-error invalid update value type: boolean column rejects a string
void (() => db.update(users).set({ active: "yes" }));
// @ts-expect-error invalid value type: text column rejects a boolean
void (() => db.insert(users).values({ email: "x@x.com", name: true }));
// @ts-expect-error invalid value type: boolean column rejects a string
void (() => db.insert(users).values({ email: "x@x.com", active: "yes" }));
// @ts-expect-error invalid value type: timestamp column rejects a number
void (() => db.insert(users).values({ email: "x@x.com", createdAt: 123 }));
// @ts-expect-error unknown insert key (excess property)
void (() => db.insert(users).values({ email: "x@x.com", nope: 1 }));
// @ts-expect-error unknown update key
void (() => db.update(users).set({ nope: 1 }));
// @ts-expect-error unknown column in a projection source
void (() => db.select({ x: users.bogus }));
// @ts-expect-error unknown column in a predicate
void (() => db.select().from(users).where(eq(users.bogus, 1)));
// @ts-expect-error unknown table key in db.query
void (() => db.query.bogus);

// @ts-expect-error unknown relation name in `with` must fail compilation
void (() => db.query.users.findFirst({ with: { totallyUnknownRelation: true } }));
// @ts-expect-error unknown relation name in `with` (findMany too)
void (() => db.query.users.findMany({ with: { alsoUnknown: true } }));
// @ts-expect-error unknown property key in args.columns
void (() => db.query.users.findMany({ columns: ["nonexistent"] }));
// @ts-expect-error false is not a relation selection
void (() => db.query.users.findMany({ with: { posts: false } }));
// @ts-expect-error unknown relation name in a NESTED with must fail compilation
void (() => db.query.users.findMany({ with: { posts: { with: { nope: true } } } }));
// @ts-expect-error unknown property key in nested args.columns
void (() => db.query.users.findMany({ with: { posts: { columns: ["nope"] } } }));
// @ts-expect-error per-child limit on a to-one relation is rejected (limit is to-many only)
void (() => db.query.posts.findMany({ with: { author: { limit: 1 } } }));
// @ts-expect-error per-child orderBy on a to-one relation is rejected too (rework m1 — single row, ordering cannot change the result)
void (() => db.query.posts.findMany({ with: { author: { orderBy: [desc(users.id)] } } }));

// Natural (non-serial) primary keys are required: NOT NULL without default.
const naturalKey = pgTable("natural_key", {
  id: bigint("id").primaryKey(),
  email: text("email").notNull(),
});
// @ts-expect-error bigint PK is NOT NULL without default — required on insert
void (() => db.insert(naturalKey).values({ email: "x@x.com" }));
const _natOk: typeof naturalKey.$inferInsert = { id: "9007199254740993", email: "x@x.com" };
// @ts-expect-error null is not assignable to a NOT NULL primary key
void (() => db.insert(naturalKey).values({ id: null, email: "x@x.com" }));

// Metadata-name collisions compile: columns/tableName/indexes are ordinary
// column names; metadata access goes through the accessor helpers.
const _colOk: typeof metaNames.$inferInsert = { columns: "c", tableName: null, indexes: 1 };
// @ts-expect-error the "columns" column is NOT NULL without default — required
void (() => db.insert(metaNames).values({ tableName: null }));
// @ts-expect-error invalid value for the collision column (boolean for text)
void (() => db.insert(metaNames).values({ columns: true }));
const _colName: string = metaNames.tableName.columnName;
const _colTable: string = getTableName(metaNames);
void [_natOk, _colOk, _colName, _colTable];

// Positive controls — must keep compiling:
// nullable column accepts null on insert and update; defaults/serials keep
// inserts optional; string|number|bigint all write int8/numeric columns.
const _p1 = db.insert(users).values({ email: "x@x.com", name: null });
const _p2 = db.update(users).set({ name: null });
const _p3 = db.insert(users).values({ email: "x@x.com" });
// serial is writable by design (PostgreSQL semantics: default, not generated).
// The generated-field-write rejection lands with generated/identity columns (Q07).
const _serialWrite = db.insert(users).values({ id: 5, email: "seed@x.com" });
// collision-named columns work in predicates too
const _p4 = db.select().from(metaNames).where(eq(metaNames.columns, "c")).limit(1);
void [_p1, _p2, _p3, _serialWrite, _p4];

async function absentAndUnrequestedFixtures(): Promise<void> {
  const projected = await db.select({ email: users.email }).from(users);
  // @ts-expect-error "name" is absent from the projection's result type
  void projected[0].name;

  const withPosts = await db.query.users.findMany({ with: { posts: true } });
  // @ts-expect-error "comments" was not requested — absent from the row type
  void withPosts[0].comments;

  const plain = await db.query.users.findMany();
  // @ts-expect-error relation keys are absent unless requested through `with`
  void plain[0].posts;

  const withAuthor = await db.query.posts.findMany({ with: { author: true } });
  // @ts-expect-error to-many cardinality: posts is an array, not one row
  const asOne: PostRow = withPosts[0].posts;
  // @ts-expect-error to-one outer-join nullability: author may be null
  const notNull: UserChildRow = withAuthor[0].author;
  // @ts-expect-error to-one is a single row, never an array
  const asMany: UserChildRow[] = withAuthor[0].author;
  void [asOne, notNull, asMany];
}
void absentAndUnrequestedFixtures;

// ---------------------------------------------------------------------------
// V04 exact one-level result types: rows carry exactly the selected columns
// plus exactly the requested relation edges (AssertEq demands type identity).
// ---------------------------------------------------------------------------

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

type UserRow = {
  id: number;
  email: string;
  name: string | null;
  active: boolean;
  createdAt: string;
};
/** users row as a relation CHILD: same leaf types as the flat path (the
 *  child JSON leaves decode through the same codecs). */
type UserChildRow = {
  id: number;
  email: string;
  name: string | null;
  active: boolean;
  createdAt: string;
};
type PostRow = {
  id: number;
  userId: number;
  title: string;
  body: string | null;
  published: boolean;
};

async function relationalTypeFixtures(): Promise<void> {
  const noArgs = await db.query.users.findMany();
  const withPosts = await db.query.users.findMany({ with: { posts: true } });
  const withAuthor = await db.query.posts.findFirst({ with: { author: true } });
  const selected = await db.query.users.findMany({ columns: ["id", "email"], with: { posts: true } });

  const eqBase: AssertEq<(typeof noArgs)[number], UserRow> = true;
  const eqMany: AssertEq<(typeof withPosts)[number], { id: number; email: string; name: string | null; active: boolean; createdAt: string; posts: PostRow[] }> = true;
  const eqOne: AssertEq<NonNullable<typeof withAuthor>, { id: number; userId: number; title: string; body: string | null; published: boolean; author: UserChildRow | null }> = true;
  const eqSelected: AssertEq<(typeof selected)[number], { id: number; email: string; posts: PostRow[] }> = true;
  void [eqBase, eqMany, eqOne, eqSelected];
  void [noArgs, withPosts, withAuthor, selected];
}
void relationalTypeFixtures;

// ---------------------------------------------------------------------------
// Q05: exact nested result types at depth 3 — per-child columns, per-child
// options, one-chains and many-chains, to-one nullability at the leaf.
// ---------------------------------------------------------------------------

async function q05RelationalTypeFixtures(): Promise<void> {
  // Depth-3 many->many->one with column subsets at every level.
  const chain = await db.query.users.findMany({
    columns: ["id", "name"],
    with: { posts: { columns: ["id", "title"], with: { author: { columns: ["email"] } } } },
  });
  const eqChain: AssertEq<
    (typeof chain)[number],
    { id: number; name: string | null; posts: Array<{ id: number; title: string; author: { email: string } | null }> }
  > = true;

  // Depth-3 one->many->many with a per-child limit on a to-many edge.
  const back = await db.query.posts.findMany({
    with: { author: { columns: ["email"], with: { posts: { orderBy: [desc(posts.id)], limit: 2 } } } },
  });
  const eqBack: AssertEq<
    (typeof back)[number],
    { id: number; userId: number; title: string; body: string | null; published: boolean; author: { email: string; posts: PostRow[] } | null }
  > = true;

  // Per-child where narrows nothing at the type level (runtime filtering),
  // but nested unrequested relations stay absent at every level.
  const filtered = await db.query.users.findMany({ with: { posts: { columns: ["id"], where: eq(posts.published, true) } } });
  const eqFiltered: AssertEq<(typeof filtered)[number], { id: number; email: string; name: string | null; active: boolean; createdAt: string; posts: Array<{ id: number }> }> = true;
  // @ts-expect-error columns subset at depth 2: absent keys are compile errors
  void (filtered[0].posts[0].title);

  void [eqChain, eqBack, eqFiltered];
  void [chain, back, filtered];

  // Pure inspection: toSQL/explainQuery need no connection and no execution.
  const compiled = db.query.users.toSQL({ with: { posts: { with: { author: true } } } });
  const sqlText: string = compiled.sql;
  const params: unknown[] = compiled.params;
  const plan = db.query.users.explainQuery({ with: { posts: true } });
  const table: string = plan.table;
  const n: number = plan.statementCount;
  const depth: number = plan.depth;
  const caps: readonly string[] = plan.capabilities;
  void [sqlText, params, table, n, depth, caps];
}
void q05RelationalTypeFixtures;

// ---------------------------------------------------------------------------
// F02 exact schema-level types: $inferSelect / $inferInsert / projections
// are pinned by identity. Insert optionality follows PK/default/NOT-NULL
// state: serial PK and defaulted columns optional, NOT NULL-without-default
// required, nullable columns accept null.
// ---------------------------------------------------------------------------

type UsersInsertModel = typeof users.$inferInsert;
const eqInsert: AssertEq<
  UsersInsertModel,
  {
    id?: number | undefined;
    email: string;
    name?: string | null | undefined;
    active?: boolean | undefined;
    createdAt?: string | Date | undefined;
  }
> = true;
void eqInsert;

type UsersSelectModel = typeof users.$inferSelect;
const eqSelectModel: AssertEq<UsersSelectModel, UserRow> = true;
void eqSelectModel;

// Natural bigint PK: required (NOT NULL, no default), accepts string|number|bigint.
type NaturalInsertModel = typeof naturalKey.$inferInsert;
const eqNatural: AssertEq<NaturalInsertModel, { id: string | number | bigint; email: string }> = true;
void eqNatural;

// Collision table: the metadata-like names are ordinary required/optional columns.
type MetaNamesInsertModel = typeof metaNames.$inferInsert;
const eqMetaNames: AssertEq<MetaNamesInsertModel, { id?: number | undefined; columns: string; tableName?: string | null | undefined; indexes?: number | null | undefined }> = true;
void eqMetaNames;

// The user-facing table surface types those names as the columns they are.
const eqCollisionSurface: AssertEq<typeof metaNames.columns, ColumnBuilder<"text", true, false>> = true;
const eqCollisionSurface2: AssertEq<typeof metaNames.indexes, ColumnBuilder<"integer", false, false>> = true;
void [eqCollisionSurface, eqCollisionSurface2];

async function projectionExactTypes(): Promise<void> {
  const rows = await db.select({ email: users.email, n: sql`count(*)` }).from(users);
  const eqProjection: AssertEq<(typeof rows)[number], { email: string; n: unknown }> = true;
  void eqProjection;
  void rows;
}
void projectionExactTypes;

// ---------------------------------------------------------------------------
// F03: relation child leaves decode through the same codecs as the flat path
// — int8 defaults to bigint (string/number modes opt-in), temporals are
// canonical strings, bytea decodes to Uint8Array from its \x hex text form.
// AssertEq demands type identity.
// ---------------------------------------------------------------------------

const leafKinds = pgTable("leaf_kinds", {
  id: serial("id").primaryKey(),
  n: integer("n"),
  big: bigint("big"),
  amt: numeric("amt"),
  flt: double("flt"),
  at: timestamp("at"),
  atz: timestamptz("atz"),
  d: date("d"),
  bin: bytea("bin"),
  flag: boolean("flag"),
  s: text("s"),
});
type ColumnsOfTable<T> = T extends PgTableCore<infer C> ? C : never;
type LeafChildRow = RelationChildModelOf<ColumnsOfTable<typeof leafKinds>>;
const eqLeafKinds: AssertEq<
  LeafChildRow,
  {
    id: number;
    n: number | null;
    big: bigint | null;
    amt: string | null;
    flt: number | null;
    at: string | null;
    atz: string | null;
    d: string | null;
    bin: Uint8Array | null;
    flag: boolean | null;
    s: string | null;
  }
> = true;
void eqLeafKinds;

// ---------------------------------------------------------------------------
// F03 codecs: modes, lossless wire acquisition, write encoding, JSON null.
// ---------------------------------------------------------------------------

import { jsonNull, isJsonNull } from "./codecs.js";

const codecTable = pgTable("f03_unit", {
  id: serial("id").primaryKey(),
  big: bigint("big"),
  bigStr: bigint("big_str", { mode: "string" }),
  bigNum: bigint("big_num", { mode: "number" }),
  at: timestamp("at"),
  atDate: timestamp("at_date", { mode: "date" }),
  atz: timestamptz("atz"),
  atzDate: timestamptz("atz_date", { mode: "date" }),
  amt: numeric("amt"),
  amtDec: numeric("amt_dec", { decoder: (raw: string) => raw.length }),
  bin: bytea("bin"),
  doc: jsonb("doc"),
});

test("codecs: declared modes drive exact read types", () => {
  type Row = typeof codecTable.$inferSelect;
  const eqRow: AssertEq<
    Row,
    {
      id: number;
      big: bigint | null;
      bigStr: string | null;
      bigNum: number | null;
      at: string | null;
      atDate: Date | null;
      atz: string | null;
      atzDate: Date | null;
      amt: string | null;
      amtDec: number | null;
      bin: Uint8Array | null;
      doc: unknown;
    }
  > = true;
  void eqRow;
});

test("codecs: unknown mode values fail at factory time", () => {
  assert.throws(() => bigint("x", { mode: "decimal" } as never), /unknown bigint codec mode "decimal"/);
  assert.throws(() => timestamp("x", { mode: "number" } as never), /unknown temporal codec mode "number"/);
  assert.throws(() => timestamptz("x", { mode: "string!" } as never), /unknown temporal codec mode/);
});

test("codecs: lossy-native columns project lossless text wire forms", () => {
  const q = db.select().from(codecTable).toSQL();
  assert.equal(
    q.sql,
    'select "f03_unit"."id", "f03_unit"."big", "f03_unit"."big_str" as "bigStr", "f03_unit"."big_num" as "bigNum", ' +
      'to_jsonb("f03_unit"."at")::text as "at", to_jsonb("f03_unit"."at_date")::text as "atDate", ' +
      `to_jsonb("f03_unit"."atz" at time zone 'UTC')::text as "atz", ` +
      `to_jsonb("f03_unit"."atz_date" at time zone 'UTC')::text as "atzDate", ` +
      '"f03_unit"."amt", "f03_unit"."amt_dec" as "amtDec", ' +
      '"f03_unit"."bin", "f03_unit"."doc" from "f03_unit"',
  );
  // int8/numeric/bytea keep their native (already exact) acquisition.
  assert.ok(!q.sql.includes('"big"::text'));
});

test("codecs: predicate values encode with text-typed sites where required", () => {
  // Predicates are AST value nodes since F04; their rendered form is checked
  // through a compiled one-condition statement (SQL + params exact).
  const cond = (c: Condition): { sql: string; params: readonly unknown[] } =>
    compileStatement(selectStatement({ from: ident("t"), where: [c] }));

  const atz = cond(eq(codecTable.atz, "2026-03-08T07:30:00.123456Z"));
  assert.equal(atz.sql, 'select * from "t" where ("f03_unit"."atz" = $1::text::timestamptz)');
  assert.deepEqual(atz.params, ["2026-03-08T07:30:00.123456Z"]);

  const atzDate = cond(eq(codecTable.atzDate, new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 678))));
  assert.equal(atzDate.sql, 'select * from "t" where ("f03_unit"."atz_date" = $1::text::timestamptz)');
  assert.deepEqual(atzDate.params, ["2026-01-02T03:04:05.678Z"]);

  const at = cond(lt(codecTable.at, "2026-01-01T00:00:00.000001"));
  assert.equal(at.sql, 'select * from "t" where ("f03_unit"."at" < $1::text::timestamp)');

  const big = cond(gt(codecTable.big, 1n));
  assert.equal(big.sql, 'select * from "t" where ("f03_unit"."big" > $1)');
  assert.deepEqual(big.params, [1n]);

  const many = cond(inArray(codecTable.atz, ["2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z"]));
  assert.equal(many.sql, 'select * from "t" where ("f03_unit"."atz" in ($1::text::timestamptz, $2::text::timestamptz))');

  const doc = cond(eq(codecTable.doc, jsonNull));
  assert.equal(doc.sql, 'select * from "t" where ("f03_unit"."doc" = $1::text::jsonb)');
  assert.deepEqual(doc.params, ["null"]);
});

test("codecs: temporal and json writes bind canonical text at text-typed sites", () => {
  const q = db
    .insert(codecTable)
    .values({
      big: "9007199254740993",
      at: "2026-01-01T19:04:05.678123",
      atz: new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 678)),
      doc: jsonNull,
    })
    .toSQL();
  assert.equal(
    q.sql,
    'insert into "f03_unit" ("big", "at", "atz", "doc") values ($1, $2::text::timestamp, $3::text::timestamptz, $4::text::jsonb)',
  );
  assert.deepEqual(q.params, ["9007199254740993", "2026-01-01T19:04:05.678123", "2026-01-02T03:04:05.678Z", "null"]);
});

test("codecs: json writes encode the JS value as its JSON encoding", () => {
  const q = db
    .insert(codecTable)
    .values({ doc: "null" })
    .toSQL();
  assert.equal(q.sql, 'insert into "f03_unit" ("doc") values ($1::text::jsonb)');
  assert.deepEqual(q.params, ['"null"'], "the JS string 'null' binds as the JSON string");
  const q2 = db
    .insert(codecTable)
    .values({ doc: { a: 1, b: [null, "x"] } })
    .toSQL();
  assert.deepEqual(q2.params, ['{"a":1,"b":[null,"x"]}']);
});

test("codecs: jsonNull is a frozen branded singleton", () => {
  assert.equal(Object.isFrozen(jsonNull), true);
  assert.equal(isJsonNull(jsonNull), true);
  assert.equal(isJsonNull(null), false);
  assert.equal(isJsonNull("null"), false);
  assert.equal(isJsonNull({}), false);
});

test("codecs: write validation rejects before execution with column context", () => {
  assert.throws(() => db.insert(codecTable).values({ big: "1.0" as never }).toSQL(), /"1\.0" is not an integer string/);
  assert.throws(() => db.insert(codecTable).values({ at: "2026-01-01" as never }).toSQL(), /is not a canonical timestamp string/);
  assert.throws(() => db.insert(codecTable).values({ atz: "2026-01-01T00:00:00" as never }).toSQL(), /is not a canonical timestamptz string/);
  assert.throws(() => db.insert(codecTable).values({ doc: { big: 1n } as never }).toSQL(), /JSON-representable/);
  assert.throws(() => db.insert(codecTable).values({ bin: "00ff" as never }).toSQL(), /bytea columns accept Uint8Array values/);
});

test("codecs: returning projects the same lossless wire forms as select", () => {
  const q = db.insert(codecTable).values({ big: 1n }).returning().toSQL();
  assert.ok(q.sql.includes('returning "f03_unit"."id", "f03_unit"."big", "f03_unit"."big_str" as "bigStr", "f03_unit"."big_num" as "bigNum", to_jsonb("f03_unit"."at")::text as "at"'));
  assert.ok(q.sql.includes(`to_jsonb("f03_unit"."atz" at time zone 'UTC')::text as "atz"`));
  assert.ok(q.sql.includes(`to_jsonb("f03_unit"."atz_date" at time zone 'UTC')::text as "atzDate"`));
});

// ---------------------------------------------------------------------------
// F04: CRUD builders are immutable (copy-on-write, frozen), compile through
// the one AST compiler deterministically, and carry decode plans + capability
// requirements with their compiled statements.
// ---------------------------------------------------------------------------

test("F04: CRUD builders are immutable — fluent calls fork, never mutate", () => {
  const base = db.select().from(users);
  const withFilter = base.where(eq(users.email, "a@x.com", "users"));
  const withLimit = base.limit(5);

  assert.equal(Object.isFrozen(base), true, "builder instances are frozen");
  assert.equal(base.toSQL().sql.includes("where"), false, "base builder unaffected by forks");
  assert.ok(withFilter.toSQL().sql.includes('where ("users"."email" = $1)'));
  assert.ok(withLimit.toSQL().sql.endsWith("limit 5"));
  // Two requests from one base query cannot contaminate each other (V02).
  assert.deepEqual(base.toSQL().params, []);

  const insBase = db.insert(users);
  const insA = insBase.values({ email: "a@x.com" });
  const insB = insBase.values({ email: "b@x.com" }).returning();
  assert.throws(() => insBase.toSQL(), /requires \.values\(\)/);
  assert.equal(insA.toSQL().sql, 'insert into "users" ("email") values ($1)');
  assert.ok(insB.toSQL().sql.startsWith('insert into "users" ("email") values ($1) returning'));

  const updBase = db.update(users).set({ name: "X" });
  const updA = updBase.where(eq(users.id, 1));
  const updB = updBase.where(eq(users.id, 2)).returning();
  assert.throws(() => updBase.toSQL(), /without \.where\(\)/);
  assert.equal(updA.toSQL().sql, 'update "users" set "name" = $1 where ("users"."id" = $2)');
  assert.ok(updB.toSQL().sql.includes("returning"));

  const delBase = db.delete(users);
  const delA = delBase.where(eq(users.id, 1));
  assert.throws(() => delBase.toSQL(), /without \.where\(\)/);
  assert.equal(delA.toSQL().sql, 'delete from "users" where ("users"."id" = $1)');
});

test("F04: CRUD compilation is pure — repeated compiles are byte-identical", () => {
  const q = db.select().from(users).where(and(eq(users.active, true, "users"), gt(users.id, 0, "users"))).limit(3);
  const a = q.toSQL();
  const b = q.toSQL();
  assert.equal(a.sql, b.sql);
  assert.deepEqual(a.params, b.params);

  const i = db.insert(posts).values([{ userId: 1, title: "p" }, { userId: 2, title: "q" }]).returning();
  assert.deepEqual(i.toSQL(), i.toSQL());
});

test("F04: compiled statements carry projection decoders and capability requirements", () => {
  const plan = db.select().from(codecTable).toCompiled();
  const keys = plan.decoders.map((d) => d.key);
  assert.ok(keys.includes("big"), "bigint decode rides along");
  assert.ok(keys.includes("at"), "timestamp text-wire decode rides along");
  assert.ok(keys.includes("atz"), "timestamptz text-wire decode rides along");
  assert.ok(!keys.includes("s"), "identity columns carry no decoder");
  for (const d of plan.decoders) {
    assert.ok(d.codec, "decoder entries carry the serializable ColumnCodec plan");
    if (d.codec.textWire) assert.equal(d.wire, "text");
  }
  assert.deepEqual(plan.capabilities, ["jsonb-functions"], "wire-read projections require jsonb functions");

  const plain = db.select({ email: users.email }).from(users).toCompiled();
  assert.equal(plain.capabilities.length, 0, "plain projections require no capabilities");
  assert.equal(plain.decoders.length, 0, "varchar/text need no decode");

  // A decode plan actually decodes driver rows (text-wire form -> value).
  const rows: Array<Record<string, unknown>> = [{ at: '"2026-01-02T03:04:05.678912"', big: "9007199254740993" }];
  const decoders = db.select().from(codecTable).toCompiled().decoders;
  for (const d of decoders) {
    const raw = rows[0][d.key];
    if (raw !== undefined) rows[0][d.key] = d.decode(raw);
  }
  assert.equal(rows[0].at, "2026-01-02T03:04:05.678912");
  assert.equal(rows[0].big, 9007199254740993n);

  // Mutations without returning carry no decoders; with returning they do.
  assert.equal(db.update(users).set({ name: "x" }).where(eq(users.id, 1)).toCompiled().decoders.length, 0);
  assert.ok(db.update(users).set({ name: "x" }).where(eq(users.id, 1)).returning().toCompiled().decoders.length > 0);
});

test("F04: chained .where() calls with OR fragments are safely parenthesized", () => {
  // Old regex assembly joined raw fragment text with " and " — two OR
  // fragments produced precedence corruption (a or b and c or d). The
  // compiler wraps every fragment condition.
  const q = db.select({ id: users.id })
    .from(users)
    .where(sql`${users.id} = ${1} or ${users.active} = ${false}`)
    .where(sql`${users.id} = ${2} or ${users.active} = ${true}`)
    .toSQL();
  assert.equal(q.sql, 'select "users"."id" from "users" where ("users"."id" = $1 or "users"."active" = $2) and ("users"."id" = $3 or "users"."active" = $4)');
  assert.deepEqual(q.params, [1, false, 2, true]);
});

test("F04: legacy SqlFragment assignments are rejected with a rebuild hint", () => {
  const legacy = raw("upper('a')");
  assert.throws(() => db.update(users).set({ name: legacy } as never), /update set: legacy SqlFragment \{sql, params\} cannot be used here/);
  // Structural fragments (sql``) keep working in projections and assignments.
  const q = db.select({ n: sql`count(*)` }).from(users).toSQL();
  assert.ok(q.sql.includes("count(*)"));
});

// ---------------------------------------------------------------------------
// F04 rework (attempt 2): reviewer findings — connective grouping (MAJOR-1),
// projection copy-on-fork (MAJOR-2), legacy-fragment rejection on every slot
// (MINOR-1).
// ---------------------------------------------------------------------------

test("F04 rework MAJOR-1: and()/not() parenthesize fragment args (connective grouping)", () => {
  // Reviewer live proof (attempt-1 code): and(eq(a,true), sql`b or c`) compiled
  // to `(("a" = $1) and "b" or "c")` — Postgres parses the bare `or` tighter
  // than the enclosing `and`, silently returning 5 of 8 truth-table rows
  // instead of 3. Every connective application now delimits text-bearing args
  // exactly like the where list does.
  const andFrag = and(eq(users.active, true, "users"), sql`${users.id} <> ${1} or ${users.active} = ${false}`);
  assert.equal(
    db.select({ id: users.id }).from(users).where(andFrag).toSQL().sql,
    'select "users"."id" from "users" where ("users"."active" = $1) and ("users"."id" <> $2 or "users"."active" = $3)',
  );

  // Fragment on the LEFT of the connective too.
  const andFragLeft = and(sql`${users.active} = ${true} or ${users.id} = ${9}`, eq(users.id, 1, "users"));
  assert.equal(
    db.select({ id: users.id }).from(users).where(andFragLeft).toSQL().sql,
    'select "users"."id" from "users" where ("users"."active" = $1 or "users"."id" = $2) and ("users"."id" = $3)',
  );

  // not(): any top-level connective inside the fragment must stay inside.
  const notFrag = not(sql`${users.active} = ${true} or ${users.id} = ${9}`);
  assert.equal(
    db.select({ id: users.id }).from(users).where(notFrag).toSQL().sql,
    'select "users"."id" from "users" where (not ("users"."active" = $1 or "users"."id" = $2))',
  );

  // Nested: a connective buried inside or() — where-list flattening cannot
  // save it because the and() is not a direct child of the where list.
  const nested = or(and(eq(users.active, true, "users"), sql`${users.id} > ${0} or ${users.id} = ${9}`), eq(users.id, 5, "users"));
  const nestedOut = db.select({ id: users.id }).from(users).where(nested).toSQL();
  assert.equal(
    nestedOut.sql,
    'select "users"."id" from "users" where ((("users"."active" = $1) and ("users"."id" > $2 or "users"."id" = $3)) or ("users"."id" = $4))',
  );
  assert.deepEqual(nestedOut.params, [true, 0, 9, 5]);

  // Raw AST path (selectStatement) gets the same grouping. The where list is
  // not builder-flattened here, so the whole and() renders as one
  // self-parenthesized expr with its fragment operand delimited.
  const rawAst = compileStatement(
    selectStatement({ projections: [{ kind: "projection", expr: ident("id"), alias: "id" }], from: ident("users"), where: [andFrag] }),
  );
  assert.equal(
    rawAst.sql,
    'select "id" as "id" from "users" where (("users"."active" = $1) and ("users"."id" <> $2 or "users"."active" = $3))',
  );

  // trusted segments as connective args are delimited the same way.
  const trustedCond = and(eq(users.active, true, "users"), sql`${trustSql("users.id <> 1 or users.active = false", TRUSTED_SQL_ACK)}`);
  assert.equal(
    db.select({ id: users.id }).from(users).where(trustedCond).toSQL().sql,
    'select "users"."id" from "users" where ("users"."active" = $1) and (users.id <> 1 or users.active = false)',
  );
});

test("F04 rework MAJOR-2: projection is copied on fork — post-fork caller mutation is isolated", () => {
  // F01 review-2 carry-forward: the reviewer showed a post-fork `evil` key
  // reaching BOTH sibling forks' compiled SQL when the projection object was
  // shared by reference.
  const projection: Record<string, unknown> = { id: users.id, email: users.email };
  const base = db.select(projection as never).from(users);
  const b1 = base.where(eq(users.id, 1));
  const b2 = base.where(eq(users.id, 2));
  const before = b1.toSQL().sql;
  // Hostile post-fork mutation of the CALLER's object the builder was built from.
  projection.evil = sql`1`;
  const after1 = b1.toSQL().sql;
  const after2 = b2.toSQL().sql;
  assert.equal(after1, before, "fork 1 compiled SQL must not change after post-fork caller mutation");
  assert.ok(!after2.includes("evil"), "fork 2 must not see post-fork caller mutation");
  // The base builder itself is equally isolated, and later forks of it.
  assert.ok(!base.limit(5).toSQL().sql.includes("evil"));
  // Compiling before the mutation and after still agrees (snapshot semantics).
  assert.equal(after1, b1.toSQL().sql);
});

test("F04 rework MINOR-1: legacy {sql, params} fragments fail closed with one rebuild hint on every slot", () => {
  const legacy = raw("id = $1", [1]);
  const hint = /legacy SqlFragment \{sql, params\} cannot be used here .* raw\(\) \+ driver\.query/s;

  // where — select/update/delete (CRUD) and the AST builder.
  assert.throws(() => db.select().from(users).where(legacy as never), hint);
  assert.throws(() => db.update(users).set({ name: "x" }).where(legacy as never), hint);
  assert.throws(() => db.delete(users).where(legacy as never), hint);
  assert.throws(() => astSelect().from(users).where(legacy as never), hint);

  // orderBy — CRUD and AST builder.
  assert.throws(() => db.select().from(users).orderBy(legacy as never), hint);
  assert.throws(() => astSelect().from(users).orderBy(legacy as never), hint);

  // set assignment (attempt-1 already had this slot; message now shared).
  assert.throws(() => db.update(users).set({ name: legacy } as never), hint);

  // projection slots — CRUD select and astSelect.
  assert.throws(() => db.select({ id: users.id, evil: legacy } as never).from(users).toSQL(), hint);
  assert.throws(() => astSelect({ id: users.id, evil: legacy as never }).from(users).toSQL(), hint);

  // connective arguments — accepted-at-construction was the attempt-1 hole.
  assert.throws(() => and(eq(users.id, 1), legacy as never), hint);
  assert.throws(() => or(eq(users.id, 1), legacy as never), hint);
  assert.throws(() => not(legacy as never), hint);

  // insert cell values.
  assert.throws(() => db.insert(users).values({ email: legacy } as never).toSQL(), hint);

  // relational args (where via the RQB surface).
  const usersEntries = resolveRelations([usersRelations, postsRelations]).byTable.get("users")!;
  assert.throws(() => buildRelationalSQL(users, usersEntries, { where: legacy as never }), hint);

  // sql template interpolation (pre-existing slot, same hint shape since rework).
  assert.throws(() => sql`${legacy as never}`, hint);
});
