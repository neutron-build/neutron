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
  eq,
  gt,
  inArray,
  asc,
  desc,
  sql,
  schemaToDDL,
  createTableSQL,
  type AnyColumnBuilder,
  type Relation,
} from "./index.js";
import { buildRelationalSQL, resolveRelations } from "./relations.js";
import type { RQBArgs } from "./relations.js";
import type { TableRelations } from "./schema.js";
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
  driver: { driver: "postgres" },
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
    'select "users"."id", "users"."email", "users"."name", "users"."active", "users"."created_at" as "createdAt" from "users"',
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
    'select "users"."id", "users"."email", "users"."name", "users"."active", "users"."created_at" as "createdAt" from "users" where (("users"."id" = $1) and ("users"."id" > $2)) order by "users"."created_at" desc, "users"."id" asc limit 10 offset 20',
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
  assert.equal(q.sql, 'select "users"."id", "users"."email", "users"."name", "users"."active", "users"."created_at" as "createdAt" from "users" where (("users"."id" = $1) or ("users"."email" in ($2, $3)))');
  assert.deepEqual(q.params, [5, "a@x.com", "b@x.com"]);
});

test("select: aliased projection labels columns with the requested key", () => {
  const q = db.select({ displayName: users.email, createdAt: users.createdAt }).from(users).toSQL();
  assert.equal(q.sql, 'select "users"."email" as "displayName", "users"."created_at" as "createdAt" from "users"');
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
    'insert into "users" ("email", "name") values ($1, $2) returning "id", "email", "name", "active", "created_at" as "createdAt"',
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
    /invalid value for column "name" \("name"\) on users: nested object/,
  );
  // B01 review minor 2: pg silently JSON-stringifies and postgres.js stores
  // "[object Object]" — both must now fail before any driver call.
  const b = db.insert(users).values({ email: "a@x.com", active: { nested: true } as never });
  assert.throws(() => b.toSQL(), /invalid value for column "active" \("active"\) on users/);
  assert.throws(() => db.insert(users).values({ email: "a@x.com", name: [1, 2] as never }).toSQL(), /array values are not bindable for text/);
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
    'update "users" set "name" = $1, "active" = not $2 where "users"."id" = $3 returning "id", "email", "name", "active", "created_at" as "createdAt"',
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
  assert.equal(q.sql, 'update "people_map" set "first_name" = $1, "nick" = $2 where "people_map"."id" = $3');
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
  assert.equal(q.sql, 'update "people_map2" set "first_name" = $1 where "people_map2"."id" = $2');
  // An update whose .set() leaves no assignments is an error.
  assert.throws(() => db.update(people).set({ nick: undefined }).where(eq(people.id, 1)).toSQL(), /no assignments/);
  // Fragments keep working in assignments (B01-supported API).
  const frag = db.update(people).set({ firstName: sql`upper(${"bob"})` }).where(eq(people.id, 1)).toSQL();
  assert.equal(frag.sql, 'update "people_map2" set "first_name" = upper($1) where "people_map2"."id" = $2');
});

test("update: invalid set values rejected before execution with column context", () => {
  const people = pgTable("people_map3", {
    id: serial("id").primaryKey(),
    firstName: text("first_name").notNull(),
    meta: jsonb("meta"),
  });
  assert.throws(() => db.update(people).set({ firstName: { nested: true } as never }), /invalid value for column "firstName"/);
  assert.throws(() => db.update(people).set({ firstName: new Map() as never }), /not bindable/);
  // json/jsonb columns keep accepting JSON values (objects bind as values).
  assert.doesNotThrow(() => db.update(people).set({ meta: { a: 1, b: [null, "x"] } }));
  assert.throws(() => db.update(people).set({ meta: { a: 1n } as never }), /JSON-representable/);
});

test("update without where throws", () => {
  assert.throws(() => db.update(users).set({ name: "x" }).toSQL(), /where/);
});

test("delete: where + returning", () => {
  const q = db.delete(posts).where(eq(posts.userId, 7)).returning().toSQL();
  assert.equal(q.sql, 'delete from "posts" where "posts"."user_id" = $1 returning "id", "user_id" as "userId", "title", "body", "published"');
  assert.deepEqual(q.params, [7]);
});

test("relational: findMany with posts (many) aggregates one independent subquery", () => {
  const { sqlText } = relational(usersRelations, { with: { posts: true } });
  assert.equal(
    sqlText,
    'select "users"."id", "users"."email", "users"."name", "users"."active", "users"."created_at" as "createdAt", ' +
      '(select coalesce(jsonb_agg(jsonb_build_object(\'id\', "__rel_posts"."id", \'userId\', "__rel_posts"."user_id", ' +
      '\'title\', "__rel_posts"."title", \'body\', "__rel_posts"."body", \'published\', "__rel_posts"."published") ' +
      'order by "__rel_posts"."id"), \'[]\'::jsonb) from "posts" as "__rel_posts" where "__rel_posts"."user_id" = "users"."id") as "posts" ' +
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
      'from "users" as "__rel_author" where "__rel_author"."id" = "posts"."user_id") as "author" ' +
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
  assert.ok(sqlText.includes('where "__rel_manager"."id" = "self_users"."manager_id"'));
});

test("relational: where + limit + orderBy apply to the parent row", () => {
  const { sqlText, params } = relationalWithParams(usersRelations, {
    where: eq(users.email, "a@x.com", "users"),
    with: { posts: true },
    orderBy: [desc(users.id, "users")],
    limit: 5,
  });
  assert.ok(sqlText.includes('where "users"."email" = $1'));
  assert.ok(sqlText.endsWith('order by "users"."id" desc limit 5'));
  assert.deepEqual(params, ["a@x.com"]);
});

test("relational: unknown relation throws", () => {
  assert.throws(
    () => relational(usersRelations, { with: { nope: true } }),
    /unknown relation "nope"/,
  );
});

test("relational: same target table twice in one with is rejected explicitly", () => {
  const reviewerPosts = pgTable("reviewer_posts", {
    id: serial("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    reviewerId: integer("reviewer_id"),
  });
  const revRelations = relations(reviewerPosts, ({ one }) => ({
    author: one(users, { fields: [reviewerPosts.authorId], references: [users.id], relationName: "author" }),
    reviewer: one(users, { fields: [reviewerPosts.reviewerId], references: [users.id], relationName: "reviewer" }),
  }));
  assert.throws(
    () => relational(revRelations, { with: { author: true, reviewer: true } }),
    /relations "author" and "reviewer" on reviewer_posts both target table "users" in one with clause/,
  );
  // Each relation on its own still builds.
  assert.ok(relational(revRelations, { with: { author: true } }).sqlText.includes('as "author"'));
  assert.ok(relational(revRelations, { with: { reviewer: true } }).sqlText.includes('as "reviewer"'));
});

test("relational: non-true with values (nested/per-relation options) are rejected", () => {
  assert.throws(
    () => relational(usersRelations, { with: { posts: { with: {} } as never } }),
    /relation "posts" in with on users: only `true` is supported/,
  );
  assert.throws(
    () => relational(usersRelations, { with: { posts: false as never } }),
    /only `true` is supported/,
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
    many.sqlText.includes('where "__rel_txs"."account_id" = "cast_accounts"."id"'),
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
        driver: { driver: "postgres" },
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
    /duplicate relationName "same" on dup_posts: relations "a" and "b" declare it/,
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

// keep imports referenced for type-only uses
void jsonb;
void uuid;

// ---------------------------------------------------------------------------
// V04 compile-time fixtures.
// tsc (pnpm run lint and the build inside pnpm test) fails when any expected
// error disappears: an unused @ts-expect-error directive is a compile error.
// ---------------------------------------------------------------------------

// @ts-expect-error missing required insert key "email" (NOT NULL, no default)
void db.insert(users).values({});
// @ts-expect-error null is not assignable to a NOT NULL column on insert
void db.insert(users).values({ email: "x@x.com", active: null });
// @ts-expect-error null is not assignable to a NOT NULL column on update
void db.update(users).set({ active: null });

// @ts-expect-error unknown relation name in `with` must fail compilation
void db.query.users.findFirst({ with: { totallyUnknownRelation: true } });
// @ts-expect-error unknown relation name in `with` (findMany too)
void db.query.users.findMany({ with: { alsoUnknown: true } });
// @ts-expect-error unknown property key in args.columns
void db.query.users.findMany({ columns: ["nonexistent"] });
// @ts-expect-error nested with / per-relation options are not a `true` selection
void db.query.users.findMany({ with: { posts: { with: {} } } });
// @ts-expect-error false is not a relation selection
void db.query.users.findMany({ with: { posts: false } });

// Positive controls — must keep compiling:
// nullable column accepts null on insert and update; defaults/serials keep
// inserts optional; string|number|bigint all write int8/numeric columns.
const _p1 = db.insert(users).values({ email: "x@x.com", name: null });
const _p2 = db.update(users).set({ name: null });
const _p3 = db.insert(users).values({ email: "x@x.com" });
void [_p1, _p2, _p3];

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
  createdAt: Date;
};
/** users row as a relation CHILD: the temporal leaf arrives as a string
 *  through the JSON path (typed decode is F03). */
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
  const eqMany: AssertEq<(typeof withPosts)[number], { id: number; email: string; name: string | null; active: boolean; createdAt: Date; posts: PostRow[] }> = true;
  const eqOne: AssertEq<NonNullable<typeof withAuthor>, { id: number; userId: number; title: string; body: string | null; published: boolean; author: UserChildRow | null }> = true;
  const eqSelected: AssertEq<(typeof selected)[number], { id: number; email: string; posts: PostRow[] }> = true;
  void [eqBase, eqMany, eqOne, eqSelected];
  void [noArgs, withPosts, withAuthor, selected];
}
void relationalTypeFixtures;

// ---------------------------------------------------------------------------
// F2: relation child leaves are typed honestly — int8/numeric (::text inside
// the aggregation) and temporal/bytea (to_jsonb string forms) are strings;
// int4/float8 stay numbers. AssertEq demands type identity.
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
type LeafChildRow = RelationChildModelOf<typeof leafKinds.columns>;
const eqLeafKinds: AssertEq<
  LeafChildRow,
  {
    id: number;
    n: number | null;
    big: string | null;
    amt: string | null;
    flt: number | null;
    at: string | null;
    atz: string | null;
    d: string | null;
    bin: string | null;
    flag: boolean | null;
    s: string | null;
  }
> = true;
void eqLeafKinds;
