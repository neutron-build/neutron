import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  integer,
  text,
  timestamp,
  relations,
  eq,
  gt,
  desc,
  asc,
  sql,
  ident,
  subquery,
  trustSql,
  TRUSTED_SQL_ACK,
  selectStatement,
  projection,
  expr as exprNode,
  type Relation,
  type TableRelations,
} from "./index.js";
import { buildRelationalPlan, buildRelationalSQL, MAX_RELATION_DEPTH, type RQBArgs } from "./relations.js";
import { resolveRelations } from "./relations.js";

// Q05 unit battery (V03/V04 compile-side subset + V07 purity): nested SQL
// shapes, per-child filter/order/limit placement, alias generation and the
// 63-byte fallback, cycle-bounded depth rejection, remap fail-closed rules,
// and PURE compile/explain inspection. Live execution belongs to
// live.q05.postgres.test.ts.

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  managerId: integer("manager_id"),
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

const usersRelations = relations(users, ({ one, many }) => ({
  posts: many(posts, { relationName: "author" }),
  comments: many(comments),
  manager: one(users, { fields: [users.managerId], references: [users.id], relationName: "reports" }),
  reports: many(users, { relationName: "reports" }),
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

const REL_SETS = [usersRelations, postsRelations, commentsRelations];
resolveRelations(REL_SETS);
import { getTableName, createDatabase } from "./index.js";
const RELATIONS_BY_TABLE = new Map<string, Record<string, Relation>>(
  REL_SETS.map((set) => [getTableName(set.table), set.entries] as const),
);

function plan(rel: TableRelations, args: RQBArgs): ReturnType<typeof buildRelationalPlan> {
  return buildRelationalPlan(rel.table, rel.entries, args, RELATIONS_BY_TABLE);
}

test("q05 unit: depth-3 many->many->one nests correlated subqueries with path aliases", () => {
  const built = plan(usersRelations, {
    with: { posts: { with: { comments: { with: { commenter: { columns: ["name"] } } } } } },
  });
  const s = built.sql;
  // Level 1: users -> posts under __rel_posts.
  assert.ok(s.includes('from "posts" as "__rel_posts" where ("__rel_posts"."author_id" = "users"."id")'), s);
  // Level 2: posts -> comments nested inside the jsonb object, correlated to
  // the posts alias, aliased by PATH (__rel_posts__comments).
  assert.ok(
    s.includes('from "comments" as "__rel_posts__comments" where ("__rel_posts__comments"."post_id" = "__rel_posts"."id")'),
    s,
  );
  // Level 3: comments -> commenter correlated to the comments alias.
  assert.ok(
    s.includes(
      'from "users" as "__rel_posts__comments__commenter" where ("__rel_posts__comments__commenter"."id" = "__rel_posts__comments"."commenter_id")',
    ),
    s,
  );
  // The commenter object carries only the selected column.
  assert.ok(s.includes("'name', \"__rel_posts__comments__commenter\".\"name\""));
  assert.ok(!s.includes("'id', \"__rel_posts__comments__commenter\".\"id\""));
  assert.equal(built.depth, 3);
  assert.equal(built.statementCount, 1);
  assert.deepEqual(built.capabilities, ["jsonb-functions"]);
  // Edge tree mirrors the shape.
  const postsEdge = built.edges.find((e) => e.key === "posts");
  assert.ok(postsEdge);
  assert.equal(postsEdge.target, "posts");
  assert.equal(postsEdge.cardinality, "many");
  const commentsEdge = postsEdge.nested.find((e) => e.key === "comments");
  assert.ok(commentsEdge);
  const commenterEdge = commentsEdge.nested.find((e) => e.key === "commenter");
  assert.ok(commenterEdge);
  assert.equal(commenterEdge.cardinality, "one");
  assert.deepEqual(commenterEdge.columns, ["name"]);
});

test("q05 unit: depth-3 self-relation tree (users -> reports x3) terminates by structure", () => {
  const built = plan(usersRelations, {
    with: { reports: { with: { reports: { with: { reports: { columns: ["id"] } } } } } },
  });
  const s = built.sql;
  assert.ok(s.includes('as "__rel_reports"'));
  assert.ok(s.includes('as "__rel_reports__reports"'));
  assert.ok(s.includes('as "__rel_reports__reports__reports"'));
  // Every level correlates manager_id -> id against its own enclosing alias.
  assert.ok(s.includes('where ("__rel_reports"."manager_id" = "users"."id")'));
  assert.ok(s.includes('where ("__rel_reports__reports"."manager_id" = "__rel_reports"."id")'));
  assert.ok(s.includes('where ("__rel_reports__reports__reports"."manager_id" = "__rel_reports__reports"."id")'));
  // Cycles cannot loop: the nesting depth IS the bound (documented contract).
  assert.equal(built.depth, 3);
});

test("q05 unit: depth-3 to-one chain (comments -> post -> author -> manager)", () => {
  const built = plan(commentsRelations, {
    with: { post: { with: { author: { with: { manager: { columns: ["name"] } } } } } },
  });
  const s = built.sql;
  assert.ok(s.includes('from "posts" as "__rel_post" where ("__rel_post"."id" = "comments"."post_id")'));
  assert.ok(s.includes('from "users" as "__rel_post__author" where ("__rel_post__author"."id" = "__rel_post"."author_id")'));
  assert.ok(
    s.includes(
      'from "users" as "__rel_post__author__manager" where ("__rel_post__author__manager"."id" = "__rel_post__author"."manager_id")',
    ),
  );
  // A missing to-one at any level renders as JSON null (subquery under the
  // object key), never as a missing key.
  assert.ok(s.includes("'manager', (select jsonb_build_object("));
});

test("q05 unit: per-child where/order/limit compile inside the child subquery, remapped to its alias", () => {
  const built = plan(usersRelations, {
    with: {
      posts: {
        where: gt(posts.views, 2),
        orderBy: [desc(posts.views)],
        limit: 3,
        offset: 1,
        columns: ["id", "title", "views"],
      },
    },
  });
  const s = built.sql;
  // Limited many shape: aggregate over a limited derived table. The derived
  // table projects EVERY target column (order keys and nested-edge FK
  // correlations resolve against it) — rework regression pin for the
  // selected-∪-PK projection that 42703'd on unselected references.
  assert.ok(
    s.includes(
      'select "__rel_posts"."id", "__rel_posts"."author_id", "__rel_posts"."reviewer_id", "__rel_posts"."title", "__rel_posts"."views" from "posts" as "__rel_posts"',
    ),
    s,
  );
  assert.ok(s.includes('where ("__rel_posts"."author_id" = "users"."id") and ("__rel_posts"."views" > $1)'), s);
  // The limit applies to the derived table (per parent), after correlation.
  assert.ok(s.includes('order by "__rel_posts"."views" desc, "__rel_posts"."id" asc limit 3 offset 1'), s);
  // The aggregate runs over the derived alias with the same effective order
  // (asc omitted — PostgreSQL's default rendering for tie-breakers).
  assert.ok(s.includes('order by "__rel_posts_lim"."views" desc, "__rel_posts_lim"."id"),'), s);
  assert.ok(s.includes(') as "__rel_posts_lim"'));
  assert.deepEqual(built.params, [2]);
  const edge = built.edges[0];
  assert.equal(edge.filtered, true);
  assert.equal(edge.ordered, true);
  assert.equal(edge.limit, 3);
  assert.equal(edge.offset, 1);
  assert.deepEqual(edge.columns, ["id", "title", "views"]);
});

test("q05 unit: per-child limit is per-parent by construction — the LIMIT sits inside the correlated subquery", () => {
  const built = plan(usersRelations, { with: { posts: { limit: 2 } } });
  const s = built.sql;
  // The ONLY limit in the statement is inside the posts subquery's derived
  // table; the outer (parent) statement carries no limit.
  assert.ok(s.includes(" limit 2)"));
  assert.ok(!s.includes("limit 2\") as"), "parent statement must not carry the child limit");
  const outerTail = s.slice(s.lastIndexOf(') as "posts"'));
  assert.ok(!/ limit \d+/.test(outerTail), `outer statement tail must be limit-free: ${outerTail}`);
});

test("q05 unit: per-child where referencing a foreign table fails closed before SQL", () => {
  assert.throws(
    () => plan(usersRelations, { with: { posts: { where: eq(users.name, "x") } } }),
    /users\.posts: where\/orderBy may reference only the relation's own table "posts" — found "users"."name"/,
  );
  assert.throws(
    () => plan(usersRelations, { with: { posts: { orderBy: [asc(users.id)] } } }),
    /where\/orderBy may reference only the relation's own table "posts" — found "users"."id"/,
  );
});

test("q05 unit: subqueries and trusted segments in per-child expressions fail closed", () => {
  const trusted = trustSql("posts.views > 3", TRUSTED_SQL_ACK);
  assert.throws(
    () => plan(usersRelations, { with: { posts: { where: sql`${trusted}` } } }),
    /users\.posts: trusted SQL segments cannot be rewritten to the relation's alias/,
  );
  const inner = subquery(
    selectStatement({
      projections: [projection(exprNode("call", "count", []), undefined)],
      from: ident("posts"),
    }),
  );
  assert.throws(
    () => plan(usersRelations, { with: { posts: { where: sql`${posts.views} > ${inner}` } } }),
    /users\.posts: subqueries inside per-relation where\/orderBy are not supported/,
  );
});

test("q05 unit: nesting beyond MAX_RELATION_DEPTH is rejected before SQL", () => {
  const tooDeep: RQBArgs = { with: { posts: { with: { comments: { with: { post: { with: { author: { with: { manager: { with: { reports: true } } } } } } } } } } } };
  assert.throws(
    () => plan(usersRelations, tooDeep),
    new RegExp(`relation nesting depth ${MAX_RELATION_DEPTH + 1} exceeds the maximum of ${MAX_RELATION_DEPTH}: users\\.posts\\.comments\\.post\\.author\\.manager\\.reports`),
  );
  // Exactly at the bound still compiles.
  const atBound: RQBArgs = { with: { posts: { with: { comments: { with: { post: { with: { author: { with: { manager: true } } } } } } } } } };
  assert.doesNotThrow(() => plan(usersRelations, atBound));
});

test("q05 unit: limit/offset/orderBy on a to-one relation is rejected", () => {
  assert.throws(
    () => plan(postsRelations, { with: { author: { limit: 1 } } }),
    /relation "author" in with on posts: limit\/offset apply to to-many relations only/,
  );
  assert.throws(
    () => plan(commentsRelations, { with: { post: { offset: 2 } } }),
    /relation "post" in with on comments: limit\/offset apply to to-many relations only/,
  );
  // orderBy on a to-one is a no-op by cardinality (single row) — rejected
  // precisely instead of being silently accepted (rework m1).
  assert.throws(
    () => plan(postsRelations, { with: { author: { orderBy: [desc(users.id)] } } }),
    /relation "author" in with on posts: orderBy applies to to-many relations only — a to-one matches at most one row, so ordering cannot change the result/,
  );
});

test("q05 unit: unknown nested relation and nested column name the path", () => {
  assert.throws(
    () => plan(usersRelations, { with: { posts: { with: { nope: true } } } } as never),
    /unknown relation "nope" on posts/,
  );
  assert.throws(
    () => plan(usersRelations, { with: { posts: { columns: ["nope"] } } } as never),
    /unknown column "nope" in args.columns on users\.posts/,
  );
});

test("q05 unit: long relation paths fall back to compact aliases inside the 63-byte limit", () => {
  const longKeyTable = pgTable("q05_long_a", {
    id: serial("id").primaryKey(),
    refId: integer("ref_id"),
    filler: text("filler").notNull().default(""),
  });
  // Build a deep chain with very long key names via hand-wired relations.
  const k1 = "aVeryLongRelationKeyNameOfAboutThirtyBytes";
  const k2 = "anotherExtremelyLongRelationKeyNameAlsoThirty";
  const k3 = "yetAnotherLongRelationNameThatAddsMoreBytes";
  const longRels = relations(longKeyTable, ({ one, many }) => ({
    [k1]: many(longKeyTable, { relationName: "pair" }),
    [k2]: one(longKeyTable, { fields: [longKeyTable.refId], references: [longKeyTable.id], relationName: "pair" }),
  }));
  resolveRelations([longRels]);
  const byTable = new Map<string, Record<string, import("./index.js").Relation>>([["q05_long_a", longRels.entries]]);
  const deep: RQBArgs = { with: { [k1]: { with: { [k1]: { with: { [k1]: { with: { [k1]: {} } } } } } } } } as never;
  const built = buildRelationalPlan(longKeyTable, longRels.entries, deep, byTable);
  // Every generated alias fits the 63-byte identifier limit.
  for (const m of built.sql.matchAll(/as "(__rel_[^"]*|__r\d+)"/g)) {
    assert.ok(m[1].length <= 63, `alias ${m[1]} exceeds 63 bytes`);
  }
  // The deepest aliases were compacted (deterministic counter names appear).
  assert.ok(/__r\d+/.test(built.sql), "expected compact fallback aliases in the deep chain");
  // Determinism: same args compile byte-identically again.
  const again = buildRelationalPlan(longKeyTable, longRels.entries, deep, byTable);
  assert.equal(again.sql, built.sql);
});

test("q05 unit: composite FK keys zip positionally regardless of declaration order", () => {
  const invoices = pgTable("q05_inv", {
    tenantId: integer("tenant_id").notNull().primaryKey(),
    id: integer("id").notNull().primaryKey(),
    amount: integer("amount").notNull(),
  });
  const lines = pgTable("q05_lines", {
    id: serial("id").primaryKey(),
    tenantId: integer("tenant_id").notNull(),
    invoiceId: integer("invoice_id").notNull(),
    memo: text("memo").notNull(),
  });
  const invRels = relations(invoices, ({ many }) => ({ lines: many(lines) }));
  // Deliberately REVERSED field/reference order: correlation must zip by
  // POSITION (invoiceId->invoices.id, tenantId->invoices.tenant_id).
  const lineRels = relations(lines, ({ one }) => ({
    invoice: one(invoices, {
      fields: [lines.invoiceId, lines.tenantId],
      references: [invoices.id, invoices.tenantId],
    }),
  }));
  resolveRelations([invRels, lineRels]);
  const byTable = new Map<string, Record<string, import("./index.js").Relation>>([
    ["q05_inv", invRels.entries],
    ["q05_lines", lineRels.entries],
  ]);
  const manySide = buildRelationalPlan(invoices, invRels.entries, { with: { lines: true } }, byTable);
  // The many() pairs with the one() on lines, whose declaration is reversed —
  // the correlation zips THAT order positionally.
  assert.ok(
    manySide.sql.includes('("__rel_lines"."invoice_id" = "q05_inv"."id") and ("__rel_lines"."tenant_id" = "q05_inv"."tenant_id")'),
    manySide.sql,
  );
  const oneSide = buildRelationalPlan(lines, lineRels.entries, { with: { invoice: true } }, byTable);
  assert.ok(
    oneSide.sql.includes('("__rel_invoice"."id" = "q05_lines"."invoice_id") and ("__rel_invoice"."tenant_id" = "q05_lines"."tenant_id")'),
    oneSide.sql,
  );
});

test("q05 unit: toSQL and explainQuery are pure — repeated compiles are byte-identical", () => {
  const args: RQBArgs = {
    where: eq(users.id, 7),
    with: { posts: { where: gt(posts.views, 2), with: { comments: { limit: 4 } } } },
  };
  const a = plan(usersRelations, args);
  const b = plan(usersRelations, args);
  assert.equal(a.sql, b.sql);
  assert.deepEqual(a.params, b.params);
  // Deterministic placeholder order follows compilation traversal: parent
  // projections first (the child subqueries live in the projection list, so
  // their filters bind before the parent's own where).
  assert.deepEqual(a.params, [2, 7]);
  assert.ok(a.sql.includes('("__rel_posts"."views" > $1)'));
  assert.ok(a.sql.includes('("users"."id" = $2)'));
});

test("q05 unit: explainQuery describes statements, decoders and capability requirements", () => {
  const built = plan(usersRelations, { columns: ["id", "name"], with: { posts: true } });
  assert.equal(built.table, "users");
  assert.equal(built.statementCount, 1);
  assert.deepEqual(built.capabilities, ["jsonb-functions"]);
  // Parent decode plan rides along in serializable form (none for
  // int/text columns here).
  assert.deepEqual(built.decoders, []);

  // A temporal parent column produces a text-wire decoder entry with the
  // serializable codec description and column context.
  const events = pgTable("q05_events", {
    id: serial("id").primaryKey(),
    seen: timestamp("seen").notNull(),
  });
  const eventsRels = relations(events, () => ({}));
  const tsPlan = buildRelationalPlan(events, eventsRels.entries, {}, new Map([[getTableName(events), eventsRels.entries]]));
  assert.equal(tsPlan.decoders.length, 1);
  assert.equal(tsPlan.decoders[0].key, "seen");
  assert.equal(tsPlan.decoders[0].wire, "text");
  assert.equal(tsPlan.decoders[0].codec.read, "timestamp-text");
  assert.equal(tsPlan.decoders[0].context.tableName, "q05_events");
  // Temporal wire acquisition requires jsonb-functions even with no relations.
  assert.deepEqual(tsPlan.capabilities, ["jsonb-functions"]);
});

test("q05 unit: buildRelationalSQL keeps the one-level shape byte-compatible", () => {
  const built = buildRelationalSQL(usersRelations.table as never, usersRelations.entries, { with: { posts: true } }, RELATIONS_BY_TABLE);
  assert.ok(built.sql.includes('select "users"."id", "users"."name", "users"."manager_id" as "managerId",'));
  assert.ok(built.sql.includes("coalesce(jsonb_agg(jsonb_build_object("));
  assert.ok(built.sql.includes('order by "__rel_posts"."id"), \'[]\'::jsonb)'));
});

test("q05 unit: bare identifiers in child predicates resolve against the child FROM", () => {
  // ident("views") renders unqualified — PostgreSQL resolves it against the
  // subquery's own FROM (the edge alias), so no remap is needed or performed.
  const built = plan(usersRelations, { with: { posts: { where: sql`${ident("views")} > ${5}` } } });
  assert.ok(built.sql.includes('"views" > $1'));
});

test("q05 unit: db.query.toSQL/explainQuery work on a database that never connects", async () => {
  // Inert URL: postgres.js connects lazily, so createDatabase resolves without
  // a server — toSQL/explainQuery never touch the driver.
  const db = await createDatabase({
    url: "postgres://q05-inert:nouser@127.0.0.1:1/none",
    driverOptions: { driver: "postgres" },
    tables: { users, posts, comments },
    relations: { users: usersRelations, posts: postsRelations, comments: commentsRelations },
  });
  const compiled = db.query.users.toSQL({ with: { posts: { with: { comments: { limit: 2 } } } } });
  assert.ok(compiled.sql.startsWith('select '));
  assert.ok(compiled.sql.includes('as "__rel_posts"'));
  assert.ok(compiled.sql.includes('as "__rel_posts__comments"'));
  assert.deepEqual(compiled.params, []);
  const plan = db.query.users.explainQuery({ with: { posts: true } });
  assert.equal(plan.statementCount, 1);
  assert.equal(plan.depth, 1);
  assert.deepEqual(plan.capabilities, ["jsonb-functions"]);
  assert.equal(plan.edges[0].target, "posts");
  await db.close();
});

test("q05 unit: fragment order expressions survive the limited shape (aggregate order re-remapped)", () => {
  // A complex (fragment) per-child order key must render against the DERIVED
  // alias in the aggregate — not the physical alias, which is out of scope
  // there (self-review adversarial: only plain-column keys were re-targeted
  // by the first implementation).
  const built = plan(usersRelations, {
    with: { posts: { orderBy: [{ expr: sql`${posts.views} + ${0}`, direction: "desc" as const }], limit: 2, columns: ["views"] } },
  });
  const s = built.sql;
  // The aggregate order references the DERIVED alias inside the fragment.
  assert.ok(s.includes('order by "__rel_posts_lim"."views" + $1 desc'), s);
  // The derived's own order references the physical alias.
  assert.ok(s.includes('order by "__rel_posts"."views" + $2 desc, "__rel_posts"."id" asc limit 2'), s);
});

test("q05 unit: limited shape derives UNSELECTED order keys (rework M1)", () => {
  // columns subset + limit + orderBy on a column OUTSIDE the subset compiled
  // and then 42703'd at runtime ("__rel_posts_lim.views does not exist") —
  // the attempt-1 fix #1 (derive selected ∪ PK) did not cover order keys.
  const built = plan(usersRelations, {
    with: { posts: { columns: ["id"], orderBy: [desc(posts.views)], limit: 2 } },
  });
  const s = built.sql;
  // The derived table projects every target column, order key included.
  assert.ok(
    s.includes(
      'select "__rel_posts"."id", "__rel_posts"."author_id", "__rel_posts"."reviewer_id", "__rel_posts"."title", "__rel_posts"."views" from "posts" as "__rel_posts"',
    ),
    s,
  );
  // The aggregate's order key resolves against the derived alias.
  assert.ok(s.includes('order by "__rel_posts_lim"."views" desc, "__rel_posts_lim"."id")'), s);
  // The emitted JSON still carries ONLY the selected column.
  assert.ok(s.includes("jsonb_build_object('id', \"__rel_posts_lim\".\"id\")"), s);
  assert.ok(!s.includes("'title',"), s);
  assert.ok(!s.includes("'views',"), s);
});

test("q05 unit: limited shape derives UNSELECTED nested-edge FK columns (rework M2)", () => {
  // columns subset + limit + nested with whose to-one FK correlation reads
  // the derived alias 42703'd at runtime ("__rel_posts_lim.author_id does
  // not exist") — the FK column was only present when selected or PK.
  const built = plan(usersRelations, {
    with: { posts: { columns: ["id", "title"], limit: 2, with: { author: { columns: ["name"] } } } },
  });
  const s = built.sql;
  // author_id is in the derived projection even though unselected...
  assert.ok(
    s.includes(
      'select "__rel_posts"."id", "__rel_posts"."author_id", "__rel_posts"."reviewer_id", "__rel_posts"."title", "__rel_posts"."views" from "posts" as "__rel_posts"',
    ),
    s,
  );
  // ...and the nested edge's correlation resolves against the DERIVED alias.
  assert.ok(s.includes('("__rel_posts__author"."id" = "__rel_posts_lim"."author_id")'), s);
  // The emitted JSON carries the selected columns plus the nested key only.
  assert.ok(s.includes("'title', \"__rel_posts_lim\".\"title\""), s);
  assert.ok(!s.includes("'views',"), s);
});

test("q05 unit: combined unselected order key + unselected FK, and raw-text fragment order keys, under limit", () => {
  // Both rework defects in ONE query: orderBy on an unselected column AND a
  // nested with whose FK is unselected, under limit.
  const built = plan(usersRelations, {
    with: { posts: { columns: ["id"], orderBy: [desc(posts.views)], limit: 2, with: { author: { columns: ["name"] } } } },
  });
  const s = built.sql;
  assert.ok(s.includes('order by "__rel_posts_lim"."views" desc'), s);
  assert.ok(s.includes('("__rel_posts__author"."id" = "__rel_posts_lim"."author_id")'), s);

  // A fragment order key naming its column ONLY as raw text has no
  // structural node to remap or extract — the derived table projecting
  // every column is what makes it resolve (this is WHY the fix derives all
  // columns instead of walking order expressions for references).
  const frag = plan(usersRelations, {
    with: { posts: { columns: ["id"], orderBy: [sql`length(title)`], limit: 1 } },
  });
  assert.ok(frag.sql.includes("order by length(title), \"__rel_posts_lim\".\"id\""), frag.sql);
  assert.ok(frag.sql.includes('"__rel_posts"."title"'), frag.sql);
});

test("q05 unit: columns subset WITHOUT limit keeps the lean projection (no derived table)", () => {
  // The derive-everything fix must not bloat the unlimited path: no derived
  // table exists there at all, and the aggregate object emits exactly the
  // selected columns — order keys are raw references, not projections.
  const built = plan(usersRelations, {
    with: { posts: { columns: ["id"], orderBy: [desc(posts.views)] } },
  });
  const s = built.sql;
  assert.ok(!s.includes("_lim"), "no limited derived table without limit/offset");
  assert.ok(s.includes("jsonb_build_object('id', \"__rel_posts\".\"id\") order by \"__rel_posts\".\"views\" desc"), s);
  assert.ok(!s.includes("'title',"), s);
  assert.ok(!s.includes("'views',"), s);
  assert.ok(!s.includes("'author_id',"), s);
});
