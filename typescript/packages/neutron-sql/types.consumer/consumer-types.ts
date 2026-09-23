// F02 consumer type fixture — compiled with tsc AGAINST THE PACKED DECLARATIONS
// (dist/index.d.ts, reached through the package's own `exports` mapping via the
// self-referencing import below). Source-level fixtures live in src/sql.test.ts;
// this file guards the emitted/packed surface: if declaration emit erodes any
// exactness (required insert keys, literal relation maps, collision-free
// metadata, exact row types), the @ts-expect-error directives go unused (a
// compile error) or the AssertEq assertions stop holding.
//
// Never executed — `pnpm test:types` only type-checks it.
import {
  pgTable,
  pgSchema,
  alias,
  serial,
  integer,
  numeric,
  boolean,
  serial as serial2,
  text,
  timestamp,
  timestamptz,
  bigint,
  jsonNull,
  relations,
  eq,
  and,
  or,
  not,
  sql,
  raw,
  asc,
  desc,
  astSelect,
  exportSchemaV2,
  canonicalSchemaJson,
  readSchemaDocumentV1,
  getTableName,
  getTableColumns,
  getTableIndexes,
  getTableSchema,
  isAliasHandle,
  isDerivedTableHandle,
  createDatabase,
  count,
  countDistinct,
  sum,
  avg,
  min,
  max,
  stringAgg,
  boolAnd,
  boolOr,
  exists,
  excluded,
  cteTable,
  derivedTable,
  type ColumnBuilder,
  type Condition,
  type OrderExpression,
  type OrderSpec,
  type ValueNode,
  type AggregateNode,
  type CompiledStatement,
  type ProjectionDecoder,
  type StatementCapability,
  type SchemaDocumentV2,
  type AliasedTable,
  type DerivedTable,
  type SetOpBuilder,
} from "@neutron-build/sql";
void numeric; void boolean; void serial2;

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  name: text("name"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id),
  title: text("title").notNull(),
});

const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

// Metadata-name collisions: ordinary columns named columns/tableName/indexes.
const collision = pgTable("collision", {
  id: serial("id").primaryKey(),
  columns: text("columns").notNull(),
  tableName: text("tableName"),
  indexes: integer("indexes"),
});

// --- exact insert/select models through the packed declarations -----------

type UsersInsert = typeof users.$inferInsert;
const eqInsert: AssertEq<
  UsersInsert,
  { id?: number | undefined; email: string; name?: string | null | undefined; createdAt?: string | Date | undefined }
> = true;
type UsersSelect = typeof users.$inferSelect;
const eqSelect: AssertEq<UsersSelect, { id: number; email: string; name: string | null; createdAt: string }> = true;
void [eqInsert, eqSelect];

// F03 codec modes through the packed declarations: bigint defaults to bigint
// with opt-in string/number modes; temporals default to canonical strings
// with an explicit Date mode.
const modes = pgTable("modes", {
  big: bigint("big"),
  bigStr: bigint("big_str", { mode: "string" }),
  bigNum: bigint("big_num", { mode: "number" }),
  at: timestamptz("at"),
  atDate: timestamptz("at_date", { mode: "date" }),
});
type ModesSelect = typeof modes.$inferSelect;
const eqModes: AssertEq<
  ModesSelect,
  { big: bigint | null; bigStr: string | null; bigNum: number | null; at: string | null; atDate: Date | null }
> = true;
void eqModes;
// temporal and jsonNull writes type-check through the packed declarations
void modes.$inferInsert;
declare function modesAre(v: typeof modes.$inferInsert): void;
modesAre({ big: 1n, bigStr: "1", bigNum: 2, at: "2026-01-01T00:00:00Z", atDate: new Date() });
modesAre({ at: new Date() });

type CollisionInsert = typeof collision.$inferInsert;
const eqCollisionInsert: AssertEq<
  CollisionInsert,
  { id?: number | undefined; columns: string; tableName?: string | null | undefined; indexes?: number | null | undefined }
> = true;
void eqCollisionInsert;

// The user surface types those names as columns; accessors return metadata.
const eqSurface: AssertEq<typeof collision.tableName, ColumnBuilder<"text", false, false>> = true;
const eqSurface2: AssertEq<typeof collision.columns, ColumnBuilder<"text", true, false>> = true;
const metaName: string = getTableName(collision);
const metaColumnCount: number = Object.keys(getTableColumns(collision)).length;
const metaIndexCount: number = getTableIndexes(collision).length;
void [eqSurface, eqSurface2, metaName, metaColumnCount, metaIndexCount];

// --- negative fixtures: rejection must survive declaration emit -------------

declare function valuesAre(v: UsersInsert): void;
// @ts-expect-error missing required insert key "email" (NOT NULL, no default)
valuesAre({ name: "no email" });

// @ts-expect-error null for a NOT NULL column
const badNull: UsersInsert = { email: "x", createdAt: null };
// @ts-expect-error jsonNull only binds json/jsonb columns
const badJsonNull: UsersInsert = { email: "x", createdAt: jsonNull };
// @ts-expect-error invalid value type (boolean for text)
const badValue: UsersInsert = { email: "x", name: true };
// @ts-expect-error unknown insert key
const badKey: UsersInsert = { email: "x", nope: 1 };
// @ts-expect-error collision column "columns" is required
const badCollision: CollisionInsert = { tableName: null };
// @ts-expect-error invalid value for the collision column
const badCollisionValue: CollisionInsert = { columns: 42 };
void [badNull, badJsonNull, badValue, badKey, badCollision, badCollisionValue];

// --- exact relational row types through the packed declarations ------------

type ExpectedUserRow = { id: number; email: string; name: string | null; createdAt: string };
/** users row as a relation CHILD: same leaf types as the flat path. */
type ExpectedUserChildRow = { id: number; email: string; name: string | null; createdAt: string };
type ExpectedPostChild = { id: number; userId: number; title: string };

async function relationalFixtures(): Promise<void> {
  const db = await createDatabase({
    url: "postgres://type-fixture:not-run@localhost:1/none",
    tables: { users, posts, collision },
    relations: { users: usersRelations, posts: postsRelations },
  });

  const withPosts = await db.query.users.findMany({ with: { posts: true } });
  const eqMany: AssertEq<
    (typeof withPosts)[number],
    { id: number; email: string; name: string | null; createdAt: string; posts: ExpectedPostChild[] }
  > = true;
  const withAuthor = await db.query.posts.findFirst({ with: { author: true } });
  const eqOne: AssertEq<
    NonNullable<typeof withAuthor>,
    { id: number; userId: number; title: string; author: ExpectedUserChildRow | null }
  > = true;
  const selected = await db.query.users.findMany({ columns: ["id", "email"] });
  const eqSelected: AssertEq<(typeof selected)[number], { id: number; email: string }> = true;
  void [eqMany, eqOne, eqSelected];
  void [withPosts, withAuthor, selected];

  // @ts-expect-error unknown relation name in with
  void db.query.users.findMany({ with: { nope: true } });
  // @ts-expect-error unrequested relation access is absent from the row type
  void withPosts[0].comments;
  // @ts-expect-error nested with (depth 2) is rejected until Q05
  void db.query.users.findMany({ with: { posts: { with: { author: true } } } });
  // @ts-expect-error to-one result is nullable (outer-join semantics)
  const notNull: ExpectedUserRow = (await db.query.posts.findFirst({ with: { author: true } }))!.author;
  // @ts-expect-error to-many cardinality is an array
  const asOne: ExpectedPostChild = withPosts[0].posts;
  void [notNull, asOne];

  // Collision table CRUD stays typed through the packed declarations.
  await db.insert(collision).values({ columns: "c", tableName: null, indexes: 1 });
  // @ts-expect-error required collision column missing
  await db.insert(collision).values({ tableName: null });
  const rows = await db.select().from(collision).where(eq(collision.columns, "c"));
  const eqCollisionRow: AssertEq<
    (typeof rows)[number],
    { id: number; columns: string; tableName: string | null; indexes: number | null }
  > = true;
  void [rows, eqCollisionRow];
}
void relationalFixtures;

// --- F04 public shapes through the packed declarations ----------------------
// Condition/OrderExpression/CompiledStatement exactness and the F04 migration
// claims (predicates return AST nodes; legacy raw() fragments are not
// conditions; asc()/desc() return order specs; compiled statements carry
// decode plans + capability requirements).

// Condition is the AST ValueNode union: every predicate/connective returns it.
const cond: Condition = and(eq(users.id, 1), sql`${users.name}`, not(or(eq(users.email, "x"), sql`${users.name} is null`)));
const eqCondIsValueNode: AssertEq<Condition, ValueNode> = true;
void [cond, eqCondIsValueNode];
// @ts-expect-error legacy raw() fragments are not Conditions (direct execution only)
const badCond: Condition = raw("id = $1", [1]);
// @ts-expect-error destructured .sql on predicates no longer exists (F04 migration note)
const { sql: gone } = eq(users.id, 1);
void [badCond, gone];

// asc()/desc() return frozen order specs; OrderExpression is spec-or-node.
const spec: OrderSpec = asc(users.id);
const dirIsClosed: AssertEq<(typeof spec)["direction"], "asc" | "desc"> = true;
const descSpec: OrderSpec = desc(users.createdAt);
const anyOrder: OrderExpression[] = [spec, descSpec, sql`${users.id} + 1`];
void [dirIsClosed, anyOrder];

// CompiledStatement: sql + params + decode plans + capability requirements,
// and toCompiled() is the builder entry for it.
async function compiledStatementFixtures(): Promise<void> {
  const db = await createDatabase({ url: "postgres://type-fixture:not-run@localhost:1/none", tables: { users, posts, collision } });
  const compiled: CompiledStatement = db.select().from(users).where(eq(users.id, 1)).toCompiled();
  const sqlText: string = compiled.sql;
  const params: readonly unknown[] = compiled.params;
  const decoders: readonly ProjectionDecoder[] = compiled.decoders;
  const capabilities: readonly StatementCapability[] = compiled.capabilities;
  const capIsJsonb: AssertEq<StatementCapability, "jsonb-functions"> = true;
  const mutated: CompiledStatement = db.update(users).set({ name: "x" }).where(eq(users.id, 1)).returning().toCompiled();
  void [sqlText, params, decoders, capabilities, mutated, capIsJsonb];
}
void compiledStatementFixtures;

// Schema export v2: typed document tree, deterministic canonical bytes,
// explicit v1 compatibility reader.
const doc: SchemaDocumentV2 = exportSchemaV2({ users, posts });
const docVersion: AssertEq<(typeof doc)["version"], 2> = true;
const canonical: string = canonicalSchemaJson(doc);
const upgraded: SchemaDocumentV2 = readSchemaDocumentV1('{"version":1,"dialect":"postgresql","tables":[]}');
void [docVersion, canonical, upgraded];

// --- Q01 joins and aliases through the packed declarations -------------------
// Outer-join nullability is exact: a NOT NULL column on the nullable side of
// an outer join reads `| null`; the preserved side stays exact. Self joins
// and same-SQL-name tables in two schemas keep distinct keys and types.

const reviews = pgTable("reviews", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  body: text("body").notNull(),
  at: timestamp("at"),
});

const legacy = pgSchema("legacy");
const legacyUsers = legacy.table("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
});

async function joinFixtures(): Promise<void> {
  const db = await createDatabase({ url: "postgres://type-fixture:not-run@localhost:1/none", tables: { users, posts } });
  const r = alias(reviews, "r");

  const inner = db.select({ email: users.email, body: r.body }).from(users).innerJoin(r, sql`${r.userId} = ${users.id}`);
  const eqInner: AssertEq<Awaited<typeof inner>[number], { email: string; body: string }> = true;

  const left = db.select({ email: users.email, body: r.body, at: r.at }).from(users).leftJoin(r, sql`${r.userId} = ${users.id}`);
  const eqLeft: AssertEq<Awaited<typeof left>[number], { email: string; body: string | null; at: string | null }> = true;
  // @ts-expect-error left-joined NOT NULL columns are nullable in results
  const badLeft: { email: string; body: string; at: string | null } = {} as Awaited<typeof left>[number];

  const right = db.select({ email: users.email, body: r.body }).from(users).rightJoin(r, sql`${r.userId} = ${users.id}`);
  const eqRight: AssertEq<Awaited<typeof right>[number], { email: string | null; body: string }> = true;

  const full = db.select({ email: users.email, body: r.body }).from(users).fullJoin(r, sql`${r.userId} = ${users.id}`);
  const eqFull: AssertEq<Awaited<typeof full>[number], { email: string | null; body: string | null }> = true;

  const cross = db.select({ email: users.email, body: r.body }).from(users).crossJoin(r);
  const eqCross: AssertEq<Awaited<typeof cross>[number], { email: string; body: string }> = true;

  // expression projections over joined columns are unknown without a decoder
  const expr = db.select({ older: sql`${r.at} < ${"2026-01-01T00:00:00"}` }).from(users).innerJoin(r, sql`${r.userId} = ${users.id}`);
  const eqExpr: AssertEq<Awaited<typeof expr>[number], { older: unknown }> = true;

  // self join: two handles of one table with independent nullability
  const mgr = alias(users, "mgr");
  const mentor = alias(users, "mentor");
  const selfQ = db.select({ name: users.name, boss: mgr.name, guide: mentor.name })
    .from(users)
    .innerJoin(mgr, sql`${mgr.id} = ${users.id}`)
    .leftJoin(mentor, sql`${mentor.id} = ${users.id}`);
  const eqSelf: AssertEq<Awaited<typeof selfQ>[number], { name: string | null; boss: string | null; guide: string | null }> = true;

  // same SQL table name in a second schema: distinct keys, no collision
  const lu = alias(legacyUsers, "lu");
  const two = db.select({ current: users.email, legacy: lu.email }).from(users).leftJoin(lu, sql`${lu.id} = ${users.id}`);
  const eqTwo: AssertEq<Awaited<typeof two>[number], { current: string; legacy: string | null }> = true;
  const schemaName: string | undefined = getTableSchema(legacyUsers);
  const noSchema: string | undefined = getTableSchema(users);
  const handleCheck: boolean = isAliasHandle(lu) && !isAliasHandle(users);
  void [schemaName, noSchema, handleCheck];

  // @ts-expect-error joins take alias() handles, not raw tables
  db.select({ email: users.email }).from(users).leftJoin(reviews, sql`1 = 1`);
  db.insert(mgr).values({ email: "x" }); // runtime-rejected (handles are join identities)

  void [eqInner, eqLeft, eqRight, eqFull, eqCross, eqExpr, eqSelf, eqTwo, badLeft];
  void [inner, left, right, full, cross, expr, selfQ, two];
}
void joinFixtures;


// --- Q02 subqueries/CTEs/aggregates/set operations through the packed
// declarations: aggregate result types (count never null; sum/avg/min/max/
// string_agg/bool_* nullable on empty input), CTE/derived row types derived
// from projections, set-op rows typed by the first branch.

async function q02Fixtures(): Promise<void> {
  const db = await createDatabase({ url: "postgres://type-fixture:not-run@localhost:1/none", tables: { users, posts, reviews } });

  // Aggregate result types follow PostgreSQL exactly.
  const g = db.select({
    userId: reviews.userId,
    n: count(),
    nd: countDistinct(reviews.userId),
    bodies: stringAgg(reviews.body, ", "),
  }).from(reviews).groupBy(reviews.userId);
  const eqAgg: AssertEq<
    Awaited<typeof g>[number],
    { userId: number; n: bigint; nd: bigint; bodies: string | null }
  > = true;
  // @ts-expect-error string_agg is nullable on empty input
  const badBodies: { bodies: string } = ({} as Awaited<typeof g>[number]);

  // sum/avg/min/max typing per column type: numerics sum to exact strings,
  // integer columns to bigint (the int8 codec), min/max mirror the column.
  const ledger = pgTable("ledger", {
    id: serial("id").primaryKey(),
    cents: integer("cents"),
    amount: numeric("amount"),
    at: timestamp("at"),
    big: bigint("big"),
  });
  const s1 = db.select({ total: sum(ledger.cents), amount: sum(ledger.amount), mean: avg(ledger.amount), lo: min(ledger.at), hi: max(ledger.big) }).from(ledger);
  const eqSum: AssertEq<
    Awaited<typeof s1>[number],
    { total: bigint | null; amount: string | null; mean: string | null; lo: string | null; hi: bigint | null }
  > = true;

  // min/max over a derived/CTE timestamptz column keep the column's read
  // type through composition (the aggregate mirrors its argument column).
  const events = pgTable("events", { id: serial("id").primaryKey(), seen: timestamptz("seen") });
  const seenSrc = derivedTable("seen_src", db.select({ id: events.id, seen: events.seen }).from(events));
  const seenAgg = db.select({ lo: min(seenSrc.seen), hi: max(seenSrc.seen) }).from(seenSrc);
  const eqSeenAgg: AssertEq<Awaited<typeof seenAgg>[number], { lo: string | null; hi: string | null }> = true;
  const seenComposed = db.select({ seen: seenSrc.seen }).from(seenSrc);
  const eqSeenComposed: AssertEq<Awaited<typeof seenComposed>[number], { seen: string | null }> = true;

  // groupBy/having/distinct chain on the same builder without widening.
  const grouped = db.select({ userId: reviews.userId, n: count() })
    .from(reviews)
    .groupBy(reviews.userId)
    .having(sql`${count()} > ${1}`)
    .distinct();
  const eqGrouped: AssertEq<Awaited<typeof grouped>[number], { userId: number; n: bigint }> = true;

  // CTE/derived row types derive from the source's projections; count stays
  // bigint through the handle.
  const src = db.select({ userId: reviews.userId, n: count() }).from(reviews).groupBy(reviews.userId);
  const agg = cteTable("agg", src);
  const fromCte = db.select({ userId: agg.userId, n: agg.n }).from(agg);
  const eqCte: AssertEq<Awaited<typeof fromCte>[number], { userId: number; n: bigint }> = true;
  // @ts-expect-error the derived column keeps its source type
  const badCte: { n: number } = ({} as Awaited<typeof fromCte>[number]);

  const wrapped = derivedTable("wrapped", fromCte);
  const fromDerived = db.select().from(wrapped);
  const eqDerived: AssertEq<Awaited<typeof fromDerived>[number], { userId: number; n: bigint }> = true;

  // leftJoin over a derived handle nulls its columns (the handle's own name
  // is the join identity).
  const lj = db.select({ email: users.email, n: wrapped.n })
    .from(users)
    .leftJoin(wrapped, sql`${wrapped.userId} = ${users.id}`);
  const eqLj: AssertEq<Awaited<typeof lj>[number], { email: string; n: bigint | null }> = true;
  // @ts-expect-error left-joined derived columns are nullable
  const badLj: { email: string; n: bigint } = ({} as Awaited<typeof lj>[number]);

  // Set operations type rows by the first branch.
  const u = db.select({ id: reviews.id }).from(reviews).union(db.select({ id: posts.id }).from(posts));
  const eqU: AssertEq<Awaited<typeof u>[number], { id: number }> = true;
  const compound: SetOpBuilder<{ id: number }> = u;
  void compound;

  // exists() takes a subquery node or any builder with .subquery().
  const cond: Condition = exists(astSelect({ one: sql`1` }).from(reviews).where(sql`${reviews.userId} = ${users.id}`));
  void db.select({ email: users.email }).from(users).where(cond);

  // handle guards distinguish derived/CTE handles from tables and aliases.
  const guard: boolean = isDerivedTableHandle(wrapped) && !isDerivedTableHandle(users) && !isAliasHandle(wrapped);
  void guard;
  const derivedType: DerivedTable<"wrapped", { userId: number; n: bigint }> = wrapped;
  void derivedType;

  void [eqAgg, badBodies, eqSum, eqGrouped, eqCte, badCte, eqDerived, eqLj, badLj, eqU, eqSeenAgg, eqSeenComposed];
  void [g, s1, grouped, fromCte, fromDerived, lj, u, seenAgg, seenComposed];
}
void q02Fixtures;

// --- Q03 conflict handling and write expressions through the packed
// declarations: on-conflict builders keep the insert's R (number without
// returning, rows with), returning subsets are exact, excluded() values fit
// set inputs, and arity is honest (0..n rows for conflicted statements).

async function q03Fixtures(): Promise<void> {
  const db = await createDatabase({ url: "postgres://type-fixture:not-run@localhost:1/none", tables: { users, posts } });

  // do-nothing: R stays number without returning; rows array with it (0..n).
  const plain = db.insert(users).values({ email: "a@x.com" }).onConflictDoNothing();
  const eqPlain: AssertEq<Awaited<typeof plain>, number> = true;
  const rowsBack = db.insert(users).values({ email: "a@x.com" }).onConflictDoNothing().returning();
  const eqRows: AssertEq<Awaited<typeof rowsBack>[number], typeof users.$inferSelect> = true;

  // returning subsets are exact: only the requested keys, with column types.
  const subset = db.insert(users).values({ email: "a@x.com" }).returning(["id", "email"]);
  const eqSubset: AssertEq<Awaited<typeof subset>[number], { id: number; email: string }> = true;
  // @ts-expect-error unknown returning key is rejected
  db.insert(users).values({ email: "a@x.com" }).returning(["nope"]);
  const updSubset = db.update(users).set({ name: "n" }).where(eq(users.id, 1)).returning(["email", "name"]);
  const eqUpd: AssertEq<Awaited<typeof updSubset>[number], { email: string; name: string | null }> = true;
  void [eqPlain, eqRows, eqSubset, eqUpd];

  // upsert: excluded() values type-check in set inputs alongside literals
  // and sql expressions; targets take single/composite columns.
  const up = db
    .insert(users)
    .values({ email: "a@x.com" })
    .onConflictUpdate({ target: users.email, set: { name: sql`${excluded(users.name)}`, email: "kept@x.com" } })
    .returning(["id", "email"]);
  const eqUp: AssertEq<Awaited<typeof up>[number], { id: number; email: string }> = true;
  void [eqUp, up, subset, updSubset, rowsBack, plain];
  // @ts-expect-error unknown set key is rejected in on-conflict set maps
  db.insert(users).values({ email: "a@x.com" }).onConflictUpdate({ target: users.email, set: { nope: 1 } });
  // @ts-expect-error invalid literal type for the column
  db.insert(users).values({ email: "a@x.com" }).onConflictUpdate({ target: users.email, set: { name: 42 } });

  // Conditional upsert predicates take Conditions (fragments compose).
  const cond: Condition = or(sql`${users.name} is null`, eq(users.id, 1));
  void db
    .insert(users)
    .values({ email: "a@x.com" })
    .onConflictUpdate({ target: { columns: [users.email], where: sql`${users.name} is null` }, set: { name: "n" }, setWhere: cond });
}
void q03Fixtures;
