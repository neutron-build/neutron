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
  serial,
  integer,
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
  createDatabase,
  type ColumnBuilder,
  type Condition,
  type OrderExpression,
  type OrderSpec,
  type ValueNode,
  type CompiledStatement,
  type ProjectionDecoder,
  type StatementCapability,
  type SchemaDocumentV2,
} from "@neutron-build/sql";

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
