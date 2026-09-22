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
  relations,
  eq,
  getTableName,
  getTableColumns,
  getTableIndexes,
  createDatabase,
  type ColumnBuilder,
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
  { id?: number | undefined; email: string; name?: string | null | undefined; createdAt?: Date | undefined }
> = true;
type UsersSelect = typeof users.$inferSelect;
const eqSelect: AssertEq<UsersSelect, { id: number; email: string; name: string | null; createdAt: Date }> = true;
void [eqInsert, eqSelect];

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
// @ts-expect-error invalid value type (boolean for text)
const badValue: UsersInsert = { email: "x", name: true };
// @ts-expect-error unknown insert key
const badKey: UsersInsert = { email: "x", nope: 1 };
// @ts-expect-error collision column "columns" is required
const badCollision: CollisionInsert = { tableName: null };
// @ts-expect-error invalid value for the collision column
const badCollisionValue: CollisionInsert = { columns: 42 };
void [badNull, badValue, badKey, badCollision, badCollisionValue];

// --- exact relational row types through the packed declarations ------------

type ExpectedUserRow = { id: number; email: string; name: string | null; createdAt: Date };
/** users row as a relation CHILD: the temporal leaf arrives as a string through the JSON path. */
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
    { id: number; email: string; name: string | null; createdAt: Date; posts: ExpectedPostChild[] }
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
