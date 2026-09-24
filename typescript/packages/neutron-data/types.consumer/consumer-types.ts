// I03 consumer type fixture — compiled with tsc AGAINST THE PACKED
// DECLARATIONS (dist/drizzle.d.ts and dist/index.d.ts, reached through the
// package's own `exports` map via the self-referencing imports below). Never
// executed; `pnpm run test:types` only type-checks it.
//
// What this guards (V14/V17 subset owned by I03):
// - the /drizzle subpath returns GENUINE drizzle-orm types — Postgres
//   overload -> PostgresJsDatabase<TSchema> & { $client: Sql }, SQLite
//   overload -> LibSQLDatabase<TSchema> & { $client: Client } — with the
//   caller's schema threaded through, so select()/query results carry
//   Drizzle's own row types, never a cast to a Neutron type;
// - the root export stays usable with the loose surface (no drizzle-orm
//   types leak into the root declaration — asserted by
//   root-export-purity.test.ts over the emitted dist);
// - invalid uses fail for the intended reason (@ts-expect-error directives
//   below — an unused directive is itself a compile error).

import { pgTable, serial, text, integer } from "drizzle-orm/pg-core";
import { sqliteTable, text as sqliteText, integer as sqliteInteger } from "drizzle-orm/sqlite-core";
import { relations } from "drizzle-orm";
import { eq } from "drizzle-orm";
import type { Sql } from "postgres";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { LibSQLDatabase } from "drizzle-orm/libsql";
import type { Client as LibSqlClient } from "@libsql/client";
import { createDrizzleDatabase } from "@neutron-build/data/drizzle";
import { createDrizzleDatabase as createDrizzleDatabaseRoot } from "@neutron-build/data";
import type { DrizzleDatabase } from "@neutron-build/data";

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  createdAt: text("created_at"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  title: text("title").notNull(),
});

const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

const schema = { users, posts, usersRelations, postsRelations };
type Schema = typeof schema;

// --- Postgres overload: genuine drizzle-orm/postgres-js types ---

const pg = await createDrizzleDatabase({
  profile: { provider: "postgres", connectionString: "postgres://localhost/db" },
  schema,
});

const eqPgDb: AssertEq<typeof pg.db, PostgresJsDatabase<Schema> & { $client: Sql }> = true;
const eqPgClient: AssertEq<typeof pg.client, Sql> = true;
const eqPgProfile: AssertEq<typeof pg.profile.provider, "postgres" | "nucleus"> = true;

const pgRows = await pg.db.select().from(users).where(eq(users.id, 1));
type UsersSelect = typeof users.$inferSelect;
const eqPgRows: AssertEq<typeof pgRows, UsersSelect[]> = true;

const pgAuthor = await pg.db.query.users.findFirst({ with: { posts: true } });
const eqPgRelational: AssertEq<
  typeof pgAuthor,
  | { id: number; email: string; createdAt: string | null; posts: { id: number; userId: number; title: string }[] }
  | undefined
> = true;

// Drizzle's own insert typing rejects unknown keys — the wrapper does not
// soften or replace it.
// @ts-expect-error unknown property "nope" in insert values
await pg.db.insert(users).values({ email: "a@x.com", nope: 1 });

// --- SQLite overload: genuine drizzle-orm/libsql types ---

const todos = sqliteTable("todos", {
  id: sqliteInteger("id").primaryKey({ autoIncrement: true }),
  label: sqliteText("label").notNull(),
});

const lite = await createDrizzleDatabase({
  profile: { provider: "sqlite", connectionString: "/tmp/i03-fixture.db" },
  schema: { todos },
});

const eqLiteDb: AssertEq<typeof lite.db, LibSQLDatabase<{ todos: typeof todos }> & { $client: LibSqlClient }> = true;
const eqLiteClient: AssertEq<typeof lite.client, LibSqlClient> = true;
const eqLiteNucleus: AssertEq<typeof lite.nucleus, null> = true;

const liteRows = await lite.db.select().from(todos).where(eq(todos.id, 3));
type TodosSelect = typeof todos.$inferSelect;
const eqLiteRows: AssertEq<typeof liteRows, TodosSelect[]> = true;

// @ts-expect-error sqlite label is notNull — null is not assignable
await lite.db.insert(todos).values({ label: null });

// --- Nucleus profile rides the postgres.js overload (same wire protocol) ---

const nuc = await createDrizzleDatabase({
  profile: { provider: "nucleus", connectionString: "postgres://localhost/nucleus" },
});
const eqNucDb: AssertEq<typeof nuc.db, PostgresJsDatabase<Record<string, never>> & { $client: Sql }> = true;

// --- Root alias keeps the loose pre-I03 surface ---

const root = await createDrizzleDatabaseRoot({ profile: { provider: "postgres", connectionString: "postgres://localhost/db" } });
const eqRootDb: AssertEq<typeof root.db, unknown> = true;
const loose: DrizzleDatabase = root;
void loose;

// The loose alias is the SAME runtime function (one implementation), only
// the static surface differs; passing the typed options shape through the
// root alias is still accepted at runtime, typed loosely.
await createDrizzleDatabaseRoot();
