import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  sql,
  pgTable,
  serial,
  integer,
  text,
  bigint,
  numeric,
  jsonb,
  relations,
  schemaToDDL,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Live V02 suite: mapped properties, NULL semantics and interim type claims.
// Connection selection and skip/fail semantics come from ./live-harness.ts;
// a uniquely named throwaway database (per process) is created and dropped
// by this file.
//
// Oracles are independently written SQL statements with explicit aliases —
// expected values never come from the ORM's own mapping helpers.

const DB_NAME = uniqueDbName("neutron_orm_b02");

// camelCase properties -> snake_case physical names. nick is nullable WITHOUT
// a default so NULL (explicit) and DEFAULT (omitted) are distinguishable.
const people = pgTable("people", {
  id: serial("id").primaryKey(),
  firstName: text("first_name").notNull(),
  lastName: text("last_name").notNull(),
  nick: text("nick"),
});

// Case-style crossed mapping: each property's physical name is the OTHER
// style. Any camelCase<->snake_case string transform would cross-wire these;
// only metadata lookup can map them.
const crossed = pgTable("crossed_map", {
  firstName: text("first_name").notNull(),
  first_name: text("firstName").notNull(),
});

// Escaped/quoted physical names: embedded double quote and a reserved word.
const quotedNames = pgTable('we"ird', {
  id: serial("id").primaryKey(),
  reserved: text("group").notNull(),
  quoted: text('va"l').notNull(),
});

// int8/numeric interim type claims (values arrive as strings on both drivers)
// plus jsonb object values, which must keep binding as values.
const accounts = pgTable("accounts", {
  id: serial("id").primaryKey(),
  ownerId: bigint("owner_id").notNull(),
  balance: numeric("balance").notNull(),
  meta: jsonb("meta"),
});

const authors = pgTable("authors", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
});

const books = pgTable("books", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull().references(() => authors.id),
  title: text("title").notNull(),
});

const authorsRelations = relations(authors, ({ many }) => ({ books: many(books) }));
const booksRelations = relations(books, ({ one }) => ({
  author: one(authors, { fields: [books.authorId], references: [authors.id] }),
}));

interface SuiteFixture {
  db: TestDb;
}

type TestDb = NeutronDatabase<
  { people: typeof people; crossed: typeof crossed; quotedNames: typeof quotedNames; accounts: typeof accounts; authors: typeof authors; books: typeof books },
  { authors: typeof authorsRelations; books: typeof booksRelations }
>;

async function withSuite(driverKind: "postgres" | "pg", fn: (fx: SuiteFixture) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live mapping (${driverKind})`))) {
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
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { people, crossed, quotedNames, accounts, authors, books },
    relations: { authors: authorsRelations, books: booksRelations },
  });
  try {
    for (const stmt of schemaToDDL([people, crossed, quotedNames, accounts, authors, books])) {
      await db.driver.execute(stmt);
    }
    await fn({ db });
  } finally {
    await db.close();
    if (/^neutron_orm_b02_[0-9_]+$/.test(DB_NAME)) {
      const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
      await admin2.query(
        "select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()",
        [DB_NAME],
      );
      await admin2.query(`drop database if exists "${DB_NAME}"`);
      await admin2.end();
    }
  }
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live mapping (${driverKind}): V02 select/full/aliased/returning/update/delete-returning keyed by property names`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const inserted = await db.insert(people).values({ firstName: "Alice", lastName: "One", nick: "ali" }).returning();
      assert.deepEqual(Object.keys(inserted[0]).sort(), ["firstName", "id", "lastName", "nick"]);
      assert.equal(inserted[0].firstName, "Alice");

      // Independent oracle: hand-written SQL with explicit aliases.
      const oracle = await db.driver.query<{ firstName: string; lastName: string }>(
        'select first_name as "firstName", last_name as "lastName" from people',
      );
      assert.equal(oracle[0].firstName, "Alice");
      assert.equal(oracle[0].lastName, "One");

      const full = await db.select().from(people);
      assert.ok("firstName" in full[0] && !("first_name" in full[0]));
      assert.equal(full[0].firstName, "Alice");
      assert.equal(full[0].lastName, "One");

      const projected = await db.select({ displayName: people.firstName, fam: people.lastName }).from(people);
      assert.deepEqual(Object.keys(projected[0]).sort(), ["displayName", "fam"]);
      assert.equal(projected[0].displayName, "Alice");

      const updated = await db
        .update(people)
        .set({ firstName: "Alicia" })
        .where(eq(people.id, inserted[0].id))
        .returning();
      assert.equal(updated[0].firstName, "Alicia");
      assert.ok(!("first_name" in updated[0]));

      const deleted = await db.delete(people).where(eq(people.id, inserted[0].id)).returning();
      assert.equal(deleted.length, 1);
      assert.equal(deleted[0].firstName, "Alicia");
      assert.ok(!("first_name" in deleted[0]));
    });
  });

  test(`live mapping (${driverKind}): V02 update .set() maps through metadata — no 42703`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(people).values({ firstName: "Bob", lastName: "Two" }).returning();
      // Property key in, physical column out — must not raise SQLSTATE 42703.
      await db.update(people).set({ firstName: "Robert" }).where(eq(people.id, ins[0].id));
      const raw = await db.driver.query<{ first_name: string }>("select first_name from people where id = $1", [ins[0].id]);
      assert.equal(raw[0].first_name, "Robert");
    });
  });

  test(`live mapping (${driverKind}): V02 nullable->null and nullable->'' are separate, verified in DB state`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(people).values({ firstName: "Carl", lastName: "Three", nick: "start" }).returning();
      const id = ins[0].id;

      await db.update(people).set({ nick: null }).where(eq(people.id, id));
      let state = await db.driver.query<{ nick: string | null; is_null: boolean }>(
        "select nick, nick is null as is_null from people where id = $1",
        [id],
      );
      assert.equal(state[0].is_null, true, "null update stores SQL NULL");
      assert.equal(state[0].nick, null);

      await db.update(people).set({ nick: "" }).where(eq(people.id, id));
      state = await db.driver.query<{ nick: string | null; is_null: boolean }>(
        "select nick, nick is null as is_null from people where id = $1",
        [id],
      );
      assert.equal(state[0].is_null, false, "empty string is not NULL");
      assert.equal(state[0].nick, "");

      // Insert-side: explicit null vs omitted (DEFAULT-free nullable -> NULL).
      const a = await db.insert(people).values({ firstName: "D", lastName: "Four", nick: null }).returning();
      const b = await db.insert(people).values({ firstName: "E", lastName: "Five" }).returning();
      assert.equal(a[0].nick, null);
      assert.equal(b[0].nick, null, "omitted nullable-without-default is NULL");
    });
  });

  test(`live mapping (${driverKind}): V02 default vs undefined policy on update`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(people).values({ firstName: "Dan", lastName: "Six", nick: "keepme" }).returning();
      // undefined keys are ignored — never NULL, never DEFAULT.
      await db.update(people).set({ nick: undefined, firstName: "Danny" }).where(eq(people.id, ins[0].id));
      const state = await db.driver.query<{ nick: string | null }>("select nick from people where id = $1", [ins[0].id]);
      assert.equal(state[0].nick, "keepme", "undefined update key left the column untouched");
      // An update with no remaining assignments is an error before execution.
      await assert.rejects(
        async () => {
          await db.update(people).set({ nick: undefined }).where(eq(people.id, ins[0].id));
        },
        /no assignments/,
      );
    });
  });

  test(`live mapping (${driverKind}): V02 mapping survives escaped/quoted physical names`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(quotedNames).values({ reserved: "g1", quoted: "q1" }).returning();
      assert.deepEqual(Object.keys(ins[0]).sort(), ["id", "quoted", "reserved"]);
      assert.equal(ins[0].reserved, "g1");
      assert.equal(ins[0].quoted, "q1");

      const sel = await db.select().from(quotedNames);
      assert.ok("reserved" in sel[0] && !("group" in sel[0]));
      assert.equal(sel[0].quoted, "q1");

      const updated = await db
        .update(quotedNames)
        .set({ reserved: "g2" })
        .where(eq(quotedNames.id, ins[0].id))
        .returning();
      assert.equal(updated[0].reserved, "g2");

      // Oracle: same physical columns, independently written SQL.
      const oracle = await db.driver.query<{ reserved: string }>(
        'select "group" as "reserved" from "we""ird" where id = $1',
        [ins[0].id],
      );
      assert.equal(oracle[0].reserved, "g2");
    });
  });

  test(`live mapping (${driverKind}): V02 crossed case-style properties map by metadata, not spelling`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(crossed).values({ firstName: "camel", first_name: "snake" }).returning();
      assert.equal(ins[0].firstName, "camel");
      assert.equal(ins[0].first_name, "snake");

      const sel = await db.select().from(crossed);
      assert.equal(sel[0].firstName, "camel", "property firstName reads physical first_name");
      assert.equal(sel[0].first_name, "snake", "property first_name reads physical \"firstName\"");

      // Oracle proves the physical placement: each alias reads the other style.
      const oracle = await db.driver.query<{ a: string; b: string }>(
        'select first_name as "a", "firstName" as "b" from crossed_map',
      );
      assert.equal(oracle[0].a, "camel");
      assert.equal(oracle[0].b, "snake");
    });
  });

  test(`live mapping (${driverKind}): int8 arrives as bigint (F03 default mode) and numeric as exact strings — never coerced numbers`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await db.insert(accounts).values({ ownerId: 9007199254740993n, balance: "12345678901234567890.123456789" });
      const rows = await db.select().from(accounts);
      assert.equal(typeof rows[0].ownerId, "bigint", "int8 select arrives as bigint (default mode)");
      assert.equal(rows[0].ownerId, 9007199254740993n);
      assert.equal(typeof rows[0].balance, "string", "numeric select arrives as its exact decimal string");
      assert.equal(rows[0].balance, "12345678901234567890.123456789");

      const ret = await db.insert(accounts).values({ ownerId: "9007199254740995", balance: 1.5 }).returning();
      assert.equal(typeof ret[0].ownerId, "bigint", "int8 returning arrives as bigint");
      assert.equal(ret[0].ownerId, 9007199254740995n);
      assert.equal(ret[0].balance, "1.5");

      // Oracle: raw text representation, no ORM in the path.
      const oracle = await db.driver.query<{ owner_id: string; balance: string }>(
        "select owner_id::text, balance::text from accounts order by id",
      );
      assert.equal(oracle[0].owner_id, "9007199254740993");
      assert.equal(oracle[1].owner_id, "9007199254740995");
    });
  });

  test(`live mapping (${driverKind}): V02 jsonb object values bind as values on insert and update`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const payload = { a: 1, b: [null, "x"], nested: { deep: true } };
      const ins = await db.insert(accounts).values({ ownerId: 1n, balance: "0", meta: payload }).returning();
      assert.deepEqual(ins[0].meta, payload);

      const next = { a: 2 };
      await db.update(accounts).set({ meta: next }).where(eq(accounts.id, ins[0].id));
      const raw = await db.driver.query<{ meta: { a: number } }>("select meta from accounts where id = $1", [ins[0].id]);
      assert.deepEqual(raw[0].meta, next);
    });
  });

  test(`live mapping (${driverKind}): V02 runtime rejects missing required, null-for-NOT-NULL and invalid values before execution`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      await assert.rejects(
        async () => {
          await db.insert(people).values({ nick: "only-nick" } as never);
        },
        /missing required column\(s\) "firstName" \("first_name"\), "lastName" \("last_name"\)/,
      );
      await assert.rejects(
        async () => {
          await db.insert(people).values({ firstName: null, lastName: "X" } as never);
        },
        /null is not allowed for NOT NULL column "firstName" \("first_name"\)/,
      );
      await assert.rejects(
        async () => {
          await db.update(people).set({ firstName: null } as never).where(eq(people.id, 1));
        },
        /null is not allowed for NOT NULL column "firstName"/,
      );
      // Nested objects used to reach the driver and corrupt data silently
      // (pg stored '{"nested":true}', postgres.js stored '[object Object]').
      await assert.rejects(
        async () => {
          await db.insert(people).values({ firstName: { nested: true }, lastName: "X" } as never);
        },
        /column "firstName" \("first_name"\) on people: text columns accept strings/,
      );
      await assert.rejects(
        async () => {
          await db.update(people).set({ lastName: { nested: true } as never }).where(eq(people.id, 1));
        },
        /column "lastName" \("last_name"\) on people: text columns accept strings/,
      );
      const cnt = await db.driver.query<{ n: number }>("select count(*)::int as n from people");
      assert.equal(cnt[0].n, 0, "rejected operations never touched the table");
    });
  });

  test(`live mapping (${driverKind}): V02 sql fragments keep working in .set() assignments`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const ins = await db.insert(people).values({ firstName: "lower", lastName: "Frag" }).returning();
      await db.update(people).set({ firstName: sql`upper(${"lower"})` }).where(eq(people.id, ins[0].id));
      const raw = await db.driver.query<{ first_name: string }>("select first_name from people where id = $1", [ins[0].id]);
      assert.equal(raw[0].first_name, "LOWER");
    });
  });

  test(`live mapping (${driverKind}): V02 relation results keyed by property names (metadata fallout)`, async () => {
    await withSuite(driverKind, async ({ db }) => {
      const a = await db.insert(authors).values({ email: "a@x.com" }).returning();
      await db.insert(books).values({ authorId: a[0].id, title: "B1" });

      const withAuthor = await db.query.books.findFirst({ with: { author: true } });
      assert.ok(withAuthor);
      assert.deepEqual(Object.keys(withAuthor).sort(), ["author", "authorId", "id", "title"], "top-level keys are property names");
      assert.ok(withAuthor.author);
      assert.deepEqual(Object.keys(withAuthor.author).sort(), ["email", "id"], "nested to-one keys are property names");
      assert.equal(withAuthor.author.email, "a@x.com");

      const withBooks = await db.query.authors.findFirst({ with: { books: true } });
      assert.ok(withBooks);
      assert.ok(Array.isArray(withBooks.books) && withBooks.books.length === 1);
      // many() result typing is loose in v0.1 (B05/F02 own relation typing);
      // V02's subject is the runtime key mapping, narrowed like driver output.
      const bookRow = withBooks.books[0] as Record<string, unknown>;
      assert.deepEqual(Object.keys(bookRow).sort(), ["authorId", "id", "title"], "nested to-many keys are property names");
      assert.equal(bookRow["title"], "B1");
    });
  });
}
