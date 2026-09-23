import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  pgTable,
  serial,
  integer,
  text,
  timestamp,
  relations,
  eq,
  wrapPgPool,
  wrapPostgresJs,
  loadDriver,
  ConnectionFailedError,
  MissingDriverError,
  ServerSqlError,
  getSqlState,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// I01 live battery (V14 subset owned by this card): adapter ownership,
// engine/capability detection on a real server, SQLSTATE retention through
// the wrappers, and connection-failure classification. Transactions,
// cancellation and retry belong to I02 and are not pulled forward.

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
});

const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

type Tables = { users: typeof users; posts: typeof posts };
type Rels = { users: typeof usersRelations; posts: typeof postsRelations };
type TestDb = NeutronDatabase<Tables, Rels>;

async function withDatabase(driverKind: "postgres" | "pg", fn: (db: TestDb) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live i01 (${driverKind})`))) return;
  const { Pool } = (await import("pg")) as unknown as {
    Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
  };
  const DB_NAME = uniqueDbName("i01");
  const admin = new Pool({ connectionString: TEST_URL, max: 1 });
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const db = await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { users, posts },
    relations: { users: usersRelations, posts: postsRelations } as Rels,
  });
  await db.driver.execute(`create table "users" ("id" serial primary key, "email" text not null unique, "created_at" timestamp not null default now())`);
  await db.driver.execute(`create table "posts" ("id" serial primary key, "user_id" integer not null references "users"("id") on delete cascade, "title" text not null)`);
  try {
    await fn(db);
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}"`);
    await admin2.end();
  }
}

for (const driverKind of ["postgres", "pg"] as const) {
  test(`live i01 (${driverKind}): engine identity matches the server's own version setting`, async () => {
    await withDatabase(driverKind, async (db) => {
      const engine = await db.engine();
      assert.equal(engine.product, "postgres");
      // Independent oracle: the server's own server_version setting. Vendor
      // builds append tags ("17.11 (Homebrew)"), so compare against the
      // leading numeric version, not the whole setting string.
      const rows = await db.driver.query<{ server_version: string }>("show server_version");
      const serverVersion = rows[0].server_version;
      assert.ok(typeof serverVersion === "string" && serverVersion.length > 0);
      const numeric = /^(\d+(?:\.\d+)*)/.exec(serverVersion)?.[1];
      assert.ok(numeric !== undefined, `server_version carried no numeric version: ${serverVersion}`);
      assert.equal(engine.version, numeric, "engine() version must equal the server's reported version");
      assert.ok(engine.raw.startsWith("PostgreSQL"));
    });
  });

  test(`live i01 (${driverKind}): jsonb-functions resolves supported by version fact`, async () => {
    await withDatabase(driverKind, async (db) => {
      const status = await db.capability("jsonb-functions");
      assert.equal(status.status, "supported");
      assert.match(status.evidence, /9\.4/);
      assert.equal(status.engine.product, "postgres");
      // A capability-bearing statement (timestamp wire read) executes: the
      // gate resolved supported and let it through.
      const inserted = await db.insert(users).values({ email: "gate@x.com" }).returning();
      assert.equal(typeof inserted[0].createdAt, "string");
      const relational = await db.query.users.findFirst({ where: eq(users.email, "gate@x.com"), with: { posts: true } });
      assert.ok(relational);
      assert.deepEqual(relational.posts, []);
    });
  });

  test(`live i01 (${driverKind}): SQLSTATE survives the wrapper end to end`, async () => {
    await withDatabase(driverKind, async (db) => {
      await db.insert(users).values({ email: "dup@x.com" });
      const err = await db.insert(users).values({ email: "dup@x.com" }).then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof ServerSqlError, `expected ServerSqlError, got ${String(err)}`);
      assert.equal(err.sqlstate, "23505");
      assert.equal(getSqlState(err), "23505");
      assert.equal((err.cause as { code?: string }).code, "23505", "original driver error preserved as cause");

      const missing = await db.driver.query("select * from i01_no_such_table").then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(missing instanceof ServerSqlError);
      assert.equal(missing.sqlstate, "42P01");
    });
  });

  test(`live i01 (${driverKind}): connection refused is ConnectionFailedError, never MissingDriverError`, async () => {
    // A port with nothing listening: a real refused TCP connection, not a mock.
    const driver = await loadDriver("postgres://i01@127.0.0.1:1/i01_none", { driver: driverKind });
    const err = await driver.query("select 1").then(
      () => null,
      (e: unknown) => e,
    );
    assert.ok(err instanceof ConnectionFailedError, `expected ConnectionFailedError, got ${String(err)}`);
    assert.ok(!(err instanceof MissingDriverError));
    assert.equal(err.code, "ECONNREFUSED");
  });

  test(`live i01 (${driverKind}): owned adapter terminates exactly once; use after close is connection-class`, async () => {
    await withDatabase(driverKind, async (db) => {
      assert.equal(db.driver.lifecycle.ownership, "owned");
      assert.equal(db.driver.lifecycle.terminated, false);
      await Promise.all([db.close(), db.close()]); // concurrent double-close: one terminate
      assert.equal(db.driver.lifecycle.terminated, true);
      const err = await db.driver.query("select 1").then(
        () => null,
        (e: unknown) => e,
      );
      assert.ok(err instanceof ConnectionFailedError, `query after close must be ConnectionFailedError, got ${String(err)}`);
      await db.close(); // idempotent, no throw
    });
  });
}

// --- injected adapters: borrowed lifecycle (V14 exit criterion) -----------

test("live i01 (pg): closing a borrowed pool leaves its owner functional", async () => {
  if (!(await ensureLive("live i01 (borrowed-pg)"))) return;
  const { Pool } = (await import("pg")) as unknown as { Pool: new (o: object) => import("./index.js").PgPoolLike };
  const DB_NAME = uniqueDbName("i01");
  const admin = new Pool({ connectionString: TEST_URL, max: 1 }) as unknown as import("./index.js").PgPoolLike;
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const pool = new Pool({ connectionString: url.toString(), max: 4 });
  const driver = wrapPgPool(pool); // borrowed by default
  assert.equal(driver.lifecycle.ownership, "borrowed");

  const db = await createDatabase({
    driver,
    tables: { users, posts } as Tables,
    relations: { users: usersRelations, posts: postsRelations } as Rels,
  });
  await db.driver.execute(`create table "users" ("id" serial primary key, "email" text not null unique, "created_at" timestamp not null default now())`);
  await db.insert(users).values({ email: "borrowed@x.com" });
  // A transaction on the borrowed pool returns its connection, not ends it.
  await db.transaction(async (tx) => {
    await tx.insert(users).values({ email: "borrowed-tx@x.com" });
  });

  await db.close();
  await db.close(); // idempotent

  // V14: the owner's pool is still functional after the borrowed close.
  const alive = await pool.query("select count(*)::int as n from users");
  const rows = alive.rows as Array<{ n: number }>;
  assert.equal(rows[0].n, 2, "owner's pool must still query after borrowed close");
  await pool.end();

  const admin2 = new Pool({ connectionString: TEST_URL, max: 1 }) as unknown as import("./index.js").PgPoolLike;
  await admin2.query(`drop database if exists "${DB_NAME}"`);
  await admin2.end();
});

test("live i01 (postgres): closing a borrowed client leaves its owner functional", async () => {
  if (!(await ensureLive("live i01 (borrowed-postgres)"))) return;
  const { Pool } = (await import("pg")) as unknown as { Pool: new (o: object) => import("./index.js").PgPoolLike };
  const DB_NAME = uniqueDbName("i01");
  const admin = new Pool({ connectionString: TEST_URL, max: 1 }) as unknown as import("./index.js").PgPoolLike;
  await admin.query(`drop database if exists "${DB_NAME}"`);
  await admin.query(`create database "${DB_NAME}"`);
  await admin.end();

  const url = new URL(TEST_URL);
  url.pathname = `/${DB_NAME}`;
  const postgres = (await import("postgres")) as unknown as { default: (u: string, o?: object) => import("./index.js").PostgresJsClient };
  const client = postgres.default(url.toString(), { max: 4 });
  const driver = wrapPostgresJs(client); // borrowed by default
  assert.equal(driver.lifecycle.ownership, "borrowed");

  const db = await createDatabase({
    driver,
    tables: { users, posts } as Tables,
    relations: { users: usersRelations, posts: postsRelations } as Rels,
  });
  await db.driver.execute(`create table "users" ("id" serial primary key, "email" text not null unique, "created_at" timestamp not null default now())`);
  await db.insert(users).values({ email: "borrowed@x.com" });
  await db.transaction(async (tx) => {
    await tx.insert(users).values({ email: "borrowed-tx@x.com" });
  });

  await db.close();

  const alive = (await client.unsafe("select count(*)::int as n from users")) as unknown as Array<{ n: number }>;
  assert.equal(alive[0].n, 2, "owner's client must still query after borrowed close");
  await client.end();

  const admin2 = new Pool({ connectionString: TEST_URL, max: 1 }) as unknown as import("./index.js").PgPoolLike;
  await admin2.query(`drop database if exists "${DB_NAME}"`);
  await admin2.end();
});
