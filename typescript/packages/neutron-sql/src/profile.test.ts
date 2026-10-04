import assert from "node:assert/strict";
import test from "node:test";
import {
  CapabilityRequirementError,
  NUCLEUS_CANDIDATE_PROFILE,
  POSTGRES_DIRECT_PROFILE,
  ProfileRefusedError,
  admitEndpoint,
  bigint,
  boolean,
  createDatabase,
  eq,
  integer,
  jsonb,
  numeric,
  pgSchema,
  pgTable,
  serial,
  text,
  timestamptz,
  varchar,
  type Driver,
  type PinnedExecutor,
} from "./index.js";
import { assertFiniteTable, finiteStatementGuard } from "./profile.js";

// NP01 unit battery. The fake drivers below only record statements and answer
// the two fixed identity queries; every database-facing behavior against the
// exact Nucleus binary and a PostgreSQL control belongs to the native
// qualifier (conformance/polyglot/nucleus/ts_admission_native.mjs).

const NUCLEUS_STARTUP = "16.0 (Nucleus)";
const NUCLEUS_VERSION = "PostgreSQL 16.0 (Nucleus 1.2.0 — The Definitive Database)";
const STARTUP_SQL = "select current_setting('server_version') as server_version";
const VERSION_SQL = "select version() as version";

interface Call {
  sql: string;
  params: unknown[] | undefined;
}

interface Fake extends Driver {
  calls: Call[];
  pinned: Call[];
  closed: number;
  released: number;
}

function fakeDriver(startup: unknown = NUCLEUS_STARTUP, version: unknown = NUCLEUS_VERSION, pinnable = true): Fake {
  const calls: Call[] = [];
  const pinned: Call[] = [];
  const state = { closed: 0, released: 0 };
  const pin: PinnedExecutor = {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      pinned.push({ sql, params });
      return [] as T[];
    },
    async execute(sql: string, params?: unknown[]): Promise<number> {
      pinned.push({ sql, params });
      return 0;
    },
    release(): void {
      state.released += 1;
    },
  };
  const driver: Driver = {
    async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
      calls.push({ sql, params });
      if (sql === STARTUP_SQL) return [{ server_version: startup }] as T[];
      if (sql === VERSION_SQL) return [{ version: version }] as T[];
      return [] as T[];
    },
    async execute(sql: string, params?: unknown[]): Promise<number> {
      calls.push({ sql, params });
      return 0;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return fn(driver);
    },
    async close(): Promise<void> {
      state.closed += 1;
    },
    lifecycle: { ownership: "borrowed", terminated: false, terminate: () => Promise.resolve() },
    ...(pinnable ? { pin: () => Promise.resolve(pin) } : {}),
  };
  return Object.defineProperties(driver, {
    calls: { value: calls },
    pinned: { value: pinned },
    closed: { get: () => state.closed },
    released: { get: () => state.released },
  }) as Fake;
}

/** Caller statements: identity queries and the jsonb capability probe excluded. */
function userCalls(driver: Fake): Call[] {
  return driver.calls.filter((c) => c.sql !== STARTUP_SQL && c.sql !== VERSION_SQL && !c.sql.startsWith("select to_jsonb(1)"));
}

const ns = pgSchema("np01");
const docs = ns.table("docs", {
  id: bigint("id").primaryKey(),
  active: boolean("active").notNull(),
  title: text("title").notNull(),
  data: jsonb("data"),
  stamp: timestamptz("stamp").notNull(),
  n: integer("n"),
});
const plain = ns.table("plain", { id: integer("id").primaryKey(), title: text("title") });
const ROW = { id: 1, active: false, title: "", data: null, stamp: "2026-01-02T03:04:05.123456Z", n: 0 };

async function nucleusDb(driver: Fake = fakeDriver()) {
  return createDatabase({ driver, profile: NUCLEUS_CANDIDATE_PROFILE, tables: { docs, plain } });
}

async function refused(driver: Fake, body: () => unknown): Promise<void> {
  const before = userCalls(driver).length;
  await assert.rejects(
    async () => {
      await body();
    },
    (error: unknown) => error instanceof ProfileRefusedError || error instanceof CapabilityRequirementError,
  );
  assert.equal(userCalls(driver).length, before, "refused operation must not reach the driver");
}

test("NP01: omitting profile runs no admission and leaves the driver unwrapped", async () => {
  const driver = fakeDriver();
  const db = await createDatabase({ driver, tables: { docs } });
  assert.deepEqual(driver.calls, []);
  assert.equal(db.endpointIdentity, undefined);
  assert.equal(db.driver, driver);
  await db.driver.execute("create table x (a int)");
  assert.equal(driver.calls.length, 1);
});

test("NP01: an unknown profile is refused before any connection use", async () => {
  const driver = fakeDriver();
  await assert.rejects(createDatabase({ driver, profile: "nucleus" as never }), ProfileRefusedError);
  assert.deepEqual(driver.calls, []);
  assert.equal(driver.closed, 0);
});

test("NP01: the named Nucleus candidate is exact, uncertified and immutable", async () => {
  const driver = fakeDriver();
  const db = await nucleusDb(driver);
  const identity = db.endpointIdentity;
  assert.ok(identity !== undefined);
  assert.equal(identity.engine, "nucleus");
  assert.equal(identity.version, "1.2.0");
  assert.equal(identity.profile, NUCLEUS_CANDIDATE_PROFILE);
  assert.equal(identity.packageEnabled, false);
  assert.equal(identity.qualification, "uncertified-finite-candidate");
  assert.deepEqual([...identity.capabilities], ["point-crud", "read-committed-transaction", "savepoint"]);
  assert.ok(!identity.capabilities.includes("bounded-stream"));
  assert.ok(Object.isFrozen(identity) && Object.isFrozen(identity.capabilities));
  assert.throws(() => {
    (identity as { packageEnabled: boolean }).packageEnabled = true;
  }, TypeError);
  assert.throws(() => {
    (db as { endpointIdentity?: unknown }).endpointIdentity = undefined;
  }, TypeError);
  assert.deepEqual(driver.calls.map((c) => c.sql), [STARTUP_SQL, VERSION_SQL]);
  assert.equal(db.driver.prepare, undefined);
});

test("NP01: contradictory or unknown reported identities are refused and the adapter is closed", async () => {
  const cases: Array<[unknown, unknown]> = [
    ["16.0", NUCLEUS_VERSION],
    [NUCLEUS_STARTUP, "PostgreSQL 16.0"],
    [NUCLEUS_STARTUP, NUCLEUS_VERSION.replace("1.2.0", "1.2.1")],
    ["17.6 (Debian 17.6-1)", "PostgreSQL 17.6 on x86_64"],
    [undefined, NUCLEUS_VERSION],
    [NUCLEUS_STARTUP, undefined],
  ];
  for (const [startup, version] of cases) {
    const driver = fakeDriver(startup, version);
    await assert.rejects(nucleusDb(driver), ProfileRefusedError);
    assert.equal(driver.closed, 1);
    assert.deepEqual(userCalls(driver), []);
  }
});

test("NP01: postgres-direct admits PostgreSQL identity only", async () => {
  const pg = fakeDriver("17.6 (Debian 17.6-1)", "PostgreSQL 17.6 on x86_64-pc-linux-gnu");
  const db = await createDatabase({ driver: pg, profile: POSTGRES_DIRECT_PROFILE });
  assert.equal(db.endpointIdentity?.engine, "postgresql");
  assert.equal(db.endpointIdentity?.version, "17.6");
  assert.equal(db.driver, pg);
  for (const [startup, version] of [
    [NUCLEUS_STARTUP, NUCLEUS_VERSION],
    ["17.6", "CockroachDB v24.1"],
    ["17.6", "PostgreSQL 16.6"],
    ["unknown", "PostgreSQL 17.6"],
    ["17.6", "PostgreSQL 17.6 YugabyteDB"],
  ] as Array<[unknown, unknown]>) {
    const driver = fakeDriver(startup, version);
    await assert.rejects(createDatabase({ driver, profile: POSTGRES_DIRECT_PROFILE }), ProfileRefusedError);
    assert.equal(driver.closed, 1);
  }
  assert.throws(() => admitEndpoint(NUCLEUS_STARTUP, NUCLEUS_VERSION, POSTGRES_DIRECT_PROFILE), ProfileRefusedError);
});

test("NP01: table metadata outside the finite families is refused before any statement", async () => {
  const unqualified = pgTable("loose", { id: integer("id").primaryKey() });
  const bad = [
    ns.table("with_numeric", { id: integer("id").primaryKey(), v: numeric("v") }),
    ns.table("with_varchar", { id: integer("id").primaryKey(), v: varchar("v", 10) }),
    ns.table("with_serial", { id: serial("id").primaryKey() }),
    ns.table("with_array", { id: integer("id").primaryKey(), v: text("v").array() }),
    unqualified,
  ];
  for (const table of bad) {
    assert.throws(() => assertFiniteTable(table), ProfileRefusedError);
    const driver = fakeDriver();
    await assert.rejects(createDatabase({ driver, profile: NUCLEUS_CANDIDATE_PROFILE, tables: { table } }), ProfileRefusedError);
    assert.deepEqual(driver.calls, []);
    assert.equal(driver.closed, 1);
  }
  assert.doesNotThrow(() => assertFiniteTable(docs));
});

test("NP01: a nonpinnable adapter cannot hold the finite profile", async () => {
  const driver = fakeDriver(NUCLEUS_STARTUP, NUCLEUS_VERSION, false);
  await assert.rejects(nucleusDb(driver), ProfileRefusedError);
  assert.deepEqual(driver.calls, []);
});

test("NP01: the guard admits every statement the builders emit for point CRUD", async () => {
  const plainDb = await createDatabase({ driver: fakeDriver(), tables: { docs } });
  const guard = finiteStatementGuard([docs]);
  const emitted = [
    plainDb.select().from(docs).where(eq(docs.id, 1)).toSQL(),
    plainDb.select({ title: docs.title, stamp: docs.stamp }).from(docs).where(eq(docs.title, "")).toSQL(),
    plainDb.select().from(docs).where(eq(docs.stamp, "2026-01-02T03:04:05.123456Z")).toSQL(),
    plainDb.insert(docs).values(ROW).toSQL(),
    plainDb.insert(docs).values({ ...ROW, data: { a: 1 } }).returning().toSQL(),
    plainDb.update(docs).set({ title: "x", active: false, n: 0 }).where(eq(docs.id, 1)).toSQL(),
    plainDb.update(docs).set({ title: "" }).where(eq(docs.id, 1)).returning().toSQL(),
    plainDb.delete(docs).where(eq(docs.id, 1)).toSQL(),
  ];
  for (const statement of emitted) assert.doesNotThrow(() => guard(statement.sql, statement.params, false), statement.sql);
});

test("NP01: the guard refuses everything outside the finite point grammar", () => {
  const guard = finiteStatementGuard([docs]);
  const point = '("np01"."docs"."id" = $1)';
  const table = '"np01"."docs"';
  const refusedSql: Array<[string, unknown[]]> = [
    ["create table surprise (id int)", []],
    ["select pg_cancel_backend(1)", []],
    [`select "np01"."docs"."id" from ${table}`, []],
    [`select "np01"."docs"."id" from ${table} where ${point} limit 1`, [1]],
    [`select "np01"."docs"."id" from ${table} where ${point} order by "np01"."docs"."id" asc`, [1]],
    [`select "np01"."docs"."id" from ${table} where ("np01"."docs"."id" > $1)`, [1]],
    [`select "np01"."docs"."id" from ${table} where ${point} or ${point}`, [1]],
    [`select "np01"."docs"."id" from ${table} join "np01"."plain" on true where ${point}`, [1]],
    [`select count(*) from ${table} where ${point}`, [1]],
    [`update ${table} set "title" = $1`, ["x"]],
    [`delete from ${table}`, []],
    [`delete from ${table} where ${point}; drop table ${table}`, [1]],
    [`delete from ${table} where ${point} -- tail`, [1]],
    [`delete from ${table} where ("np01"."docs"."id" = pg_cancel_backend(1))`, []],
    [`delete from "np01"."other" where ("np01"."other"."id" = $1)`, [1]],
    [`delete from ${table} where ${point}`, []],
    [`delete from ${table} where ("np01"."docs"."id" = $2)`, [1]],
    [`insert into ${table} ("id") values ($1), ($2)`, [1, 2]],
    [`insert into ${table} ("id") values ($1) on conflict do nothing`, [1]],
    [`insert into ${table} ("id") select 1`, []],
    [`declare "c" no scroll cursor for select 1`, []],
    ["begin", []],
    ["commit", []],
  ];
  for (const [sql, params] of refusedSql) {
    assert.throws(() => guard(sql, params, false), ProfileRefusedError, sql);
  }
  for (const value of [1.5, Number.NaN, new Date(0), {}, [], undefined, new Uint8Array(1), Symbol("x"), () => 1]) {
    assert.throws(() => guard(`delete from ${table} where ${point}`, [value], false), ProfileRefusedError);
  }
  assert.throws(
    () => guard(`delete from ${table} where ("np01"."docs"."stamp" = $1::text::timestamptz)`, ["2026-01-02T03:04:05+02:00"], false),
    ProfileRefusedError,
  );
  assert.doesNotThrow(() => guard(`delete from ${table} where ("np01"."docs"."stamp" = $1::text::timestamptz)`, ["2026-01-02T03:04:05.5+00:00"], false));
  assert.doesNotThrow(() => guard(`delete from ${table} where ${point}`, [1n], false));
  assert.doesNotThrow(() => guard(`delete from ${table} where ${point}`, [Number.MAX_SAFE_INTEGER], false));
});

test("NP01: transaction control is admitted only for the runner, only for read committed", () => {
  const guard = finiteStatementGuard([docs]);
  for (const sql of ["begin", "begin isolation level read committed", "commit", "rollback", 'savepoint "neutron_sp_1"', 'rollback to savepoint "neutron_sp_1"', 'release savepoint "neutron_sp_1"']) {
    assert.doesNotThrow(() => guard(sql, [], true), sql);
    assert.throws(() => guard(sql, [], false), ProfileRefusedError, sql);
  }
  for (const sql of [
    "begin isolation level repeatable read",
    "begin isolation level serializable",
    "begin read only",
    "begin; drop table x",
    'savepoint "a"; drop table x',
    'savepoint "bad name"',
    "set transaction isolation level serializable",
    "rollback prepared 'x'",
  ]) {
    assert.throws(() => guard(sql, [], true), ProfileRefusedError, sql);
  }
  assert.throws(() => guard("commit", [1], true), ProfileRefusedError);
});

test("NP01: point CRUD reaches the driver and raw, query, stream and prepared operations never do", async () => {
  const driver = fakeDriver();
  const db = await nucleusDb(driver);
  await db.insert(docs).values(ROW);
  await db.insert(docs).values({ ...ROW, id: 2 }).returning();
  await db.update(docs).set({ title: "", active: false, n: 0 }).where(eq(docs.id, 1));
  await db.delete(docs).where(eq(docs.id, 1));
  await db.select().from(docs).where(eq(docs.id, 1));
  const sent = userCalls(driver).map((c) => c.sql);
  assert.equal(sent.length, 5);
  assert.match(sent[0], /^insert into "np01"\."docs"/);
  assert.match(sent[2], /^update "np01"\."docs" set/);
  assert.match(sent[3], /^delete from "np01"\."docs" where/);
  assert.match(sent[4], /^select .* from "np01"\."docs" where/);

  await refused(driver, () => db.driver.execute("create table surprise (id int)"));
  await refused(driver, () => db.driver.query("select pg_cancel_backend(1)"));
  await refused(driver, () => db.driver.execute('delete from "np01"."plain"'));
  await refused(driver, () => db.select().from(plain));
  await refused(driver, () => db.select().from(plain).where(eq(plain.id, 1)).limit(1));
  await refused(driver, () => db.insert(plain).values([{ id: 1 }, { id: 2 }]));
  await refused(driver, () => db.update(plain).set({ title: "x" }).where(eq(plain.id, 1)).where(eq(plain.title, "a")));
  await refused(driver, () => db.select().from(plain).where(eq(plain.id, 1)).stream().next());
  await refused(driver, () => db.select().from(plain).where(eq(plain.id, 1)).streamBatches().next());
  assert.deepEqual(driver.pinned, [], "refused streams must not open a transaction");
});

test("NP01: owned transactions and savepoints run through the guarded pin", async () => {
  const driver = fakeDriver();
  const db = await nucleusDb(driver);
  await db.transaction(async (tx) => {
    await tx.update(docs).set({ title: "outer" }).where(eq(docs.id, 1));
    await tx.transaction(async (inner) => {
      await inner.delete(docs).where(eq(docs.id, 1));
    });
  });
  assert.deepEqual(
    driver.pinned.map((c) => (c.sql.startsWith("update") ? "update" : c.sql.startsWith("delete") ? "delete" : c.sql)),
    ["begin", "update", 'savepoint "neutron_sp_1"', "delete", 'release savepoint "neutron_sp_1"', "commit"],
  );
  assert.equal(driver.released, 1);
});

test("NP01: stronger transaction modes and batch defaults are refused before any dispatch", async () => {
  const driver = fakeDriver();
  const db = await nucleusDb(driver);
  await assert.rejects(db.transaction(async () => undefined, { isolation: "serializable" }), ProfileRefusedError);
  await assert.rejects(db.transaction(async () => undefined, { isolation: "repeatable-read" }), ProfileRefusedError);
  await assert.rejects(db.transaction(async () => undefined, { readOnly: true }), ProfileRefusedError);
  await assert.rejects(async () => {
    await db.batch([db.delete(docs).where(eq(docs.id, 1))]);
  }, ProfileRefusedError);
  assert.equal(driver.pinned.length, 0);
  assert.equal(driver.released, 4, "every pinned connection is released after the refused BEGIN");
  await db.transaction(async () => undefined, { isolation: "read-committed" });
  assert.deepEqual(driver.pinned.map((c) => c.sql), ["begin isolation level read committed", "commit"]);
});

test("NP01: an operation refused inside an owned transaction rolls the transaction back", async () => {
  const driver = fakeDriver();
  const db = await nucleusDb(driver);
  await assert.rejects(
    db.transaction(async (tx) => {
      await tx.delete(docs).where(eq(docs.id, 1));
      await tx.select().from(plain);
    }),
    ProfileRefusedError,
  );
  assert.deepEqual(
    driver.pinned.map((c) => (c.sql.startsWith("delete") ? "delete" : c.sql)),
    ["begin", "delete", "rollback"],
  );
});
