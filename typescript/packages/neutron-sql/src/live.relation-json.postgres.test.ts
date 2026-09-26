import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  asc,
  pgTable,
  serial,
  integer,
  double,
  real,
  json,
  relations,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// Relation JSON leaves that the json_* builders (R01, 327d94ee) carry
// differently from jsonb_*: float -0 keeps its sign, a json column keeps its
// key order, and a json column holding "\u0000" still reads. Each relation
// leaf must equal the SAME column read at the top level (Object.is, key
// order included), which is what the driver returns for a plain select.
// jsonb would have turned -0 into 0, sorted the keys, and refused "\u0000".

const DB_NAME = uniqueDbName("neutron_orm_r01json");

const owners = pgTable("r01j_owners", {
  id: serial("id").primaryKey(),
});

const leaves = pgTable("r01j_leaves", {
  id: serial("id").primaryKey(),
  ownerId: integer("owner_id").notNull().references(() => owners.id),
  f8: double("f8"),
  f4: real("f4"),
  doc: json("doc"),
});

const ownersRelations = relations(owners, ({ many }) => ({
  leaves: many(leaves),
}));
const leavesRelations = relations(leaves, ({ one }) => ({
  owner: one(owners, { fields: [leaves.ownerId], references: [owners.id] }),
}));

type TestDb = NeutronDatabase<
  { r01j_owners: typeof owners; r01j_leaves: typeof leaves },
  { r01j_owners: typeof ownersRelations; r01j_leaves: typeof leavesRelations }
>;

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live relation json leaves (${driverKind})`))) {
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
    tables: { r01j_owners: owners, r01j_leaves: leaves },
    relations: { r01j_owners: ownersRelations, r01j_leaves: leavesRelations },
  });
  try {
    await db.driver.execute(`create table "r01j_owners" ("id" serial primary key)`);
    await db.driver.execute(
      `create table "r01j_leaves" ("id" serial primary key, "owner_id" integer not null references "r01j_owners"("id"), "f8" double precision, "f4" real, "doc" json)`,
    );
    await db.driver.execute(`insert into "r01j_owners" ("id") values (1), (2)`);
    await db.driver.execute(
      `insert into "r01j_leaves" ("id", "owner_id", "f8", "f4", "doc") values
         (1, 1, '-0', '-0', '{"b": 1, "a": {"z": -0, "y": 2}}'),
         (2, 1, '1.5', '0.25', '{"d": [3, -0], "c": null}'),
         (3, 2, '2', '2', '{"s": "nul\\u0000here"}')`,
    );
    await fn(db);
  } finally {
    await db.close();
    const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin2.query(`drop database if exists "${DB_NAME}" with (force)`);
    await admin2.end();
  }
}

/** Deep equality that also distinguishes -0 from 0 and compares key order. */
function assertSameLeaf(actual: unknown, expected: unknown, path: string): void {
  if (typeof expected === "number") {
    assert.ok(Object.is(actual, expected), `${path}: ${String(actual)} is not ${Object.is(expected, -0) ? "-0" : String(expected)}`);
    return;
  }
  if (expected !== null && typeof expected === "object") {
    assert.ok(actual !== null && typeof actual === "object", `${path}: not an object`);
    assert.deepEqual(Object.keys(actual as object), Object.keys(expected as object), `${path}: key order`);
    for (const k of Object.keys(expected as object)) {
      assertSameLeaf((actual as Record<string, unknown>)[k], (expected as Record<string, unknown>)[k], `${path}.${k}`);
    }
    return;
  }
  assert.equal(actual, expected, path);
}

for (const driverKind of ["pg", "postgres"] as const) {
  test(`live relation json leaves (${driverKind}): float -0 and json key order match the top-level column read`, async () => {
    await withSuite(driverKind, async (db) => {
      const top = await db.select().from(leaves).where(eq(leaves.ownerId, 1)).orderBy(asc(leaves.id));
      assert.equal(top.length, 2);
      // The top-level read is the oracle; check it independently first.
      assert.ok(Object.is(top[0]!.f8, -0), "top-level float8 -0");
      assert.ok(Object.is(top[0]!.f4, -0), "top-level float4 -0");
      assert.deepEqual(Object.keys(top[0]!.doc as object), ["b", "a"]);

      // to-many leaves
      const [owner] = await db.query.r01j_owners.findMany({
        where: eq(owners.id, 1),
        with: { leaves: { orderBy: [asc(leaves.id)] } },
      });
      assert.equal(owner!.leaves.length, 2);
      owner!.leaves.forEach((leaf, i) => assertSameLeaf(leaf, top[i], `leaves[${i}]`));

      // the same leaves one level deeper, under a to-one parent
      const viaOne = await db.query.r01j_leaves.findMany({
        where: eq(leaves.id, 1),
        with: { owner: { with: { leaves: { orderBy: [asc(leaves.id)] } } } },
      });
      viaOne[0]!.owner!.leaves.forEach((leaf, i) => assertSameLeaf(leaf, top[i], `owner.leaves[${i}]`));
    });
  });

  test(`live relation json leaves (${driverKind}): a json column holding \\u0000 reads through a relation`, async () => {
    await withSuite(driverKind, async (db) => {
      const top = await db.select().from(leaves).where(eq(leaves.ownerId, 2));
      assert.equal((top[0]!.doc as { s: string }).s, "nul\u0000here");
      const [owner] = await db.query.r01j_owners.findMany({ where: eq(owners.id, 2), with: { leaves: true } });
      assertSameLeaf(owner!.leaves[0], top[0], "leaves[0]");
    });
  });
}
