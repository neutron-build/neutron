import assert from "node:assert/strict";
import test from "node:test";
import {
  createDatabase,
  eq,
  index,
  pgTable,
  serial,
  integer,
  text,
  relations,
  schemaToDDL,
  getTableName,
  getTableColumns,
  getTableIndexes,
  type NeutronDatabase,
} from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// F02 live suite (V04 metadata-collision runtime leg): tables whose user
// columns are literally named `columns`, `tableName` and `indexes` must CRUD,
// map and relation-read correctly — metadata lives in the symbol-keyed
// internal record, so those names cannot clobber it. Both drivers run against
// a uniquely named throwaway database (never the shared dev instance).

const DB_NAME = uniqueDbName("neutron_orm_f02");

const metaNames = pgTable(
  "meta_names",
  {
    id: serial("id").primaryKey(),
    columns: text("columns").notNull(),
    tableName: text("table_name"),
    indexes: integer("indexes"),
  },
  (t) => [index("meta_names_columns_idx").on(t.columns)],
);

const metaChildren = pgTable("meta_children", {
  id: serial("id").primaryKey(),
  parentId: integer("parent_id").notNull().references(() => metaNames.id),
  label: text("label").notNull(),
});

const metaNamesRelations = relations(metaNames, ({ many }) => ({
  children: many(metaChildren),
}));
const metaChildrenRelations = relations(metaChildren, ({ one }) => ({
  parent: one(metaNames, { fields: [metaChildren.parentId], references: [metaNames.id] }),
}));

const RAW_SELECT =
  'select id, "columns", table_name as "tableName", "indexes" from meta_names order by id';

type MetaRow = { id: number; columns: string; tableName: string | null; indexes: number | null };

type Tables = { metaNames: typeof metaNames; metaChildren: typeof metaChildren };
type RelationsMap = { metaNames: typeof metaNamesRelations; metaChildren: typeof metaChildrenRelations };
type TestDb = NeutronDatabase<Tables, RelationsMap>;

async function withSuite(driverKind: "postgres" | "pg", fn: (db: TestDb) => Promise<void>): Promise<void> {
  if (!(await ensureLive(`live collision (${driverKind})`))) {
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
  const db = (await createDatabase({
    url: url.toString(),
    driverOptions: { driver: driverKind },
    tables: { metaNames, metaChildren },
    relations: { metaNames: metaNamesRelations, metaChildren: metaChildrenRelations },
  })) as unknown as TestDb;
  try {
    for (const stmt of schemaToDDL([metaNames, metaChildren])) {
      await db.driver.execute(stmt);
    }
    await fn(db);
  } finally {
    await db.close();
    if (/^neutron_orm_f02_[0-9_]+$/.test(DB_NAME)) {
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
  test(`live collision (${driverKind}): metadata-named columns insert, return and read back exactly`, async () => {
    await withSuite(driverKind, async (db) => {
      // Metadata sanity on the live tables: accessors see the real metadata.
      assert.equal(getTableName(metaNames), "meta_names");
      assert.deepEqual(Object.keys(getTableColumns(metaNames)), ["id", "columns", "tableName", "indexes"]);
      assert.equal(getTableIndexes(metaNames).length, 1);

      const back = await db
        .insert(metaNames)
        .values([
          { columns: "c1", tableName: "t1", indexes: 1 },
          { columns: "c2", tableName: null }, // explicit null stays NULL, omitted indexes NULL too
          { columns: "c3", indexes: 3 }, // omitted tableName -> NULL
        ])
        .returning();
      assert.equal(back.length, 3);
      assert.deepEqual(
        back.map((r) => ({ columns: r.columns, tableName: r.tableName, indexes: r.indexes })),
        [
          { columns: "c1", tableName: "t1", indexes: 1 },
          { columns: "c2", tableName: null, indexes: null },
          { columns: "c3", tableName: null, indexes: 3 },
        ],
      );

      // Independent oracle: raw SQL with explicit aliases (physical names).
      const oracle = (await db.driver.query<MetaRow>(RAW_SELECT)) as MetaRow[];
      assert.deepEqual(
        oracle.map((r) => [r.columns, r.tableName, r.indexes]),
        [
          ["c1", "t1", 1],
          ["c2", null, null],
          ["c3", null, 3],
        ],
      );
    });
  });

  test(`live collision (${driverKind}): ORM select/update/delete map through metadata, not property names`, async () => {
    await withSuite(driverKind, async (db) => {
      await db.insert(metaNames).values({ columns: "u1", tableName: "old", indexes: 7 });

      const rows = await db.select().from(metaNames).where(eq(metaNames.columns, "u1"));
      assert.equal(rows.length, 1);
      assert.equal(rows[0].tableName, "old");
      assert.equal(rows[0].indexes, 7);

      const updated = await db
        .update(metaNames)
        .set({ tableName: "new", indexes: null })
        .where(eq(metaNames.columns, "u1"))
        .returning();
      assert.equal(updated[0].tableName, "new");
      assert.equal(updated[0].indexes, null);
      const check = (await db.driver.query<MetaRow>(RAW_SELECT)) as MetaRow[];
      assert.deepEqual([check[0].tableName, check[0].indexes], ["new", null]);

      const deleted = await db.delete(metaNames).where(eq(metaNames.tableName, "new")).returning();
      assert.equal(deleted.length, 1);
      assert.equal(deleted[0].columns, "u1");
      const after = (await db.driver.query<{ n: number }>("select count(*)::int as n from meta_names"))[0].n;
      assert.equal(after, 0);
    });
  });

  test(`live collision (${driverKind}): relational reads nest collision-named columns correctly`, async () => {
    await withSuite(driverKind, async (db) => {
      const parents = await db
        .insert(metaNames)
        .values([{ columns: "p1" }, { columns: "p2" }])
        .returning();
      const p1 = parents[0].id;
      const p2 = parents[1].id;
      await db.insert(metaChildren).values([
        { parentId: p1, label: "a" },
        { parentId: p1, label: "b" },
        { parentId: p2, label: "c" },
      ]);

      // To-many: exactly two children on p1, one on p2 — no fan-out, property
      // keys including the collision names survive the JSON path.
      const many = await db.query.metaNames.findMany({
        where: eq(metaNames.columns, "p1"),
        with: { children: true },
      });
      assert.equal(many.length, 1);
      assert.equal(many[0].children.length, 2);
      assert.deepEqual(
        many[0].children.map((c) => c.label).sort(),
        ["a", "b"],
      );

      // To-one from the child side: the parent's collision-named columns come
      // back keyed by property names, values intact.
      const child = await db.query.metaChildren.findFirst({
        where: eq(metaChildren.label, "c"),
        with: { parent: true },
      });
      assert.equal(child?.parent?.columns, "p2");
      assert.equal(child?.parent?.tableName, null);
      assert.equal(child?.parent?.indexes, null);
    });
  });
}
