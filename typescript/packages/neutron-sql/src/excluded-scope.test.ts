import assert from "node:assert/strict";
import test from "node:test";
import {
  compileStatement,
  createDatabase,
  excluded,
  fragment,
  ident,
  insertStatement,
  integer,
  onConflictClause,
  param,
  pgTable,
  projection,
  qual,
  selectStatement,
  serial,
  sql,
  subquery,
  text,
  type NeutronDatabase,
} from "./index.js";
import { schemaToDDL } from "./ddl.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

// NA-12: `excluded` handling must be semantic (only the excluded() constructor
// creates the pseudo-relation reference) and scope-aware (a scalar subquery in
// DO UPDATE SET/WHERE is correlated to the insert and CAN see excluded —
// live-verified on PostgreSQL 17 before this change).

const audit = pgTable("na12_audit", {
  id: integer("id").primaryKey(),
  body: text("body").notNull(),
});

const excludedTable = pgTable("excluded", {
  id: serial("id").primaryKey(),
});

// ---------------------------------------------------------------------------
// Unit leg: compile-time behavior
// ---------------------------------------------------------------------------

/** Ordinary identifiers named `excluded` — a schema-qualified reference, and a
 *  bare table — are ordinary SQL and must compile like any other name.
 *  Name-based pseudo-relation detection used to reject these (NA-12 A). */
test("excluded-scope: ordinary identifiers named excluded compile", () => {
  const tableRef = compileStatement(
    selectStatement({ from: qual("excluded", "events"), where: [qual("excluded", "id")] }),
  );
  assert.equal(tableRef.sql, 'select * from "excluded"."events" where "excluded"."id"');

  const bareTable = compileStatement(selectStatement({ from: ident("excluded") }));
  assert.ok(bareTable.sql.includes('from "excluded"'));
});

/** A scalar subquery in DO UPDATE SET inherits the excluded binding — in its
 *  projections and its WHERE alike — because PostgreSQL correlates it to the
 *  insert (NA-12 B). The old blanket rule rejected statements the database
 *  accepts. */
test("excluded-scope: scalar subquery in DO UPDATE SET sees excluded", async () => {
  const unitDb = await createUnitDb();
  const out = unitDb
    .insert(audit)
    .values({ id: 1, body: "after" })
    .onConflictUpdate({
      target: audit.id,
      set: { body: sql`(select ${excluded(audit.body)} || '-seen')` },
    })
    .toSQL();
  assert.match(out.sql, /\(select "excluded"\."body" \|\| '-seen'\)/);
});

test("excluded-scope: a nested statement inside the correlated subquery also inherits", () => {
  const inner = subquery(
    selectStatement({
      projections: [projection(subquery(selectStatement({ projections: [projection(excluded(audit.body))] })))],
    }),
  );
  const out = compileStatement(
    insertStatement({
      table: ident("na12_audit"),
      columns: ["id", "body"],
      rows: [[param(1), param("x")]],
      onConflict: onConflictClause({
        action: "update",
        targetColumns: ["id"],
        sets: [{ column: "body", value: fragment("(", inner, ")") }],
      }),
    }),
  );
  assert.ok(out.sql.includes('"excluded"."body"'));
});

/** The genuine rejections stay: outside a conflict-update expression tree the
 *  special reference cannot work and must fail BEFORE SQL. */
test("excluded-scope: unbound special references still fail before SQL", () => {
  const bad = excluded(audit.body);
  assert.throws(
    () => compileStatement(selectStatement({ from: ident("members"), where: [bad] })),
    /only valid directly inside on-conflict do-update/,
  );
  // A subquery hanging off a plain SELECT does not inherit anything.
  assert.throws(
    () =>
      compileStatement(
        selectStatement({
          from: ident("members"),
          where: [subquery(selectStatement({ from: ident("members"), where: [bad] }))],
        }),
      ),
    /only valid directly inside on-conflict/,
  );
  // Insert RETURNING still cannot see it (live-verified PG error).
  assert.throws(
    () =>
      compileStatement(
        insertStatement({
          table: ident("na12_audit"),
          columns: ["id"],
          rows: [[param(1)]],
          returning: [projection(bad)],
        }),
      ),
    /insert returning/,
  );
});

async function createUnitDb(): Promise<NeutronDatabase> {
  return await createDatabase({ url: "unused://unit", tables: { audit } });
}

// ---------------------------------------------------------------------------
// Live leg: the audit report's exact fixtures, executed on real PostgreSQL
// ---------------------------------------------------------------------------

for (const driverKind of ["postgres", "pg"] as const) {
  test(`excluded-scope (${driverKind}): live NA-12 fixtures on PostgreSQL`, async () => {
    const label = `excluded-scope (${driverKind})`;
    if (!(await ensureLive(label))) return;
    const { Pool } = (await import("pg")) as unknown as {
      Pool: new (o: object) => { query: (s: string, p?: unknown[]) => Promise<unknown>; end: () => Promise<void> };
    };
    const dbName = uniqueDbName("na12_excluded");
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`drop database if exists "${dbName}"`);
    await admin.query(`create database "${dbName}"`);
    await admin.end();

    const url = new URL(TEST_URL);
    url.pathname = `/${dbName}`;
    const db = await createDatabase({
      url: url.toString(),
      driverOptions: { driver: driverKind },
      tables: { audit, excludedTable },
    });
    try {
      for (const stmt of schemaToDDL([audit, excludedTable])) {
        await db.driver.execute(stmt);
      }

      // The report's fixture, by hand: scalar subquery referencing excluded.
      await db.driver.execute(
        "insert into na12_audit values (1, 'before')",
      );
      await db.driver.execute(
        "insert into na12_audit values (1, 'after') on conflict (\"id\") do update set \"body\" = (select \"excluded\".\"body\" || '-seen')",
      );
      const hand = (await db.select({ body: audit.body }).from(audit)) as Array<{ body: string }>;
      assert.equal(hand[0].body, "after-seen");

      // The same shape built through the ORM, including the correlated
      // subquery, executes end to end.
      await db.driver.execute("update na12_audit set body = 'orm' where id = 1");
      const back = await db
        .insert(audit)
        .values({ id: 1, body: "live" })
        .onConflictUpdate({
          target: audit.id,
          set: { body: sql`(select ${excluded(audit.body)} || '-orm')` },
        })
        .returning(["id", "body"]);
      assert.equal(back[0].body, "live-orm");

      // An ordinary table actually named `excluded` selects normally.
      const rows = (await db.select({ id: excludedTable.id }).from(excludedTable)) as Array<{
        id: number | null;
      }>;
      assert.ok(Array.isArray(rows));
    } finally {
      await db.close();
      const admin2 = new Pool({ connectionString: TEST_URL, max: 1 });
      await admin2.query(
        "select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()",
        [dbName],
      );
      await admin2.query(`drop database if exists "${dbName}"`);
      await admin2.end();
    }
  });
}
