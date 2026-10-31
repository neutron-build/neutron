import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import postgres from "postgres";
import { createDatabase, pgTable, integer, bigint, text, jsonb, jsonNull, bytea, timestamptz, schemaToDDL, getTableColumns, wrapPgPool, wrapPostgresJs, type AnyColumnBuilder, type PgPoolLike, type PostgresJsClient } from "./index.js";
import type { AnyPgTable } from "./schema.js";
import { pgVector, l2Distance } from "./pgvector.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

for (const kind of ["pg", "postgres"] as const) {
  test(`TSD-01/02/14 live ${kind}: cold max-one transaction, decoder cleanup and executable DDL`, { timeout: 20000 }, async () => {
    if (!(await ensureLive(`TSD ${kind}`))) return;
    const admin = new pg.Pool({ connectionString: TEST_URL });
    const name = uniqueDbName(`tsd_${kind}`);
    await admin.query(`create database "${name}"`);
    const url = new URL(TEST_URL); url.pathname = `/${name}`;
    const pool = kind === "pg" ? new pg.Pool({ connectionString: url.toString(), max: 1, connectionTimeoutMillis: 1000 }) : null;
    const client = kind === "postgres" ? postgres(url.toString(), { max: 1, connect_timeout: 2 }) : null;
    const driver = pool ? wrapPgPool(pool as unknown as PgPoolLike) : wrapPostgresJs(client as unknown as PostgresJsClient);
    try {
      await driver.execute("create table tsd_rows (id bigint primary key, value text, seen timestamptz)");
      await driver.execute("insert into tsd_rows values (9007199254740993, 'bad', now())");
      const normal = pgTable("tsd_rows", { id: bigint("id").primaryKey(), value: text("value"), seen: timestamptz("seen") });
      const db = await createDatabase({ tables: { normal }, driver });
      // Cold gate: no db.engine() or top-level warm-up before owning the pin.
      await db.transaction(async (tx) => {
        const rows = await tx.select().from(normal).for("update");
        assert.equal(rows.length, 1);
        assert.equal(typeof rows[0].seen, "string");
      });
      const vectors = pgTable("tsd_rows", { value: pgVector("value", 1) });
      await db.transaction(async (tx) => {
        await assert.rejects(() => tx.select({ distance: l2Distance(vectors.value, [1]) }).from(vectors).then(() => {}), /requires capabilities/);
        // Failed probe is rolled back to its savepoint; transaction still works.
        assert.equal((await tx.select().from(normal)).length, 1);
      });
      const nestedDb = await createDatabase({ tables: { normal }, driver });
      await nestedDb.query.normal.create({ data: { id: 2n, value: "nested", seen: "2026-01-01T00:00:00Z" } });
      await driver.execute("delete from tsd_rows where id = 2");
      for (const column of [bigint("id", { mode: "number" }), text("value").codec({ encode: (v: string) => v, decode: () => { throw new Error("custom decoder rejected"); } })]) {
        const broken = pgTable("tsd_rows", { broken: column });
        const brokenDb = await createDatabase({ tables: { broken }, driver });
        for (const batchSize of [1, 2]) {
          const stream = brokenDb.select().from(broken).stream({ batchSize });
          await assert.rejects(() => stream.next(), /safe|decoder/);
          await stream.return();
          assert.equal((await db.select().from(normal)).length, 1);
          const batches = brokenDb.select().from(broken).streamBatches({ batchSize });
          await assert.rejects(() => batches.next(), /safe|decoder/);
          await batches.return();
          assert.equal((await db.select().from(normal)).length, 1);
          await brokenDb.transaction(async (tx) => {
            const enclosed = tx.select().from(broken).stream({ batchSize });
            await assert.rejects(() => enclosed.next(), /safe|decoder/);
            await enclosed.return();
            assert.equal((await tx.select().from(normal)).length, 1);
          });
        }
      }
      let a: AnyPgTable; let b: AnyPgTable;
      a = pgTable("tsd_a", { id: integer("id").primaryKey(), b: integer("b").references(() => getTableColumns(b).id as AnyColumnBuilder) });
      b = pgTable("tsd_b", { id: integer("id").primaryKey(), a: integer("a").references(() => getTableColumns(a).id as AnyColumnBuilder) });
      let self: AnyPgTable;
      self = pgTable("tsd_self", { id: integer("id").primaryKey(), parent: integer("parent").references(() => getTableColumns(self).id as AnyColumnBuilder) });
      const defaults = pgTable("tsd_defaults", {
        object: jsonb("object").default({ ready: true }), array: jsonb("array").default([1, "x"]),
        nil: jsonb("nil").default(jsonNull), sqlNull: jsonb("sql_null").default(null),
        bytes: bytea("bytes").default(new Uint8Array([0, 39, 255])),
        instant: timestamptz("instant").default(new Date("2026-01-02T03:04:05Z")),
        coded: text("coded").codec({ encode: (value: string) => `encoded:${value}`, decode: (value: unknown) => String(value).slice(8) }).default("quote'\\value"),
      });
      for (const ddl of schemaToDDL([a, b, self, defaults])) await driver.execute(ddl);
      const constraints = await driver.query<{ n: string }>("select count(*)::text n from pg_constraint where contype = 'f' and conrelid in ('tsd_a'::regclass, 'tsd_b'::regclass, 'tsd_self'::regclass)");
      assert.equal(constraints[0].n, "3");
      await driver.execute("insert into tsd_defaults default values");
      const rows = await driver.query<any>("select *, nil is null as nil_sql_null, jsonb_typeof(nil) as nil_json_type, sql_null is null as sql_is_null, jsonb_typeof(sql_null) as sql_json_type, extract(epoch from instant) * 1000 as instant_ms, encode(bytes, 'hex') as hex from tsd_defaults");
      assert.deepEqual(rows[0].object, { ready: true });
      assert.deepEqual(rows[0].array, [1, "x"]);
      assert.equal(rows[0].hex, "0027ff");
      assert.equal(rows[0].nil_sql_null, false);
      assert.equal(rows[0].nil_json_type, "null");
      assert.equal(rows[0].sql_is_null, true);
      assert.equal(rows[0].sql_json_type, null);
      // Raw driver bypasses the ORM decoder: independently check encoded storage.
      assert.equal(rows[0].coded, "encoded:quote'\\value");
      assert.equal(rows[0].sql_null, null);
      assert.equal(Number(rows[0].instant_ms), Date.parse("2026-01-02T03:04:05Z"));
    } finally {
      await pool?.end(); await client?.end();
      await admin.query(`drop database "${name}"`); await admin.end();
    }
  });
}
