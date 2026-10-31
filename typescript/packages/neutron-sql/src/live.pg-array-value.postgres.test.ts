import test from "node:test";
import assert from "node:assert/strict";
import { PgArray, bigint, createDatabase, eq, integer, jsonb, jsonNull, pgTable, text, type NeutronDatabase } from "./index.js";
import { TEST_URL, ensureLive } from "./live-harness.js";

const arrays = pgTable("native_arrays", {
  id: integer("id").primaryKey(),
  integers: bigint("integers").nativeArray(),
  labels: text("labels").nativeArray(),
  docs: jsonb("docs").nativeArray(),
});
for (const driver of ["pg", "postgres"] as const) {
  test(`native dimensioned arrays (${driver}): native bounds/member oracle, RETURNING and NULL identity`, async () => {
    if (!(await ensureLive(`native dimensioned arrays ${driver}`))) return;
    const { Pool } = await import("pg");
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    const name = `native_arrays_${driver}_${process.pid}_${Date.now()}`;
    let db: NeutronDatabase<{ arrays: typeof arrays }> | undefined;
    try {
      await admin.query(`create database "${name}"`);
      const url = new URL(TEST_URL); url.pathname = `/${name}`;
      db = await createDatabase({ url: url.toString(), driverOptions: { driver }, tables: { arrays } });
      await db.driver.execute("create table native_arrays(id integer primary key, integers bigint[], labels text[], docs jsonb[])");
      const dimensions = [{ length: 2, lowerBound: -2 }, { length: 2, lowerBound: 0 }];
      const integers = new PgArray(dimensions, [-9223372036854775808n, null, 9223372036854775807n, 0n]);
      const labels = new PgArray(dimensions, ["NULL", null, 'quote"slash\\', "{comma,}"]);
      const docs = new PgArray<unknown>([{ length: 3, lowerBound: 0 }], [jsonNull, null, [1, null]]);
      const returned = await db.insert(arrays).values({ id: 1, integers, labels, docs }).returning();
      assert.deepEqual(returned[0].integers!.dimensions, dimensions);
      assert.deepEqual(returned[0].integers!.elements, integers.elements);
      assert.deepEqual(returned[0].docs!.elements, docs.elements);
      const rows = await db.select({ renamed: arrays.labels }).from(arrays).where(eq(arrays.id, 1));
      assert.deepEqual(rows[0].renamed!.elements, labels.elements);
      const native = await db.driver.query<{ shape: string; low: number; high: number; minimum: string; maximum: string; member_null: boolean; json_null: boolean; sql_null: boolean; json_array: string }>("select array_dims(integers) shape, array_lower(integers,1) low, array_upper(integers,2) high, integers[-2][0]::text minimum, integers[-1][0]::text maximum, integers[-2][1] is null member_null, docs[0] = 'null'::jsonb json_null, docs[1] is null sql_null, docs[2]::text json_array from native_arrays where id=1");
      assert.deepEqual(native[0], { shape: "[-2:-1][0:1]", low: -2, high: 1, minimum: "-9223372036854775808", maximum: "9223372036854775807", member_null: true, json_null: true, sql_null: true, json_array: "[1, null]" });
      await db.insert(arrays).values({ id: 2, integers: new PgArray([], []), labels: null, docs: null });
      const empty = (await db.select().from(arrays).where(eq(arrays.id, 2)))[0];
      assert.deepEqual(empty.integers!.dimensions, []); assert.deepEqual(empty.integers!.elements, []);
      assert.equal(empty.labels, null); assert.equal(empty.docs, null);
      await db.driver.execute("insert into native_arrays(id,integers) values(3, '[0:1]={4,5}'::bigint[])");
      const nativeWritten = (await db.select().from(arrays).where(eq(arrays.id, 3)))[0];
      assert.deepEqual(nativeWritten.integers!.dimensions, [{ length: 2, lowerBound: 0 }]);
      assert.deepEqual(nativeWritten.integers!.elements, [4n, 5n]);
    } finally {
      await db?.close();
      try { await admin.query(`drop database if exists "${name}"`); } finally { await admin.end(); }
    }
  });
}
