import test from "node:test";
import assert from "node:assert/strict";
import { createDatabase, createQueryTelemetry, integer, pgTable, QueryCanceledError, SqlRequestLifecycle, sql, type SqlEvent } from "./index.js";
import { TEST_URL, ensureLive } from "./live-harness.js";

const records = pgTable("request_records", { id: integer("id").primaryKey() });
for (const driver of ["pg", "postgres"] as const) {
  test(`request lifetime (${driver}): native cancellation drains and rolls back before owned close`, async () => {
    if (!(await ensureLive(`request lifetime ${driver}`))) return;
    const { Pool } = await import("pg");
    const name = `request_lifetime_${driver}_${process.pid}_${Date.now()}`;
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`create database "${name}"`);
    const url = new URL(TEST_URL); url.pathname = `/${name}`;
    const oracle = new Pool({ connectionString: url.toString(), max: 1 });
    const emitted: unknown[] = [];
    const telemetry = createQueryTelemetry(() => ({ addEvent: (event, attributes) => { emitted.push({ event, attributes }); } }));
    const db = await createDatabase({ url: url.toString(), tables: { records }, driverOptions: { driver }, logger: (event: SqlEvent) => telemetry.logger(event) });
    const lifetime = new SqlRequestLifecycle(db);
    try {
      await oracle.query("create table request_records(id integer primary key)");
      await oracle.query("insert into request_records values(0)");
      const running = telemetry.run(async () => {
        try {
          await lifetime.run((owned, signal) => owned.transaction(async tx => {
            await tx.insert(records).values({ id: 1 });
            await tx.select({ wait: sql`(select 1 from pg_sleep(30))` }).from(records).execute({ signal });
          }));
          assert.fail("native canceled request must fail");
        } catch (error) { assert.ok(error instanceof QueryCanceledError); }
        return telemetry.current();
      });
      let reached = false;
      for (let attempt = 0; attempt < 200; attempt++) {
        const native = await oracle.query("select count(*)::int n from pg_catalog.pg_stat_activity where datname = current_database() and pid <> pg_backend_pid() and state='active' and query like '%pg_sleep(30)%'");
        if (native.rows[0].n > 0) { reached = true; break; }
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      assert.ok(reached, "owned native backend did not reach the blocked query");
      await lifetime.shutdown({ graceMs: 0, cancelMs: 5000 });
      const result = await running;
      assert.equal(lifetime.activeRequests, 0);
      assert.ok(result.metrics.started >= 2);
      assert.equal(result.metrics.failed, 1);
      assert.equal(result.metrics.canceled, 1);
      assert.deepEqual((await oracle.query("select id from request_records order by id")).rows, [{ id: 0 }]);
      const live = await oracle.query("select count(*)::int n from pg_catalog.pg_stat_activity where datname=current_database() and pid<>pg_backend_pid()");
      assert.equal(live.rows[0].n, 0);
      assert.doesNotMatch(JSON.stringify(emitted), /pg_sleep|request_records|postgres(?:ql)?:\/\//);
      await assert.rejects(() => lifetime.run(async () => 1), /admission closed/);
    } finally {
      await lifetime.shutdown({ graceMs: 0, cancelMs: 5000 });
      await oracle.end();
      try { await admin.query(`drop database if exists "${name}"`); } finally { await admin.end(); }
    }
  });
}
