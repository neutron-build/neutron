import assert from 'node:assert/strict';
import test from 'node:test';
import { randomBytes } from 'node:crypto';
import pg from 'pg';
import { loadDriver, QueryCanceledError, getSqlState, type Driver } from './index.js';
import { TEST_URL, ensureLive } from './live-harness.js';

// Real privileges and server policies enforce isolation without a WHERE
// predicate. The role neither owns the table nor bypasses row-level security.
for (const kind of ['pg', 'postgres'] as const) {
  test(`live RLS (${kind}): alternating tenants and failures reset a single pooled backend`, async () => {
    if (!(await ensureLive(`live RLS (${kind})`))) return;
    const suffix = randomBytes(10).toString('hex');
    const schema = `rls_${suffix}`;
    const role = `rls_role_${suffix}`;
    const password = randomBytes(32).toString('hex');
    const table = `"${schema}".records`;
    const admin = new pg.Pool({ connectionString: TEST_URL, max: 1 });
    let driver: Driver | undefined;
    let oracle: pg.Pool | undefined;
    const setup = async (sql: string): Promise<void> => {
      try { await admin.query(sql); }
      catch { throw new Error('owned RLS fixture operation failed (private diagnostics suppressed)'); }
    };
    try {
      await setup(`create role "${role}" login nosuperuser nocreatedb nocreaterole noinherit nobypassrls password '${password}'`);
      await setup(`create schema "${schema}"`);
      await setup(`create table ${table}(tenant text not null,id integer not null,value text not null,primary key(tenant,id))`);
      await setup(`insert into ${table} values ('a',1,'alpha'),('b',1,'beta')`);
      await setup(`alter table ${table} enable row level security`);
      await setup(`alter table ${table} force row level security`);
      await setup(`create policy tenant_isolation on ${table} using(tenant=current_setting('app.tenant',true)) with check(tenant=current_setting('app.tenant',true))`);
      await setup(`grant usage on schema "${schema}" to "${role}"`);
      await setup(`grant select,insert,update,delete on ${table} to "${role}"`);
      const url = new URL(TEST_URL);
      url.username = role;
      url.password = password;
      oracle = new pg.Pool({ connectionString: url.toString(), max: 1 });
      assert.deepEqual((await oracle.query('select rolsuper,rolbypassrls from pg_roles where rolname=current_user')).rows, [{ rolsuper: false, rolbypassrls: false }]);
      assert.deepEqual((await oracle.query(`select tenant,id,value from ${table}`)).rows, []);
      driver = await loadDriver(url.toString(), { driver: kind, max: 1 });
      const pid = (await driver.query<{ pid: number }>('select pg_backend_pid() as pid'))[0]!.pid;
      const read = `select tenant,id,value from ${table} order by tenant,id`;
      const setTenant = async (tx: Driver, tenant: string): Promise<void> => {
        await tx.query("select set_config('app.tenant',$1,true)", [tenant]);
      };
      for (const [tenant, value] of [['a', 'alpha'], ['b', 'beta'], ['a', 'alpha']]) {
        await driver.begin(async tx => {
          await setTenant(tx, tenant!);
          assert.deepEqual(await tx.query(read), [{ tenant, id: 1, value }]);
          assert.equal((await tx.query<{ pid: number }>('select pg_backend_pid() as pid'))[0]!.pid, pid);
        });
        await assert.rejects(driver.begin(async tx => {
          await setTenant(tx, tenant!);
          await tx.execute(`insert into ${table} values ($1,9,'forbidden')`, [tenant === 'a' ? 'b' : 'a']);
        }), (error: unknown) => getSqlState(error) === '42501');
        await driver.begin(async tx => { assert.deepEqual(await tx.query(read), []); });
      }
      await assert.rejects(driver.begin(async tx => {
        await setTenant(tx, 'b');
        assert.deepEqual(await tx.query(read), [{ tenant: 'b', id: 1, value: 'beta' }]);
        throw new Error('application abort');
      }), /application abort/);
      await driver.begin(async tx => { assert.deepEqual(await tx.query(read), []); });
      await assert.rejects(driver.begin(async tx => {
        await setTenant(tx, 'a');
        await tx.query('select pg_sleep(2)', [], { deadlineMs: 60 });
      }), QueryCanceledError);
      await driver.begin(async tx => {
        assert.deepEqual(await tx.query(read), []);
        assert.equal((await tx.query<{ pid: number }>('select pg_backend_pid() as pid'))[0]!.pid, pid);
        await setTenant(tx, 'b');
        assert.deepEqual(await tx.query(read), [{ tenant: 'b', id: 1, value: 'beta' }]);
      });
      assert.deepEqual((await admin.query(read)).rows, [{ tenant: 'a', id: 1, value: 'alpha' }, { tenant: 'b', id: 1, value: 'beta' }]);
    } finally {
      await driver?.close();
      await oracle?.end();
      try {
        await setup(`drop schema if exists "${schema}" cascade`);
        await setup(`drop role if exists "${role}"`);
      } finally { await admin.end(); }
    }
  });
}
