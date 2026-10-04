#!/usr/bin/env node
/**
 * Fresh-installed finite TypeScript admission facts with a PostgreSQL oracle.
 *
 * AUTHORED, NOT EXECUTED. Nothing here is qualification until the coordinator
 * runs it against owned endpoints and reviews the report.
 *
 * The coordinator supplies owned endpoint URLs through environment variables
 * (only the variable NAMES are arguments; URLs and credentials never appear in
 * reports), a fresh INSTALLED @neutron-build/sql package root (a path inside
 * node_modules whose dist/ was built from the revision under test, with pg and
 * postgres installed beside it), and the exact Nucleus binary file plus its
 * recorded SHA-256. Provisioning and native state checks use the raw `pg`
 * client, never ORM DDL. Both bundled adapters (pg and postgres.js) are run
 * against a PostgreSQL control first-class and against the Nucleus candidate.
 *
 * The binary hash only verifies the file passed to this script. The report is
 * NOT attestation: the coordinator must bind the actual endpoint process to
 * that binary (process image, listening port, data directory) and record the
 * engine source/config out of band. This bounded gate has no timing, soak,
 * package enablement or PostgreSQL parity claim and leaves packageEnabled false.
 *
 * Usage (coordinator exports PG_OWNED_URL and NUCLEUS_OWNED_URL):
 *   node conformance/polyglot/nucleus/ts_admission_native.mjs \
 *     --postgres-url-env PG_OWNED_URL --nucleus-url-env NUCLEUS_OWNED_URL \
 *     --package-root /consumer/node_modules/@neutron-build/sql \
 *     --binary-file /path/to/exact/nucleus --binary-sha256 RECORDED_SHA256 \
 *     --report /path/to/evidence/ts-nucleus-finite.json
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const NUCLEUS_CANDIDATE_PROFILE = 'nucleus-relational-rc-v1-candidate';
const STAMP = '2026-01-02T03:04:05.123456Z';
const VALUES = { id: 1, active: false, title: '', data: null, stamp: STAMP, n: 0 };
const DRIVERS = ['pg', 'postgres'];

class RollbackMarker extends Error {}

const digest = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const quote = (identifier) => '"' + identifier.replaceAll('"', '""') + '"';

function listFiles(directory) {
  const files = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...listFiles(full));
    else files.push(full);
  }
  return files;
}

function installedVersion(require, name) {
  let directory = path.dirname(require.resolve(name));
  for (;;) {
    const manifest = path.join(directory, 'package.json');
    if (fs.existsSync(manifest)) {
      const parsed = JSON.parse(fs.readFileSync(manifest, 'utf8'));
      if (parsed.name === name) return parsed.version;
    }
    const parent = path.dirname(directory);
    if (parent === directory) throw new Error('installed package manifest not found for ' + name);
    directory = parent;
  }
}

function normalized(row) {
  return { id: String(row.id), active: row.active, title: row.title, data: row.data, stamp: row.stamp, n: row.n };
}

function nativeSnapshotSql(schema, table) {
  return 'SELECT id::text AS id, active, title, data::text AS data, (data IS NULL) AS data_is_sql_null, n FROM '
    + quote(schema) + '.' + quote(table) + ' ORDER BY id';
}

function modelFor(mod, schema, name) {
  return mod.pgSchema(schema).table(name, {
    id: mod.bigint('id').primaryKey(),
    active: mod.boolean('active').notNull(),
    title: mod.text('title').notNull(),
    data: mod.jsonb('data'),
    stamp: mod.timestamptz('stamp').notNull(),
    n: mod.integer('n'),
  });
}

async function refusal(mod, label, body) {
  try {
    await body();
  } catch (error) {
    if (error instanceof mod.ProfileRefusedError || error instanceof mod.CapabilityRequirementError) return label;
    throw new assert.AssertionError({ message: label + ': refused for the wrong reason (' + (error?.constructor?.name ?? typeof error) + ')' });
  }
  throw new assert.AssertionError({ message: label + ': operation outside the finite profile was admitted' });
}

async function refusalChecks(mod, db, table, url, kind, schema, name) {
  const { eq } = mod;
  const refused = [];
  refused.push(await refusal(mod, 'raw-ddl', () => db.driver.execute('CREATE TABLE ' + quote(schema) + '.surprise (id int)')));
  refused.push(await refusal(mod, 'raw-delete-no-predicate', () => db.driver.execute('DELETE FROM ' + quote(schema) + '.' + quote(name))));
  refused.push(await refusal(mod, 'raw-function-query', () => db.driver.query('SELECT pg_cancel_backend(1)')));
  refused.push(await refusal(mod, 'unbound-scan', () => db.select().from(table)));
  refused.push(await refusal(mod, 'limit-query-algebra', () => db.select().from(table).where(eq(table.id, 1)).limit(1)));
  refused.push(await refusal(mod, 'stream', () => db.select().from(table).where(eq(table.id, 1)).stream().next()));
  refused.push(await refusal(mod, 'serializable-transaction', () => db.transaction(async () => undefined, { isolation: 'serializable' })));
  refused.push(await refusal(mod, 'read-only-transaction', () => db.transaction(async () => undefined, { readOnly: true })));
  refused.push(await refusal(mod, 'default-isolation-batch', async () => { await db.batch([db.delete(table).where(eq(table.id, 1))]); }));
  const numeric = mod.pgSchema(schema).table('numeric_surface', { id: mod.bigint('id').primaryKey(), v: mod.numeric('v') });
  refused.push(await refusal(mod, 'numeric-table', () => mod.createDatabase({
    url, driverOptions: { driver: kind }, profile: NUCLEUS_CANDIDATE_PROFILE, tables: { numeric },
  })));
  return refused;
}

async function ormFacts(mod, native, url, kind, profile, schema, name) {
  const { eq, jsonNull } = mod;
  const table = modelFor(mod, schema, name);
  const snapshot = async () => (await native.query(nativeSnapshotSql(schema, name))).rows;
  const db = await mod.createDatabase({ url, driverOptions: { driver: kind, max: 2 }, profile, tables: { t: table } });
  try {
    const facts = [];
    const inserted = await db.insert(table).values(VALUES).returning();
    facts.push({ phase: 'insert', orm: normalized(inserted[0]), native: await snapshot() });
    let inTransaction;
    await db.transaction(async (tx) => {
      await tx.update(table).set({ title: 'outer', data: jsonNull }).where(eq(table.id, 1));
      try {
        await tx.transaction(async (inner) => {
          await inner.update(table).set({ title: 'inner' }).where(eq(table.id, 1));
          throw new RollbackMarker();
        });
      } catch (error) {
        if (!(error instanceof RollbackMarker)) throw error;
      }
      inTransaction = normalized((await tx.select().from(table).where(eq(table.id, 1)))[0]);
    });
    facts.push({ phase: 'savepoint-recovery-commit', orm: inTransaction, native: await snapshot() });
    try {
      await db.transaction(async (tx) => {
        await tx.update(table).set({ title: 'must-rollback' }).where(eq(table.id, 1));
        throw new RollbackMarker();
      });
    } catch (error) {
      if (!(error instanceof RollbackMarker)) throw error;
    }
    facts.push({ phase: 'outer-rollback', orm: normalized((await db.select().from(table).where(eq(table.id, 1)))[0]), native: await snapshot() });
    let refusals = [];
    if (profile === NUCLEUS_CANDIDATE_PROFILE) {
      assert.equal(db.endpointIdentity.packageEnabled, false);
      assert.equal(Object.isFrozen(db.endpointIdentity), true);
      refusals = await refusalChecks(mod, db, table, url, kind, schema, name);
      facts.push({ phase: 'after-refusals', orm: normalized((await db.select().from(table).where(eq(table.id, 1)))[0]), native: await snapshot() });
    }
    return { facts, refusals };
  } finally {
    await db.close();
  }
}

async function main() {
  const { values: args } = parseArgs({ options: {
    'postgres-url-env': { type: 'string' }, 'nucleus-url-env': { type: 'string' }, 'package-root': { type: 'string' },
    'binary-file': { type: 'string' }, 'binary-sha256': { type: 'string' }, report: { type: 'string' },
  } });
  for (const required of ['postgres-url-env', 'nucleus-url-env', 'package-root', 'binary-file', 'binary-sha256', 'report']) {
    if (!args[required]) throw new Error('missing required argument --' + required);
  }
  const root = fs.realpathSync(path.resolve(args['package-root']));
  if (!root.split(path.sep).includes('node_modules')) throw new Error('package root must be a fresh installed package inside node_modules');
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
  if (manifest.name !== '@neutron-build/sql') throw new Error('package root is not @neutron-build/sql');
  const dist = path.join(root, 'dist');
  if (!fs.existsSync(path.join(dist, 'profile.js'))) throw new Error('installed package has no NP01 profile admission (dist/profile.js)');
  const packageFiles = { 'package.json': digest(path.join(root, 'package.json')) };
  for (const file of listFiles(dist).filter((f) => f.endsWith('.js') && !f.endsWith('.test.js')).sort()) {
    packageFiles[path.relative(root, file)] = digest(file);
  }
  const binary = fs.realpathSync(path.resolve(args['binary-file']));
  if (digest(binary) !== args['binary-sha256']) throw new Error('Nucleus binary does not match required coordinator SHA256');

  const require = createRequire(path.join(root, 'package.json'));
  const pg = require('pg');
  const mod = await import(pathToFileURL(path.join(dist, 'index.js')).href);
  const urls = {};
  for (const [engine, variable] of [['postgres', args['postgres-url-env']], ['nucleus', args['nucleus-url-env']]]) {
    if (!process.env[variable]) throw new Error('environment variable ' + variable + ' is not set');
    urls[engine] = process.env[variable];
  }

  const report = {
    status: 'fail', package_enabled: false, profile: NUCLEUS_CANDIDATE_PROFILE,
    scope: 'finite TypeScript admission/CRUD/rollback facts only',
    probeSha256: digest(fileURLToPath(import.meta.url)),
    nodeVersion: process.version, nodeSha256: digest(fs.realpathSync(process.execPath)),
    packageName: manifest.name, packageVersion: manifest.version, packageFiles,
    pgVersion: installedVersion(require, 'pg'), postgresJsVersion: installedVersion(require, 'postgres'),
    binarySha256: args['binary-sha256'],
    binaryAttestation: 'coordinator-provided executed binary; endpoint report alone is not attestation',
    facts: {},
  };
  const schema = 'np01_' + crypto.randomBytes(6).toString('hex');
  const natives = [];
  let failure = null;
  try {
    const outputs = {};
    for (const [engine, profile] of [['postgres', 'postgres-direct'], ['nucleus', NUCLEUS_CANDIDATE_PROFILE]]) {
      const native = new pg.Client({ connectionString: urls[engine] });
      await native.connect();
      natives.push(native);
      await native.query('CREATE SCHEMA ' + quote(schema));
      for (const kind of DRIVERS) {
        await native.query('CREATE TABLE ' + quote(schema) + '.' + quote(kind) + ' (id bigint PRIMARY KEY, active boolean NOT NULL, title text NOT NULL, data jsonb, stamp timestamptz NOT NULL, n integer)');
        outputs[engine + ':' + kind] = await ormFacts(mod, native, urls[engine], kind, profile, schema, kind);
      }
      if (engine === 'nucleus') {
        for (const kind of DRIVERS) {
          await refusal(mod, 'default-profile-' + kind, () => mod.createDatabase({ url: urls.nucleus, driverOptions: { driver: kind }, profile: 'postgres-direct' }));
        }
      }
    }
    for (const kind of DRIVERS) {
      const control = outputs['postgres:' + kind].facts;
      const candidate = outputs['nucleus:' + kind].facts;
      assert.deepStrictEqual(control, candidate.slice(0, control.length), kind + ': PostgreSQL finite checkpoints disagree');
      assert.deepStrictEqual(candidate.at(-1).native, control.at(-1).native, kind + ': refused operations changed committed rows');
      assert.equal(control[0].orm.title, '');
      assert.equal(control[0].orm.active, false);
      assert.equal(control[0].orm.n, 0);
      assert.equal(control[0].native[0].data_is_sql_null, true, 'SQL NULL must be preserved');
      assert.equal(control[1].native[0].data_is_sql_null, false, 'JSON null must stay distinct from SQL NULL');
      assert.equal(control[1].native[0].data, 'null');
      assert.equal(control[1].orm.title, 'outer');
      assert.deepStrictEqual(control[2], { ...control[1], phase: 'outer-rollback' }, kind + ': rollback did not restore committed state');
    }
    assert.deepStrictEqual(outputs['postgres:pg'].facts, outputs['postgres:postgres'].facts, 'pg and postgres.js control checkpoints disagree');
    assert.deepStrictEqual(outputs['nucleus:pg'].facts, outputs['nucleus:postgres'].facts, 'pg and postgres.js Nucleus checkpoints disagree');
    assert.deepStrictEqual(outputs['nucleus:pg'].refusals, outputs['nucleus:postgres'].refusals, 'pg and postgres.js refusal lists disagree');
    assert.ok(outputs['nucleus:pg'].refusals.length > 0, 'no refusal checks ran against the Nucleus candidate');
    report.facts = {
      checkpointAgreement: true, driverAgreement: true, defaultProfileRefusesNucleus: true,
      refusals: outputs['nucleus:pg'].refusals, unsupportedOperationsPreserveRows: true,
    };
    report.status = 'pass';
  } catch (error) {
    failure = error;
    report.failure = { class: error?.constructor?.name ?? typeof error, code: error?.code ?? null,
      reason: error instanceof assert.AssertionError ? String(error.message) : null,
      cause: error?.cause ? { class: error.cause?.constructor?.name ?? typeof error.cause, code: error.cause?.code ?? null,
        message: String(error.cause?.message ?? '').split('\n')[0].slice(0, 300) } : null };
  } finally {
    for (const native of natives.reverse()) {
      try {
        await native.query('DROP SCHEMA IF EXISTS ' + quote(schema) + ' CASCADE');
      } catch (dropError) {
        let error = dropError;
        if (dropError?.code === '0A000') {
          // The engine has no DROP SCHEMA: remove the fixture tables instead and
          // record the empty schema as residue (an owned engine's data directory
          // is removed by its runner).
          try {
            for (const kind of DRIVERS) await native.query('DROP TABLE IF EXISTS ' + quote(schema) + '.' + quote(kind));
            (report.cleanupResidue ??= []).push('schema ' + schema + ' remains: the engine does not support DROP SCHEMA');
            error = null;
          } catch (tableError) {
            error = tableError;
          }
        }
        if (error !== null) {
          failure = error;
          report.status = 'fail';
          report.cleanupFailure = { class: error?.constructor?.name ?? typeof error, code: error?.code ?? null, message: String(error?.message ?? '').split('\n')[0].slice(0, 300) };
        }
      } finally {
        await native.end().catch(() => undefined);
      }
    }
    fs.mkdirSync(path.dirname(path.resolve(args.report)), { recursive: true });
    fs.writeFileSync(args.report, JSON.stringify(report, null, 2) + '\n');
  }
  if (failure !== null) process.exitCode = 1;
}

main().catch((error) => {
  console.error('qualifier setup failed: ' + (error instanceof Error ? error.message : String(error)));
  process.exitCode = 2;
});
