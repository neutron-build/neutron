// Reads the runner-owned scalar fixture through an actual installed SQL package.
// --module must name the installed package entry; artifact hashes are verified
// by the coordinator before fixture setup, never inferred from source versions.
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import path from 'node:path';

let db;
try {
  const request = JSON.parse(readFileSync(0, 'utf8'));
  if (request.protocol !== 'polyglot-conformance-v1' || request.case_id !== 'scalar-extremes' || request.action !== 'observe' || request.profile !== 'postgres-direct' || !/^neutron_polyglot_[0-9a-f]{32}$/.test(request.schema_scope)) throw new Error('unsupported adapter request');
  const position = process.argv.indexOf('--module');
  if (position < 0 || !process.argv[position + 1]) throw new Error('installed module entry required');
  const driver = process.argv.includes('--postgres-js') ? 'postgres' : 'pg';
  const { createDatabase, pgSchema, integer, bigint, numeric, timestamptz, text, jsonb, asc, sql } = await import(pathToFileURL(path.resolve(process.argv[position + 1])).href);
  const table = pgSchema(request.schema_scope).table('values_fixture', {
    id: integer('id').primaryKey(), big: bigint('big', { mode: 'string' }),
    precise: numeric('precise'), moment: timestamptz('moment'),
    sqlNull: text('sql_null'), document: jsonb('document'),
  });
  const url = process.env.NEUTRON_TEST_DATABASE_URL;
  if (!url) throw new Error('disposable database environment required');
  db = await createDatabase({ url, driverOptions: { driver }, tables: { values: table } });
  // SQL NULL's predicate is observed independently of JSON's JS null decoder.
  const values = await db.select({ id: table.id, big: table.big, precise: table.precise,
    moment: table.moment, sqlNull: sql`${table.sqlNull} is null`, document: table.document }).from(table).orderBy(asc(table.id));
  const rows = values.map(row => {
    if (typeof row.moment !== 'string' || !/^\d{4}-\d\d-\d\dT.*Z$/.test(row.moment)) throw new Error('instant must preserve canonical string precision');
    return [String(row.id), row.big, row.precise, row.moment.replace('T', ' ').replace(/Z$/, '+00'), row.sqlNull, JSON.stringify(row.document)];
  });
  console.log(JSON.stringify({ protocol: request.protocol, case_id: request.case_id,
    profile: request.profile, schema_scope: request.schema_scope,
    artifact_hashes: request.artifact_hashes, status: 'pass', rows }));
} catch {
  // Native causes can contain values/credentials; diagnostics remain bounded.
  console.log(JSON.stringify({ status: 'fail', diagnostics: 'TypeScript installed adapter refused or failed' }));
  process.exitCode = 1;
} finally {
  if (db) await db.close();
}
