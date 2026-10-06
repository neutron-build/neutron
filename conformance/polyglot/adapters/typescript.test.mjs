import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

test('failed native shutdown emits one redacted failure envelope', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'neutron-adapter-shutdown-'));
  try {
    const module = path.join(dir, 'client.mjs');
    // The integer builder is needed before connection acquisition.
    writeFileSync(module, `export const integer=()=>({primaryKey:()=>0}),bigint=()=>0,numeric=()=>0,timestamptz=()=>0,text=()=>0,jsonb=()=>0,asc=()=>0,sql=()=>0;
export const pgSchema=()=>({table:()=>({})});
export const createDatabase=async()=>({select:()=>({from:()=>({orderBy:async()=>[]})}),close:async()=>{throw new Error('postgres://private:shutdown-secret@example/private')}});`);
    const child = spawnSync(process.execPath, [new URL('./typescript.mjs', import.meta.url).pathname, '--module', module], {
      input: JSON.stringify({ protocol: 'polyglot-conformance-v1', case_id: 'scalar-extremes', action: 'observe', profile: 'postgres-direct', schema_scope: 'neutron_polyglot_' + 'a'.repeat(32), artifact_hashes: {} }),
      env: { ...process.env, NEUTRON_TEST_DATABASE_URL: 'fixture-only' }, encoding: 'utf8', timeout: 10000,
    });
    assert.equal(child.status, 1);
    assert.equal(JSON.parse(child.stdout).diagnostics, 'TypeScript installed adapter cleanup failed');
    assert.equal(child.stderr, '');
    assert.equal((child.stdout + child.stderr).includes('shutdown-secret'), false);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
