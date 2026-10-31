import { expect, it } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { adapterVercel } from './vercel.js';
it('TS-F13 generated Vercel writer respects bounded backpressure and cancels disconnects', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'neutron-final-stream-'));
  const entry = path.join(root, 'runtime.mjs');
  await fs.writeFile(entry, `
    globalThis.__flow = { pulls: 0, cancels: 0 };
    export async function handleNeutronRequest() {
      const headers = new Headers(); headers.append('Set-Cookie', 'a=1'); headers.append('Set-Cookie', 'b=2');
      return new Response(new ReadableStream({ pull(c) { if (++globalThis.__flow.pulls > 8) { c.close(); return; } c.enqueue(new Uint8Array(65536)); }, cancel() { globalThis.__flow.cancels++; } }), { headers });
    }
  `);
  await adapterVercel().adapt({ rootDir: root, outDir: root, routes: { total: 1, static: 0, app: 1 }, log() {}, async ensureRuntimeBundle() { return { target: 'node', outDir: root, entryPath: entry, entryRelativePath: 'runtime.mjs' }; } });
  const script = `
    import handler from ${JSON.stringify(path.join(root, 'api/__neutron.mjs'))};
    import { Writable } from 'node:stream'; import assert from 'node:assert/strict';
    const headers = {}; const callbacks = [];
    const sink = new Writable({ highWaterMark: 1, write(chunk, encoding, callback) { callbacks.push(callback); } });
    sink.setHeader = (key, value) => headers[key] = value;
    const operation = handler({ headers: { host: 'example.test' }, method: 'GET', url: '/' }, sink);
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.ok(globalThis.__flow.pulls < 8, 'source was drained while sink blocked');
    assert.equal(callbacks.length, 1); assert.deepEqual(headers['set-cookie'], ['a=1', 'b=2']);
    sink.destroy(new Error('disconnect')); await assert.rejects(operation, /disconnect/);
    assert.equal(globalThis.__flow.cancels, 1); console.log('bounded-backpressure-and-cancel PASS');
  `;
  try { const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script]); expect(result.stdout).toContain('PASS'); }
  finally { await fs.rm(root, { recursive: true, force: true }); }
}, 10000);
