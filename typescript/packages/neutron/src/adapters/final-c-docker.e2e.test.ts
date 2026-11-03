import { expect, it } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { adapterDocker } from './docker.js';
it('TS-F03 actual generated Docker server serves only manifested public files', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'neutron-final-docker-'));
  await fs.mkdir(path.join(root, 'assets')); await fs.writeFile(path.join(root, 'assets/app.js'), 'public-browser-code');
  await fs.writeFile(path.join(root, 'index.html'), 'public-page'); await fs.writeFile(path.join(root, '.secret'), 'private-metadata');
  // Nonhidden metadata is private unless a PUBLIC producer explicitly admits it.
  await fs.mkdir(path.join(root, 'metadata')); await fs.writeFile(path.join(root, 'metadata/private.json'), 'private');
  await fs.writeFile(path.join(root, 'deployment.json'), 'private');
  await adapterDocker().adapt({ rootDir: root, outDir: root, routes: { total: 2, static: 1, app: 1 }, publicArtifacts: ['assets/app.js', 'index.html'], log() {}, async ensureRuntimeBundle() {
    const dir = path.join(root, 'server/node'); await fs.mkdir(dir, { recursive: true });
    const file = path.join(dir, 'entry.js');
    await fs.writeFile(file, '/* SERVER-ONLY-MARKER */ export async function handleNeutronRequest() { return new Response("Not Found", { status: 404 }) }');
    return { target: 'node', outDir: dir, entryPath: file, entryRelativePath: 'server/node/entry.js' };
  } });
  const child = spawn(process.execPath, [path.join(root, 'server.mjs')], { env: { ...process.env, PORT: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; const ready = new Promise<string>((resolve, reject) => {
    child.stdout.on('data', chunk => { output += chunk; const match = output.match(/listening on http:\/\/0\.0\.0\.0:(\d+)/); if (match) resolve('http://127.0.0.1:' + match[1]); });
    child.once('exit', code => reject(new Error('Docker server exited ' + code))); child.once('error', reject);
  });
  try {
    const base = await ready;
    expect(await (await fetch(base + '/assets/app.js')).text()).toBe('public-browser-code');
    expect(await (await fetch(base + '/')).text()).toBe('public-page');
    for (const file of ['/metadata/private.json', '/deployment.json', '/server/node/entry.js', '/server.mjs', '/.secret', '/Dockerfile']) { const response = await fetch(base + file); expect(response.status).toBe(404); expect(await response.text()).not.toContain('SERVER-ONLY-MARKER'); }
  } finally { const exited = once(child, 'exit'); child.kill('SIGTERM'); await exited; await fs.rm(root, { recursive: true, force: true }); }
}, 30000);
it('generated Docker server sends precompressed siblings it was handed, never by their own URL', async () => {
  const { brotliCompressSync, gzipSync } = await import('node:zlib');
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'neutron-final-docker-br-'));
  const js = 'export const value = "' + 'precompressed '.repeat(200) + '";';
  await fs.mkdir(path.join(root, 'assets'));
  await fs.writeFile(path.join(root, 'assets/app.js'), js);
  await fs.writeFile(path.join(root, 'assets/app.js.br'), brotliCompressSync(js));
  await fs.writeFile(path.join(root, 'assets/app.js.gz'), gzipSync(js));
  await adapterDocker().adapt({ rootDir: root, outDir: root, routes: { total: 1, static: 1, app: 0 }, publicArtifacts: ['assets/app.js'], log() {} });
  const child = spawn(process.execPath, [path.join(root, 'server.mjs')], { env: { ...process.env, PORT: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; const ready = new Promise<string>((resolve, reject) => {
    child.stdout.on('data', chunk => { output += chunk; const match = output.match(/listening on http:\/\/0\.0\.0\.0:(\d+)/); if (match) resolve('http://127.0.0.1:' + match[1]); });
    child.once('exit', code => reject(new Error('Docker server exited ' + code))); child.once('error', reject);
  });
  try {
    const base = await ready;
    const br = await fetch(base + '/assets/app.js', { headers: { 'accept-encoding': 'gzip, br' } });
    expect(br.headers.get('content-encoding')).toBe('br');
    expect(br.headers.get('vary')).toBe('Accept-Encoding');
    expect(await br.text()).toBe(js);
    const gzip = await fetch(base + '/assets/app.js', { headers: { 'accept-encoding': 'gzip, br;q=0' } });
    expect(gzip.headers.get('content-encoding')).toBe('gzip');
    expect(await gzip.text()).toBe(js);
    const identity = await fetch(base + '/assets/app.js', { headers: { 'accept-encoding': 'identity' } });
    expect(identity.headers.get('content-encoding')).toBeNull();
    expect(await identity.text()).toBe(js);
    expect((await fetch(base + '/assets/app.js.br')).status).toBe(404);
  } finally { const exited = once(child, 'exit'); child.kill('SIGTERM'); await exited; await fs.rm(root, { recursive: true, force: true }); }
}, 30000);
