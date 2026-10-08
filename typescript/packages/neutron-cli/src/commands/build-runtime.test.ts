import { it } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { generateRuntimeEntrySource } from './build.js';
import type { Route } from '@neutron-build/core';

const route: Route = { id: 'r', path: '/percent%value', file: '', parentId: null, params: [], config: { mode: 'app', cache: { loaderMaxAge: 120 } } };
async function generated(module: string, global?: string) {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-final-c-generated-'));
  const entry = path.join(root, 'entry.mjs'); const file = path.join(root, 'route.mjs'); const gm = path.join(root, 'middleware.mjs');
  await fs.writeFile(file, module); if (global !== undefined) await fs.writeFile(gm, global);
  const definition = { id: 'r', path: route.path, file, parentId: null, params: [], mode: 'app' as const, cache: route.config.cache, isLayout: false };
  let code = generateRuntimeEntrySource([definition], [route], null, [], entry, undefined, global === undefined ? undefined : gm);
  code = code.replace('"@neutron-build/core/runtime-edge"', JSON.stringify(pathToFileURL(path.resolve('../neutron/dist/runtime/edge.js')).href));
  await fs.writeFile(entry, code);
  return { root, load: () => import(pathToFileURL(entry).href) };
}
for (const global of ['export default {}', 'export const middleware = []', 'export const middleware = [() => {}, 1]', 'export const unrelated = true']) it('TS-F02 generated boot rejects invalid global export: ' + global, async () => {
  const fixture = await generated('export async function loader() { return new Response("ok") }', global);
  try { await assert.rejects(fixture.load(), /Invalid global middleware/); } finally { await fs.rm(fixture.root, { recursive: true, force: true }); }
});
for (const global of ['export default async (r,c,next) => next()', 'export const middleware = [async (r,c,next) => next()]']) it('TS-F02/F09 actual generated handler admits valid globals and immutable redirects: ' + global, async () => {
  const fixture = await generated('export async function loader() { return Response.redirect("https://example.test/next", 307) }', global);
  try {
    const runtime = await fixture.load(); const response = await runtime.handleNeutronRequest(new Request('https://example.test/percent%25value'));
    assert.equal(response.status, 307); assert.equal(response.headers.get('Location'), 'https://example.test/next'); assert.equal(response.headers.get('X-Frame-Options'), 'DENY');
  } finally { await fs.rm(fixture.root, { recursive: true, force: true }); }
});
it('TS-F05 fences a GET fill started during mutation completion on a literal percent path', async () => {
  let entered!: () => void; let release!: () => void;
  const started = new Promise<void>(r => entered = r); const held = new Promise<void>(r => release = r);
  (globalThis as any).__finalCBarrier = { entered, held };
  const fixture = await generated(`
    let value = 0;
    export default function Page() { return null; }
    export async function loader() { return { value }; }
    export async function action() { globalThis.__finalCBarrier.entered(); await globalThis.__finalCBarrier.held; value++; return { ok: true }; }
  `);
  try {
    const runtime = await fixture.load(); const url = 'https://example.test/percent%25value';
    const read = async () => { const response = await runtime.handleNeutronRequest(new Request(url, { headers: { Accept: 'application/json' } })); return response.json(); };
    const mutation = runtime.handleNeutronRequest(new Request(url, { method: 'POST', headers: { Accept: 'application/json' } }));
    await started; await read(); release(); await mutation;
    const { decodeSerializedPayload } = await import('@neutron-build/core');
    assert.equal(decodeSerializedPayload<any>(await read()).r.value, 1);
  } finally { release(); delete (globalThis as any).__finalCBarrier; await fs.rm(fixture.root, { recursive: true, force: true }); }
});
it('TS-F05 failed actions also fence fills started during mutation', async () => {
  let entered!: () => void; let release!: () => void;
  const started = new Promise<void>(r => entered = r); const held = new Promise<void>(r => release = r);
  (globalThis as any).__finalCBarrier = { entered, held };
  const fixture = await generated(`
    let value = 0;
    export default function Page() { return null; }
    export async function loader() { return { value }; }
    export async function action() { globalThis.__finalCBarrier.entered(); await globalThis.__finalCBarrier.held; value++; throw new Error('action failed'); }
  `);
  try {
    const runtime = await fixture.load(); const url = 'https://example.test/percent%25value';
    const read = async () => { const response = await runtime.handleNeutronRequest(new Request(url, { headers: { Accept: 'application/json' } })); return response.json(); };
    const mutation = runtime.handleNeutronRequest(new Request(url, { method: 'POST', headers: { Accept: 'application/json' } }));
    await started; await read(); release(); await mutation;
    const { decodeSerializedPayload } = await import('@neutron-build/core');
    assert.equal(decodeSerializedPayload<any>(await read()).r.value, 1);
  } finally { release(); delete (globalThis as any).__finalCBarrier; await fs.rm(fixture.root, { recursive: true, force: true }); }
});

it('packed-consumer contract makes Core and CLI share the application Preact peer', async () => {
  for (const dir of ['../neutron', '.']) {
    const manifest = JSON.parse(await fs.readFile(path.resolve(dir, 'package.json'), 'utf8'));
    assert.equal(manifest.dependencies.preact, undefined);
    assert.equal(manifest.peerDependencies.preact, '^10.25.4');
    assert.equal(manifest.devDependencies.preact, '^10.25.4');
    assert.notEqual(manifest.peerDependenciesMeta?.preact?.optional, true);
  }
});

for (const kind of ['route', 'layout', 'global', 'global-resource']) it('TS-F01 real CLI refuses static publication behind ' + kind + ' middleware', { timeout: 30000 }, async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-static-gate-cli-'));
  const routes = path.join(root, 'src/routes'); await fs.mkdir(routes, { recursive: true });
  await fs.writeFile(path.join(root, 'package.json'), JSON.stringify({ name: 'static-gate-fixture', private: true, type: 'module' }));
  const gate = 'export async function middleware() { return new Response("denied", { status: 403 }); }';
  const name = kind === 'global-resource' ? 'secret.txt.ts' : 'index.tsx';
  await fs.writeFile(path.join(routes, name), 'export const config = { mode: "static" }; export async function loader() { return ' + (kind === 'global-resource' ? 'new Response("PRIVATE-STATIC-MARKER")' : '{ secret: "PRIVATE-STATIC-MARKER" }') + '; } ' + (kind === 'global-resource' ? '' : 'export default function Page() { return null; }') + (kind === 'route' ? gate : ''));
  if (kind === 'layout') await fs.writeFile(path.join(routes, '_layout.tsx'), 'export default function Layout({ children }) { return children; } ' + gate);
  if (kind.startsWith('global')) await fs.writeFile(path.join(root, 'src/middleware.ts'), 'export default async function middleware() { return new Response("denied", { status: 403 }); }');
  const { execFile } = await import('node:child_process'); const { promisify } = await import('node:util');
  try {
    await assert.rejects(promisify(execFile)(process.execPath, [path.resolve('bin/neutron-ts.mjs'), 'build', '--preset', 'static'], { cwd: root }), error => {
      const failure = error as Error & { stdout: string; stderr: string };
      assert.match(failure.stdout + failure.stderr, /Cannot prerender/); return true;
    });
    await assert.rejects(fs.access(path.join(root, 'dist', kind === 'global-resource' ? 'secret.txt' : 'index.html')));
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

it('TS-F03 real CLI Docker build keeps its actual server bundle outside the public allowlist', { timeout: 60000 }, async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-final-c-docker-build-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  await fs.mkdir(path.join(root, 'public'));
  await fs.writeFile(path.join(root, 'package.json'), JSON.stringify({ name: 'docker-build-fixture', private: true, type: 'module' }));
  await fs.writeFile(path.join(root, 'public/asset.txt'), 'PUBLIC-BROWSER-ASSET');
  await fs.writeFile(path.join(root, 'index.html'), '<html><body><div id="app"></div><script type="module" src="/src/main.ts"></script></body></html>');
  await fs.writeFile(path.join(root, 'src/main.ts'), 'console.log("public-client-entry");');
  await fs.writeFile(path.join(root, 'src/routes/index.tsx'), 'export const config = { mode: "static" }; export default function Page() { return "PUBLIC-PAGE"; }');
  await fs.writeFile(path.join(root, 'src/routes/runtime.ts'), 'export const config = { mode: "app" }; export async function loader() { return new Response("ACTUAL-SERVER-ONLY-MARKER"); }');
  const { execFile, spawn } = await import('node:child_process');
  const { promisify } = await import('node:util');
  let child: ReturnType<typeof spawn> | undefined;
  try {
    await promisify(execFile)(process.execPath, [path.resolve('bin/neutron-ts.mjs'), 'build', '--preset', 'docker'], { cwd: root, timeout: 45000 });
    const entry = await fs.readFile(path.join(root, 'dist/server/node/entry.js'), 'utf8');
    assert.match(entry, /ACTUAL-SERVER-ONLY-MARKER/);
    child = spawn(process.execPath, [path.join(root, 'dist/server.mjs')], { env: { ...process.env, PORT: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    const base = await new Promise<string>((resolve, reject) => {
      child!.stdout!.on('data', chunk => { output += chunk; const port = output.match(/listening on http:\/\/0\.0\.0\.0:(\d+)/); if (port) resolve('http://127.0.0.1:' + port[1]); });
      child!.once('exit', code => reject(new Error('Docker process exited ' + code))); child!.once('error', reject);
    });
    assert.match(await (await fetch(base + '/')).text(), /PUBLIC-PAGE/);
    assert.equal(await (await fetch(base + '/asset.txt')).text(), 'PUBLIC-BROWSER-ASSET');
    assert.equal(await (await fetch(base + '/runtime')).text(), 'ACTUAL-SERVER-ONLY-MARKER');
    for (const url of ['/server/node/entry.js', '/server.mjs', '/Dockerfile']) {
      const response = await fetch(base + url); assert.equal(response.status, 404); assert.doesNotMatch(await response.text(), /ACTUAL-SERVER-ONLY-MARKER/);
    }
  } finally {
    if (child && child.exitCode === null) { const exited = new Promise<void>(resolve => child!.once('exit', () => resolve())); child.kill('SIGTERM'); await exited; }
    await fs.rm(root, { recursive: true, force: true });
  }
});
