import { once } from 'node:events';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { expect, it } from 'vitest';
import { createServer } from './index.js';

it('TS-F04/06/07 final privacy, revalidation directives and full Accept values remain isolated', async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-final-c-cache-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  await fs.writeFile(path.join(root, 'src/routes/value.ts'), `
    export const config = { mode: 'app', cache: { maxAge: 120 } };
    let calls = 0;
    export async function loader({ request }) { return new Response(request.headers.get('Accept') + ':' + (++calls), { headers: { Vary: 'Accept' } }); }
  `);
  await fs.writeFile(path.join(root, 'src/routes/private.ts'), `
    export const config = { mode: 'app', cache: { maxAge: 120 } };
    let calls = 0;
    export async function loader() { return new Response(String(++calls)); }
    export async function middleware(request, context, next) { const response = await next(); response.headers.set('Cache-Control', 'private'); return response; }
  `);
  const running = await createServer({ rootDir: root, port: 0, host: '127.0.0.1', compress: false });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address(); if (!address || typeof address === 'string') throw new Error('no port');
    const base = `http://127.0.0.1:${address.port}`;
    const read = async (accept: string, control?: string, method = 'GET') => {
      const response = await fetch(base + '/value', { method, headers: { Accept: accept, ...(control ? { 'Cache-Control': control } : {}) } });
      return { text: await response.text(), cache: response.headers.get('x-neutron-cache') };
    };
    expect((await read('text/csv')).text).toBe('text/csv:1');
    expect((await read('text/csv')).cache).toBe('HIT');
    expect((await read('application/xml')).text).toBe('application/xml:2');
    expect((await read('text/csv', 'No-Cache')).text).toBe('text/csv:3');
    expect((await read('text/csv', 'NO-STORE')).text).toBe('text/csv:4');
    expect((await read('text/csv', 'No-Cache', 'HEAD')).cache).not.toBe('HIT');
    expect(await (await fetch(base + '/private')).text()).toBe('1');
    expect(await (await fetch(base + '/private')).text()).toBe('2');
  } finally { await running.close(); await fs.rm(root, { recursive: true, force: true }); }
}, 30000);

it('O2 a hung remote publication bounds tracker admission and later reads bypass in finite time', async () => {
  const { createMemoryAppCacheStore } = await import('./cache-store.js');
  const { CACHE_MAX_OPERATIONS, CACHE_OPERATION_TIMEOUT_MS } = await import('./cache-publication.js');
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-final-c-hung-cache-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  await fs.writeFile(path.join(root, 'src/routes/[id].ts'), `
    export const config = { mode:'app', cache:{maxAge:120} };
    export async function loader() { return new Response('fresh'); }
  `);
  const backing = createMemoryAppCacheStore();
  let publications = 0, reads = 0;
  // Declares the deadline contract but never completes; no write is performed.
  // The backend deadline violation is contained without claiming cancellation.
  const store = { ...backing,
    async get(key: string) { reads++; return backing.get(key); },
    setIfGeneration() { publications++; return new Promise<boolean>(() => {}); },
  };
  const running = await createServer({ rootDir: root, port: 0, host: '127.0.0.1', compress: false, cache: { app: store } });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address(); if (!address || typeof address === 'string') throw new Error('no port');
    const base = `http://127.0.0.1:${address.port}`;
    for (let n = 0; n < CACHE_MAX_OPERATIONS * 4; n++) expect(await (await fetch(`${base}/${n}`)).text()).toBe('fresh');
    expect(publications).toBeGreaterThan(0); expect(publications).toBeLessThanOrEqual(CACHE_MAX_OPERATIONS);
    const readCount = reads, publicationCount = publications, start = Date.now();
    const response = await fetch(base + '/0'); expect(await response.text()).toBe('fresh');
    expect(response.headers.get('x-neutron-cache')).not.toBe('HIT');
    expect(Date.now() - start).toBeLessThan(CACHE_OPERATION_TIMEOUT_MS + 1000);
    // Give bounded capture+publication trackers their maximum lifetime. The
    // dead provider remains quarantined; finite waits are not a recovery claim.
    await new Promise(resolve => setTimeout(resolve, CACHE_OPERATION_TIMEOUT_MS * 2 + 100));
    expect(await (await fetch(base + '/0')).text()).toBe('fresh');
    expect(publications).toBe(publicationCount); expect(reads).toBe(readCount);
  } finally { await running.close(); await fs.rm(root, { recursive: true, force: true }); }
}, 30000);
