import { once } from 'node:events';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { expect, it } from 'vitest';
import { decodeSerializedPayload } from '../core/serialization.js';
import { createMemoryLoaderCacheStore, type NeutronLoaderDataCacheEntry, type NeutronLoaderCacheStore } from './cache-store.js';
import { createServer } from './index.js';

it('never publishes a delayed external loader fill after a completed mutation', { timeout: 30_000 }, async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-neutron-atomic-cache-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  await fs.writeFile(path.join(root, 'src/routes/item.ts'), `
    let version = 0;
    export const config = { mode: 'app', cache: { loaderMaxAge: 120 } };
    export async function loader() { return { version }; }
    export async function action() { version++; return { ok: true }; }
    export default function Page() { return null; }
  `);
  let entered!: () => void;
  let release!: () => void;
  const waiting = new Promise<void>(resolve => { entered = resolve; });
  const released = new Promise<void>(resolve => { release = resolve; });
  let first = true;
  const pause = async () => { if (first) { first = false; entered(); await released; } };
  const backing = createMemoryLoaderCacheStore();
  const store: NeutronLoaderCacheStore = {
    ...backing,
    async set(key, entry) { await pause(); await backing.set(key, entry); },
  };
  // The fixed store contract performs the generation comparison at the remote
  // commit point, after the simulated network delay rather than before it.
  if ('setIfGeneration' in backing) {
    Object.assign(store, {
      async setIfGeneration(key: string, entry: NeutronLoaderDataCacheEntry, generation: string) {
        await pause();
        return backing.setIfGeneration!(key, entry, generation);
      },
    });
  }
  const running = await createServer({ rootDir: root, host: '127.0.0.1', port: 0,
    compress: false, cache: { loader: store } });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address();
    if (!address || typeof address === 'string') throw new Error('No HTTP port');
    const url = `http://127.0.0.1:${address.port}/item`;
    const staleRead = fetch(url, { headers: { Accept: 'application/json' } });
    await waiting;
    const mutation = await fetch(url, { method: 'POST', headers: { Accept: 'application/json' } });
    expect(mutation.status).toBe(200);
    release();
    expect((await staleRead).status).toBe(200);
    const response = await fetch(url, { headers: { Accept: 'application/json' } });
    const payload = decodeSerializedPayload<Record<string, { version: number }>>(await response.json());
    expect(Object.values(payload)[0].version).toBe(1);
  } finally {
    release();
    await running.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

it('rejects an external cache without atomic publication before opening resources', async () => {
  const legacy: NeutronLoaderCacheStore = {
    async get() { return null; }, async set() {}, async deleteByPath() {}, async clear() {},
  };
  await expect(createServer({ mode: 'raw', port: 0, cache: { loader: legacy } }))
    .rejects.toThrow('atomic getGeneration/setIfGeneration');
});


it('evicts a GET fill published during an ordinary action when the mutation completes', { timeout: 30_000 }, async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-neutron-action-cache-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  let started!: () => void;
  let release!: () => void;
  const waiting = new Promise<void>(resolve => { started = resolve; });
  const released = new Promise<void>(resolve => { release = resolve; });
  const key = `neutron-review-action-${root}`;
  (globalThis as any)[key] = { started, released };
  await fs.writeFile(path.join(root, 'src/routes/item.ts'), `
    let version = 0;
    export const config = { mode: 'app', cache: { loaderMaxAge: 120, maxAge: 120 } };
    export async function loader() { return { version }; }
    export async function action() {
      const control = globalThis[${JSON.stringify(key)}];
      control.started(); await control.released;
      version++;
      return { ok: true };
    }
    export default function Page() { return null; }
  `);
  const running = await createServer({ rootDir: root, host: '127.0.0.1', port: 0, compress: false });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address();
    if (!address || typeof address === 'string') throw new Error('No HTTP port');
    const url = `http://127.0.0.1:${address.port}/item`;
    const read = async () => {
      const response = await fetch(url, { headers: { Accept: 'application/json' } });
      const payload = decodeSerializedPayload<Record<string, { version: number }>>(await response.json());
      return Object.values(payload)[0].version;
    };
    const mutation = fetch(url, { method: 'POST', headers: { Accept: 'application/json' } });
    await waiting;
    expect(await read()).toBe(0);
    release(); expect((await mutation).status).toBe(200);
    expect(await read()).toBe(1);
  } finally {
    release(); delete (globalThis as any)[key];
    await running.close(); await fs.rm(root, { recursive: true, force: true });
  }
});


it('fences a delayed fill begun during another server’s action at the shared backing store', { timeout: 30_000 }, async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-neutron-cross-server-cache-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  let actionStarted!: () => void, releaseAction!: () => void;
  let fillStarted!: () => void, releaseFill!: () => void;
  const actionWaiting = new Promise<void>(resolve => { actionStarted = resolve; });
  const actionReleased = new Promise<void>(resolve => { releaseAction = resolve; });
  const fillWaiting = new Promise<void>(resolve => { fillStarted = resolve; });
  const fillReleased = new Promise<void>(resolve => { releaseFill = resolve; });
  const key = `neutron-cross-server-action-${root}`;
  (globalThis as any)[key] = { version: 0, actionStarted, actionReleased };
  await fs.writeFile(path.join(root, 'src/routes/item.ts'), `
    export const config = { mode: 'app', cache: { loaderMaxAge: 120 } };
    export async function loader() { return { version: globalThis[${JSON.stringify(key)}].version }; }
    export async function action() {
      const control = globalThis[${JSON.stringify(key)}];
      control.actionStarted(); await control.actionReleased;
      control.version++; return { ok: true };
    }
    export default function Page() { return null; }
  `);
  const backing = createMemoryLoaderCacheStore();
  let firstFill = true;
  const store: NeutronLoaderCacheStore = {
    ...backing,
    async setIfGeneration(key, entry, generation) {
      if (firstFill) { firstFill = false; fillStarted(); await fillReleased; }
      return backing.setIfGeneration!(key, entry, generation);
    },
  };
  const servers = [] as Awaited<ReturnType<typeof createServer>>[];
  try {
    for (let index = 0; index < 2; index++) servers.push(await createServer({ rootDir: root,
      host: '127.0.0.1', port: 0, compress: false, cache: { loader: store } }));
    const urls = await Promise.all(servers.map(async running => {
      if (!running.server.listening) await once(running.server, 'listening');
      const address = running.server.address();
      if (!address || typeof address === 'string') throw new Error('No HTTP port');
      return `http://127.0.0.1:${address.port}/item`;
    }));
    const mutation = fetch(urls[1], { method: 'POST', headers: { Accept: 'application/json' } });
    await actionWaiting;
    const staleRead = fetch(urls[0], { headers: { Accept: 'application/json' } });
    await fillWaiting;
    releaseAction(); expect((await mutation).status).toBe(200);
    releaseFill(); expect((await staleRead).status).toBe(200);
    const response = await fetch(urls[0], { headers: { Accept: 'application/json' } });
    const payload = decodeSerializedPayload<Record<string, { version: number }>>(await response.json());
    expect(Object.values(payload)[0].version).toBe(1);
  } finally {
    releaseAction(); releaseFill(); delete (globalThis as any)[key];
    await Promise.all(servers.map(running => running.close()));
    await fs.rm(root, { recursive: true, force: true });
  }
});
