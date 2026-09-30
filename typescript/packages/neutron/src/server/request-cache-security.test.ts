import { once } from 'node:events';
import { AsyncLocalStorage } from 'node:async_hooks';
import { afterEach, expect, it } from 'vitest';
import { cache, clearCache } from '../core/cache.js';
import { createServer, type NeutronServer } from './index.js';

let running: NeutronServer | undefined;
afterEach(async () => { await running?.close(); running = undefined; clearCache(); });

it('isolates authenticated results between HTTP requests while deduplicating within each request', async () => {
  const principal = new AsyncLocalStorage<string>();
  let calls = 0;
  const currentUser = cache(async () => { calls++; await new Promise(resolve => setTimeout(resolve, 2)); return principal.getStore(); });
  running = await createServer({ mode: 'raw', host: '127.0.0.1', port: 0, compress: false });
  if (!running.server.listening) await once(running.server, 'listening');
  const address = running.server.address();
  if (!address || typeof address === 'string') throw new Error('No bound HTTP port');
  const base = `http://127.0.0.1:${address.port}`;
  running.app.get('/private-user', c => principal.run(c.req.header('x-principal')!, async () => {
    const first = currentUser();
    const second = currentUser();
    return c.json({ user: await first, deduplicated: first === second });
  }));
  const alice = await fetch(`${base}/private-user`, { headers: { 'x-principal': 'alice' } });
  const bob = await fetch(`${base}/private-user`, { headers: { 'x-principal': 'bob' } });
  expect(await alice.json()).toEqual({ user: 'alice', deduplicated: true });
  expect(await bob.json()).toEqual({ user: 'bob', deduplicated: true });
  const people = ['carol', 'dave', 'erin', 'frank'];
  const responses = await Promise.all(people.map(user => fetch(`${base}/private-user`, {
    headers: { 'x-principal': user },
  }).then(response => response.json())));
  expect(responses).toEqual(people.map(user => ({ user, deduplicated: true })));
  expect(calls).toBe(6);
});

it('does not implicitly retain private data outside a server request', async () => {
  let calls = 0;
  const privateRead = cache(async () => ++calls);
  expect(await privateRead()).toBe(1);
  expect(await privateRead()).toBe(2);
});

it('allows explicit bounded sharing for public data and explicit invalidation', async () => {
  let calls = 0;
  const publicRead = cache(async () => ++calls, { scope: 'shared' });
  expect(await publicRead()).toBe(1);
  expect(await publicRead()).toBe(1);
  clearCache('shared');
  expect(await publicRead()).toBe(2);
});
