import { afterEach, expect, it, vi } from 'vitest';
import { createMemoryAppCacheStore, createMemoryLoaderCacheStore } from './cache-store.js';
afterEach(() => vi.useRealTimers());

it('quarantines a hung provider, returns in finite time and prevents unbounded detached publications', async () => {
  vi.resetModules(); vi.useFakeTimers();
  const { publishCacheEntry, CACHE_MAX_OPERATIONS, CACHE_OPERATION_TIMEOUT_MS, cacheOperation } = await import('./cache-publication.js');
  const store = { publicationDeadline: true as const, setIfGeneration: vi.fn(() => new Promise<boolean>(() => {})) };
  const pending = Array.from({ length: CACHE_MAX_OPERATIONS }, (_, n) => publishCacheEntry(store, String(n), {}, 'g'));
  await Promise.resolve(); await Promise.resolve();
  expect(store.setIfGeneration).toHaveBeenCalledTimes(CACHE_MAX_OPERATIONS);
  expect(await publishCacheEntry(store, 'overflow', {}, 'g')).toBe(false);
  await vi.advanceTimersByTimeAsync(CACHE_OPERATION_TIMEOUT_MS);
  expect(await Promise.all(pending)).toEqual(Array(CACHE_MAX_OPERATIONS).fill(false));
  const read = vi.fn(async () => 'late');
  expect(await cacheOperation(read, null, store)).toBeNull();
  expect(read).not.toHaveBeenCalled();
  for (let n = 0; n < 32; n++) expect(await publishCacheEntry(store, 'again', {}, 'g')).toBe(false);
  expect(store.setIfGeneration).toHaveBeenCalledTimes(CACHE_MAX_OPERATIONS);
});
it('unsupported providers are never invoked: racing a promise cannot cancel a write', async () => {
  vi.resetModules();
  const { publishCacheEntry } = await import('./cache-publication.js');
  const setIfGeneration = vi.fn(async () => true);
  expect(await publishCacheEntry({ setIfGeneration }, 'key', {}, 'g')).toBe(false);
  expect(setIfGeneration).not.toHaveBeenCalled();
});
it('a delayed compliant atomic provider refuses publication after the caller deadline', async () => {
  vi.resetModules(); vi.useFakeTimers();
  const { publishCacheEntry, CACHE_OPERATION_TIMEOUT_MS } = await import('./cache-publication.js');
  const backing = createMemoryAppCacheStore();
  let release!: () => void;
  const barrier = new Promise<void>(resolve => { release = resolve; });
  const store = { ...backing, async setIfGeneration(...args: Parameters<NonNullable<typeof backing.setIfGeneration>>) { await barrier; return backing.setIfGeneration!(...args); } };
  const entry = { status: 200, statusText: '', headers: [] as [string, string][], body: new Uint8Array([1]), expiresAt: Date.now() + 60000 };
  const pending = publishCacheEntry(store, 'key', entry, await backing.getGeneration!());
  await vi.advanceTimersByTimeAsync(CACHE_OPERATION_TIMEOUT_MS);
  expect(await pending).toBe(false);
  release(); await Promise.resolve(); await Promise.resolve();
  expect(await backing.get('key')).toBeNull();
});

it('the memory loader store checks deadline at publication after cloning owned data', async () => {
  vi.useFakeTimers();
  const store = createMemoryLoaderCacheStore(), controller = new AbortController();
  const start = Date.now(), generation = await store.getGeneration!();
  const data = { get value() { vi.setSystemTime(start + 2001); return 'expired'; } };
  expect(await store.setIfGeneration!('key', { data, expiresAt: start + 60000 }, generation, { signal: controller.signal, deadline: start + 2000 })).toBe(false);
  expect(await store.get('key')).toBeNull();
});
