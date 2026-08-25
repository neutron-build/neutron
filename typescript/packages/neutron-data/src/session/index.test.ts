import assert from "node:assert/strict";
import test from "node:test";
import { MemoryCacheClient } from "../cache/index.js";
import type { CacheClient } from "../cache/index.js";
import { createSessionStore } from "./index.js";

test("createSessionStore round-trips a session", async () => {
  const store = createSessionStore({ cache: new MemoryCacheClient() });

  const created = await store.create({ userId: 7, theme: "dark" });
  assert.equal(typeof created.id, "string");
  assert.deepEqual(created.data, { userId: 7, theme: "dark" });

  const loaded = await store.get(created.id);
  assert.deepEqual(loaded, { id: created.id, data: { userId: 7, theme: "dark" } });
});

test("createSessionStore.get returns null for unknown ids", async () => {
  const store = createSessionStore({ cache: new MemoryCacheClient() });
  assert.equal(await store.get("no-such-session"), null);
});

test("createSessionStore.set overwrites the stored data", async () => {
  const store = createSessionStore({ cache: new MemoryCacheClient() });
  const { id } = await store.create({ step: 1 });

  await store.set(id, { step: 2 });
  const loaded = await store.get<{ step: number }>(id);
  assert.equal(loaded?.data.step, 2);
});

test("createSessionStore.destroy removes the session", async () => {
  const store = createSessionStore({ cache: new MemoryCacheClient() });
  const { id } = await store.create({ a: 1 });

  await store.destroy(id);
  assert.equal(await store.get(id), null);
});

test("createSessionStore keys sessions with the prefix and the configured TTL", async () => {
  const sets: Array<{ key: string; ttlSec: number | undefined }> = [];
  const recording: CacheClient = {
    get: async () => null,
    set: async (key, value, ttlSec) => {
      sets.push({ key, ttlSec });
    },
    del: async () => {},
    incr: async () => 0,
  };

  const store = createSessionStore({ cache: recording, prefix: "sess:", ttlSec: 120 });
  const { id } = await store.create({});

  assert.equal(sets.length, 1);
  assert.equal(sets[0]?.key, `sess:${id}`);
  assert.equal(sets[0]?.ttlSec ?? 0, 120);
});
