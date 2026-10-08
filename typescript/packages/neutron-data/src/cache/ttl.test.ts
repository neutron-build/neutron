import assert from "node:assert/strict";
import test from "node:test";
import { MemoryCacheClient } from "./index.js";
import { NucleusCacheClient, type NucleusKVLike } from "./nucleus.js";
import { RedisCacheClient } from "./redis.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// The CacheClient contract: `incr(key, ttlSec)` anchors the TTL at key
// creation (Redis INCR + EXPIRE-on-first semantics). Re-arming the TTL on
// every increment makes the three backends disagree — a key incremented
// repeatedly never expires on memory/nucleus while its Redis twin does.

test("MemoryCacheClient.incr anchors TTL at creation, not last increment", async () => {
  const client = new MemoryCacheClient();

  await client.incr("rl:anchored", 1);
  await sleep(600);
  const second = await client.incr("rl:anchored", 1);
  assert.equal(second, 2);

  // 1.2s after the FIRST increment — past the original 1s TTL. A
  // reset-on-every-increment implementation still holds the key here.
  await sleep(650);
  const value = await client.get("rl:anchored");
  assert.equal(value, null);
});

test("NucleusCacheClient refuses non-atomic TTL increments before mutation", async () => {
  let mutations = 0;
  const kv: NucleusKVLike = {
    get: async () => null, set: async () => {}, delete: async () => false,
    incr: async () => ++mutations, incrChecked: async () => ++mutations, expire: async () => true,
  };
  const client = new NucleusCacheClient({ kv });
  await assert.rejects(() => client.incr("bucket", 30), /atomic increment-with-expiry/);
  assert.equal(mutations, 0);
  assert.equal(await client.incr("bucket"), 1);
});

test("NucleusCacheClient delegates TTL to the atomic primitive", async () => {
  const calls: unknown[][] = [];
  const kv: NucleusKVLike = {
    get: async () => null, set: async () => {}, delete: async () => false,
    incr: async () => { throw new Error("split operation forbidden"); },
    expire: async () => { throw new Error("split operation forbidden"); },
    incrWithExpiry: async (...args) => { calls.push(args); return calls.length; },
  };
  const client = new NucleusCacheClient({ kv });
  assert.equal(await client.incr("bucket", 30), 1);
  assert.equal(await client.incr("bucket", 30), 2);
  assert.deepEqual(calls, [["cache:bucket", 30], ["cache:bucket", 30]]);
});

test("RedisCacheClient TTL increment uses one atomic command and rejects fractional TTL", async () => {
  const calls: unknown[][] = [];
  const redis = {
    eval: async (...args: unknown[]) => { calls.push(args); return calls.length; },
    incr: async () => { throw new Error("split operation forbidden"); },
    expire: async () => { throw new Error("split operation forbidden"); },
  };
  const client = new RedisCacheClient(redis as any, "p:");
  assert.equal(await client.incr("bucket", 30), 1);
  assert.equal(await client.incr("bucket", 30), 2);
  assert.deepEqual(calls[0].slice(1), [1, "p:bucket", 30]);
  assert.match(String(calls[0][0]), /TTL.*== -1.*EXPIRE/);
  await assert.rejects(() => client.incr("bucket", 0.5), /integer/);
  assert.equal(calls.length, 2);
});

test("MemoryCacheClient repairs an existing nonexpiring counter without extending later expiry", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const cache = new MemoryCacheClient();
  await cache.set("legacy", "4");
  assert.equal(await cache.incr("legacy", 2), 5);
  t.mock.timers.setTime(2000);
  assert.equal(await cache.incr("legacy", 20), 6);
  t.mock.timers.setTime(3001);
  assert.equal(await cache.get("legacy"), null);
});

test("counter TTL validation rejects every supplied invalid value before backend dispatch", async () => {
  let dispatches = 0;
  const kv: NucleusKVLike = { get: async () => null, set: async () => {}, delete: async () => false,
    incr: async () => ++dispatches, expire: async () => true,
    incrChecked: async () => ++dispatches, incrWithExpiry: async () => ++dispatches };
  const clients = [new MemoryCacheClient(), new RedisCacheClient({ eval: async () => ++dispatches } as any, ""), new NucleusCacheClient({ kv })];
  for (const client of clients) {
    for (const ttl of [0, -1, NaN, Infinity, -Infinity, 0.5, 2147483648]) {
      const before = dispatches;
      await assert.rejects(() => client.incr("invalid", ttl), /counter TTL/);
      assert.equal(dispatches, before);
    }
  }
  assert.equal(await clients[0].get("invalid"), null);
});

test("memory malformed and unsafe counters refuse unchanged before expiry repair", async () => {
  const cache = new MemoryCacheClient();
  for (const raw of ["wat", "1.5", "1junk", "01", "+1", "-0", "", "9007199254740991", "-9007199254740992"]) {
    await cache.set("value", raw);
    await assert.rejects(() => cache.incr("value", 1), /counter value/);
    assert.equal(await cache.get("value"), raw);
  }
  await cache.set("value", "-9007199254740991");
  assert.equal(await cache.incr("value"), -9007199254740990);
});

test("Nucleus unchecked increments refuse before calling the native mutator", async () => {
  let mutations = 0;
  const kv: NucleusKVLike = { get: async () => "malformed", set: async () => {}, delete: async () => false,
    incr: async () => ++mutations, expire: async () => true };
  await assert.rejects(() => Promise.resolve().then(() => new NucleusCacheClient({ kv, counterMode: "strict" }).incr("bad")), /atomic checked increment/);
  assert.equal(mutations, 0);
});
