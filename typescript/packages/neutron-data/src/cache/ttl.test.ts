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

test("NucleusCacheClient.incr sets expire only on the creating increment", async () => {
  const calls: Array<{ op: string; key?: string; ttlSec?: number }> = [];
  let counter = 0;
  const kv: NucleusKVLike = {
    get: async () => null,
    set: async () => {},
    delete: async () => false,
    incr: async (key: string) => {
      calls.push({ op: "incr", key });
      counter += 1;
      return counter;
    },
    expire: async (key: string, seconds: number) => {
      calls.push({ op: "expire", key, ttlSec: seconds });
      return true;
    },
  };
  const client = new NucleusCacheClient({ kv });

  await client.incr("rl:nucleus", 30);
  await client.incr("rl:nucleus", 30);
  await client.incr("rl:nucleus", 30);

  const expires = calls.filter((call) => call.op === "expire");
  assert.equal(expires.length, 1);
  assert.equal(expires[0]?.ttlSec, 30);
});

test("RedisCacheClient.incr sets expire only on the first increment", async () => {
  const expireCalls: string[] = [];
  let counter = 0;
  const redis = {
    get: async () => null,
    set: async () => {},
    del: async () => {},
    incr: async () => {
      counter += 1;
      return counter;
    },
    expire: async (key: string) => {
      expireCalls.push(key);
    },
    quit: async () => {},
  };
  const client = new RedisCacheClient(redis as any, "");

  await client.incr("rl:redis", 30);
  await client.incr("rl:redis", 30);
  await client.incr("rl:redis", 30);

  assert.equal(expireCalls.length, 1);
  assert.equal(expireCalls[0], "rl:redis");
});
