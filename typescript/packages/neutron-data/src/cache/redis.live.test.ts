import assert from "node:assert/strict";
import test from "node:test";
import Redis from "ioredis";
import { RedisCacheClient } from "./redis.js";

test("TSD-12 live Redis: concurrent counters anchor TTL and repair nonexpiring keys", async (t) => {
  const url = process.env.NEUTRON_TEST_REDIS_URL;
  if (!url) return t.skip("NEUTRON_TEST_REDIS_URL is not set");
  const redis = new Redis(url, { maxRetriesPerRequest: 1 });
  const prefix = `tsd12:${process.pid}:`;
  const client = new RedisCacheClient(redis as unknown as ConstructorParameters<typeof RedisCacheClient>[0], prefix);
  try {
    const values = await Promise.all(Array.from({ length: 40 }, () => client.incr("counter", 3)));
    assert.equal(Math.max(...values), 40);
    assert.equal(new Set(values).size, 40);
    const initial = await redis.pttl(`${prefix}counter`);
    assert.ok(initial > 0 && initial <= 3000);
    await new Promise((r) => setTimeout(r, 100));
    await client.incr("counter", 100);
    assert.ok((await redis.pttl(`${prefix}counter`)) < initial);
    await redis.set(`${prefix}legacy`, "4");
    assert.equal(await client.incr("legacy", 3), 5);
    assert.ok((await redis.ttl(`${prefix}legacy`)) > 0);
    for (const raw of ["not an integer", "1.5", "1junk", "01", "+1", "-0", "", "9007199254740991", "-9007199254740992"]) {
      await redis.set(`${prefix}bad`, raw);
      for (const ttl of [undefined, 3]) {
        await assert.rejects(() => client.incr("bad", ttl));
        assert.equal(await redis.get(`${prefix}bad`), raw);
        assert.equal(await redis.ttl(`${prefix}bad`), -1);
      }
    }
    for (const ttl of [0, -1, NaN, Infinity, 0.5, 2147483648]) {
      await assert.rejects(() => client.incr("bad", ttl), /counter TTL/);
      assert.equal(await redis.get(`${prefix}bad`), "-9007199254740992");
    }
  } finally {
    await redis.del(`${prefix}counter`, `${prefix}legacy`, `${prefix}bad`);
    await client.close();
  }
});
