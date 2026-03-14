import assert from "node:assert/strict";
import test from "node:test";
import { RedisCacheClient } from "./redis.js";

// Mock Redis client
class MockRedisClient {
  private data: Map<string, { value: string; expireAt?: number }> = new Map();

  async get(key: string): Promise<string | null> {
    const entry = this.data.get(key);
    if (!entry) return null;
    if (entry.expireAt && entry.expireAt < Date.now()) {
      this.data.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string, mode?: "EX", ttlSec?: number): Promise<void> {
    let expireAt: number | undefined;
    if (mode === "EX" && ttlSec) {
      expireAt = Date.now() + ttlSec * 1000;
    }
    this.data.set(key, { value, expireAt });
  }

  async del(key: string): Promise<void> {
    this.data.delete(key);
  }

  async incr(key: string): Promise<number> {
    const entry = this.data.get(key);
    const current = entry ? parseInt(entry.value, 10) : 0;
    const next = current + 1;
    this.data.set(key, { value: String(next), expireAt: entry?.expireAt });
    return next;
  }

  async expire(key: string, ttlSec: number): Promise<void> {
    const entry = this.data.get(key);
    if (entry) {
      entry.expireAt = Date.now() + ttlSec * 1000;
    }
  }

  async quit(): Promise<void> {
    this.data.clear();
  }
}

test("RedisCacheClient.get returns stored value", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.set("key1", "value1");
  const result = await client.get("key1");

  assert.equal(result, "value1");
});

test("RedisCacheClient.get returns null for missing key", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  const result = await client.get("nonexistent");

  assert.equal(result, null);
});

test("RedisCacheClient.set without TTL", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.set("key1", "value1");
  const result = await client.get("key1");

  assert.equal(result, "value1");
});

test("RedisCacheClient.set with TTL", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.set("key1", "value1", 1);
  let result = await client.get("key1");
  assert.equal(result, "value1");

  // Simulate expiration
  await new Promise((resolve) => setTimeout(resolve, 1100));
  result = await client.get("key1");
  assert.equal(result, null);
});

test("RedisCacheClient.del removes key", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.set("key1", "value1");
  await client.del("key1");
  const result = await client.get("key1");

  assert.equal(result, null);
});

test("RedisCacheClient.incr increments counter", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  const first = await client.incr("counter");
  const second = await client.incr("counter");
  const third = await client.incr("counter");

  assert.equal(first, 1);
  assert.equal(second, 2);
  assert.equal(third, 3);
});

test("RedisCacheClient.incr respects TTL on first increment", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.incr("counter", 1);
  await new Promise((resolve) => setTimeout(resolve, 1100));
  const value = await client.get("counter");

  assert.equal(value, null);
});

test("RedisCacheClient prepends keyPrefix", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "app:");

  await client.set("key1", "value1");

  // Check that the prefixed key is used
  const directResult = await mockRedis.get("app:key1");
  assert.equal(directResult, "value1");
});

test("RedisCacheClient.close calls quit", async () => {
  const mockRedis = new MockRedisClient();
  const client = new RedisCacheClient(mockRedis as any, "");

  await client.set("key1", "value1");
  await client.close();
  const result = await client.get("key1");

  assert.equal(result, null);
});
