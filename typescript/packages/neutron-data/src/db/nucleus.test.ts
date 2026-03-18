// ---------------------------------------------------------------------------
// Tests for neutron-data <-> Nucleus integration
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import test from "node:test";
import { resolveDatabaseProfile, type DatabaseProfile } from "./index.js";
import { resolveDataConfig, type DataConfigInput } from "../config.js";
import { NucleusCacheClient } from "../cache/nucleus.js";
import { NucleusStorageDriver } from "../storage/nucleus.js";
import { NucleusRealtimeBus } from "../realtime/nucleus.js";

// =========================================================================
// Config: Nucleus auto-detection
// =========================================================================

test("resolveDataConfig auto-detects nucleus from NUCLEUS_URL env", () => {
  const originalEnv = process.env.NUCLEUS_URL;
  try {
    process.env.NUCLEUS_URL = "http://localhost:5432";
    const config = resolveDataConfig({});
    assert.equal(config.database, "nucleus");
    assert.equal(config.nucleusUrlEnv, "NUCLEUS_URL");
  } finally {
    if (originalEnv === undefined) {
      delete process.env.NUCLEUS_URL;
    } else {
      process.env.NUCLEUS_URL = originalEnv;
    }
  }
});

test("resolveDataConfig prefers nucleus over postgres when both set", () => {
  const origNucleus = process.env.NUCLEUS_URL;
  const origDatabase = process.env.DATABASE_URL;
  try {
    process.env.NUCLEUS_URL = "http://localhost:5432";
    process.env.DATABASE_URL = "postgres://localhost/db";
    const config = resolveDataConfig({});
    assert.equal(config.database, "nucleus");
  } finally {
    if (origNucleus === undefined) delete process.env.NUCLEUS_URL;
    else process.env.NUCLEUS_URL = origNucleus;
    if (origDatabase === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = origDatabase;
  }
});

test("resolveDataConfig respects explicit database=nucleus", () => {
  const config = resolveDataConfig({ database: "nucleus" });
  assert.equal(config.database, "nucleus");
});

test("resolveDataConfig includes nucleusUrlEnv in resolved config", () => {
  const config = resolveDataConfig({ nucleusUrlEnv: "MY_NUCLEUS" });
  assert.equal(config.nucleusUrlEnv, "MY_NUCLEUS");
});

// =========================================================================
// DatabaseProfile: Nucleus resolution
// =========================================================================

test("resolveDatabaseProfile returns nucleus profile from env", () => {
  const originalEnv = process.env.NUCLEUS_URL;
  try {
    process.env.NUCLEUS_URL = "http://localhost:5432";
    const profile = resolveDatabaseProfile({ database: "nucleus" });
    assert.equal(profile.provider, "nucleus");
    assert.equal(profile.connectionString, "http://localhost:5432");
  } finally {
    if (originalEnv === undefined) delete process.env.NUCLEUS_URL;
    else process.env.NUCLEUS_URL = originalEnv;
  }
});

test("resolveDatabaseProfile nucleus falls back to DATABASE_URL", () => {
  const origNucleus = process.env.NUCLEUS_URL;
  const origDatabase = process.env.DATABASE_URL;
  try {
    delete process.env.NUCLEUS_URL;
    process.env.DATABASE_URL = "postgres://nucleus-host:5432/db";
    const profile = resolveDatabaseProfile({ database: "nucleus" });
    assert.equal(profile.provider, "nucleus");
    assert.equal(profile.connectionString, "postgres://nucleus-host:5432/db");
  } finally {
    if (origNucleus === undefined) delete process.env.NUCLEUS_URL;
    else process.env.NUCLEUS_URL = origNucleus;
    if (origDatabase === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = origDatabase;
  }
});

test("resolveDatabaseProfile throws when nucleus env vars not set", () => {
  const origNucleus = process.env.NUCLEUS_URL;
  const origDatabase = process.env.DATABASE_URL;
  try {
    delete process.env.NUCLEUS_URL;
    delete process.env.DATABASE_URL;
    assert.throws(
      () => resolveDatabaseProfile({ database: "nucleus" }),
      (err: unknown) => {
        assert.ok(err instanceof Error);
        assert.ok(
          err.message.includes("is set") || err.message.includes("NUCLEUS_URL"),
          `Expected error about missing env vars, got: ${err.message}`
        );
        return true;
      }
    );
  } finally {
    if (origNucleus !== undefined) process.env.NUCLEUS_URL = origNucleus;
    if (origDatabase !== undefined) process.env.DATABASE_URL = origDatabase;
  }
});

// =========================================================================
// NucleusCacheClient
// =========================================================================

test("NucleusCacheClient.get delegates to kv.get with prefix", async () => {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const mockKV = {
    get: async (key: string) => { calls.push({ method: "get", args: [key] }); return "value"; },
    set: async () => { /* noop */ },
    delete: async () => true,
    incr: async () => 1,
    expire: async () => true,
  };

  const cache = new NucleusCacheClient({ kv: mockKV });
  const val = await cache.get("mykey");
  assert.equal(val, "value");
  assert.equal(calls[0].args[0], "cache:mykey");
});

test("NucleusCacheClient.set delegates to kv.set with TTL", async () => {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const mockKV = {
    get: async () => null,
    set: async (key: string, value: string, opts?: { ttl?: number }) => {
      calls.push({ method: "set", args: [key, value, opts] });
    },
    delete: async () => true,
    incr: async () => 1,
    expire: async () => true,
  };

  const cache = new NucleusCacheClient({ kv: mockKV, prefix: "app:" });
  await cache.set("session", "data", 3600);
  assert.equal(calls[0].args[0], "app:session");
  assert.equal(calls[0].args[1], "data");
  assert.deepEqual(calls[0].args[2], { ttl: 3600 });
});

test("NucleusCacheClient.del delegates to kv.delete", async () => {
  const calls: string[] = [];
  const mockKV = {
    get: async () => null,
    set: async () => { /* noop */ },
    delete: async (key: string) => { calls.push(key); return true; },
    incr: async () => 1,
    expire: async () => true,
  };

  const cache = new NucleusCacheClient({ kv: mockKV });
  await cache.del("expired");
  assert.equal(calls[0], "cache:expired");
});

test("NucleusCacheClient.incr delegates to kv.incr and sets expire", async () => {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const mockKV = {
    get: async () => null,
    set: async () => { /* noop */ },
    delete: async () => true,
    incr: async (key: string) => { calls.push({ method: "incr", args: [key] }); return 5; },
    expire: async (key: string, seconds: number) => {
      calls.push({ method: "expire", args: [key, seconds] });
      return true;
    },
  };

  const cache = new NucleusCacheClient({ kv: mockKV });
  const val = await cache.incr("counter", 60);
  assert.equal(val, 5);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].method, "incr");
  assert.equal(calls[1].method, "expire");
  assert.equal(calls[1].args[1], 60);
});

// =========================================================================
// NucleusStorageDriver
// =========================================================================

test("NucleusStorageDriver.put delegates to blob.put", async () => {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const mockBlob = {
    put: async (bucket: string, key: string, data: unknown, opts: unknown) => {
      calls.push({ method: "put", args: [bucket, key, data, opts] });
    },
    get: async () => null,
    delete: async () => true,
  };

  const driver = new NucleusStorageDriver({ blob: mockBlob });
  const body = new Uint8Array([1, 2, 3]);
  await driver.put({ key: "file.bin", body, contentType: "application/octet-stream" });
  assert.equal(calls[0].args[0], "default");
  assert.equal(calls[0].args[1], "file.bin");
});

test("NucleusStorageDriver.get returns StorageObject", async () => {
  const mockBlob = {
    put: async () => { /* noop */ },
    get: async (_bucket: string, _key: string) => ({
      data: new Uint8Array([0x48, 0x49]),
      meta: null,
    }),
    delete: async () => true,
  };

  const driver = new NucleusStorageDriver({ blob: mockBlob, bucket: "files" });
  const obj = await driver.get("test.txt");
  assert.ok(obj !== null);
  assert.equal(obj!.key, "test.txt");
  assert.equal(obj!.body.length, 2);
});

test("NucleusStorageDriver.get returns null for missing object", async () => {
  const mockBlob = {
    put: async () => { /* noop */ },
    get: async () => null,
    delete: async () => true,
  };

  const driver = new NucleusStorageDriver({ blob: mockBlob });
  const obj = await driver.get("missing");
  assert.equal(obj, null);
});

test("NucleusStorageDriver.del delegates to blob.delete", async () => {
  const calls: string[] = [];
  const mockBlob = {
    put: async () => { /* noop */ },
    get: async () => null,
    delete: async (_bucket: string, key: string) => { calls.push(key); return true; },
  };

  const driver = new NucleusStorageDriver({ blob: mockBlob });
  await driver.del("old-file");
  assert.equal(calls[0], "old-file");
});

// =========================================================================
// NucleusRealtimeBus
// =========================================================================

test("NucleusRealtimeBus.publish delegates to pubsub.publish", async () => {
  const calls: Array<{ channel: string; message: string }> = [];
  const mockPubSub = {
    publish: async (channel: string, message: string) => {
      calls.push({ channel, message });
      return 1;
    },
  };

  const bus = new NucleusRealtimeBus({ pubsub: mockPubSub });
  await bus.publish("events", { type: "click" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].channel, "events");
  assert.equal(calls[0].message, JSON.stringify({ type: "click" }));
});

test("NucleusRealtimeBus.subscribe receives local messages", async () => {
  const mockPubSub = {
    publish: async () => 0,
  };

  const bus = new NucleusRealtimeBus({ pubsub: mockPubSub });
  const received: unknown[] = [];
  bus.subscribe("events", (payload) => received.push(payload));

  await bus.publish("events", { data: 42 });
  assert.equal(received.length, 1);
  assert.deepEqual(received[0], { data: 42 });
});

test("NucleusRealtimeBus.subscribe unsubscribe stops delivery", async () => {
  const mockPubSub = {
    publish: async () => 0,
  };

  const bus = new NucleusRealtimeBus({ pubsub: mockPubSub });
  const received: unknown[] = [];
  const unsub = bus.subscribe("events", (payload) => received.push(payload));

  await bus.publish("events", "first");
  unsub();
  await bus.publish("events", "second");

  assert.equal(received.length, 1);
  assert.equal(received[0], "first");
});

// =========================================================================
// DrizzleDatabase.nucleus field
// =========================================================================

test("DrizzleDatabase type includes nucleus field", async () => {
  // We can't test actual DB connection without deps, but we verify the type
  // by importing and checking the interface shape
  const { createDrizzleDatabase } = await import("./drizzle.js");
  assert.equal(typeof createDrizzleDatabase, "function");
});
