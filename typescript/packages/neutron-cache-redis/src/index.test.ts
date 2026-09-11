import assert from "node:assert/strict";
import test from "node:test";
import {
  createRedisNeutronCacheStoresFromClient,
  type RedisLikeClient,
} from "./index.js";

class FakeRedisClient implements RedisLikeClient {
  public keysCallCount = 0;
  public scanCallCount = 0;
  public delBatchSizes: number[] = [];
  public scan?:
    | ((cursor: string, ...args: Array<string | number>) => Promise<[nextCursor: string, keys: string[]]>)
    | undefined;
  private readonly kv = new Map<string, string>();
  private readonly sets = new Map<string, Set<string>>();
  private readonly expiresAt = new Map<string, number>();

  constructor(enableScan: boolean) {
    if (enableScan) {
      this.scan = async (
        cursor: string,
        ...args: Array<string | number>
      ): Promise<[nextCursor: string, keys: string[]]> => {
        this.scanCallCount += 1;

        const argList = args.map((value) => String(value));
        const matchIndex = argList.findIndex((value) => value.toUpperCase() === "MATCH");
        const countIndex = argList.findIndex((value) => value.toUpperCase() === "COUNT");
        const pattern = matchIndex >= 0 ? argList[matchIndex + 1] : "*";
        const count = countIndex >= 0 ? Number.parseInt(argList[countIndex + 1], 10) : 10;

        const all = this.getMatchingKeys(pattern);
        const offset = Number.parseInt(cursor, 10) || 0;
        const slice = all.slice(offset, offset + count);
        const nextOffset = offset + slice.length;
        const nextCursor = nextOffset >= all.length ? "0" : String(nextOffset);
        return [nextCursor, slice];
      };
    }
  }

  async get(key: string): Promise<string | null> {
    this.pruneExpired();
    return this.kv.get(key) ?? null;
  }

  async set(
    key: string,
    value: string,
    mode?: "EX",
    ttlSec?: number
  ): Promise<unknown> {
    this.pruneExpired();
    this.kv.set(key, value);
    if (mode === "EX" && typeof ttlSec === "number" && ttlSec > 0) {
      this.expiresAt.set(key, Date.now() + ttlSec * 1000);
    }
    return "OK";
  }

  async del(...keys: string[]): Promise<unknown> {
    this.pruneExpired();
    this.delBatchSizes.push(keys.length);
    let deleted = 0;
    for (const key of keys) {
      if (this.kv.delete(key)) {
        deleted += 1;
      }
      if (this.sets.delete(key)) {
        deleted += 1;
      }
      this.expiresAt.delete(key);
    }
    return deleted;
  }

  async sadd(key: string, ...members: string[]): Promise<unknown> {
    this.pruneExpired();
    const bucket = this.sets.get(key) ?? new Set<string>();
    for (const member of members) {
      bucket.add(member);
    }
    this.sets.set(key, bucket);
    return bucket.size;
  }

  async smembers(key: string): Promise<string[]> {
    this.pruneExpired();
    const bucket = this.sets.get(key);
    return bucket ? Array.from(bucket) : [];
  }

  async expire(key: string, ttlSec: number): Promise<unknown> {
    this.pruneExpired();
    if (this.kv.has(key) || this.sets.has(key)) {
      this.expiresAt.set(key, Date.now() + ttlSec * 1000);
      return 1;
    }
    return 0;
  }

  async ttl(key: string): Promise<number> {
    this.pruneExpired();
    if (!this.kv.has(key) && !this.sets.has(key)) {
      return -2;
    }
    const deadline = this.expiresAt.get(key);
    if (deadline === undefined) {
      return -1;
    }
    return Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
  }

  async rename(source: string, destination: string): Promise<unknown> {
    this.pruneExpired();
    const hasValue = this.kv.has(source);
    const hasSet = this.sets.has(source);
    if (!hasValue && !hasSet) {
      throw new Error("ERR no such key");
    }
    if (hasValue) {
      this.kv.set(destination, this.kv.get(source)!);
      this.kv.delete(source);
    }
    if (hasSet) {
      this.sets.set(destination, this.sets.get(source)!);
      this.sets.delete(source);
    }
    const deadline = this.expiresAt.get(source);
    if (deadline !== undefined) {
      this.expiresAt.set(destination, deadline);
      this.expiresAt.delete(source);
    }
    return "OK";
  }

  async keys(pattern: string): Promise<string[]> {
    this.keysCallCount += 1;
    return this.getMatchingKeys(pattern);
  }

  async quit(): Promise<unknown> {
    return "OK";
  }

  hasKey(key: string): boolean {
    this.pruneExpired();
    return this.kv.has(key) || this.sets.has(key);
  }

  private getMatchingKeys(pattern: string): string[] {
    this.pruneExpired();
    const matcher = wildcardToRegExp(pattern);
    const keys = new Set<string>([
      ...this.kv.keys(),
      ...this.sets.keys(),
    ]);
    return Array.from(keys).filter((key) => matcher.test(key));
  }

  private pruneExpired(): void {
    const now = Date.now();
    for (const [key, deadline] of this.expiresAt.entries()) {
      if (deadline <= now) {
        this.expiresAt.delete(key);
        this.kv.delete(key);
        this.sets.delete(key);
      }
    }
  }
}

test("app.clear uses SCAN when client supports it", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.app.set("html:/home", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "<h1>Home</h1>",
    expiresAt: Date.now() + 60_000,
  });
  await stores.loader.set("/home::route::{}", {
    data: { ok: true },
    expiresAt: Date.now() + 60_000,
  });

  await stores.app.clear();

  assert.equal(client.scanCallCount > 0, true);
  assert.equal(client.keysCallCount, 0);
  assert.equal(client.hasKey("test:app:html:/home"), false);
  assert.equal(client.hasKey("test:idx:app:/home"), false);
  assert.equal(client.hasKey("test:ldr:/home::route::{}"), true);
});

test("app.clear falls back to KEYS when scan is unavailable", async () => {
  const client = new FakeRedisClient(false);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.app.set("html:/about", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "<h1>About</h1>",
    expiresAt: Date.now() + 60_000,
  });

  await stores.app.clear();

  assert.equal(client.keysCallCount > 0, true);
  assert.equal(client.hasKey("test:app:html:/about"), false);
  assert.equal(client.hasKey("test:idx:app:/about"), false);
});

test("app.deleteByPath removes indexed entries for the selected pathname only", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.app.set("html:/home?view=1", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "<h1>Home A</h1>",
    expiresAt: Date.now() + 60_000,
  });
  await stores.app.set("html:/home?view=2", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "<h1>Home B</h1>",
    expiresAt: Date.now() + 60_000,
  });
  await stores.app.set("html:/about", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "<h1>About</h1>",
    expiresAt: Date.now() + 60_000,
  });

  await stores.app.deleteByPath("/home");

  assert.equal(client.hasKey("test:app:html:/home?view=1"), false);
  assert.equal(client.hasKey("test:app:html:/home?view=2"), false);
  assert.equal(client.hasKey("test:idx:app:/home"), false);
  assert.equal(client.hasKey("test:app:html:/about"), true);
  assert.equal(client.hasKey("test:idx:app:/about"), true);
});

test("app.clear deletes keys in bounded chunks when many keys match", async () => {
  const client = new FakeRedisClient(false);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  for (let i = 0; i < 1_205; i++) {
    await client.set(`test:app:key-${i}`, `value-${i}`);
  }

  await stores.app.clear();

  assert.equal(client.hasKey("test:app:key-0"), false);
  assert.equal(client.hasKey("test:app:key-1204"), false);
  assert.equal(client.delBatchSizes.length >= 3, true);
  assert.equal(client.delBatchSizes.every((size) => size <= 500), true);
});

// A short-lived variant written after a long-lived one must not shorten the
// shared pathname index below the long-lived entry's life, or the entry
// becomes uninvalidatable until its own expiry (audit neutron-14).
test("app path index ttl is never shortened by a shorter-lived variant", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  const longEntry = {
    status: 200 as const,
    statusText: "OK",
    headers: [] as [],
    body: "<h1>long lived</h1>",
    expiresAt: Date.now() + 600_000,
  };
  await stores.app.set("html:/deep", longEntry);
  const indexKey = "test:idx:app:/deep";
  const ttlAfterLong = await client.ttl(indexKey);
  assert.equal(ttlAfterLong >= 595, true);

  await stores.app.set("html:/deep?view=quick", {
    ...longEntry,
    body: "<h1>short lived</h1>",
    expiresAt: Date.now() + 10_000,
  });
  const ttlAfterShort = await client.ttl(indexKey);
  assert.equal(
    ttlAfterShort >= 595,
    true,
    `index ttl dropped to ${ttlAfterShort} after a 10s variant; the 600s entry would lose invalidation`
  );

  // Invalidation must still reach the long-lived entry.
  await stores.app.deleteByPath("/deep");
  assert.equal(client.hasKey("test:app:html:/deep"), false);
  assert.equal(client.hasKey("test:app:html:/deep?view=quick"), false);
  assert.equal(client.hasKey(indexKey), false);
});

// The same guarantee for the loader store.
test("loader path index ttl is never shortened by a shorter-lived variant", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.loader.set("/page::route::{}", {
    data: { ok: true },
    expiresAt: Date.now() + 600_000,
  });
  await stores.loader.set("/page::route::{lang:fr}", {
    data: { ok: true },
    expiresAt: Date.now() + 10_000,
  });

  const ttlAfter = await client.ttl("test:idx:ldr:/page");
  assert.equal(ttlAfter >= 595, true);

  await stores.loader.deleteByPath("/page");
  assert.equal(client.hasKey("test:ldr:/page::route::{}"), false);
});

// A writer that lands between an invalidation's claim and its deletes must
// stay indexed: the index is claimed atomically via RENAME, so the writer
// SADDs into a fresh index and a subsequent invalidation can still reach
// it (audit neutron-15).
test("concurrent write during invalidation stays invalidatable", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.app.set("html:/race?v=1", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "variant 1",
    expiresAt: Date.now() + 60_000,
  });

  // Barrier: pause the first invalidation after RENAME, before the member
  // deletes — the exact window where a writer used to be orphaned.
  let releaseDeletes: (() => void) | undefined;
  const deletesGate = new Promise<void>((resolve) => {
    releaseDeletes = resolve;
  });
  const originalDel = client.del.bind(client);
  let delIntercepted = false;
  client.del = async (...keys: string[]) => {
    if (!delIntercepted) {
      delIntercepted = true;
      await deletesGate;
    }
    return originalDel(...keys);
  };

  const firstInvalidation = stores.app.deleteByPath("/race");
  // Wait until the rename has happened: the original index key is gone,
  // replaced by the claimed one.
  await new Promise<void>((resolve) => {
    const poll = () => {
      if (!client.hasKey("test:idx:app:/race")) {
        resolve();
        return;
      }
      setTimeout(poll, 1);
    };
    poll();
  });

  // The racing write: SADDs into the fresh index the rename made possible.
  await stores.app.set("html:/race?v=2", {
    status: 200,
    statusText: "OK",
    headers: [],
    body: "variant 2",
    expiresAt: Date.now() + 60_000,
  });
  assert.equal(client.hasKey("test:idx:app:/race"), true);

  releaseDeletes!();
  await firstInvalidation;

  // Variant 1 is gone from the first invalidation; variant 2 survived it
  // (it raced in) but a SECOND complete invalidation must reach it — that
  // is the defect: the orphaned variant used to be unreachable forever.
  await stores.app.deleteByPath("/race");

  assert.equal(client.hasKey("test:app:html:/race?v=1"), false);
  assert.equal(client.hasKey("test:app:html:/race?v=2"), false);
  assert.equal(client.hasKey("test:idx:app:/race"), false);
});

// Deleting a path that was never indexed is a no-op, not an error.
test("deleteByPath on an unknown path is a no-op", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client, {
    keyPrefix: "test:",
  });

  await stores.app.deleteByPath("/never-indexed");
  await stores.loader.deleteByPath("/never-indexed");
});

function wildcardToRegExp(pattern: string): RegExp {
  const escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^${escaped.replace(/\*/g, ".*")}$`);
}
