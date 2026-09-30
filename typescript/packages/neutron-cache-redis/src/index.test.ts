import assert from "node:assert/strict";
import test from "node:test";
import { serializeTransportData } from "@neutron-build/core";
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

  private readonly controls = new Map<string, { generation: string; epoch: string }>();
  // Model outcomes only; a separate real-server suite verifies the Lua scripts.
  async eval(script: string, count: number, ...values: Array<string | number>): Promise<unknown> {
    this.pruneExpired();
    const keys = values.slice(0, count).map(String), args = values.slice(count).map(String);
    let control = this.controls.get(keys[0]);
    if (!control) { control = { generation: args[0], epoch: args[0] }; this.controls.set(keys[0], control); }
    if (script.endsWith("return generation")) return control.generation;
    if (script.includes("entry.payload")) {
      const raw = this.kv.get(keys[1]); if (!raw) return null;
      const entry = JSON.parse(raw);
      return entry.neutronCacheV === 1 && entry.epoch === control.epoch ? entry.payload : null;
    }
    if (script.includes("local ttl =")) {
      if ((args[1] && args[1] !== control.generation) || Number(args[3]) <= 0) return 0;
      this.kv.set(keys[1], JSON.stringify({ neutronCacheV: 1, epoch: control.epoch, payload: args[2] }));
      this.expiresAt.set(keys[1], Date.now() + Number(args[3]) * 1000);
      const members = this.sets.get(keys[2]) ?? new Set<string>();
      members.add(keys[1]); this.sets.set(keys[2], members);
      this.expiresAt.set(keys[2], Math.max(this.expiresAt.get(keys[2]) ?? 0, Date.now() + Math.max(Number(args[3]),60)*1000));
      return 1;
    }
    control.generation = args[1];
    if (script.includes("SMEMBERS")) {
      if (!this.sets.has(keys[1])) control.epoch = args[1];
      for (const key of this.sets.get(keys[1]) ?? []) { this.kv.delete(key); this.expiresAt.delete(key); }
      this.sets.delete(keys[1]); this.expiresAt.delete(keys[1]);
    } else control.epoch = args[1];
    return 1;
  }

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


function wildcardToRegExp(pattern: string): RegExp {
  return new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g,".*")}$`);
}
const appEntry = (ttl = 60000) => ({ status: 200, statusText: "OK", headers: [] as [string,string][], body: new Uint8Array([0,255,128]), expiresAt: Date.now()+ttl });

test("path invalidation matches current app keys exactly and preserves other paths", async () => {
  const stores = createRedisNeutronCacheStoresFromClient(new FakeRedisClient(true));
  const key = "html\nhttps://app.example\n/a..b\n?q=1\nen";
  await stores.app.set(key, appEntry());
  await stores.app.set("html:/a..bc", appEntry());
  assert.deepEqual((await stores.app.get(key))?.body, new Uint8Array([0,255,128]));
  await stores.app.deleteByPath("/a..b/");
  assert.equal(await stores.app.get(key), null);
  assert.ok(await stores.app.get("html:/a..bc"));
});
test("empty path invalidation rejects delayed app and loader publications", async () => {
  const stores = createRedisNeutronCacheStoresFromClient(new FakeRedisClient(true));
  for (const store of [stores.app, stores.loader]) {
    const generation = await store.getGeneration!();
    await store.deleteByPath("/absent");
    assert.notEqual(await store.getGeneration!(), generation);
  }
  const old = await stores.app.getGeneration!();
  await stores.app.deleteByPath("/a");
  assert.equal(await stores.app.setIfGeneration!("html:/a", appEntry(),old),false);
  assert.equal(await stores.app.get("html:/a"),null);
});
test("clear logically removes its store without scanning and admits new fills", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client);
  await stores.app.set("html:/a", appEntry());
  await stores.loader.set("/a::route", {data: 1,expiresAt:Date.now()+60000});
  const old = await stores.app.getGeneration!();
  await stores.app.clear();
  assert.equal(await stores.app.get("html:/a"),null);
  assert.ok(await stores.loader.get("/a::route"));
  assert.equal(await stores.app.setIfGeneration!("html:/a",appEntry(),old),false);
  assert.equal(await stores.app.setIfGeneration!("html:/a",appEntry(),await stores.app.getGeneration!()),true);
  assert.ok(await stores.app.get("html:/a"));
  assert.equal(client.scanCallCount+client.keysCallCount,0);
});
test("index TTL retains the longest app and loader variant", async () => {
  const client = new FakeRedisClient(true);
  const stores = createRedisNeutronCacheStoresFromClient(client,{keyPrefix:"test:"});
  await stores.app.set("html:/a",appEntry(600000));
  await stores.app.set("html:/a?q=short",appEntry(10000));
  await stores.loader.set("/b::long",{data:1,expiresAt:Date.now()+600000});
  await stores.loader.set("/b::short",{data:2,expiresAt:Date.now()+10000});
  assert.ok(await client.ttl("test:idx:app:/a")>=595);
  assert.ok(await client.ttl("test:idx:ldr:/b")>=595);
  await stores.loader.deleteByPath("/b");
  assert.equal(await stores.loader.get("/b::long"),null);
});
test("legacy unversioned payloads are cold misses after upgrade", async () => {
  const client = new FakeRedisClient(true);
  await client.set("test:app:html:/legacy",serializeTransportData(appEntry()),"EX",60);
  assert.equal(await createRedisNeutronCacheStoresFromClient(client,{keyPrefix:"test:"}).app.get("html:/legacy"),null);
});
test("clients without EVAL are refused before stores are created", () => {
  const client = new FakeRedisClient(true);
  Object.assign(client,{eval:undefined});
  assert.throws(()=>createRedisNeutronCacheStoresFromClient(client),/atomic EVAL/);
});
