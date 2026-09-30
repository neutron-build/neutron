import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { createRedisNeutronCacheStoresFromClient, type RedisLikeClient } from "./index.js";

const url = process.env.NEUTRON_TEST_REDIS_URL;
test("real Redis atomic publication, invalidation, epochs and concurrent fills", { skip: !url && "NEUTRON_TEST_REDIS_URL not set" }, async () => {
  const imported = await new Function("return import('ioredis')")() as {default: new(url:string)=>RedisLikeClient};
  const client = new imported.default(url!);
  const peer = new imported.default(url!);
  const prefix = `neutron-test:${randomUUID()}:`;
  const stores = createRedisNeutronCacheStoresFromClient(client,{keyPrefix:prefix});
  const other = createRedisNeutronCacheStoresFromClient(peer,{keyPrefix:prefix});
  const appKey = "html\nhttps://a.example\n/race\n?q=1\nen";
  const entry = {status:200,statusText:"OK",headers:[] as [string,string][],body:new Uint8Array([0,255,128]),expiresAt:Date.now()+60000};
  try {
    for (const [store, remote, key, payload] of [
      [stores.app,other.app,appKey,entry],
      [stores.loader,other.loader,"/race::route",{data:{version:1},expiresAt:Date.now()+60000}],
    ] as const) {
      const old = await store.getGeneration!();
      await remote.deleteByPath("/race");
      assert.equal(await store.setIfGeneration!(key,payload as never,old),false);
      assert.equal(await store.get(key),null);
      const current = await store.getGeneration!();
      assert.equal(await store.setIfGeneration!(key,payload as never,current),true);
      assert.deepEqual(await remote.get(key),payload);
      await remote.clear();
      assert.equal(await store.get(key),null);
      assert.equal(await store.setIfGeneration!(key,payload as never,current),false);
      assert.equal(await store.setIfGeneration!(key,payload as never,await store.getGeneration!()),true);
      assert.deepEqual(await store.get(key),payload);
    }
    // Two independent clients race publication and invalidation. Whichever
    // script runs first, the invalidation acknowledgment leaves no old fill.
    for (let i=0;i<100;i++) {
      const old = await stores.app.getGeneration!();
      await Promise.all([stores.app.setIfGeneration!(appKey,entry,old),other.app.deleteByPath("/race")]);
      assert.equal(await stores.app.get(appKey),null);
      assert.equal(await stores.app.setIfGeneration!(appKey,entry,old),false);
    }
    // Removing only control metadata simulates eviction/recreation: opaque
    // fresh generations fence old tokens and old epoch payloads.
    const old = await stores.app.getGeneration!();
    await stores.app.setIfGeneration!(appKey,entry,old);
    await peer.del(`${prefix}control:app`);
    assert.notEqual(await stores.app.getGeneration!(),old);
    assert.equal(await stores.app.get(appKey),null);
    assert.equal(await stores.app.setIfGeneration!(appKey,entry,old),false);
    // Lua atomically extends rather than shortens the path index's expiry.
    await stores.app.set(appKey,{...entry,expiresAt:Date.now()+600000});
    await other.app.set("html\nhttps://a.example\n/race\n?short\nen",{...entry,expiresAt:Date.now()+10000});
    assert.ok(await client.ttl!(`${prefix}idx:app:/race`)>=595);
    await stores.app.deleteByPath("/race");
    assert.equal(await stores.app.get(appKey),null);
  } finally {
    // A unique test namespace has no concurrent consumers; cleanup is safe.
    const keys = await client.keys(`${prefix}*`);
    if(keys.length) await client.del(...keys);
    await Promise.all([stores.close(),other.close()]);
  }
});
