# @neutron-build/cache-redis

Redis-backed app response and loader caches for Neutron.

```bash
npm install @neutron-build/cache-redis ioredis
```

```typescript
import { createRedisNeutronCacheStores } from "@neutron-build/cache-redis";

const cacheStores = await createRedisNeutronCacheStores({
  url: process.env.REDIS_URL,
  keyPrefix: "my-app:",
});
// Pass cacheStores to createServer({ ..., cache: cacheStores }).
// At shutdown:
await cacheStores.close();
```

Both stores implement the core atomic publication contract. Capture a generation
before rendering, then call `setIfGeneration(key, entry, generation)`. A successful
path invalidation advances the shared generation even if that path has no cached
entries. Redis Lua scripts compare and publish the payload plus its pathname index
atomically; invalidation and index expiry updates also execute atomically.

`clear()` switches to a fresh epoch in constant time. Older entries immediately
become misses and older in-flight fills are refused. Old payloads and pathname
indexes remain physically present until their TTLs expire; clear does not scan
and delete keys belonging to newly admitted fills. App and loader epochs are
independent. Path invalidation removes that path's indexed variants. If its
index is missing (including independent Redis eviction), invalidation switches
the entire store to a fresh epoch: other paths become cold misses too. This
conservative fallback prevents an orphaned old payload from surviving a missing
index.

Clients passed to `createRedisNeutronCacheStoresFromClient` must support `EVAL`;
clients without it are refused. Unversioned entries from earlier adapter releases
are treated as cold misses. Upgrade every writer sharing a prefix together, or
use a new prefix: older adapter binaries do not participate in this protocol.
Opaque generation and epoch identifiers prevent an evicted control key from
revalidating older tokens or payloads.

Use a standalone Redis endpoint. Redis Cluster is unsupported because a script's
control, entry and index keys are not guaranteed to share a hash slot. Dragonfly
compatibility depends on its Redis Lua support; the live regression suite here
is verified against Redis. Redis cache state is disposable: restoring a previous
snapshot or failing over to a replica that has not received an acknowledged
invalidation can restore old generations. This adapter does not promise durable
invalidation across those operations.

Run the real Redis tests with `NEUTRON_TEST_REDIS_URL` pointing to a test endpoint.
They use and remove a unique namespace, including concurrent fills from separate
clients, empty-path invalidation, clear, control-key recreation and index TTLs.

[Documentation](https://neutron.build)
