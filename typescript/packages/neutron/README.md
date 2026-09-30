# @neutron-build/core

The unified TypeScript web framework.

Static sites with zero JS, app routes with Preact SSR, islands architecture, content collections, and deploy-anywhere adapters.

## Installation

```bash
npm install @neutron-build/core
```

## Usage

```typescript
import { defineConfig } from "@neutron-build/core";
import { Island } from "@neutron-build/core/client";
import { getCollection } from "@neutron-build/core/content";
```

## Cached functions

`cache()` deduplicates calls within each HTTP request created by the Neutron
server adapter. Concurrent requests have independent results. Outside a server
request it calls the function directly. In the browser, results live for five
seconds by default. Each cached function has its own identity, including when
functions use the same `keyPrefix`.

```typescript
import { cache, clearCache } from "@neutron-build/core";

const currentUser = cache(async () => loadAuthenticatedUser());
// Explicit process-wide caching is appropriate only for public data:
const publicCatalog = cache(async () => loadPublicCatalog(), {
  scope: "shared", ttl: 30_000, maxEntries: 512,
});
clearCache("shared");
```

Scopes retain at most 1,024 entries by default (`maxEntries`: 1–4,096). Eviction,
rejection, expiration, and invalidation remove tag references too. Tag and prefix
invalidation accept `"shared"` as their final argument to target an explicitly
shared cache from a server request. Custom Node HTTP adapters must establish
request isolation themselves; without the Neutron adapter there is no implicit
server cache.

## Shared response and loader stores

The default memory response and loader stores support atomic invalidation.
Custom stores supplied to `createServer({ cache })` must implement
`getGeneration()` and `setIfGeneration(key, entry, expectedGeneration)`.
`deleteByPath()` and `clear()` must advance the backing store generation even
when no existing entry matches. Compare the generation and write the entry in
one backing-store transaction or server-side script. This prevents a delayed
fill from restoring stale data after a mutation completes, including across
servers sharing a store. Stores with only the former `get/set/deleteByPath/clear`
interface are refused at startup; migrate their implementation before upgrading.

## Documentation

[neutron.build](https://neutron.build)

## License

MIT
