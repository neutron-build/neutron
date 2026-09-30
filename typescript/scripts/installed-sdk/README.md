# Installed Nucleus and data SDK gates

Build the packages with the frozen workspace lockfile, then test actual
`pnpm pack` tarballs in engine-strict npm projects outside the checkout:

```sh
cd typescript
pnpm install --frozen-lockfile --filter '@neutron-build/nucleus' --filter '@neutron-build/data'
cd ..
NEUTRON_NUCLEUS_TEST_URL=postgres://postgres@localhost:55432/postgres \
NEUTRON_TEST_PG_URL=postgres://postgres:postgres@localhost:5432/postgres \
node typescript/scripts/installed-sdk-gate.mjs --package nucleus --out nucleus-installed.local.json
# Same endpoints, repeat with --package data; run both on Node 22 and Node 24.
```

Both endpoints are required. The gate does not skip absent live services.
`--keep-tarball <directory>` retains the artifacts; `--keep-work` retains
consumer projects for diagnostics. Nothing is published. JSON records the
source trees, artifact hashes, Node version, password-free endpoints, exact
TypeScript diagnostics and operation checks.

The Nucleus gate imports all 15 public subpaths, compiles real installed
consumer declarations with TypeScript 5.7.2 and 5.9.3 under NodeNext and
Bundler (`strict: true`, `skipLibCheck: false`), and exercises bounded
SQL/KV/document/graph/time-series/columnar/geo/blob operations. A real
PostgreSQL control must refuse native KV, vector and FTS features. Imports
are counted separately from live operations. Native vector/FTS success,
universal parity, durability and concurrent coherence require other gates.

The data gate checks a no-optional-peer consumer, root declarations without
optional drivers, genuine Drizzle row/relational/negative application types,
SQL commit/rollback with independent raw-driver observations, optional-SDK
absence, native KV/blob adapters and capability/connection-failure refusal.
Its `/drizzle` application fixture uses the documented `skipLibCheck: true`
while retaining strict application checks. Full Drizzle library declaration
checks use `skipLibCheck: false` and must reproduce a separately classified
upstream unsupported profile; if that profile starts compiling, review it
and promote support instead of leaving stale limitations silently accepted.
This is not an expected live-operation failure or a claim of full declaration
support.

Each TypeScript invocation has a 120-second timeout and a 768-MiB heap limit.
Supported fixtures additionally cap instantiations at 1,000,000 and reported
memory at 256 MiB. The unsupported full-library diagnostic profile has a
separate 5,000,000 / 640-MiB cap, and reports its measured cost and unsupported
status. Node 22/24 jobs in `orm-artifacts.yml` build one engine from the same
source and run both SDK gates against it and PostgreSQL 17.
