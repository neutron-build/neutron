# neutron-data (Foundation)

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Package: `packages/neutron-data`

This package now provides the initial backend foundation layer for Neutron:

- Database profile resolution (`sqlite` dev default, `postgres` when `DATABASE_URL` is present).
- Cache client interface + in-memory implementation.
- Session store built on cache.
- Sliding-window rate-limit helper.
- Queue/jobs interfaces + in-memory queue driver.
- Storage and realtime in-memory drivers.
- `neutron worker` CLI command support for background runner modules.

Current scope is foundation primitives and contracts. Production driver integrations
(Drizzle, Dragonfly/Redis-compatible cache, BullMQ workers, S3 providers) are now
available as optional driver modules with lazy dependency loading.

## Optional Driver APIs

- `createDrizzleDatabase(...)` (Postgres + SQLite via Drizzle adapters)
- `createRedisCacheClient(...)` (Dragonfly/Redis protocol)
- `createRedisSessionStore(...)` (session store backed by Dragonfly/Redis)
- `createBullMqQueueDriver(...)` (BullMQ over Dragonfly/Redis)
- `createS3StorageDriver(...)` (AWS SDK S3-compatible backends)

## Install Optional Deps

Install only what you use:

```bash
pnpm add drizzle-orm postgres @libsql/client
pnpm add ioredis bullmq
pnpm add @aws-sdk/client-s3
```

## Reference App Integration

`apps/playground` now includes a `src/data` runtime profile:

- default (`NEUTRON_DATA_PROFILE` unset): in-memory drivers
- production profile (`NEUTRON_DATA_PROFILE=production`): Drizzle DB + Redis-compatible cache/session/queue + optional S3 storage

Playground scripts:

```bash
pnpm --dir apps/playground run worker
pnpm --dir apps/playground run db:generate
pnpm --dir apps/playground run db:migrate
pnpm --dir apps/playground run db:seed
```

For migration/seed tooling in the playground app, install:

```bash
pnpm --dir apps/playground add drizzle-orm postgres @libsql/client ioredis bullmq @aws-sdk/client-s3
```

Smoke checks:

```bash
pnpm run ci:data-profiles
```

This always validates the `memory` profile. To enable `production` profile smoke checks:

```bash
NEUTRON_DATA_RUN_PRODUCTION_SMOKE=1 DRAGONFLY_URL=redis://127.0.0.1:6379 pnpm run ci:data-profiles
```
