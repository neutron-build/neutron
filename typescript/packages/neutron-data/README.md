# @neutron-build/data

Data layer for Neutron applications: a typed Drizzle interop wrapper for
Postgres/SQLite/Nucleus, plus pluggable cache, session, queue, storage and
rate-limiting drivers.

## Two database paths, separately named

- **First-party SQL path: [`@neutron-build/sql`](../neutron-sql)** — Neutron's
  own schema/compiler/transaction/migration surface (`createDatabase`,
  `pgTable`, `db.query…`). Use it for new application SQL.
- **Drizzle interop path: this package** — `createDrizzleDatabase` returns
  *genuine* drizzle-orm databases for existing Drizzle users and for Nucleus
  profiles that want the multi-model client alongside relational SQL. It is a
  thin factory: no query passes through Neutron code, nothing is re-typed or
  cast to a Neutron type, and Drizzle's own API (including
  `db.transaction`) works unchanged.

## Installation

```bash
npm install @neutron-build/data
# plus the peers you actually use:
npm install drizzle-orm postgres            # Postgres / Nucleus profiles
npm install drizzle-orm @libsql/client      # SQLite profile
```

## Drizzle wrapper

Import the typed entry from the `/drizzle` subpath (it carries the real
drizzle-orm declarations; the root export stays usable without drizzle-orm
installed):

```ts
import { createDrizzleDatabase } from "@neutron-build/data/drizzle";
import { pgTable, serial, text, integer } from "drizzle-orm/pg-core";
import { eq } from "drizzle-orm";

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  score: integer("score").notNull().default(0),
});

const database = await createDrizzleDatabase({
  profile: { provider: "postgres", connectionString: process.env.DATABASE_URL! },
  schema: { users },
});

// database.db is a genuine PostgresJsDatabase<typeof schema>:
const rows = await database.db.select().from(users).where(eq(users.email, "a@x.com"));
await database.db.transaction(async (tx) => {
  await tx.update(users).set({ score: 1 }).where(eq(users.id, rows[0]!.id));
});
await database.close();
```

SQLite is the same shape with a genuine `LibSQLDatabase`:

```ts
import { sqliteTable, integer, text } from "drizzle-orm/sqlite-core";

const todos = sqliteTable("todos", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  label: text("label").notNull(),
});

const database = await createDrizzleDatabase({
  profile: { provider: "sqlite", connectionString: "./dev.db" },
  schema: { todos },
});
// database.db: LibSQLDatabase<{ todos: typeof todos }> — select/insert/
// transaction all carry Drizzle's own row types.
```

### Overloads

The provider is the overload key:

| Call shape | Returns |
|---|---|
| `profile.provider: "postgres"` or `"nucleus"` | `{ db: PostgresJsDatabase<TSchema> & { $client: Sql }, client: Sql, nucleus: unknown \| null, … }` |
| `profile.provider: "sqlite"` | `{ db: LibSQLDatabase<TSchema> & { $client: Client }, client: Client, nucleus: null, … }` |
| no `profile` (env auto-detect or `config`) | provider resolved at runtime; `db: unknown` — narrow with `result.profile.provider` or pass an explicit profile for typed results |

`TSchema` is the `schema` option you passed, threaded through to Drizzle, so
`database.db.select()`/`database.db.query.<table>` carry Drizzle's own result
types with zero casts.

One TypeScript note for partial installs: drizzle-orm's own declarations
reference its *other* optional drivers (`mysql2`, `gel`, …), so consumers
that install only the Postgres or only the SQLite peer set need
`skipLibCheck: true` (the ecosystem norm for drizzle-orm) to compile against
`/drizzle`. Your own code is still fully checked; only drizzle-orm's
internal declarations are skipped.

The `nucleus` provider connects the same postgres.js driver over Nucleus's
pg-wire protocol and additionally connects an `@neutron-build/nucleus` client
(non-relational models). A failed Nucleus connect rejects — it never silently
degrades to Drizzle-only mode.

### Back-compatibility note (pre-1.0)

The root export still provides `createDrizzleDatabase` and the
`DrizzleDatabase` type with the original loose surface (`db: unknown`) — the
same runtime function. Code that typed Drizzle results through the root
import should move to `@neutron-build/data/drizzle`; the root alias exists so
projects that never touch Drizzle do not need drizzle-orm's types to compile.

## Runtime support (Node.js)

This package requires Node.js (`engines: ">= 20"`). The `/drizzle` entry
evaluates without any Node builtin (its `node:path` use is lazy), and
`createDrizzleDatabase` checks for a Node process positively
(`process.versions.node`) and fails with one precise error on runtimes
without one — before any driver import. The **root** entry statically
re-exports the session/queue surfaces (which import `node:crypto` /
`node:os`), so importing `@neutron-build/data` itself in a no-Node
environment fails at module resolution with the runtime's own `node:` error.
There is no edge/browser adapter; none will be claimed without real
transport fixtures.

## Verified peer combinations

The wrapper is tested against the peer versions the workspace actually
resolves (pinned by the consumers' matrix test, which fails on lockfile
drift outside the declared ranges):

| Peer | Declared range | Version under test |
|---|---|---|
| `drizzle-orm` | `^0.44.5` | 0.44.7 |
| `postgres` | `^3.4.7` | 3.4.8 |
| `@libsql/client` | `^0.17.0` | 0.17.0 |
| `@neutron-build/nucleus` | `workspace:^` | 0.1.2 (import + connect-failure path only; no live Nucleus engine is claimed) |

Live coverage: the Postgres leg runs typed CRUD + Drizzle transactions
against a real server (disposable `i03_`-prefixed databases); the SQLite leg
runs the same against real database files in temp directories. Version
claims outside this table are not made.

## Other drivers

Cache (memory/Redis/Nucleus), sessions (memory/Redis), queues
(memory/BullMQ/Postgres), storage (memory/S3/Nucleus), rate limiting and
realtime buses are lazy-loaded behind their optional peers:

```ts
import { createS3StorageDriver } from "@neutron-build/data";
```

## Documentation

[neutron.build](https://neutron.build)

## License

MIT
