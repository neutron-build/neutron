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
reference its *other* optional drivers (`mysql2`, `gel`, …). This package
derives the SQLite client type from Drizzle's return type instead of directly
importing the optional `@libsql/client` driver into its shared declaration.
Consumers with only the Postgres or SQLite peer set can still need
`skipLibCheck: true` to compile against `/drizzle` because of upstream
Drizzle declarations. Your application remains strictly checked. The root
entry compiles with `skipLibCheck: false` and no optional peers installed.
The installed SDK gate records upstream strict-library refusals separately
from successful application typing; it does not call those declarations
fully supported.

The `nucleus` provider connects the same postgres.js driver over Nucleus's
pg-wire protocol. A configured companion can provide non-relational plugins;
legacy automatic allocation is plugin-free. A failed companion connect rejects
and disposes owned resources; an absent optional peer permits Drizzle-only mode.

### Back-compatibility note (pre-1.0)

The root export still provides `createDrizzleDatabase` and the
`DrizzleDatabase` type with the original loose surface (`db: unknown`) — the
same runtime function. Code that typed Drizzle results through the root
import should move to `@neutron-build/data/drizzle`; the root alias exists so
projects that never touch Drizzle do not need drizzle-orm's types to compile.

## Runtime support (Node.js)

This package requires Node.js (`engines: ">=22"`). The `/drizzle` entry
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
| `@neutron-build/nucleus` | `^0.2.0` | 0.2.0 (import + connect-failure path only; no live Nucleus engine is claimed) |

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

Leased Postgres queue handlers receive `job.signal`, which aborts if a
heartbeat or final acknowledgement fails, or the attempt loses ownership. Configure `onLeaseLost` to
observe that outcome. Only the running slot is leased; `batchSize` bounds
sequential throughput per poll. Acknowledgements are fenced by the attempt
generation, even when `workerId` is reused. External effects still require
idempotency because delivery is at least once. An uncertain success/retry/dead
acknowledgement is sent once, aborts the signal, and invokes `onLeaseLost` once.
The worker stops and retains the original failure in `workerError`; `close()`
still ends the SQL client and rejects with that failure (and cleanup errors if
present). Inspect server state for that attempt before replacing the worker.
A committed acknowledgement whose response was lost is not automatically replayed.

The BullMQ adapter defers unknown job names by one second without consuming
an attempt. Such jobs remain durable across worker restarts and can execute
when a worker registers their handler. A shared queue with disjoint handlers
may spend time deferring jobs; use separate queue names for efficient routing.
Custom injected worker implementations lacking BullMQ's native delayed-job
support reject unknown jobs into the failed set for explicit operator retry.

Counter TTL omission means no new expiry; every supplied TTL must be integer
seconds in `1..2147483647`. Zero, negatives, NaN, Infinity and fractions reject
before dispatch or memory mutation. Redis and Memory require canonical decimal
safe integers with a safe successor; malformed/unsafe values refuse unchanged.
Redis validates and increments atomically in EVAL and attaches an expiry only
to nonexpiring keys; existing expiry remains anchored. Memory follows these rules.
Nucleus's default `counterMode: 'native'` supports the bundled KVModel's plain
`incr(key)` through one native KV_INCR. It inherits backend stored-value semantics:
use valid integer counters in a dedicated namespace; it does not promise checked
malformed-value refusal or atomic validation of safe successors. An unsafe numeric
acknowledgement rejects after dispatch and requires reconciliation before retry.
Consumers requiring pre-mutation validation must select `counterMode: 'strict'`
and supply a genuine atomic `incrChecked` provider; construction refuses if absent.
`counterCapabilities.plain` distinguishes native from checked semantics. Any TTL
increment still requires a genuine checked atomic `incrWithExpiry` provider; bundled
KVModel does not supply it and `incr(key, ttl)` refuses before mutation. No split
INCR/EXPIRE or client GET/check/INCR is used. Existing bundled plain-increment
consumers can keep their calls; strict-counter consumers must migrate explicitly.
Expiring `set` remains a separate operation with its existing TTL semantics.

The development in-memory scheduler bounds long timer waits, emits no job
before its cron deadline, and emits one catch-up job after process suspension
before calculating its next occurrence from the current time.


Nucleus Drizzle profiles accept an already connected, caller-configured companion:

```ts
const database = await createDrizzleDatabase({
  profile: { provider: "nucleus", connectionString: nucleusUrl },
  schema,
  nucleusCompanion: { client: configuredClient, ownership: "borrowed" },
});
```

`borrowed` preserves the companion across startup failure and close. `owned`
transfers cleanup to the factory at entry. `nucleusCompanion: null` opts out.
Omission retains legacy optional plugin-free allocation; it does not provide
KV/vector/graph properties. Only caller-installed plugins provide models.
Every allocated SQL/SQLite client is owned immediately. Startup cleanup preserves
the primary error, and aggregates any cleanup errors with it as `cause`. Close
is memoized, attempts every owned resource, and reports all failures afterward.
The three SQL stacks retain distinct driver/public APIs: Drizzle preserves real
Drizzle objects, Nucleus owns its transport, and first-party SQL owns scoped pins.
Their different cancellation and ambiguity contracts are not interchangeable.

The data SessionStore is an unconditional cache store for application data. It
has no revision/CAS save or conditional destruction contract and cannot serve as
the core revisioned SessionStore. A future adapter must implement revision-aware
atomic load/save/destruction and reject legacy unconditional writers.
Memory stores are development/test adapters: they are process-local, expire
lazily without a global retention budget, and some values retain object identity.
Redis realtime `subscribe()` returns before native SUBSCRIBE readiness; a publish
immediately afterward can be missed. Await `subscribeAsync()` before publishing
when readiness matters. Acquisition/unsubscription serialize, close drains late
acquisitions and attempts every owned connection, and failures remain observable.
An injected publisher defaults to borrowed; set `publisherOwnership: "owned"`
to transfer cleanup. Native subscriptions remain transient, not durable delivery.
The synchronous compatibility API logs failures; subsequent registration retries.
Awaitable registration rejects instead of returning a success-shaped handle. Queue adapters share method names, but
memory is process-local, Postgres claims only registered names, and BullMQ consumes
then defers unknown names. Registration is not an atomic complete-handler registry
or an application-wide start/stop protocol. Draining a worker cannot undo external
effects; application idempotency remains required.

Queue `capabilities` also describe acknowledgement uncertainty, error observation,
registration/start, retry, scheduling, routing and post-close behavior. Select
requirements through `createJobs({driver, requiredCapabilities})` or `admitQueue`.
Missing custom metadata fails requested admission. `process` registers a handler
and starts PG/BullMQ workers; memory registers and drains inline. It it is not an atomic complete registry. PG stops on uncertain
acknowledgement, exposes `workerError` and reports uncertainty on close without
resending. Native BullMQ factory instances record error/failed events in workerError;
the factory retains BullMQ's default one attempt per job. Directly injected BullMQ workers have
unknown native durability/fencing/acknowledgement/scheduling guarantees. They cannot
inherit factory admission merely by passing a boolean. PG/BullMQ refuse operations
after terminal close; memory close ends scheduling only, and rejects new schedules.
Already queued memory jobs and handlers remain process-local and are not drained.
`QueueDriver.close` stays optional for custom compatibility.

All three SQL stacks use the driver-free `@neutron-build/sql/lifecycle` subpath for
ownership/drain/error semantics and typed operational admission. Data and Nucleus
now depend on SQL's package for that small module; it loads no ORM, driver or model.
`createDrizzleDatabase({requiredCapabilities})` refuses unsupported cancellation
before allocation. The Drizzle wrapper advertises cancellation as unsupported:
its raw genuine Drizzle/client handles retain their native APIs. Returned lifecycle
can assert terminal state; using retained raw handles after close remains subject
to native driver errors. Owned close is terminal, including failure; repeated close
observes the same outcome without repeating disposal. Borrowed companions survive.
Close drains independent owners together and respects queue worker→queue→Redis
ordering; startup errors retain their causes and all cleanup failures.
