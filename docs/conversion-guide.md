# Converting a TypeScript data layer from Prisma or Drizzle to Neutron SQL

This guide maps an existing TypeScript project's data layer — Prisma (schema
`schema.prisma` + generated client) or Drizzle (`drizzle-orm` tables +
drizzle-kit) — onto Neutron SQL (`@neutron-build/sql`, version 0.1.0). It
states exactly what converts mechanically, what is refused, and what stays
manual. Every API named here exists in the source tree at the revision this
guide was written against; verified limitations carry capability-matrix row
references from the internal inventory that produced them.

Status: **alpha — contained, not production-ready** (the package's own words).
PostgreSQL is the only claimed engine (verified on 15, 16, 17, 18 with both
`pg` and `postgres` drivers; wire-compatible non-Postgres engines are not
claimed). Node.js >= 22 is required; there is no edge/browser runtime.

Non-goals of this guide: benchmark or performance claims, superiority claims
about either side, and Python/Go conversion (see
[Python and Go](#python-and-go) for their honest stance).

## Hard preconditions

- **PostgreSQL only.** Converted apps target PostgreSQL. SQLite/MySQL
  workloads have no path here (capability matrix row 8.7).
- **Node.js >= 22.** The package fails with a precise error on runtimes
  without Node compatibility; there is no edge/browser transport adapter
  (row 8.4).
- **Transaction poolers cannot run Neutron migrations.** If you ever move
  migration authority to the Neutron CLI, its session-level advisory lock
  cannot survive a pooler that reassigns sessions; session pooler mode is
  required for migration runs (row 8.5). The package claims no pooler
  compatibility for the query path either way — the verified matrices ran on
  direct connections.
- **Upgrade policy is not yet published.** The package is 0.1.0 with no
  public semver/compatibility contract yet (row 6.9); early adoption means
  reading the package README's "Upgrading" notes on each change.

## The one decision first: who owns migrations

**Keep Prisma Migrate or drizzle-kit initially.** A conversion is not a
reason to re-baseline a correctly owned project or write a second migration
ledger. Neutron schema declarations describe your existing tables for typed
runtime queries; they are not a competing migration authority. Concretely:

- `prisma migrate` / `drizzle-kit migrate` keep running unchanged; the
  Neutron schema hand-mirrors the tables your migrations produce.
- Neutron's own migration tooling (the `neutron` CLI) records its history in
  its own `_neutron_migrations` table and **refuses** history rows in legacy
  or unknown formats; it never silently adopts a foreign runner's ledger.
- Nothing in the query layer requires the Neutron CLI: `createDatabase`
  connects to the database your existing runner already manages.

If you later want the Neutron CLI to own migrations for a scope, that is a
separate, explicit move — see
[Adopting Neutron migrations deliberately](#adopting-neutron-migrations-deliberately).

## Concept map

| You have (Prisma / Drizzle) | Neutron SQL equivalent | Notes |
|---|---|---|
| `schema.prisma` `model` blocks | `pgTable("name", { ... })` | schema in TypeScript, no codegen |
| `drizzle-orm/pg-core` `pgTable` | `pgTable("name", { ... })` | near-isomorphic; see value-mode differences below |
| `prisma generate` / generated client | nothing — types derive from the table objects (`typeof users.$inferSelect` / `$inferInsert`) | no codegen step exists or is needed |
| `findMany` / `include` / `select` | `db.query.<table>.findMany({ with: {...} })` relational reads | one SQL statement, typed nesting |
| `findFirst` | `db.query.<table>.findFirst({ with })` | |
| `create` / `createMany` | `db.insert(t).values(row \| rows)` | multi-row values in one statement |
| `upsert` | `.onConflictUpdate({ target, set, setWhere? })` | exact method name at this revision |
| `createMany({ skipDuplicates: true })` | `.onConflictDoNothing({ target?, where? })` | |
| `updateMany` / `deleteMany` | `db.update(t).set({...}).where(...)` / `db.delete(t).where(...)` | affected-row count from execution |
| nested `create`/`connect`/`disconnect` writes | `db.query.<t>.create({ data })` / `.update` / `.delete` with per-edge ops | explicit, guarded, atomic |
| `$transaction([...])` sequential | `db.batch([q1, q2, ...])` | one connection, one transaction |
| `$transaction(async (tx) => ...)` interactive | `db.transaction(async (tx) => ...)` | real savepoints for nesting |
| `$queryRaw` / `$executeRaw` | `` sql`...` `` template | values bind as parameters, never spliced |
| `cursor` pagination | `keyset(table, orderTerms, { perPage })` | opaque versioned cursors |
| drizzle-kit | stays external, or adopt Neutron migrations deliberately | see above |
| Prisma Studio / drizzle-kit studio | `neutron studio` | visual DB manager over the same database |

All exports above come from the package root
(`typescript/packages/neutron-sql/src/index.ts` re-exports every symbol named
in this guide; `pgvector`, `fts`, `timeseries`, `columnar`, `listen-notify`
live in optional subpath entry points).

## Schema declaration

### From Prisma

```ts
import { pgTable, serial, integer, text, timestamp, relations } from "@neutron-build/sql";

export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
});

export const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
export const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));
```

Directive mapping:

| Prisma | Neutron | Notes |
|---|---|---|
| `@id @default(autoincrement())` | `serial("id").primaryKey()` | or `integer(...).generatedAlwaysAsIdentity()` / `generatedByDefaultAsIdentity()` for identity columns |
| `@default(expr)` | `.default(expr)` | |
| `@default(now())` | `.defaultNow()` | |
| `@unique` | `.unique()` on the column | |
| `@@unique([a, b])` | `unique({ name: "...", columns: [t.a, t.b] })` table constraint | also `primaryKey`, `check(name, expr)`, `foreignKey` with `deferrable` / `MATCH` options |
| `@@index([...])` | `index("name").on(t.col, ...)` | methods incl. GIN/GIST/HNSW, partial (`where`), `include`, `with`, opclass |
| `@relation(...)` | `.references(() => other.col, { onDelete, onUpdate })` + a `relations()` pair for nested reads | referential actions: `"cascade" \| "restrict" \| "set null" \| "set default" \| "no action"` |
| `enum Role {...}` | `pgEnum("role", ["..."])` | runtime membership validation; `mySchema.enum(...)` inside a `pgSchema` |
| `Json` | `json` / `jsonb` | SQL NULL vs JSON null distinguished on writes via the `jsonNull` sentinel |
| `Bytes` | `bytea` | reads/writes `Uint8Array` |
| `Decimal @db.Decimal(p, s)` | `numeric("col", { precision: p, scale: s })` | see the numeric note below |
| `DateTime` | `timestamp` / `timestamptz` | reads are canonical strings by default, not `Date` — see value semantics |
| `@updatedAt` | **no equivalent** | set the value in your update calls, or create a DB-side trigger through your existing migration runner |

### From Drizzle

Drizzle's `pgTable` shape is nearly identical — same "column factory taking
the physical column name" pattern — so the mechanical translation is mostly
renaming imports and method spellings. The differences that bite:

- **Temporal reads default to strings, not `Date`.** Drizzle's `timestamp()`
  returns `Date` by default; Neutron's `timestamp`/`timestamptz` return
  canonical microsecond-exact strings (`YYYY-MM-DDTHH:MM:SS[.ffffff]`,
  timestamptz as `...Z` UTC) with explicit `mode: "date"` opting into `Date`
  at millisecond truncation.
- **int8 reads as `bigint` by default**, with `bigint("col")` /
  `bigint("col", { mode: "string" })` / `bigint("col", { mode: "number" })`
  (the number mode is a checked safe-integer mode, not a silent cast).
- **numeric reads as an exact decimal string** (scale and trailing zeros
  preserved); an optional `numeric("col", { decoder })` narrows it, and a
  decoder that narrows to number owns the precision loss. An optional
  `{ precision, scale }` declares the `NUMERIC(p,s)` typmod in DDL (see the
  numeric note below).
- Joins take `alias()` handles, not raw tables: ``const p = alias(posts, "p")``,
  then ``.leftJoin(p, sql`${p.userId} = ${users.id}`)``.
- Nested reads are registered on the database (`relations` passed to
  `createDatabase`), not resolved implicitly from foreign keys.

### What the schema layer refuses

These are refusals, not gaps that silently degrade:

- **`numeric(p, s)` declarable on the shared contract subset** (capability
  matrix row 1.4, implemented): `numeric("col", { precision: 10, scale: 2 })`
  emits `numeric(10,2)` DDL and exports the typmod in schema-v2 — precision
  integer 1..1000, scale integer 0..precision, scale requires precision,
  precision-only normalizes to scale 0, and invalid values are refused at
  declaration (no clamping). `numeric("col")` stays unconstrained `NUMERIC`
  with exact-string reads, and `{ decoder }` combines with the typmod.
  PostgreSQL itself also allows negative scale and scale > precision; those are
  deliberately outside the shared cross-language contract — if your existing
  column uses them, keep that DDL with your existing migration runner and
  declare plain `numeric("col")` against it (reads and writes stay exact).
- **Arrays of temporal/bytea/vector/serial elements are refused**
  (row 1.13, deliberately outside scope): `.array()` supports a fixed set of
  scalar element kinds and throws for the rest, because no lossless
  acquisition exists for those text forms. Acquire such columns via raw SQL
  with your own text parsing.
- **Domain/custom SQL types are not declarable** (row 1.24): the query-layer
  `.codec()` escape maps values without changing the SQL type; columns of
  domain/custom-OID types have no schema declaration and no lossless
  generated-model path. Keep them behind raw SQL or `.codec()`.
- **Schema-qualified tables in relational reads are refused** (row 1.20):
  `pgSchema("legacy").table("users", {...})` works in CRUD and alias joins,
  but registering it in `createDatabase`'s `tables`/`relations` throws —
  `db.query` does not address schema-declared tables at this revision.
  Multi-schema apps use the builder surface for those tables.
- **Views are read-only handles** (`pgView`), with no auto-updatable-view
  inference (rows 1.21/1.22).
- **Virtual generated columns are refused** (row 1.16); stored ones are
  supported via `generatedAlwaysAs(expr)` (row 1.15).

## Client generation

There is none, and none is needed. Row types derive from the table objects
(`typeof users.$inferSelect` / `$inferInsert`); unknown keys, unknown
relations and wrong value types are compile errors. Coming from Prisma this
deletes the `prisma generate` step from your build; coming from Drizzle
nothing changes. (A separate CLI `neutron generate` can emit typed models for
six languages from an exported schema document, but the TypeScript path never
requires it.)

## Relational reads: `findMany` / `include`

```ts
const db = await createDatabase({
  url: process.env.DATABASE_URL!,
  tables: { users, posts },
  relations: { users: usersRelations, posts: postsRelations },
});

// one statement, typed nesting
const authors = await db.query.users.findMany({
  where: eq(users.email, "a@x.com"),
  with: { posts: { orderBy: [desc(posts.id)], limit: 10 } },
});
// authors: array of user rows; each .posts is a typed array (empty is [], never [null])

const author = await db.query.users.findFirst({ with: { posts: true } });
// author.posts: typed array; a missing to-one relation is null, never [] or a missing key
```

Semantics that differ from Prisma, stated exactly:

- **One statement per query** at any nesting depth: each relation edge is a
  correlated scalar subquery in the single SELECT — no N+1, no join fan-out
  between sibling to-many children.
- **Per-child options** — `where`, `orderBy`, `limit`, `offset`, `columns`
  (subset) — ride the nested `with` value; `limit` bounds each parent's
  children (the Prisma per-relation `take` equivalent). `limit`/`offset`/
  `orderBy` are rejected on to-one edges.
- **Nesting depth is structurally bounded** (`MAX_RELATION_DEPTH = 5`).
- **`relationName` pairs** disambiguate two foreign keys to the same target
  (`author` + `reviewer` both to `users`) — name the pair on both the `one()`
  and the `many()`.
- **Parent filtering runs before child expansion**: parent
  `where`/`orderBy`/`limit`/`offset` apply to parent rows; per-row subqueries
  then expand children for exactly the emitted parents.
- **Inspection without a connection**: `db.query.<t>.toSQL(args)` and
  `explainQuery(args)` compile purely.
- Relational reads require tables registered in `createDatabase`; CRUD
  builders do not.

For shapes the relational API does not cover — schema-qualified tables
(row 1.20), parent-correlated child filters — use the typed builder
(`db.select().from(...)` with `alias()` joins), which supports the full SQL
surface: all five join kinds with typed outer-join nullability, subqueries,
CTEs (`cteTable`) and derived tables (`derivedTable`), set operations,
window functions (`over`, `rowNumber`, ...), aggregates with PostgreSQL
result typing, `groupBy`/`having`/`distinct`, row locking
(`.for("update", { skipLocked: true })`), and keyset pagination (`keyset`).

## Writes

```ts
// create / createMany
const inserted = await db.insert(users).values({ email: "a@x.com" }).returning();
const many = await db.insert(posts).values([
  { userId: 1, title: "a" },
  { userId: 1, title: "b" },
]); // one multi-row INSERT: lands whole or fails whole

// upsert (Prisma upsert / Drizzle onConflictDoUpdate)
await db.insert(members)
  .values(row)
  .onConflictUpdate({
    target: members.email,            // column, composite array, { constraint: "name" } or { columns, where }
    set: { hits: sql`${excluded(members.hits)} + 1` },
    setWhere: sql`${members.hits} < ${10}`,   // optional conditional update
  })
  .returning(["email", "hits"]);

// skipDuplicates
await db.insert(members).values(rows).onConflictDoNothing();

// updateMany / deleteMany with affected-row semantics
const updated = await db.update(posts).set({ title: "x" }).where(eq(posts.id, 1));
```

- **DEFAULT vs NULL vs omission** follows the Prisma distinction: omitting a
  key emits DEFAULT (all-default rows work), explicit `null` writes SQL NULL,
  and explicit `null` on a NOT NULL column is rejected before execution.
- **`returning()`** reads back written rows typed and losslessly decoded;
  `.returning(["col", ...])` projects a subset.
- **Nested graph writes** (`db.query.<t>.create/update/delete`) cover the
  Prisma nested `create`/`connect`/`disconnect`/`update`/`delete` vocabulary
  with explicit per-edge ownership, unique-key selectors, guarded affected-row
  counts, and declared cascade dispositions — one transaction (a savepoint
  inside `db.transaction`).
- **`INSERT ... SELECT` is not implemented** — `.values()` takes plain rows;
  compose from queries via explicit SQL.
- Writes to generated columns and `GENERATED ALWAYS` identity columns are
  rejected at compile time and runtime; `serial` stays writable.

## Transactions

```ts
await db.transaction(
  async (tx) => {
    await tx.insert(posts).values({ userId: 1, title: "hello" });
    // nested transaction = a real SAVEPOINT; outer stays usable on inner failure
    await tx.transaction(async (tx2) => { /* ... */ });
  },
  {
    isolation: "serializable",   // "read-committed" | "repeatable-read" | "serializable"
    retry: { maxAttempts: 3, idempotent: true },
  },
);
```

- The interactive `$transaction` shape maps directly. The Prisma sequential
  array form maps to `db.batch([q1, q2, ...])` — one connection, one
  transaction (REPEATABLE READ default), typed result tuple.
- **Retry is opt-in and narrow**: only SQLSTATE `40001` (serialization
  failure) and `40P01` (deadlock), whole-callback replay, requires
  `idempotent: true`. This differs from hand-rolled retry loops you may be
  bringing over: a `CommitAmbiguityError` (unknown COMMIT outcome after a
  connection failure) is **never replayed automatically** — inspect state
  before resubmitting.
- **Cancellation reaches the server**: `deadlineMs` / `AbortSignal` options
  on statements and relational queries map Prisma's `timeout`/
  `AbortController` habits onto `pg_cancel_backend` (pg) or native
  `Query.cancel()` (postgres.js); canceled connections stay usable.
- Isolation levels are the three PostgreSQL actually supports; there is no
  `read-uncommitted`.

## Raw SQL escape hatches

- `` sql`select ${x} from t where id = ${id}` `` is the structural template:
  interpolated **values bind as parameters** (never spliced into the text),
  while columns, tables and AST nodes splice structurally. This is the
  `$queryRaw` equivalent and composes into builder slots.
- `trustSql(text, TRUSTED_SQL_ACK)` is the single trusted-text boundary for
  raw SQL fragments you author and vouch for; operators delimit it so spliced
  text cannot escape grouping.
- `raw(text, params)` plus `db.driver.query(...)` / `db.driver.execute(...)`
  is direct execution for parameterized raw text with `$n` placeholders.
- Legacy `{ sql, params }` fragments are rejected everywhere with a fail-closed
  error.

## Value semantics you will actually notice

| Column | Neutron read type | Prisma/Drizzle habit it replaces |
|---|---|---|
| int2/int4/serial | `number` | same as both |
| int8/bigint | `bigint` (default) / `string` / safe-checked `number` via mode | Prisma `BigInt`, Drizzle `bigint({ mode })` |
| numeric | exact decimal `string`; optional `decoder`; optional `{ precision, scale }` DDL typmod | Prisma `Decimal` object, Drizzle `numeric` string |
| float4/float8 | `number` (non-finite rejected on write) | same |
| timestamp/timestamptz | canonical microsecond string (default) or `Date` via `mode: "date"` (ms-truncated) | Prisma `DateTime` → `Date`; Drizzle `timestamp` → `Date` |
| date | `YYYY-MM-DD` string (Dates rejected on write) | |
| boolean/text/uuid | `boolean`/`string`/`string` | |
| bytea | `Uint8Array` | Prisma `Uint8Array` (Bytes) |
| json/jsonb | `unknown`; `jsonNull` writes JSON null vs SQL NULL | Prisma `JsonNull`/`DbNull` sentinel family |

Timezone semantics are deliberate: `timestamptz` reads canonical UTC strings
independent of process and server timezones; a `Date` writes its exact
instant. Code that relied on `Date` objects everywhere must either opt into
`mode: "date"` per column (accepting millisecond truncation) or handle
canonical strings.

## Adopting Neutron migrations deliberately

When a scope's migration authority should genuinely move to Neutron (for
example a new service sharing the database), the CLI path is:
`neutron schema pull` (introspect, read-only) or export
`exportSchemaV2` + `canonicalSchemaJson` from the TS schema;
`neutron schema baseline` to record an existing database as the snapshot-chain
root; `neutron migrate adopt` to graduate a legacy `_neutron_migrations`
history once, in one transaction; `neutron migrate generate --schema
neutron.schema.json --name ...` (rename-aware diff against the live database)
to write reviewable `{version}_{name}.up.sql` / `.down.sql` pairs;
`neutron migrate` to apply; `neutron migrate status` to inspect. Boundaries
to know before choosing this:

- The history table is `_neutron_migrations`; ordinary runs refuse legacy or
  unknown-format rows — adoption is the only graduation, and rows whose
  checksums cannot be verified stay unverified, never fabricated.
- Migrations run only an allowlist of statement kinds; functions, triggers,
  grants and `DO` blocks are refused, with no bypass flag. Such objects stay
  in your other runner or hand-maintained SQL.
- Down files are schema reversion, not data restoration; irreversible
  migrations are refused on `migrate down`.
- An interrupted non-transactional migration (e.g. `CREATE INDEX
  CONCURRENTLY`) leaves partial effects and no history row; `neutron migrate
  resolve <version>` inspects it and never replays a statement whose outcome
  it cannot prove.
- Transaction-pooled proxies are unsupported for migration connections
  (row 8.5).
- Objects marked `managed: false` in a schema document are never planned,
  compared or dropped — the escape hatch for coexistence with another owner.

**Expand/backfill/contract changes are manual** (row 5.7): there is no guided
workflow or tooling for long column-type changes. The supported manual
practice: add the compatible nullable/defaulted column, deploy readers and
writers that handle both shapes, run a bounded resumable backfill with
checkpoints, verify old/new parity, retire the old consumers, and contract in
a later migration. No destructive down migration that would lose the new
data; exercise forward repair instead.

## Python and Go

This guide is TypeScript-only because the native Python and Go paths are
deliberately **SQL-first**: raw SQL plus typed scanning (Python:
Pydantic models over asyncpg — rows 2.20, 4.11; Go: `Query[T]`/`QueryOne[T]`
struct scanning — rows 2.21, 4.12). There is no typed query builder, keyset
helper or savepoint surface in those SDKs; converting a TS ORM's query
habits to them means writing SQL. Keeping SQLAlchemy or GORM alongside the
native clients is an integration question, not a conversion this guide
covers.

## What has no verified equivalent (plain statements)

- **Prisma `@updatedAt`** — set the timestamp in each update or maintain a
  trigger through your migration runner.
- **Prisma Client middleware / client extensions** — no hook surface on the
  query path; the structured event logger (`logger: true`, redacted by
  default, `NEUTRON_SQL_LOG_PARAMS=1` opts into parameter logging) covers
  observability, not mutation.
- **Prisma Accelerate / Data Proxy / edge deployment** — no edge/browser
  runtime exists (row 8.4).
- **Multi-dialect support (SQLite/MySQL)** — none; Drizzle users who need
  LibSQL keep Drizzle through the `@neutron-build/data/drizzle` interop
  (`createDrizzleDatabase`, `typescript/packages/neutron-data/src/db/drizzle.ts`),
  which returns genuine drizzle-orm databases and can run alongside the
  first-party SQL path.
- **Studio-level guarantees for `drizzle-kit push`-style sync** — the
  prototype equivalent is `neutron db push` (apply schema straight to a
  disposable database); production workflow is generated, reviewed SQL
  files.

## Limitation index (capability-matrix rows cited in this guide)

| Row | Limitation | Status at this revision |
|---|---|---|
| 1.4 | `numeric(p, s)` declarable on the shared-contract subset (precision 1..1000, scale 0..precision); negative scale / scale > precision remain outside the contract | implemented — declare via `numeric(name, { precision, scale })`; runner-owned SQL for the wider domain |
| 1.13 | arrays of temporal/bytea/vector/serial elements refused | deliberately outside scope |
| 1.16 | virtual generated columns refused | deliberately outside scope |
| 1.20 | schema-qualified tables rejected by relational reads (`db.query`) | inspected absence — use CRUD/join builders |
| 1.22 | no auto-updatable-view writes | deliberately outside scope |
| 1.24 | domain/custom types not declarable; `.codec()` is query-layer value mapping | inspected absence |
| 2.20 / 2.21 | Python/Go have no typed query builder (SQL-first stance) | inspected absence (by design) |
| 4.11 / 4.12 | Python/Go no savepoint/isolation API surface | inspected absence |
| 5.7 | expand/backfill/contract workflow absent; manual practice documented above | unknown/absent tooling |
| 6.9 | no published upgrade/semver policy for the 0.1.0 package | unknown |
| 8.4 | no edge/browser runtime | deliberately outside scope |
| 8.5 | transaction poolers unsupported for migration runs | documented limitation |
| 8.7 | non-PostgreSQL databases unsupported | deliberately outside scope |

Source of record for the package surface: `typescript/packages/neutron-sql/`
(README, `src/index.ts` exports, `src/schema.ts` column factories,
`src/db.ts` `createDatabase`, `src/relations.ts`, `src/builder.ts`,
`src/transactions.ts`, `src/ast.ts`, `src/pagination.ts`). If this guide and
that source disagree, the source wins and this guide needs a fix.
