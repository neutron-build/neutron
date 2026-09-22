# @neutron-build/sql

Drizzle-shaped TypeScript SQL ORM for Postgres. Schema in code, no codegen,
one readable SQL statement per query, `toSQL()` on everything. Zero runtime
dependencies — bring `postgres` or `pg` (or both) as optional peers.

**Alpha — contained, not production-ready.** The verified support matrix is
PostgreSQL 17 only: CI runs the live suites against the `postgres:17` image
with both drivers; the machine that produced the recorded live evidence runs
17.11. Postgres 16/18 are release-time matrix work and are **not** claimed.
Nucleus and other Postgres-wire engines are untested here — no compatibility
claim is made for them (see [Status](#status-v01)).

## Quick start

Not yet published to npm — the first `ts/v*` tag of this monorepo publishes
it. Until then use it from the repo (`typescript/packages/neutron-sql`) as a
workspace dependency.

```ts
import { createDatabase, pgTable, serial, integer, text, timestamp, relations, eq } from "@neutron-build/sql";

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
});

const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.userId], references: [users.id] }),
}));

const db = await createDatabase({
  url: process.env.DATABASE_URL!,
  tables: { users, posts },
  relations: { users: usersRelations, posts: postsRelations },
  logger: true, // prints every SQL statement with timing in dev
});

// SQL builder — typed CRUD
const inserted = await db.insert(users).values({ email: "a@x.com" }).returning();
await db.update(users).set({ email: "b@x.com" }).where(eq(users.id, inserted[0].id!));
const rows = await db.select().from(users).where(eq(users.email, "b@x.com")).limit(10);

// Relational reads — one statement, typed nesting
const author = await db.query.users.findFirst({
  where: eq(users.email, "b@x.com"),
  with: { posts: true },
});
// author.posts: typed array; empty relations are [], never [null]
// Unknown relation names in `with` are compile errors

// Inspect without executing
console.log(db.select().from(users).toSQL());

// Transactions
await db.transaction(async (tx) => {
  await tx.insert(posts).values({ userId: 1, title: "hello" });
});
```

Types come from the schema (`typeof users.$inferSelect` / `$inferInsert`); there is no
codegen step and `db` never types as `unknown`.

## Migrations

```bash
# 1. export the schema as JSON — export-schema.mjs next to your schema:
#    import { writeFileSync } from "node:fs";
#    import { exportSchema } from "@neutron-build/sql";
#    import { users, posts } from "./schema.js";
#    writeFileSync("neutron.schema.json", JSON.stringify(exportSchema({ users, posts })));
node export-schema.mjs

# 2. point the CLI at a disposable database (env or --url)
export DATABASE_URL=postgres://user@localhost:5432/mydb

# prototype: push straight to the database
neutron db push --schema neutron.schema.json

# real migrations: reviewable SQL files, rename-aware
neutron migrate generate --schema neutron.schema.json --name add_users
neutron migrate generate --rename 'users.name>users.full_name'   # quote it: > is a shell redirect
neutron migrate          # apply (history table: _neutron_migrations)
neutron migrate status
```

`migrate generate` diffs the exported schema against the live database
(information_schema) and writes `{version}_{name}.up.sql` / `.down.sql` pairs.
When a column is added and another with the same type is dropped, it prints a
suggestion — confirm intent with `--rename 'users.old>users.new'` (the `>`
must be quoted in the shell; without quotes the shell reads it as a redirect
and the flag never reaches the CLI).

Safety semantics (contained-alpha): generated SQL never drops `_neutron_*`
metadata tables, extension-owned objects, or anything absent from the schema
unless `--allow-destructive` is passed as an explicit acknowledgement.
Catalog structures the diff cannot represent faithfully are rejected with an
error instead of producing a migration that falsely claims synchronization.
The supported surface is single-column primary keys/unique/FK constraints,
not-null and defaults, and named indexes — composite constraints, enums,
arrays and views are not certified and are reported/rejected rather than
silently mishandled.

## Drivers

`postgres` (postgres.js) and `pg` (node-postgres) are both supported and
live-tested; with no explicit choice, `postgres` is tried first and `pg` is
the fallback. Nucleus speaks the same wire protocol, but this package has no
test coverage against it — treat any non-Postgres engine as unsupported until
a conformance leg exists.

## Nucleus-only column types

`vector("embedding", 1536)` is accepted in the same schema file. On vanilla Postgres the
migration generator skips it with a `-- NUCLEUS-ONLY` comment and a warning instead of
emitting DDL that would fail.

## Property mapping, NULL and required keys

All `select`, projections, `returning` (insert/update/delete) and relational
reads return rows keyed by the declared JS property names (`firstName`), never
by physical SQL names (`first_name`). Projections use the requested key:
`db.select({ displayName: people.firstName })` returns rows keyed by
`displayName`. Predicates and assignments always compile to physical qualified
identifiers: `.set({ firstName: "Ada" })` emits `set "first_name" = $1`.
Mapping is a schema-metadata lookup, never a name-spelling transform — two
properties whose names differ only by case style (`firstName` -> `first_name`
and `first_name` -> `"firstName"`) each map to their own physical column.

Table metadata (name, column map, indexes) lives in a symbol-keyed internal
record, not in plain properties, so user columns named `columns`, `tableName`
or `indexes` are ordinary columns and cannot clobber it — CRUD, `returning`,
relational reads, DDL and schema export all work on such tables. Read
metadata through the exported helpers `getTableName(table)`,
`getTableColumns(table)` and `getTableIndexes(table)`; the pre-alpha plain
`table.tableName` / `table.columns` properties are gone. Insert and select
types are exact: required insert keys are NOT NULL-without-default columns
(primary keys imply NOT NULL; `serial` implies a server default), nullable
columns accept `null`, and invalid value types, unknown keys, unknown
relations and nested `with` (depth 2+) are compile errors as well as runtime
errors.

- Nullable columns accept `null`: `.set({ nick: null })` binds SQL NULL; the
  empty string stays an empty string. `undefined` in `.set()` is ignored, and
  an update whose `.set()` leaves no assignments is an error.
- Required insert keys are enforced at runtime before execution. Required =
  NOT NULL without default; primary keys imply NOT NULL; `serial` columns
  imply a server default. Explicit `null` for a NOT NULL column is rejected
  before execution on insert and update.
- Invalid mutation values are rejected before execution with column context:
  nested plain objects, arrays for non-array columns, `Date` for non-temporal
  columns, symbols and functions. Without this, the drivers silently corrupt
  data (pg JSON-stringifies objects into text columns; postgres.js stores
  `"[object Object]"`). `json`/`jsonb` columns accept JSON-representable
  values (recursively checked — no `bigint`, `symbol`, `undefined` or
  non-finite numbers anywhere inside). `sql` fragments remain supported in
  `.set()` assignments on every column type except `json`/`jsonb`, where
  object values always bind as values.

### Lossless value codecs (and the corrected pre-1.0 types)

Values never silently lose precision. Schema-known columns are acquired as
lossless text in SQL (`to_jsonb(col)::text` temporals, `::text` int8/numeric
inside JSON aggregations) or as driver-native values that are already exact
(int8/numeric strings, bytea buffers), then decoded per column mode. No
global driver parser is ever mutated, and lossless acquisition happens
**before** any driver Date parsing or `JSON.parse` — a decoder cannot recover
microseconds a `Date` already dropped or digits a double already rounded.

| SQL type | Default TS value | Notes |
|---|---|---|
| int2/int4/serial | `number` | integer + range validated on write |
| int8 (bigint) | `bigint` | never passes through JS Number; `mode: "string"` keeps the exact string; `mode: "number"` is a checked safe-number mode that rejects values outside ±(2^53−1) |
| numeric | `string` | exact decimal string (scale and trailing zeros preserved); optional per-column `decoder` — a decoder that narrows to number owns the precision loss |
| float4/float8 | `number` | non-finite numbers (NaN/±Infinity) are rejected on write |
| date | `string` (`YYYY-MM-DD`) | no timezone interpretation; Date values are rejected on write |
| timestamp (without tz) | `string` (`YYYY-MM-DDTHH:MM:SS[.ffffff]`) | timezone-free canonical string, microseconds preserved; explicit `mode: "date"` interprets the wall clock as UTC and truncates to milliseconds |
| timestamptz | `string` (`…Z`, canonical UTC) | microseconds preserved, independent of process and server timezones; explicit `mode: "date"` gives the exact instant at millisecond precision |
| boolean/text/uuid | `boolean`/`string`/`string` | uuid format validated on write |
| bytea | `Uint8Array` | drivers hand back buffers |
| json/jsonb | `unknown` | SQL NULL vs JSON null distinguished on writes (below) |

Temporal **writes** take canonical strings (microsecond-exact) or `Date`
values. A `Date` carries only an instant at millisecond precision: it stores
its UTC wall clock into `timestamp` columns — the same `Date` stores the same
value on every machine and process timezone — and its exact instant into
`timestamptz`. `infinity`/`-infinity` temporal strings pass through; Date mode
rejects them. Canonical write strings are validated with column context
(timestamps reject offsets; timestamptz requires `Z` or `±HH:MM`; dates must
be exactly `YYYY-MM-DD`).

**SQL NULL vs JSON null (writes).** For `json`/`jsonb` columns the JS value is
the JSON value: `null` binds SQL NULL, the exported `jsonNull` sentinel writes
the JSON null value, and every other JSON-representable value (including
strings, booleans and nested arrays) is JSON-encoded by the codec —
`"null"` the string stores the JSON string `"null"`, never JSON null.
Timestamp/json parameters bind as canonical text at explicitly text-typed
sites (`$n::text::jsonb`), which both drivers pass through untouched:
postgres.js otherwise re-encodes server-typed date/json parameters through
`new Date(...).toISOString()` (timezone shift + millisecond truncation) and
double-`JSON.stringify`s strings. On read, SQL NULL and JSON null both arrive
as JS `null` — the distinction is preserved for writes and queryable with
`sql` fragments (`data is null` vs `data = 'null'::jsonb`).

Predicate values (`eq`/`ne`/`lt`/`lte`/`gt`/`gte`/`inArray`) run through the
same codec: validated with column context and bound at the same safe sites.
Values inside raw `sql` fragments are yours — no codec touches them.

**Migration note (corrected pre-1.0 types).** Before this change: `bigint`
columns read as `string`, `timestamp`/`timestamptz`/`date` columns read as
`Date` (driver-parsed — microseconds truncated, `timestamp` reads shifted by
the process timezone, `date` reads disagreed between drivers), relation-child
`bytea` leaves read as `\x` hex strings, and json string writes were
interpreted as JSON text. Now int8 reads `bigint` by default (parse with a
safe-integer check, or declare `mode: "string"`), temporals read canonical
microsecond-exact strings (declare `mode: "date"` where a `Date` is worth the
millisecond truncation), temporal writes accept canonical strings as well as
`Date`s, `date` writes take `YYYY-MM-DD` strings only, and json writes encode
the JS value. Code that relied on `number` for int8 or `Date` for temporals
must switch to the new types or opt into the explicit modes.

## Relational reads (one level)

`db.query.<table>.findMany/findFirst` compile every requested relation edge to
its own **independent correlated scalar subquery** — a to-many child aggregates
with `jsonb_agg(... order by <target primary key>)` inside a subquery, a to-one
parent builds a single `jsonb_build_object` that is `null` when the foreign key
misses. Sibling relations never join each other, so requesting two to-many
children returns each child set exactly (no cartesian fan-out), and parent
`where`/`orderBy`/`limit`/`offset` apply to parent rows before child expansion.
Child order is deterministic (ordered by the target's primary key).

- Result types are exact at one level: requesting `with: { posts: true }` makes
  `posts` a required key typed as the target's relation-child model (`| null`
  for to-one, array for to-many); unrequested relation keys are absent, and
  `columns: [...]` restricts the row to those property keys at both runtime and
  compile time. Unknown relation names in `with` and unknown property keys in
  `columns` are compile errors (and runtime errors when values arrive without
  static types).
- Child leaf values decode through the same codecs as the flat path:
  `bigint` leaves arrive as the column's mode value (default `bigint`),
  `numeric` leaves as exact decimal strings (rendered `::text` inside the
  aggregation), `timestamp`/`date` leaves as their canonical strings,
  `timestamptz` leaves as their canonical UTC string (`to_jsonb(col at time
  zone 'UTC')` — session-timezone independent, microseconds intact) and
  `bytea` leaves as `Uint8Array` decoded from the `\x` hex text form. The
  casts apply only to the projected JSON — correlation predicates and
  ordering keys compare raw columns.
- When a table has two foreign keys to the same target (`posts.author` +
  `posts.reviewer`), name the pair with `relationName` on both the `one()` and
  the `many()`. Reverse inference without names is allowed only when exactly
  one candidate exists; otherwise relation resolution fails with an error
  naming the candidates.
- Explicitly rejected with clear errors (deferred, not implemented): nested
  `with` / per-relation options (only `true` is accepted), the same target
  table twice in one `with` clause (query them separately), empty
  `columns` arrays, and `columns` values that are not arrays. A `many()`
  relation whose target table has no primary key is rejected at
  `createDatabase` time — child ordering would be undefined.

## Tests

- `pnpm test` builds, type-checks the consumer type fixture against the packed
  `dist/*.d.ts` declarations (`types.consumer/`), and runs the unit suites plus
  the live Postgres suites (both drivers).
- Live suites run only when `NEUTRON_TEST_DATABASE_URL` points at a
  disposable Postgres URL. Each suite creates and drops its own uniquely
  named throwaway database — never point it at shared data. (The older
  `NEUTRON_SQL_TEST_URL` name still works as an alias.)
- `NEUTRON_LIVE_REQUIRED=1` (what CI sets) flips a missing URL, an
  unreachable server, or a run that executes zero live cases into failures.
  Locally, with no URL configured, every live test skips with a printed
  reason and the unit suites still run.

## Status (v0.1)

Alpha — contained, not production-ready. Known-unsafe paths found in review
were fixed or converted into explicit rejections; nothing here certifies
general-purpose use.

- Verified against PostgreSQL 17 only (CI `postgres:17`; recorded live
  evidence on 17.11), both drivers. 16/18 and non-Postgres engines: not
  claimed.
- Implemented and live-tested: typed CRUD (`select`/`insert`/`update`/
  `delete`, `returning`), batch inserts independent of key order, one-level
  relational reads with exact result types, transactions, `toSQL()`, mapped
  properties/NULL/required-key semantics, lossless codecs (bigint/string/
  safe-number int8 modes, exact numerics, microsecond temporals, bytea,
  SQL NULL vs JSON null writes) across raw select, projection, `returning`
  and relation paths on both drivers, verified under multiple process
  timezones.
- `update`/`delete` require `.where()` (foot-gun guard).
- Deferred with explicit rejection, not implemented: nested/per-relation
  `with` and repeated targets (Q05), generated/identity columns —
  the schema cannot declare them yet, so the "generated-field writes are
  rejected" guarantee lands with them; `serial` stays writable per PostgreSQL
  semantics (Q07), composite constraints/enums/arrays/views in migrations
  (M02+), composite/no-key mutation in Studio (S01).
- Studio: `neutron studio` opens the SQL browser (filter, sort, FK links)
  against a Postgres connection URL; cell edits are permitted only on tables
  with a proven single-column primary key — composite/no-key tables are
  read-only with an explanation (interim until full-key identities land).

MIT.
