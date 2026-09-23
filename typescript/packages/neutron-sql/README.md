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
#    import { exportSchemaV2, canonicalSchemaJson } from "@neutron-build/sql";
#    import { users, posts } from "./schema.js";
#    writeFileSync("neutron.schema.json", canonicalSchemaJson(exportSchemaV2({ users, posts })));
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

Two export formats exist. `exportSchema()` emits the legacy version-1 shape
the current CLI planning commands still consume. `exportSchemaV2()` emits the
cross-language schema contract v2 (`contracts/data/schema-v2.json`) and
`canonicalSchemaJson()` serializes it deterministically: identical bytes and
SHA-256 for the same schema on every machine, input key order and process
timezone irrelevant — the Go CLI and the `contracts/data` reference consumer
reproduce the same canonical bytes (CI-pinned by the golden fixture
`contracts/data/golden/valid/exported-v2.json`). `readSchemaDocumentV1()`
is the explicit compatibility reader: it upgrades legacy v1 export documents
to v2 under the same rules as the Go upgrade reader
(`contracts/data/CANONICAL.md` §6) and reports every ambiguity
(`[ambiguous-default]`) instead of guessing literal vs expression. Columns
keep declaration order (PostgreSQL `attnum` semantics); tables, constraints
and indexes normalize as sorted sets in the canonical form. Schemas the v2
document cannot represent faithfully — a `serial` column with an explicit
default, a foreign key to a column outside the export or not covered by a
primary-key/unique constraint — fail at export time with the contract error
code, never as a silently invalid document.

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
the fallback **only when the postgres.js module is genuinely not installed**
— a loaded driver's connection or URL failure propagates instead of silently
switching libraries under you. Nucleus speaks the same wire protocol, but
this package has no test coverage against it — treat any non-Postgres engine
as unsupported until a conformance leg exists.

Driver choice and pool tuning: `createDatabase({ url, driverOptions: { driver: "pg", max: 10 } })`
(`driverOptions` replaces the pre-0.1 `driver` option, which now injects an
adapter — see below).

### Injected adapters and ownership

Pass an adapter you created instead of a URL: wrap an existing `pg` pool or
postgres.js client with `wrapPgPool(pool)` / `wrapPostgresJs(client)`. Wrapped
resources are **borrowed by default** — `db.close()` (and the driver's
`close()`) never ends them; the pool/client you injected stays yours and
functional. Pass `{ ownership: "owned" }` to transfer disposal instead.
Adapters this package creates from `url` are owned: `close()` terminates
them exactly once, and repeated or concurrent `close()` calls are idempotent
(no double-`end`). Every driver exposes `driver.lifecycle`
(`{ ownership, terminated, terminate() }`) so callers can inspect and drive
the lifecycle explicitly. Custom adapters passed via `driver:` must expose the
same `lifecycle` and `close()` — wrap an existing pool/client with
`wrapPgPool`/`wrapPostgresJs` to get a conforming adapter.

### Errors and capabilities

Driver errors surface as a stable taxonomy instead of message matching:
`MissingDriverError` (the npm package is absent), `ConnectionFailedError`
(transport: refused/timeout/socket died/pool ended — with `code`, `address`,
`port` when present), and `ServerSqlError` (the server answered with an SQL
error; `sqlstate` retained verbatim, plus `severity`/`detail`/`hint`/`position`,
original driver error as `cause`). `getSqlState(err)` walks wrapper chains.

`db.engine()` returns the connected engine's identity (one `SELECT VERSION()`,
memoized; Nucleus detection per the framework contract — an unrecognized
server is `unknown`, never assumed Postgres). `db.capability(name)` resolves
capability status as `supported` / `unsupported` / `unknown` with the evidence
that produced it: documented PostgreSQL version facts, or a side-effect-free
probe on engines without them. Compiled statements carry their capability
requirements; an `unknown` requirement fails closed with
`CapabilityRequirementError` before any SQL runs — unknown is never
all-enabled. Detection runs through whichever driver you brought; a plain
Postgres connection gains no Nucleus/model dependency.

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
  non-finite numbers anywhere inside). `sql` template expressions remain
  supported in `.set()` assignments on every column type except `json`/`jsonb`,
  where object values always bind as values.

## One compiler, structural fragments

Every statement this package emits — `select`/`insert`/`update`/`delete`,
projections, `returning`, relational aggregation — is built as a frozen AST
and rendered by a single one-traversal compiler (`compileStatement`).
Placeholders and the params array are produced in the same pass, so they can
never disagree; nothing on this path ever scans or renumbers SQL text. The
old regex-splicing assembly was deleted in F04, which also fixed a real
precedence hole: chained `.where()` calls are now individually parenthesized
(`where (a or b) and (c or d)`) instead of raw-joined with `and`.

Grouping guarantee (F04 rework): **every operator the compiler applies —
`and`/`or`/`not`, comparisons, the where-list join — parenthesizes its
fragment and `trustSql` operands**, so a top-level `or`/`and`/`not` inside
spliced text can never escape the operator it was passed to
(`and(eq(a), sql`b or c`)` compiles to `((a = $1) and (b or c))`). Call-form
arguments need no wrap: the call's own parentheses and commas delimit each
argument. Fragments nested inside fragment *text* compose verbatim — the
author of the outer template owns that text; the compiler only delimits what
it applies itself.

- `sql\`…\`` is the structural template: interpolated **values bind as
  parameters** (never spliced), while columns, tables, other nodes and
  `trustSql(text, TRUSTED_SQL_ACK)` segments splice structurally. A literal
  `"$1"` inside a value or a dollar-quoted string can never collide with
  placeholder numbering. (This was `sqlAst` during F01–F03; the names
  collapsed in F04 and `sqlAst` remains as a deprecated alias.)
- Raw parameterized text survives only through the explicit direct-execution
  escape hatch: `raw(text, params)` executed via `driver.query`/`driver.execute`.
  Its `$n` placeholders are never interpolated into other statements — that
  would require scanning raw SQL. Passing a legacy `{sql, params}` fragment
  into ANY builder slot (`where`, `orderBy`, `set`, projections, connective
  arguments, insert values, relational args, template interpolation) throws
  the same fail-closed rejection with instructions to rebuild it with `sql`,
  bind a plain value, or run it directly via `raw()` + `driver.query`.
- Builders are immutable: every fluent call (`.where()`, `.set()`, `.values()`,
  `.limit()`, …) returns a new frozen builder. Reusing a base query in two
  requests cannot leak filters between them, and `.toSQL()` is pure — the
  same builder state compiles to byte-identical SQL.
- Compiled statements carry their projection **decode plans** (built from the
  serializable per-column `ColumnCodec`) and their engine **capability
  requirements** (statements using `to_jsonb`/`jsonb_agg` acquisition carry
  `jsonb-functions`; engines without those functions must reject them).
- The compiler reserves the `__q<number>` alias namespace for generated
  derived-table aliases; user aliases in that namespace are rejected.

**Migration note (F04, pre-1.0 breaking changes).** The `sql` template now
returns a structural fragment node instead of `{sql, params}`; destructured
`.sql`/`.params` accesses on `eq()`/`and()` results no longer exist
(inspect conditions through `.toSQL()`/`.toCompiled()`). `asc()`/`desc()`
return order specs (still accepted by `orderBy`). Update/delete `where`
clauses and `returning` lists are now uniformly parenthesized/qualified —
semantics unchanged, byte-level SQL text shifted. Legacy fragments are
`raw()` + direct execution only.

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

Predicate values (`eq`/`ne`/`lt`/`lte`/`gt`/`gte`/`inArray`) run through
the same codec: validated with column context and bound at the same safe sites.
Values interpolated into a raw `sql\`…\`` template bind as plain parameters —
no column codec touches them (use the typed predicates when you want
validation).

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

## Joins and aliases

All five join forms — `innerJoin`, `leftJoin`, `rightJoin`, `fullJoin`,
`crossJoin` — are available on the typed select builder. Joins take
**`alias()` handles**, not raw tables: `const p = alias(posts, "p")`, then
`.innerJoin(p, sql`${p.userId} = ${users.id}`)`. The alias is the table's
identity inside the statement — every reference (`p.title` in projections,
`eq(p.title, …)` in predicates, `asc(p.id)` in order specs) compiles to the
alias-qualified `"p"."title"`, which is what makes self joins and same-name
tables unambiguous. Two joins may not share an alias, and a join alias may
not collide with the from table's name — both fail before any SQL runs.

- **ON conditions are conditions** (the same AST values `where` takes):
  typed predicates (`eq(p.authorId, 7)`), `sql` fragments, and `and`/`or`/
  `not` combinators all work. The grouping guarantee applies: fragments
  passed to an operator are delimited, so `and(sql`a or b`, eq(…))` keeps
  the intended grouping. Column-to-column comparisons are `sql` templates
  interpolating two columns (`sql`${p.userId} = ${users.id}``). Cross joins
  take no ON.
- **Outer-join nullability is typed.** A column from the nullable side of a
  left/right/full join reads as its declared type **`| null`** — including
  NOT NULL columns, which the outer join can still null out. `leftJoin`
  nulls the joined alias; `rightJoin` nulls the from side; `fullJoin` nulls
  both; `innerJoin`/`crossJoin` add no nulls. Nulls decode through the same
  codecs as everything else: a left-joined timestamp cell that is SQL NULL
  reads `null`, never an epoch string, and int8/numeric NULLs stay `null`.
- **Output mapping rule.** With an explicit projection the row keys are
  exactly the projection keys — JavaScript object keys are unique, so two
  same-named columns (from two schemas, or a self join) simply need two
  projection keys (`{ pub: users.email, alt: au.email }`). Without an
  explicit projection the row is the **from table's columns only** — joined
  tables never leak into the default projection, so property keys cannot
  collide.
- **Parameter order is deterministic**: projections first, then each join's
  ON condition in call order, then `where`, then `order by` — `$1..$n`
  follow that traversal; `limit`/`offset` render inline.
- **Expression projections** over joined columns (`sql`${o.qty} * ${2}``)
  type as `unknown` — there is no column codec behind an arbitrary
  expression.
- Builders stay immutable: every `.join…()` call forks, so a base query can
  be reused with different joins without leakage, and `.toSQL()` is pure.

### Schema-qualified tables

`pgSchema("legacy").table("users", { … })` declares `legacy.users`. The
query layer supports them end to end: CRUD renders qualified targets
(`insert into "legacy"."users"`), alias joins render qualified join targets,
and predicates/projections reference schema-qualified columns — `public`
users and `legacy` users can be joined in one statement under distinct
aliases. This is deliberately **query-layer only**: DDL emission
(`schemaToDDL`), schema export (`exportSchema`/`exportSchemaV2`) and
relational reads (`db.query`, the `tables`/`relations` inputs) reject
schema-declared tables with explicit errors until their scoped work lands
(Q05/Q07) — they would otherwise address or export the wrong (search-path)
identity.

## Subqueries, CTEs, aggregates and set operations

- **Grouping and aggregates.** `.groupBy(col | expr, …)` sets the grouping
  terms; `.having(cond)` takes the same AST values `.where()` takes
  (aggregates, `sql` fragments, `and`/`or`/`not` — fragments are delimited,
  the grouping guarantee applies); `.distinct()` emits plain `select
  distinct`. Typed aggregates — `count()`, `count(col)`,
  `countDistinct(col)`, `sum`, `avg`, `min`, `max`, `stringAgg(col, sep)`,
  `boolAnd`, `boolOr` — project and order (`desc(count())`) with result
  types that follow PostgreSQL exactly: `count` returns **bigint through
  the int8 codec and is never null** (0 on empty input); `sum` over integer
  columns returns bigint, over int8/numeric columns exact decimal strings,
  over floats numbers; `avg` returns exact strings except over floats; and
  `sum`/`avg`/`min`/`max`/`string_agg`/`bool_and`/`bool_or` are **null on
  empty input**. `min`/`max` mirror their argument column's codec (temporal
  modes honored; int8 modes honored) — over a derived/CTE temporal column
  the aggregate parses the materialized canonical text back to the temporal
  type first (`min(col::timestamp)`), so composed `timestamptz` never
  consults the session timezone. A grouped query over an empty table
  returns zero rows — no groups exist.
- **Derived tables and CTEs.** `derivedTable(name, source)` and
  `cteTable(name, source)` turn any select builder into a table-like
  handle; the row type derives from the source's projection outputs.
  `derivedTable` inlines `(select …) as "name"` at each reference
  (from or join); `cteTable` references the bare name and auto-registers
  `with "name" as (…)` on the consuming statement — referencing the same
  handle twice registers one CTE, and two different statements under one
  name fail closed. Handles join under their own name (no `alias()` wrap;
  re-aliasing a handle is rejected because it would lose the subquery), and
  outer joins over a handle null its columns exactly like alias handles.
  Source capability requirements merge into the consuming statement.
  Temporal columns compose losslessly: a derived/CTE level materializes the
  canonical text wire form, and the outer level re-acquires it with the
  naive pass-through `to_jsonb(col::timestamp)::text` — for `timestamptz`
  too (the canonical text IS the UTC wall clock; re-parsing with
  `::timestamptz` would consult the session timezone and shift the value
  per composition level). The acquisition is idempotent, so values stay
  byte-exact through any depth in any session timezone.
- **CTE references inside `sql` fragments resolve or fail closed.**
  Interpolating a `cteTable` handle (or one of its columns) into a `sql`
  template renders the bare name, branded with the handle's CTE identity;
  compilation fails when the consuming statement does not carry that exact
  CTE in scope — an unregistered reference, or a same-name CTE built from a
  different statement that would silently capture it, is a compile-time
  error rather than a silent bind (or a missing-relation database error).
  Reference the handle from `from`/`joins` (its CTE registers) or register
  the same source with `withCte`. Interpolating a whole `derivedTable`
  handle into a fragment is rejected outright — a derived table exists only
  at its inline from/join site. Hand-typed `ident`/`qual` references stay
  trusted text (never scanned).
- **Recursive CTEs.** `with recursive` is computed structurally: a CTE whose
  statement references its own name (as a from/join target or qualified
  reference) renders recursively. Reference the working table through
  `ident("name")` / `qual("name", "col")` nodes in the recursive branch —
  a self-reference hand-typed inside `sql` fragment text is not scanned
  (this compiler never parses SQL text) and fails at the database instead.
  Cycle safety is the SQL contract: traverse cyclic graphs with `union`
  (deduplicating) or an explicit depth bound on `union all`.
- **Correlated subqueries.** `sql` templates interpolate subqueries and
  builder `.subquery()` nodes anywhere a value goes — scalar subqueries in
  projections, `in (select …)` lists, ON conditions — with parameters
  binding in traversal order. `exists(source)` builds the `exists`
  predicate.
- **Set operations.** `.union/.unionAll/.intersect/.intersectAll/
  .except/.exceptAll(other)` on a select builder (and on the resulting
  compound) compose **left-associatively in call order** — branches render
  parenthesized, so SQL's intersect-over-union precedence never reorders
  your chain. Result rows are typed by (and decode through) the **first
  branch** — PostgreSQL takes output column names from it, so branch
  projections must be structurally aligned. `orderBy`/`limit`/`offset`
  after a set operation apply to the whole compound; a first branch that
  already carries them fails closed instead of silently rebinding them.
- **Parameter and alias composition is deterministic through three
  levels**: outer projections bind first, then derived-table/CTE bodies,
  then where/group/having, then set-op branches, then order by — `$1..$n`
  follow that traversal in every composed shape.
- Derived/CTE handles are query-surface identities: mutations, DDL, schema
  export and `db.query` registration reject them.

## Conflict handling and upserts

`db.insert(table).values(...)` chains into PostgreSQL's `ON CONFLICT` clauses
(Q03). Both actions compile through the same one-traversal AST — placeholders
bind values first, then `do update set` assignments, then predicates, so
`$1..$n` are deterministic in every shape.

```ts
// ignore unique violations: conflicted rows are simply not inserted
await db.insert(members).values(rows).onConflictDoNothing();

// partial unique index: the target carries the index predicate
await db.insert(soft).values(row).onConflictDoNothing({
  target: [soft.email],
  where: sql`${soft.deletedAt} is null`,
});

// the upsert: excluded(col) references the row proposed for insertion
await db.insert(members).values(row)
  .onConflictUpdate({
    target: members.email,                    // a column, a composite array,
                                              // { constraint: "name" } for
                                              // ON CONSTRAINT, or
                                              // { columns, where } for a
                                              // partial unique index
    set: { hits: sql`${excluded(members.hits)} + 1`, score: "2.50" },
    setWhere: sql`${members.hits} < ${10}`,   // conditional update (optional)
  })
  .returning(["email", "hits"]);              // selected subset, losslessly
                                             // decoded like every projection
```

- **Targets.** A single column, a composite list, `{ columns, where }` with
  the index predicate (partial unique index inference), or
  `{ constraint: "name" }` for `on conflict on constraint`. `DO UPDATE`
  requires a target (PostgreSQL rejects targetless `DO UPDATE`);
  `DO NOTHING` without one lets PostgreSQL arbitrate over any unique index.
  Target columns must belong to the inserted table; an index predicate is
  only valid with a column-list target.
- **`excluded(col)`** renders `excluded."col"` — the proposed row — in SET
  assignments and predicates. References naming a column the table does not
  have fail before SQL; `excluded()` anywhere outside on-conflict clauses
  (e.g. a plain `update ... .set()`) fails closed the same way.
- **SET values** follow update `.set()` semantics: literals run through the
  column codec (validated, canonically encoded), `sql` fragments and
  expression/excluded/subquery nodes splice structurally on non-json columns,
  and `null` respects NOT NULL.
- **Returning arity is honest.** Upsert returning yields one row per input
  row that was inserted or updated (0..n). `DO NOTHING` **omits** conflicted
  rows from returning; a `DO UPDATE ... setWhere` that rejects a conflicted
  row means it is neither updated nor returned. The non-returning insert
  resolves to the affected-row count (0 when everything conflicted away).
- **Returning subsets.** `.returning(["email", "hits"])` on insert and update
  builders projects exactly those property keys — same lossless codec path
  as full returning (`int8`/`numeric` exact, temporal microseconds, `bytea`
  bytes). Unknown or duplicate keys are rejected before SQL.
- **Batch atomicity is statement atomicity.** One `.values([...])` call
  compiles to ONE multi-row `insert` statement: it either lands whole or
  fails whole (a mid-batch unique violation without `on conflict` leaves
  nothing behind). Splitting large imports into chunks is a **caller
  policy** — chunk boundaries, per-chunk error handling and resume are
  deliberately not implemented here and belong to future import tooling.
- **Duplicate assignments error deterministically.** Assigning the same
  physical column twice — two `.set()` calls, two property keys mapping to
  one physical column in a `.set()`/insert row/on-conflict SET map, a
  duplicated conflict-target list — throws **before SQL** with one message
  regardless of object key or call order. (Overlap between the insert's
  columns and `DO UPDATE SET` is legal upsert semantics, not a conflict.)
  Two rows in one batch that conflict with *each other* under `DO UPDATE`
  surface PostgreSQL's own error — `ON CONFLICT DO UPDATE command cannot
  affect row a second time` (SQLSTATE `21000`) — unchanged, with the
  statement atomic (nothing lands).

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
  `delete`, `returning`), batch inserts independent of key order, joins and
  aliases (inner/left/right/full/cross with typed outer-join nullability,
  self joins, schema-qualified tables in the query layer), subqueries,
  ordinary/recursive CTEs, derived tables, group by/having/distinct, typed
  aggregates with PostgreSQL empty-input semantics (count never null,
  sum/avg/min/max/string_agg/bool_and/bool_or null on empty), correlated
  subqueries and union/intersect/except (plus their ALL variants),
  on-conflict do-nothing/do-update with named/composite/constraint/partial-index
  targets, excluded references, conditional upserts and selected returning
  subsets (order-independent duplicate-assignment rejection included),
  one-level
  relational reads with exact result types, transactions, `toSQL()`, mapped
  properties/NULL/required-key semantics, lossless codecs (bigint/string/
  safe-number int8 modes, exact numerics, microsecond temporals, bytea,
  SQL NULL vs JSON null writes) across raw select, projection, `returning`
  and relation paths on both drivers, verified under multiple process
  timezones and (for derived/CTE composition and composed aggregates)
  multiple server session timezones. Since F04 every statement compiles through the one AST compiler
  with immutable builders, compiled statements carry decode plans and
  capability requirements, and schema export v2 is deterministic and
  cross-language-pinned (Go + reference consumer agree byte-for-byte);
  importing the root loads no driver module until a connection is requested.
- `update`/`delete` require `.where()` (foot-gun guard).
- Deferred with explicit rejection, not implemented: nested/per-relation
  `with` and repeated targets (Q05), generated/identity columns —
  the schema cannot declare them yet, so the "generated-field writes are
  rejected" guarantee lands with them; `serial` stays writable per PostgreSQL
  semantics (Q07), composite constraints/enums/arrays/views in migrations
  (M02+), schema-qualified tables in DDL emission/schema export/relational
  reads — the query layer supports them (Q05/Q07), composite/no-key mutation
  in Studio (S01).
- Studio: `neutron studio` opens the SQL browser (filter, sort, FK links)
  against a Postgres connection URL; cell edits are permitted only on tables
  with a proven single-column primary key — composite/no-key tables are
  read-only with an explanation (interim until full-key identities land).

MIT.
