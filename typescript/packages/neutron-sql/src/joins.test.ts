import assert from "node:assert/strict";
import test from "node:test";
import {
  alias,
  and,
  asc,
  astSelect,
  canonicalSchemaJson,
  compileStatement,
  createDatabase,
  eq,
  exportSchema,
  exportSchemaV2,
  ident,
  inArray,
  isAliasHandle,
  not,
  or,
  pgSchema,
  pgTable,
  bigint,
  integer,
  numeric,
  projection,
  qual,
  selectStatement,
  serial,
  text,
  timestamp,
  schemaToDDL,
  sql,
  type Condition,
  type SelectBuilder,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q01 — joins and aliases (V07 unit leg).
// Deterministic SQL text, ON-condition grouping, parameter ordering across
// projection + join + where + limit, immutable builders, alias identity,
// schema-qualified tables and the documented output-mapping rule. Live V13
// oracles against hand-written SQL live in live.joins.postgres.test.ts.
// ---------------------------------------------------------------------------

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  name: text("name"),
});

const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  note: timestamp("note"),
  total: numeric("total"),
  ref: bigint("ref"),
});

const employees = pgTable("employees", {
  id: serial("id").primaryKey(),
  managerId: integer("manager_id"),
  mentorId: integer("mentor_id"),
  name: text("name").notNull(),
});

const alt = pgSchema("alt");
const altUsers = alt.table("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
});

// Snapshot URL is inert: postgres.js connects lazily and no query here runs.
const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { users, orders, employees },
});

// ---------------------------------------------------------------------------
// Deterministic SQL text — the five join forms
// ---------------------------------------------------------------------------

test("joins: inner/left/right/full/cross compile to exact SQL with alias-qualified references", () => {
  const onColToCol = (): Condition => sql`${alias(orders, "o").userId} = ${users.id}`;

  const inner = db.select({ email: users.email, total: alias(orders, "o").total }).from(users).innerJoin(alias(orders, "o"), onColToCol()).toSQL();
  assert.equal(
    inner.sql,
    'select "users"."email", "o"."total" from "users" inner join "orders" as "o" on "o"."user_id" = "users"."id"',
  );
  assert.deepEqual(inner.params, []);

  const left = db.select({ email: users.email }).from(users).leftJoin(alias(orders, "o"), onColToCol()).toSQL();
  assert.equal(
    left.sql,
    'select "users"."email" from "users" left join "orders" as "o" on "o"."user_id" = "users"."id"',
  );

  const right = db.select({ email: users.email }).from(users).rightJoin(alias(orders, "o"), onColToCol()).toSQL();
  assert.equal(
    right.sql,
    'select "users"."email" from "users" right join "orders" as "o" on "o"."user_id" = "users"."id"',
  );

  const full = db.select({ email: users.email }).from(users).fullJoin(alias(orders, "o"), onColToCol()).toSQL();
  assert.equal(
    full.sql,
    'select "users"."email" from "users" full join "orders" as "o" on "o"."user_id" = "users"."id"',
  );

  const colors = pgTable("colors", { id: serial("id").primaryKey() });
  const cross = db.select({ email: users.email }).from(users).crossJoin(alias(colors, "c")).toSQL();
  assert.equal(cross.sql, 'select "users"."email" from "users" cross join "colors" as "c"');
});

test("joins: ON conditions keep operator precedence — fragments are delimited, exprs self-parenthesize", () => {
  const o = alias(orders, "o");
  // and(frag-with-or, eq): the fragment's top-level `or` must not escape.
  const grouped = db.select({ email: users.email }).from(users)
    .innerJoin(o, and(sql`${o.total} > ${10} or ${o.total} is null`, eq(o.userId, 1)))
    .toSQL();
  assert.equal(
    grouped.sql,
    'select "users"."email" from "users" inner join "orders" as "o" on (("o"."total" > $1 or "o"."total" is null) and ("o"."user_id" = $2))',
  );
  assert.deepEqual(grouped.params, [10, 1]);

  // not(or(eq, frag))
  const negated = db.select({ email: users.email }).from(users)
    .leftJoin(o, not(or(eq(o.userId, 1), sql`${o.total} < ${5}`)))
    .toSQL();
  assert.equal(
    negated.sql,
    'select "users"."email" from "users" left join "orders" as "o" on (not (("o"."user_id" = $1) or ("o"."total" < $2)))',
  );
  assert.deepEqual(negated.params, [1, 5]);

  // A bare fragment ON keeps its authored text (the ON position applies no
  // operator of its own) — mixed and/or inside is the author's grouping.
  const bare = db.select({ email: users.email }).from(users)
    .innerJoin(o, sql`${o.userId} = ${users.id} and ${o.total} > ${0}`)
    .toSQL();
  assert.equal(
    bare.sql,
    'select "users"."email" from "users" inner join "orders" as "o" on "o"."user_id" = "users"."id" and "o"."total" > $1',
  );
});

test("joins: parameters number projection → on → where in traversal order; limit stays inline", () => {
  const o = alias(orders, "o");
  const q = db.select({ bumped: sql`${users.id} + ${1}` })
    .from(users)
    .innerJoin(o, sql`${o.userId} = ${users.id} and ${o.total} > ${2}`)
    .where(eq(users.email, "a@x.com"))
    .orderBy(asc(o.total))
    .limit(5)
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."id" + $1 as "bumped" from "users" inner join "orders" as "o" ' +
      'on "o"."user_id" = "users"."id" and "o"."total" > $2 ' +
      'where ("users"."email" = $3) order by "o"."total" asc limit 5',
  );
  assert.deepEqual(q.params, [1, 2, "a@x.com"]);
});

test("joins: chained joins compile in call order with the documented output mapping", () => {
  const o = alias(orders, "o");
  const u2 = alias(users, "reviewer");
  const q = db.select({ email: users.email, total: o.total, reviewer: u2.email })
    .from(users)
    .innerJoin(o, sql`${o.userId} = ${users.id}`)
    .leftJoin(u2, sql`${u2.id} = ${o.id}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."email", "o"."total", "reviewer"."email" as "reviewer" from "users" ' +
      'inner join "orders" as "o" on "o"."user_id" = "users"."id" ' +
      'left join "users" as "reviewer" on "reviewer"."id" = "o"."id"',
  );
});

test("joins: default projection with a join stays the from table's columns", () => {
  const q = db.select().from(users).leftJoin(alias(orders, "o"), sql`${alias(orders, "o").userId} = ${users.id}`).toSQL();
  assert.equal(
    q.sql,
    'select "users"."id", "users"."email", "users"."name" from "users" left join "orders" as "o" on "o"."user_id" = "users"."id"',
  );
});

// ---------------------------------------------------------------------------
// Self joins and schema-qualified tables
// ---------------------------------------------------------------------------

test("joins: one table twice (self join) uses distinct aliases end to end", () => {
  const mgr = alias(employees, "mgr");
  const mentor = alias(employees, "mentor");
  const q = db.select({ name: employees.name, manager: mgr.name, mentorName: mentor.name })
    .from(employees)
    .leftJoin(mgr, sql`${mgr.id} = ${employees.managerId}`)
    .leftJoin(mentor, sql`${mentor.id} = ${employees.mentorId}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "employees"."name", "mgr"."name" as "manager", "mentor"."name" as "mentorName" from "employees" ' +
      'left join "employees" as "mgr" on "mgr"."id" = "employees"."manager_id" ' +
      'left join "employees" as "mentor" on "mentor"."id" = "employees"."mentor_id"',
  );
});

test("joins: same SQL table name in two schemas joins without collision", () => {
  const au = alias(altUsers, "au");
  const q = db.select({ pub: users.email, altEmail: au.email })
    .from(users)
    .innerJoin(au, sql`${au.id} = ${users.id}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."email" as "pub", "au"."email" as "altEmail" from "users" inner join "alt"."users" as "au" on "au"."id" = "users"."id"',
  );

  // CRUD on a schema-qualified table alone renders qualified targets.
  const sel = db.select().from(altUsers).where(eq(altUsers.email, "a@x.com")).toSQL();
  assert.equal(sel.sql, 'select "alt"."users"."id", "alt"."users"."email" from "alt"."users" where ("alt"."users"."email" = $1)');
  const ins = db.insert(altUsers).values({ email: "a@x.com" }).toSQL();
  assert.equal(ins.sql, 'insert into "alt"."users" ("email") values ($1)');
  const upd = db.update(altUsers).set({ email: "b@x.com" }).where(eq(altUsers.id, 1)).toSQL();
  assert.equal(upd.sql, 'update "alt"."users" set "email" = $1 where ("alt"."users"."id" = $2)');
  const del = db.delete(altUsers).where(eq(altUsers.id, 1)).toSQL();
  assert.equal(del.sql, 'delete from "alt"."users" where ("alt"."users"."id" = $1)');

  // Predicates on schema tables are schema-qualified (not the public twin).
  const pred = db.select().from(users).where(inArray(altUsers.email, ["x", "y"])).toSQL();
  assert.equal(pred.sql, 'select "users"."id", "users"."email", "users"."name" from "users" where ("alt"."users"."email" in ($1, $2))');
});

test("joins: schema-qualified surfaces outside the query layer (Q07)", () => {
  assert.throws(() => schemaToDDL([altUsers]), /legacy DDL emitter covers the default search path only; use schema export v2/);
  assert.throws(() => exportSchema({ altUsers }), /v1 export shape covers the default search path only/);
  // Q07: export v2 owns schema-qualified export now.
  const doc = exportSchemaV2({ altUsers });
  assert.equal(doc.tables.length, 1);
  assert.deepEqual(doc.tables[0].identity, { schema: "alt", name: "users" });
  assert.deepEqual(doc.schemas.map((s) => s.name), ["alt", "public"]);
  assert.doesNotThrow(() => JSON.parse(canonicalSchemaJson(doc)));
  assert.rejects(
    createDatabase({ url: "postgres://snapshot:nouser@127.0.0.1:1/none", driverOptions: { driver: "postgres" }, tables: { altUsers } }),
    /Q05\/Q07/,
  );
});

// ---------------------------------------------------------------------------
// Alias identity, rejections, immutability
// ---------------------------------------------------------------------------

test("joins: joins require alias() handles; alias names are validated", () => {
  const posts2 = () => pgTable("posts2", { id: serial("id").primaryKey(), userId: integer("user_id").notNull() });
  const p = alias(posts2(), "p");
  assert.throws(() => db.select().from(users).leftJoin(posts2() as never, sql`1 = 1`), /alias\(\) handles/);
  assert.throws(() => alias(p, "q"), /already an alias handle/);
  assert.throws(() => alias(users, "a.b"), /must not contain "\."/);
  assert.throws(() => alias(users, "__q1"), /reserved/);
  assert.throws(() => alias(users, ""), /non-empty/);
  void p;
});

test("joins: colliding aliases fail before SQL runs", () => {
  const u = alias(users, "users");
  assert.throws(
    () => db.select().from(users).innerJoin(u, sql`${u.id} = ${users.id}`).toSQL(),
    /alias "users" collides with the from table/,
  );
  const a1 = alias(orders, "dup");
  const a2 = alias(orders, "dup");
  assert.throws(
    () => db.select().from(users).innerJoin(a1, sql`${a1.userId} = ${users.id}`).innerJoin(a2, sql`${a2.id} = ${users.id}`).toSQL(),
    /alias "dup" collides with a previous join/,
  );
});

test("joins: legacy fragments are rejected in the on slot with the shared hint", () => {
  const legacy = { sql: "1 = $1", params: [1] };
  assert.throws(
    () => db.select().from(users).innerJoin(alias(orders, "o"), legacy as never),
    /legacy SqlFragment .* cannot be used here .* renumbering/s,
  );
});

// ---------------------------------------------------------------------------
// Q01 review carry-forwards fixed in Q02: crossJoin wrappers reject a runtime
// ON argument; whole-table sql interpolation renders schema-qualified.
// ---------------------------------------------------------------------------

test("joins: crossJoin wrappers reject a runtime ON argument instead of dropping it", () => {
  const c = alias(orders, "cx");
  assert.throws(
    () => db.select({ email: users.email }).from(users).crossJoin(c, sql`${c.total} > ${1}` as never),
    /crossJoin: cross joins take no on condition/,
  );
  assert.throws(
    () => astSelect({ id: qual("t", "id") }).from(ident("t")).crossJoin(ident("t"), "x", sql`1 = 1` as never),
    /ast crossJoin: cross joins take no on condition/,
  );
  // The underlying .join() rejection still fires too.
  assert.throws(
    () => astSelect().from(users).join("cross", ident("x"), "cx2", sql`1 = 1` as never),
    /cross joins take no on/,
  );
  // Explicit undefined is treated as absent (no silent cartesian trap).
  const ok = db.select({ email: users.email }).from(users).crossJoin(c, undefined).toSQL();
  assert.equal(ok.sql, 'select "users"."email" from "users" cross join "orders" as "cx"');
});

test("joins: whole-table sql interpolation renders schema-qualified references", () => {
  const frag = sql`select * from ${altUsers}`;
  const stmt = selectStatement({ projections: [projection(frag)], from: ident("x") });
  const { sql: text } = compileStatement(stmt);
  assert.equal(text, 'select select * from "alt"."users" from "x"');
  // Default-search-path tables and alias handles keep their one-part form.
  const plain = compileStatement(selectStatement({ projections: [projection(sql`select * from ${users}`)], from: ident("x") }));
  assert.equal(plain.sql, 'select select * from "users" from "x"');
  const au = alias(altUsers, "au");
  const handle = compileStatement(selectStatement({ projections: [projection(sql`select * from ${au}`)], from: ident("x") }));
  assert.equal(handle.sql, 'select select * from "au" from "x"');
});

test("joins: builders stay immutable across join forks", () => {
  const base = db.select({ email: users.email }).from(users);
  const forked = base.leftJoin(alias(orders, "o"), sql`${alias(orders, "o").userId} = ${users.id}`);
  assert.equal(base.toSQL().sql, 'select "users"."email" from "users"');
  assert.match(forked.toSQL().sql, /left join "orders" as "o"/);
  const once = forked.toSQL();
  const twice = forked.toSQL();
  assert.equal(once.sql, twice.sql);
  assert.deepEqual(once.params, twice.params);
  assert.equal(Object.isFrozen(forked), true);
  // projectionSpec copy-on-fork: caller mutation never reaches a sibling
  const proj = { email: users.email };
  const b2 = db.select(proj).from(users);
  (proj as Record<string, unknown>).evil = sql`1`;
  assert.equal(b2.toSQL().sql, 'select "users"."email" from "users"');
});

test("joins: alias handles are join identities, never mutation targets or from tables", () => {
  const u = alias(users, "u");
  assert.throws(() => db.insert(u).values({ email: "x" }), /alias handles are join identities/);
  assert.throws(() => db.update(u).set({ email: "x" }).where(eq(u.id, 1)), /alias handles are join identities/);
  assert.throws(() => db.delete(u).where(eq(u.id, 1)), /alias handles are join identities/);
  assert.throws(() => db.select().from(u), /alias handles are join identities/);
  assert.throws(() => astSelect().from(u), /alias handles are join identities/);
  assert.throws(() => astSelect().from(users).innerJoin(u, "u", sql`1 = 1`), /alias handles are join identities/);
  assert.equal(isAliasHandle(u), true);
  assert.equal(isAliasHandle(users), false);
});

test("joins: typed predicates and order specs on aliased columns reference the alias", () => {
  const o = alias(orders, "o");
  const q = db.select({ email: users.email })
    .from(users)
    .innerJoin(o, eq(o.userId, 7))
    .where(and(eq(o.total, "12.50"), not(eq(o.userId, 8))))
    .orderBy(asc(o.total))
    .toSQL();
  assert.equal(
    q.sql,
    'select "users"."email" from "users" inner join "orders" as "o" on ("o"."user_id" = $1) ' +
      'where ("o"."total" = $2) and (not ("o"."user_id" = $3)) order by "o"."total" asc',
  );
  assert.deepEqual(q.params, [7, "12.50", 8]);
});

// ---------------------------------------------------------------------------
// Decode plans: joined columns decode through the codec seam; outer-join
// nulls decode as null, never zero values
// ---------------------------------------------------------------------------

test("joins: temporal joined columns acquire lossless wire forms and decoders keyed by output key", () => {
  const o = alias(orders, "o");
  const compiled = db.select({ email: users.email, note: o.note, ref: o.ref })
    .from(users)
    .leftJoin(o, sql`${o.userId} = ${users.id}`)
    .toCompiled();
  assert.match(compiled.sql, /to_jsonb\("o"\."note"\)::text as "note"/);
  const keys = compiled.decoders.map((d) => d.key);
  assert.deepEqual(keys.sort(), ["note", "ref"]);
  assert.deepEqual(compiled.capabilities, ["jsonb-functions"]);
});

test("joins: outer-join null cells decode as null, real values decode canonically", async () => {
  const o = alias(orders, "o");
  const compiled = db.select({ note: o.note, ref: o.ref }).from(users).leftJoin(o, sql`${o.userId} = ${users.id}`).toCompiled();
  const rows: Array<Record<string, unknown>> = [
    { note: null, ref: null },
    { note: '"2026-01-02T03:04:05.678912"', ref: "9007199254740993" },
  ];
  const { applyProjectionDecoders } = await import("./index.js");
  applyProjectionDecoders(rows, compiled.decoders);
  assert.equal(rows[0].note, null);
  assert.equal(rows[0].ref, null);
  assert.notEqual(rows[0].note, "");
  assert.notEqual(rows[0].note, "1970-01-01T00:00:00");
  assert.equal(rows[1].note, "2026-01-02T03:04:05.678912");
  assert.equal(rows[1].ref, 9007199254740993n);
});

// ---------------------------------------------------------------------------
// AST builder: convenience join forms + qualified targets
// ---------------------------------------------------------------------------

test("astSelect: right/full/cross convenience methods and qualified-name targets", () => {
  const q = astSelect({ id: qual("t", "id") })
    .from(qual("alt", "users"), undefined)
    .rightJoin(ident("orders"), "o", sql`${ident("o")}."user_id" = ${qual("alt", "users", "id")}`)
    .fullJoin(qual("public", "users"), "u", sql`${ident("u")}."id" = ${ident("o")}."user_id"`)
    .crossJoin(ident("colors"), "c")
    .toSQL();
  assert.equal(
    q.sql,
    'select "t"."id" as "id" from "alt"."users" ' +
      'right join "orders" as "o" on "o"."user_id" = "alt"."users"."id" ' +
      'full join "public"."users" as "u" on "u"."id" = "o"."user_id" ' +
      'cross join "colors" as "c"',
  );
  assert.throws(() => astSelect().from(users).join("cross", ident("x"), "cx", sql`1 = 1` as never), /cross joins take no on/);
});

test("astSelect: schema-declared tables render qualified from/join targets", () => {
  const q = astSelect({ email: altUsers.email }).from(altUsers).innerJoin(users, "u", sql`${ident("u")}."id" = ${altUsers.id}`).toSQL();
  assert.equal(
    q.sql,
    'select "alt"."users"."email" from "alt"."users" inner join "users" as "u" on "u"."id" = "alt"."users"."id"',
  );
});

// ---------------------------------------------------------------------------
// V04 typing: outer-join nullability is exact at the type level
// ---------------------------------------------------------------------------

async function joinTypeFixtures(): Promise<void> {
  const o = alias(orders, "o");
  const mgr = alias(employees, "mgr");

  const inner = db.select({ email: users.email, total: o.total }).from(users).innerJoin(o, sql`${o.userId} = ${users.id}`);
  const eqInner: AssertEq<Awaited<typeof inner>[number], { email: string; total: string | null }> = true;

  const left = db.select({ email: users.email, total: o.total, ref: o.ref }).from(users).leftJoin(o, sql`${o.userId} = ${users.id}`);
  const eqLeft: AssertEq<Awaited<typeof left>[number], { email: string; total: string | null; ref: bigint | null }> = true;
  // @ts-expect-error left-joined columns are nullable even when the column is NOT NULL
  const badLeft: { email: string; total: string; ref: bigint } = ({} as Awaited<typeof left>[number]);

  const right = db.select({ email: users.email, total: o.total }).from(users).rightJoin(o, sql`${o.userId} = ${users.id}`);
  const eqRight: AssertEq<Awaited<typeof right>[number], { email: string | null; total: string | null }> = true;
  // @ts-expect-error the from side is the nullable side under a RIGHT join
  const badRight: { email: string; total: string } = ({} as Awaited<typeof right>[number]);

  const full = db.select({ email: users.email, total: o.total }).from(users).fullJoin(o, sql`${o.userId} = ${users.id}`);
  const eqFull: AssertEq<Awaited<typeof full>[number], { email: string | null; total: string | null }> = true;

  const cross = db.select({ email: users.email, total: o.total }).from(users).crossJoin(o);
  const eqCross: AssertEq<Awaited<typeof cross>[number], { email: string; total: string | null }> = true;

  // default projection under a right join: every from-table field widens
  const defaultRight = db.select().from(users).rightJoin(o, sql`${o.userId} = ${users.id}`);
  const eqDefaultRight: AssertEq<Awaited<typeof defaultRight>[number], { id: number | null; email: string | null; name: string | null }> = true;

  // a later inner join does not rescue an earlier left join's nullability
  const mixed = db.select({ manager: mgr.name })
    .from(employees)
    .leftJoin(mgr, sql`${mgr.id} = ${employees.managerId}`);
  const eqMixed: AssertEq<Awaited<typeof mixed>[number], { manager: string | null }> = true;

  // self join: two handles of one table type independently
  const mentor = alias(employees, "mentor");
  const selfJoin = db.select({ name: employees.name, manager: mgr.name, mentorName: mentor.name })
    .from(employees)
    .innerJoin(mgr, sql`${mgr.id} = ${employees.managerId}`)
    .leftJoin(mentor, sql`${mentor.id} = ${employees.mentorId}`);
  const eqSelf: AssertEq<Awaited<typeof selfJoin>[number], { name: string; manager: string; mentorName: string | null }> = true;

  // same SQL names in different schemas: keys and types stay distinct
  const au = alias(altUsers, "au");
  const twoSchemas = db.select({ pub: users.email, altEmail: au.email })
    .from(users)
    .leftJoin(au, sql`${au.id} = ${users.id}`);
  const eqSchemas: AssertEq<Awaited<typeof twoSchemas>[number], { pub: string; altEmail: string | null }> = true;

  // expression projections over joined columns are unknown until a decoder exists
  const exprProj = db.select({ scaled: sql`${o.total} * ${2}` }).from(users).innerJoin(o, sql`${o.userId} = ${users.id}`);
  const eqExpr: AssertEq<Awaited<typeof exprProj>[number], { scaled: unknown }> = true;

  // the alias is carried on the handle's column types (the nullability key)
  const tag: "o" = o.userId.aliasTag;
  // @ts-expect-error the tag is the literal alias, not any string
  const wrongTag: "x" = o.userId.aliasTag;

  // @ts-expect-error raw tables (not alias handles) are join-rejected
  db.select({ email: users.email }).from(users).leftJoin(orders, sql`1 = 1`);
  db.insert(o).values({ userId: 1 }); // runtime-rejected: handles are not mutation targets (asserted above)

  void [eqInner, eqLeft, eqRight, eqFull, eqCross, eqDefaultRight, eqMixed, eqSelf, eqSchemas, eqExpr, badLeft, badRight, tag, wrongTag];
  void [inner, left, right, full, cross, defaultRight, mixed, selfJoin, twoSchemas, exprProj];
}
void joinTypeFixtures;

// ---------------------------------------------------------------------------
// Seeded determinism fuzz over the typed join surface
// ---------------------------------------------------------------------------

test("fuzz: 150 seeded joined selects compile deterministically with sequential placeholders", () => {
  function mulberry32(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const rng = mulberry32(0x20260923);
  const pick = <T>(arr: readonly T[]): T => arr[Math.floor(rng() * arr.length)];
  const int = (min: number, max: number): number => min + Math.floor(rng() * (max - min + 1));

  const aliasNames = ["o", "x", "mgr", "au", "rev"] as const;
  const joinKinds = ["inner", "left", "right", "full", "cross"] as const;
  const totals = ["0", "1.50", "90071992547409.99"] as const;

  let withJoin = 0;
  for (let i = 0; i < 150; i++) {
    const picked: string[] = [];
    // The accumulator holds the base shape; each fork's wider nullability
    // parameters are erased back onto it (this fuzz checks determinism and
    // placeholder numbering, not nullability typing — that has its own
    // fixtures above).
    type Q = SelectBuilder<{ email: typeof users.email }, unknown, never, false>;
    let b: Q = db.select({ email: users.email }).from(users);
    const joins = int(0, 3);
    for (let j = 0; j < joins; j++) {
      let name = pick(aliasNames);
      while (picked.includes(name)) name = pick(aliasNames);
      picked.push(name);
      const handle = alias(orders, name);
      const on = sql`${handle.userId} = ${users.id} and ${handle.total} > ${pick(totals)}`;
      const kind = pick(joinKinds);
      b = (kind === "inner"
        ? b.innerJoin(handle, on)
        : kind === "left"
          ? b.leftJoin(handle, on)
          : kind === "right"
            ? b.rightJoin(handle, on)
            : kind === "full"
              ? b.fullJoin(handle, on)
              : b.crossJoin(handle)) as Q;
      withJoin++;
    }
    if (rng() < 0.5) b = b.where(eq(users.email, `u${i}@x.com`));
    if (rng() < 0.4) b = b.orderBy(asc(users.id));
    if (rng() < 0.3) b = b.limit(int(1, 10));

    const first = b.toSQL();
    const second = b.toSQL();
    assert.equal(second.sql, first.sql, `case ${i}: recompile differs`);
    assert.deepEqual(second.params, first.params, `case ${i}: params differ on recompile`);
    const placeholders = [...first.sql.matchAll(/\$(\d+)/g)].map((m) => Number(m[1]));
    assert.deepEqual(placeholders, placeholders.map((_, k) => k + 1), `case ${i}: placeholders must be $1..$n in order`);
    assert.equal(first.params.length, placeholders.length, `case ${i}: params count matches placeholders`);
  }
  assert.ok(withJoin >= 150, `joins must be well represented (got ${withJoin})`);
});
