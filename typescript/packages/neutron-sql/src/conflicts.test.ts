import assert from "node:assert/strict";
import test from "node:test";
import {
  astSelect,
  compileStatement,
  createDatabase,
  eq,
  excluded,
  fragment,
  ident,
  insertStatement,
  integer,
  numeric,
  onConflictClause,
  param,
  pgTable,
  projection,
  qual,
  raw,
  selectStatement,
  serial,
  sql,
  subquery,
  text,
  timestamp,
  type NeutronDatabase,
} from "./index.js";

// ---------------------------------------------------------------------------
// Q03 — conflict handling and write expressions (unit leg).
// Deterministic SQL text for on-conflict clauses, parameter ordering across
// values + set + predicates, returning subsets, fragment precedence in
// conflict clauses, and the order-independent duplicate-assignment detector.
// Live V13 oracles with real uniqueness conflicts live in
// live.conflicts.postgres.test.ts.
// ---------------------------------------------------------------------------

type AssertEq<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const members = pgTable("members", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  hits: integer("hits").notNull().default(0),
  score: numeric("score"),
  seen: timestamp("seen"),
});

// Two property keys mapping to ONE physical column (the duplicate-assignment
// detector's trigger) plus normal columns.
const dupPhysical = pgTable("dup_physical", {
  id: serial("id").primaryKey(),
  first: text("shared_col").notNull(),
  second: text("shared_col").notNull(),
  email: text("email").notNull(),
});

const db: NeutronDatabase = await createDatabase({
  url: "postgres://unit-fixture:not-run@localhost:1/none",
  tables: { members, dupPhysical },
});

// ---------------------------------------------------------------------------
// SQL text shapes
// ---------------------------------------------------------------------------

test("conflict: do nothing without target lets PostgreSQL arbitrate", () => {
  const out = db.insert(members).values({ email: "a@x" }).onConflictDoNothing().toSQL();
  assert.equal(out.sql, 'insert into "members" ("email") values ($1) on conflict do nothing');
  assert.deepEqual(out.params, ["a@x"]);
});

test("conflict: do nothing with a named target column", () => {
  const out = db.insert(members).values({ email: "a@x" }).onConflictDoNothing({ target: members.email }).toSQL();
  assert.equal(out.sql, 'insert into "members" ("email") values ($1) on conflict ("email") do nothing');
});

test("conflict: do nothing with a composite target renders the ordered list", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictDoNothing({ target: [members.email, members.hits] })
    .toSQL();
  assert.equal(out.sql, 'insert into "members" ("email") values ($1) on conflict ("email", "hits") do nothing');
});

test("conflict: on constraint target", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictDoNothing({ target: { constraint: "members_email_key" } })
    .toSQL();
  assert.equal(out.sql, 'insert into "members" ("email") values ($1) on conflict on constraint "members_email_key" do nothing');
});

test("conflict: partial-index target predicate renders after the column list", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictDoNothing({ target: [members.email], where: sql`${members.seen} is null` })
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) on conflict ("email") where ("members"."seen" is null) do nothing',
  );
});

test("conflict: same predicate via the target object's where", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictDoNothing({ target: { columns: [members.email], where: sql`${members.seen} is null` } })
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) on conflict ("email") where ("members"."seen" is null) do nothing',
  );
});

test("conflict: do update with literal set binds params after the values", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x", hits: 3, score: "1.5" })
    .onConflictUpdate({ target: members.email, set: { hits: 9, score: "2.50" } })
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email", "hits", "score") values ($1, $2, $3) on conflict ("email") do update set "hits" = $4, "score" = $5',
  );
  assert.deepEqual(out.params, ["a@x", 3, "1.5", 9, "2.50"]);
});

test("conflict: excluded references render the pseudo-relation", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x", hits: 3 })
    .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)} + 1` } })
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email", "hits") values ($1, $2) on conflict ("email") do update set "hits" = "excluded"."hits" + 1',
  );
});

test("conflict: conditional upsert renders targetWhere then setWhere, fragments delimited", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictUpdate({
      target: { columns: [members.email], where: sql`${members.seen} is null` },
      set: { hits: sql`${excluded(members.hits)}` },
      setWhere: sql`${members.hits} > ${2} or ${members.score} is null`,
    })
    .toSQL();
  // The setWhere fragment carries a top-level `or`; the Grouping guarantee
  // (F04) delimits it so it can never escape the DO UPDATE predicate.
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) on conflict ("email") where ("members"."seen" is null) ' +
      'do update set "hits" = "excluded"."hits" where ("members"."hits" > $2 or "members"."score" is null)',
  );
  assert.deepEqual(out.params, ["a@x", 2]);
});

test("conflict: multiple values rows then set then predicates keep sequential placeholders", () => {
  const out = db
    .insert(members)
    .values([
      { email: "a@x", hits: 1 },
      { email: "b@x", hits: 2 },
    ])
    .onConflictUpdate({
      target: members.email,
      set: { hits: sql`${excluded(members.hits)} + ${10}`, score: "3.5" },
      setWhere: sql`${members.score} <> ${"9.9"}`,
    })
    .toSQL();
  assert.deepEqual(out.params, ["a@x", 1, "b@x", 2, 10, "3.5", "9.9"]);
  assert.equal(
    out.sql,
    'insert into "members" ("email", "hits") values ($1, $2), ($3, $4) on conflict ("email") ' +
      'do update set "hits" = "excluded"."hits" + $5, "score" = $6 where ("members"."score" <> $7)',
  );
});

test("conflict: set may take an eq()-style condition node as an expression value", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictUpdate({ target: members.email, set: { score: sql`coalesce(${excluded(members.score)}, '0')` } })
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) on conflict ("email") do update set "score" = coalesce("excluded"."score", \'0\')',
  );
});

test("conflict: toSQL is pure — repeated compiles are byte-identical", () => {
  const b = db
    .insert(members)
    .values({ email: "a@x", hits: 1 })
    .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)} + ${1}` }, setWhere: sql`${members.score} is not null` });
  assert.deepEqual(b.toSQL(), b.toSQL());
});

test("conflict: builder is immutable — the base insert keeps its own clause state", () => {
  const base = db.insert(members).values({ email: "a@x" });
  const nothing = base.onConflictDoNothing();
  const update = base.onConflictUpdate({ target: members.email, set: { hits: 1 } });
  assert.ok(!base.toSQL().sql.includes("on conflict"), "the forked builder must not leak its clause back");
  assert.ok(nothing.toSQL().sql.includes("do nothing"));
  assert.ok(update.toSQL().sql.includes("do update"));
});

// ---------------------------------------------------------------------------
// Returning subsets
// ---------------------------------------------------------------------------

test("returning: selected subsets render only the requested projections", () => {
  const out = db.insert(members).values({ email: "a@x" }).returning(["email", "score", "seen"]).toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) returning "members"."email", "members"."score", to_jsonb("members"."seen")::text as "seen"',
  );
});

test("returning: single-key shorthand and update/delete builders share the subset surface", () => {
  const one = db.insert(members).values({ email: "a@x" }).returning("email").toSQL();
  assert.equal(one.sql, 'insert into "members" ("email") values ($1) returning "members"."email"');
  const upd = db.update(members).set({ hits: 1 }).where(eq(members.id, 1)).returning(["email", "seen"]).toSQL();
  assert.equal(
    upd.sql,
    'update "members" set "hits" = $1 where ("members"."id" = $2) returning "members"."email", to_jsonb("members"."seen")::text as "seen"',
  );
});

test("returning: subset composes with on-conflict clauses", () => {
  const out = db
    .insert(members)
    .values({ email: "a@x" })
    .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)}` } })
    .returning(["email", "hits"])
    .toSQL();
  assert.equal(
    out.sql,
    'insert into "members" ("email") values ($1) on conflict ("email") do update set "hits" = "excluded"."hits" returning "members"."email", "members"."hits"',
  );
});

test("returning: exact subset row types", () => {
  const rows = db.insert(members).values({ email: "a@x" }).returning(["email", "score"]);
  const eqT: AssertEq<Awaited<typeof rows>[number], { email: string; score: string | null }> = true;
  void eqT;
  const updRows = db.update(members).set({ hits: 1 }).where(eq(members.id, 1)).returning(["id", "seen"]);
  const eqU: AssertEq<Awaited<typeof updRows>[number], { id: number; seen: string | null }> = true;
  void eqU;
  const full = db.insert(members).values({ email: "a@x" }).returning();
  const eqF: AssertEq<Awaited<typeof full>[number], { id: number; email: string; hits: number; score: string | null; seen: string | null }> = true;
  void eqF;
});

test("returning: unknown and duplicate subset keys are rejected before SQL", () => {
  assert.throws(() => db.insert(members).values({ email: "a@x" }).returning(["nope"] as never), /unknown column "nope"/);
  assert.throws(() => db.insert(members).values({ email: "a@x" }).returning(["id", "id"]), /column "id" is selected twice/);
  assert.throws(() => db.insert(members).values({ email: "a@x" }).returning([]), /select at least one column/);
  assert.throws(() => db.update(members).set({ hits: 1 }).where(eq(members.id, 1)).returning([]), /select at least one column/);
});

// ---------------------------------------------------------------------------
// Duplicate/conflicting assignments — order-independent errors
// ---------------------------------------------------------------------------

test("assignments: update .set() twice on one physical column errors deterministically (both call orders)", () => {
  const err1 = captureError(() => db.update(dupPhysical).set({ first: "x" }).set({ second: "y" }).where(eq(dupPhysical.id, 1)).toSQL());
  const err2 = captureError(() => db.update(dupPhysical).set({ second: "y" }).set({ first: "x" }).where(eq(dupPhysical.id, 1)).toSQL());
  assert.ok(err1 instanceof Error, "chained duplicate .set() must error");
  assert.ok(err2 instanceof Error);
  assert.equal(err1.message, err2.message, "chained-set duplicate errors must not depend on call order");
  assert.match(err1.message, /physical column "shared_col" is assigned twice/);
});

test("assignments: duplicate-physical errors are key-order invariant", () => {
  // Same object content, two insertion orders of the keys.
  const row1 = { first: "x", second: "y", email: "e@x" };
  const row2 = { email: "e@x", second: "y", first: "x" };
  const err1 = captureError(() => db.insert(dupPhysical).values(row1).toSQL());
  const err2 = captureError(() => db.insert(dupPhysical).values(row2).toSQL());
  assert.ok(err1 instanceof Error);
  assert.ok(err2 instanceof Error);
  assert.equal(err1.message, err2.message, "duplicate-assignment errors must not depend on key order");
  assert.match(err1.message, /physical column "shared_col" is assigned twice/);
  assert.match(err1.message, /"first", "second"/);

  const set1 = captureError(() => db.update(dupPhysical).set({ first: "x", second: "y" }).where(eq(dupPhysical.id, 1)).toSQL());
  const set2 = captureError(() => db.update(dupPhysical).set({ second: "y", first: "x" }).where(eq(dupPhysical.id, 1)).toSQL());
  assert.ok(set1 instanceof Error && set2 instanceof Error);
  assert.equal(set1.message, set2.message);
  assert.match(set1.message, /physical column "shared_col" is assigned twice/);

  const up1 = captureError(() =>
    db.insert(dupPhysical).values({ email: "e@x", first: "f", second: "s" }).onConflictUpdate({ target: dupPhysical.email, set: { first: "x", second: "y" } }).toSQL(),
  );
  const up2 = captureError(() =>
    db.insert(dupPhysical).values({ first: "f", second: "s", email: "e@x" }).onConflictUpdate({ target: dupPhysical.email, set: { second: "y", first: "x" } }).toSQL(),
  );
  assert.ok(up1 instanceof Error && up2 instanceof Error);
  assert.equal(up1.message, up2.message);
  assert.match(up1.message, /physical column "shared_col" is assigned twice/);
});

test("assignments: chained .set() duplicates error with the same message in both orders", () => {
  const err1 = captureError(() => db.update(dupPhysical).set({ first: "x" }).set({ second: "y" }).where(eq(dupPhysical.id, 1)).toSQL());
  const err2 = captureError(() => db.update(dupPhysical).set({ second: "y" }).set({ first: "x" }).where(eq(dupPhysical.id, 1)).toSQL());
  assert.ok(err1 instanceof Error && err2 instanceof Error);
  assert.equal(err1.message, err2.message, "chained-set duplicate errors must not depend on call order");
  assert.match(err1.message, /physical column "shared_col" is assigned twice/);
});

test("assignments: legal overlap between insert columns and DO UPDATE SET is allowed", () => {
  // Upsert semantics: the insert supplies hits AND the conflict set assigns
  // hits — PostgreSQL applies the SET to the conflicted row. Not an error.
  const out = db
    .insert(members)
    .values({ email: "a@x", hits: 3 })
    .onConflictUpdate({ target: members.email, set: { hits: sql`${excluded(members.hits)} + 1` } })
    .toSQL();
  assert.ok(out.sql.includes('do update set "hits"'));
});

test("conflict: a second on-conflict call on one insert errors", () => {
  const base = db.insert(members).values({ email: "a@x" }).onConflictDoNothing();
  assert.throws(
    () => base.onConflictUpdate({ target: members.email, set: { hits: 1 } }),
    /already carries an on-conflict clause/,
  );
  assert.throws(() => base.onConflictDoNothing(), /already carries an on-conflict clause/);
});

// ---------------------------------------------------------------------------
// Fail-closed construction and compile rejections
// ---------------------------------------------------------------------------

test("conflict: onConflictUpdate without a target fails before SQL", () => {
  assert.throws(
    () => db.insert(members).values({ email: "a@x" }).onConflictUpdate({ target: undefined as never, set: { hits: 1 } }),
    /target is required/,
  );
});

test("conflict: target columns from a foreign table fail before SQL", () => {
  const foreign = pgTable("foreign_t", { foreignCol: text("foreign_col").notNull() });
  const capture = captureError(() =>
    db.insert(members).values({ email: "a@x" }).onConflictDoNothing({ target: foreign.foreignCol }).toSQL(),
  );
  assert.ok(capture instanceof Error);
  assert.match(capture.message, /"foreign_col" is not a column of members/);
});

test("conflict: constraint target rejects index predicates", () => {
  assert.throws(
    () =>
      db.insert(members).values({ email: "a@x" }).onConflictDoNothing({
        target: { constraint: "members_email_key" },
        where: sql`${members.seen} is null`,
      }).toSQL(),
    /only valid with a column-list target/,
  );
});

test("conflict: predicate supplied twice rejects", () => {
  assert.throws(
    () =>
      db.insert(members).values({ email: "a@x" }).onConflictDoNothing({
        target: { columns: [members.email], where: sql`${members.seen} is null` },
        where: sql`${members.seen} is null`,
      }).toSQL(),
    /index predicate supplied twice/,
  );
});

test("conflict: unknown set keys and excluded typos fail before SQL", () => {
  assert.throws(
    () => db.insert(members).values({ email: "a@x" }).onConflictUpdate({ target: members.email, set: { nope: 1 } as never }).toSQL(),
    /unknown column "nope"/,
  );
  // excluded() naming a physical column the inserted table does not have
  const foreign = pgTable("foreign_t", { nopeCol: integer("nope_col").notNull() });
  const capture = captureError(() =>
    db.insert(members).values({ email: "a@x" }).onConflictUpdate({
      target: members.email,
      set: { hits: sql`${excluded(foreign.nopeCol)} + 1` },
    }).toSQL(),
  );
  assert.ok(capture instanceof Error);
  assert.match(capture.message, /excluded reference "excluded.nope_col" is not a column of members/);
});

test("conflict: excluded() in an update .set() value fails before SQL", () => {
  assert.throws(
    () => db.update(members).set({ hits: sql`${excluded(members.hits)}` }).where(eq(members.id, 1)).toSQL(),
    /only valid in on-conflict clauses/,
  );
});

test("conflict: legacy raw() fragments are rejected in every conflict slot", () => {
  const legacy = raw("seen is null");
  assert.throws(
    () => db.insert(members).values({ email: "a@x" }).onConflictDoNothing({ where: legacy as never }),
    /legacy SqlFragment/,
  );
  assert.throws(
    () =>
      db.insert(members).values({ email: "a@x" }).onConflictUpdate({ target: members.email, set: { hits: 1 }, setWhere: legacy as never }),
    /legacy SqlFragment/,
  );
  assert.throws(
    () =>
      db.insert(members).values({ email: "a@x" }).onConflictUpdate({
        target: { columns: [members.email], where: legacy as never },
        set: { hits: 1 },
      }).toSQL(),
    /legacy SqlFragment/,
  );
});

test("conflict: AST constructor validations (frozen-node choke point)", () => {
  assert.throws(() => onConflictClause({ action: "explode" as never }), /unknown action/);
  assert.throws(() => onConflictClause({ action: "update", sets: [{ column: "x", value: sql`1` }] }), /requires a target/);
  assert.throws(() => onConflictClause({ action: "update", targetColumns: ["a"], sets: [] }), /at least one assignment/);
  assert.throws(
    () => onConflictClause({ action: "nothing", sets: [{ column: "x", value: sql`1` }] }),
    /do nothing takes no assignments/,
  );
  assert.throws(
    () => onConflictClause({ action: "nothing", targetColumns: ["a", "a"] }),
    /names a column more than once/,
  );
  assert.throws(
    () => onConflictClause({ action: "update", targetColumns: ["a"], constraint: "c", sets: [{ column: "x", value: sql`1` }] }),
    /mutually exclusive/,
  );
  assert.throws(
    () => onConflictClause({ action: "nothing", constraint: "c", targetWhere: [sql`1 = 1`] }),
    /only valid with a column-list target/,
  );
  const node = onConflictClause({ action: "update", targetColumns: ["email"], sets: [{ column: "hits", value: sql`1` }] });
  assert.equal(node.kind, "on-conflict");
  assert.equal(Object.isFrozen(node), true);
});

function captureError(fn: () => unknown): unknown {
  try {
    fn();
    return null;
  } catch (err) {
    return err;
  }
}

// ---------------------------------------------------------------------------
// Q04 entry conditions: excluded() scope is enforced at the COMPILE CHOKE
// POINT — every statement kind, every position — plus eager where-guards on
// the typed builders. Live-verified PG 17 ground truth: excluded is visible
// ONLY directly inside DO UPDATE SET expressions and the DO UPDATE WHERE
// predicate; the conflict target's index predicate errors with "invalid
// reference to FROM-clause entry", select/update/delete positions and insert
// RETURNING with "missing FROM-clause entry".
// ---------------------------------------------------------------------------

test("conflict: excluded() in select/update/delete .where() fails before SQL (Q04 entry condition)", () => {
  assert.throws(
    () => db.select().from(members).where(sql`${excluded(members.hits)} > ${5}`).toSQL(),
    /excluded\(\) references the row proposed for insertion/,
  );
  assert.throws(() => db.update(members).set({ hits: 1 }).where(sql`${excluded(members.hits)} > ${5}`).toSQL(), /excluded\(\)/);
  assert.throws(() => db.delete(members).where(sql`${excluded(members.hits)} > ${5}`).toSQL(), /excluded\(\)/);
});

test("conflict: excluded() in the on-conflict target index predicate fails before SQL", () => {
  assert.throws(
    () =>
      db.insert(members).values({ email: "a@x" }).onConflictUpdate({
        target: [members.email],
        targetWhere: sql`${excluded(members.hits)} > ${0}`,
        set: { hits: sql`${excluded(members.hits)}` },
      }).toSQL(),
    /on conflict target where on members: excluded\(\) references the row proposed for insertion/,
  );
});

test("conflict: the compile choke point rejects excluded() in every non-conflict position", () => {
  const bad = qual("excluded", "hits");
  // select order by
  assert.throws(
    () => compileStatement(selectStatement({ from: ident("members"), orderBy: [{ expr: bad, direction: "asc" }] })),
    /only valid directly inside on-conflict do-update set\/where/,
  );
  // select having
  assert.throws(
    () => compileStatement(selectStatement({ from: ident("members"), having: [bad] })),
    /only valid directly inside on-conflict/,
  );
  // a subquery inside a LEGAL DO UPDATE SET still rejects (subqueries never see excluded)
  assert.throws(
    () =>
      compileStatement(
        insertStatement({
          table: ident("members"),
          columns: ["email"],
          rows: [[param("a@x")]],
          onConflict: onConflictClause({
            action: "update",
            targetColumns: ["email"],
            sets: [{ column: "hits", value: fragment("(select ", subquery(selectStatement({ from: ident("members"), where: [bad] })), ")") }],
          }),
        }),
      ),
    /only valid directly inside on-conflict/,
  );
  // insert RETURNING cannot reference excluded (live-verified PG error)
  assert.throws(
    () =>
      compileStatement(
        insertStatement({
          table: ident("members"),
          columns: ["email"],
          rows: [[param("a@x")]],
          returning: [projection(bad)],
        }),
      ),
    /insert returning/,
  );
});

test("conflict: legal excluded() surfaces keep compiling (SET, DO UPDATE WHERE, subqueries elsewhere)", () => {
  const set = sql`${excluded(members.hits)} + 1`;
  const compiled = db.insert(members).values({ email: "a@x" }).onConflictUpdate({
    target: members.email,
    set: { hits: set },
    setWhere: sql`${excluded(members.score)} is not null`,
  }).toSQL();
  assert.match(compiled.sql, /"excluded"\."hits" \+ 1/);
  assert.match(compiled.sql, /"excluded"\."score" is not null/);
  // a subquery WITHOUT excluded in a SET value is fine
  const fine = db.insert(members).values({ email: "b@x" }).onConflictUpdate({
    target: members.email,
    set: { hits: sql`(${astSelect().from(members).subquery()})` },
  }).toSQL();
  assert.ok(fine.sql.includes("select"));
});

// Q04 entry condition 2: undefined-valued insert keys are OMITTED by the
// undefined-is-omitted convention and must not participate in duplicate
// physical-assignment detection.
test("conflict: undefined-valued duplicate-physical insert keys are omitted, not duplicates", () => {
  const dupPhysicalTable = pgTable("q04_undef_dup", {
    id: serial("id").primaryKey(),
    first: text("shared_col").notNull(),
    second: text("shared_col"),
  });
  const compiled = db.insert(dupPhysicalTable).values({ first: "a", second: undefined }).toSQL();
  const occurrences = compiled.sql.match(/"shared_col"/g) ?? [];
  assert.equal(occurrences.length, 1, `exactly one shared_col column: ${compiled.sql}`);
  // a REAL duplicate (two defined values) still errors
  assert.throws(() => db.insert(dupPhysicalTable).values({ first: "a", second: "b" }).toSQL(), /assigned twice/);
});

// Q04 entry condition 3: hand-built OnConflictNodes pass through the same
// validator when handed to insertStatement.
test("conflict: hand-built OnConflictNode duplicates are rejected at insertStatement", () => {
  const dupSets = {
    kind: "on-conflict",
    action: "update",
    target: { kind: "columns", columns: ["email"] },
    sets: [
      { column: "hits", value: sql`1` },
      { column: "hits", value: sql`2` },
    ],
  } as const;
  assert.throws(
    () => insertStatement({ table: ident("members"), columns: ["email"], rows: [[param("a@x")]], onConflict: dupSets as never }),
    /assigns a column more than once/,
  );
  const dupTarget = {
    kind: "on-conflict",
    action: "nothing",
    target: { kind: "columns", columns: ["email", "email"] },
  } as const;
  assert.throws(
    () => insertStatement({ table: ident("members"), columns: ["email"], rows: [[param("a@x")]], onConflict: dupTarget as never }),
    /names a column more than once/,
  );
  const targetless = { kind: "on-conflict", action: "update", sets: [{ column: "hits", value: sql`1` }] } as const;
  assert.throws(
    () => insertStatement({ table: ident("members"), columns: ["email"], rows: [[param("a@x")]], onConflict: targetless as never }),
    /do update requires a target/,
  );
});
