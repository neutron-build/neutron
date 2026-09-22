import assert from "node:assert/strict";
import test from "node:test";
import {
  astSelect,
  compile,
  compileStatement,
  cte,
  expr,
  fragment,
  ident,
  isValueNode,
  join,
  param,
  qual,
  ref,
  selectStatement,
  sqlAst,
  subquery,
  TRUSTED_SQL_ACK,
  trustSql,
  type SqlNode,
  type StatementNode,
  type TrustedSql,
  type ValueNode,
} from "./index.js";
import { pgTable, serial, integer, text, boolean, raw, sql } from "./index.js";

// ---------------------------------------------------------------------------
// Fixture schema (mirrors the snapshot suite's users + posts).
// ---------------------------------------------------------------------------

const users = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  active: boolean("active").notNull().default(true),
  createdAt: integer("created_at"),
});

const posts = pgTable("posts", {
  id: serial("id").primaryKey(),
  authorId: integer("author_id").notNull(),
  title: text("title").notNull(),
  published: boolean("published").notNull().default(false),
});

// ---------------------------------------------------------------------------
// Node construction, freezing and validation
// ---------------------------------------------------------------------------

test("ast: nodes are frozen, params excepted", () => {
  const n = qual("users", "id");
  assert.equal(Object.isFrozen(n), true);
  assert.equal(Object.isFrozen(n.parts), true);
  assert.throws(() => {
    (n.parts as unknown as { push(x: string): void }).push("evil");
  }, /not extensible|Cannot add/);
  const p = param({ borrowed: true });
  assert.equal(Object.isFrozen(p), true);
  assert.equal(Object.isFrozen(p.value), false, "param values are borrowed, never frozen");
});

test("ast: constructor validation rejects hostile shapes", () => {
  assert.throws(() => ident(""), /non-empty/);
  assert.throws(() => ident("bad\0name"), /NUL/);
  assert.throws(() => qual(), /at least one/);
  assert.throws(() => param(undefined), /undefined/);
  assert.throws(() => param(Symbol("s")), /symbol/);
  assert.throws(() => expr("binary", "; drop", [param(1), param(2)]), /allowed operator/);
  assert.throws(() => expr("call", "not an ident!", [param(1)]), /function identifier/);
  assert.throws(() => expr("binary", "+", [param(1)]), /exactly 2/);
  assert.throws(() => join("inner", ident("t")), /require an on/);
  assert.throws(() => join("cross", ident("t"), { on: param(true) }), /no on/);
  assert.throws(() => selectStatement({ limit: 1.5 }), /safe integer/);
  assert.throws(() => selectStatement({ limit: -1 }), /safe integer/);
});

test("ast: identifier and qualified quoting doubles embedded quotes", () => {
  const q = compileStatement(selectStatement({ from: ident('we"ird') }));
  assert.equal(q.sql, 'select * from "we""ird"');
  const qq = compileStatement(
    selectStatement({ from: qual('sche"ma', "t"), projections: [{ kind: "projection", expr: qual("t", 'va"l') }] }),
  );
  assert.equal(qq.sql, 'select "t"."va""l" from "sche""ma"."t"');
});

// ---------------------------------------------------------------------------
// compile(): purity, determinism, one traversal, global numbering
// ---------------------------------------------------------------------------

test("compile: same AST compiles byte-identical twice", () => {
  const stmt = astSelect().from(users).where(sqlAst`${ref("users", users.active)} = ${true}`).limit(10).toAST();
  const a = compileStatement(stmt);
  const b = compileStatement(stmt);
  assert.equal(a.sql, b.sql);
  assert.deepEqual(a.params, b.params);
});

test("compile: nested fragments number parameters in one global traversal", () => {
  // fragment containing a fragment containing a parameter (V07).
  const inner = sqlAst`${ref("users", users.id)} <> ${999}`;
  const middle = sqlAst`${ref("users", users.active)} = ${true} and ${inner}`;
  const outer = sqlAst`${middle} or ${ref("users", users.email)} = ${"a@x.com"}`;
  const q = astSelect().from(users).where(outer).toSQL();
  assert.equal(
    q.sql,
    'select "users"."id", "users"."email", "users"."active", "users"."created_at" as "createdAt" from "users" ' +
      'where ("users"."active" = $1 and "users"."id" <> $2 or "users"."email" = $3)',
  );
  assert.deepEqual(q.params, [true, 999, "a@x.com"]);
});

test("compile: one traversal binds exactly the param nodes visited", () => {
  const stmt = selectStatement({
    from: ident("t"),
    where: [sqlAst`${qual("t", "a")} = ${1} and ${qual("t", "b")} = ${2}`, sqlAst`${qual("t", "c")} = ${3}`],
  });
  const q = compileStatement(stmt);
  const visited: unknown[] = [];
  collectParamsPreOrder(stmt, visited);
  assert.equal(visited.length, 3);
  assert.deepEqual(q.params, visited);
  assert.deepEqual(q.params, [1, 2, 3]);
});

test("compile: repeated values bind as separate parameters (documented choice)", () => {
  // Chosen rule: every occurrence binds its own parameter. Deduplication
  // would renumber siblings whenever one repeated value changes — see the
  // F01 decision record.
  const q = astSelect()
    .from(users)
    .where(sqlAst`${ref("users", users.id)} = ${7}`)
    .where(sqlAst`${ref("users", users.active)} = ${7}`)
    .toSQL();
  assert.ok(q.sql.endsWith('where ("users"."id" = $1) and ("users"."active" = $2)'));
  assert.deepEqual(q.params, [7, 7]);
});

test("compile: a literal $1 inside a text VALUE stays a parameter", () => {
  const q = astSelect().from(users).where(sqlAst`${ref("users", users.email)} = ${"$1"}`).toSQL();
  assert.ok(q.sql.includes('where ("users"."email" = $1)'));
  assert.equal((q.sql.match(/\$\d+/g) ?? []).length, 1, "no extra placeholder from the value text");
  assert.deepEqual(q.params, ["$1"]);
});

test("compile: dollar-quoted strings in trusted segments pass through untouched", () => {
  const trusted = trustSql("$$costs $1 hundred$$", TRUSTED_SQL_ACK);
  const q = astSelect()
    .from(posts)
    .where(sqlAst`${ref("posts", posts.title)} = ${trusted} and ${ref("posts", posts.published)} = ${false}`)
    .toSQL();
  assert.equal(
    q.sql,
    'select "posts"."id", "posts"."author_id" as "authorId", "posts"."title", "posts"."published" from "posts" ' +
      'where ("posts"."title" = $$costs $1 hundred$$ and "posts"."published" = $1)',
  );
  assert.deepEqual(q.params, [false], "the $1 inside the dollar quotes never became a placeholder");
});

test("compile: deterministic auto-aliases for unnamed derived tables", () => {
  const stmtA = astSelect({ id: posts.id }).from(posts).toAST();
  const stmtB = astSelect({ id: users.id }).from(users).toAST();
  const outer = selectStatement({
    from: subquery(stmtA),
    joins: [join("left", subquery(stmtB), { on: sqlAst`${ident("__q1")}."id" = ${ident("u")}."id"` })],
  });
  const once = compileStatement(outer);
  const twice = compileStatement(outer);
  assert.equal(
    once.sql,
    'select * from (select "posts"."id" from "posts") as "__q1" ' +
      'left join (select "users"."id" from "users") as "__q2" on "__q1"."id" = "u"."id"',
  );
  assert.equal(once.sql, twice.sql, "alias numbering is a pure function of the AST");
});

test("compile: expressions render all three forms", () => {
  const e1 = expr("binary", "=", [qual("t", "a"), param(1)]);
  const e2 = expr("unary", "not", [qual("t", "b")]);
  const e3 = expr("call", "coalesce", [qual("t", "c"), param(0)]);
  const q = compileStatement(selectStatement({ from: ident("t"), where: [e1, e2, e3] }));
  assert.equal(
    q.sql,
    'select * from "t" where (("t"."a" = $1)) and ((not "t"."b")) and (coalesce("t"."c", $2))',
  );
  assert.deepEqual(q.params, [1, 0]);
});

test("compile: CTEs, order by, limit and offset render deterministically", () => {
  const recent = subquery(astSelect({ authorId: posts.authorId }).from(posts).toAST());
  const stmt = selectStatement({
    ctes: [cte("recent", recent.select, ["authorId"])],
    projections: [{ kind: "projection", expr: qual("recent", "authorId"), alias: "author" }],
    from: ident("recent"),
    orderBy: [{ expr: qual("recent", "authorId"), direction: "desc" }],
    limit: 5,
    offset: 10,
  });
  const q = compileStatement(stmt);
  assert.equal(
    q.sql,
    'with "recent" ("authorId") as (select "posts"."author_id" as "authorId" from "posts") ' +
      'select "recent"."authorId" as "author" from "recent" order by "recent"."authorId" desc limit 5 offset 10',
  );
});

// ---------------------------------------------------------------------------
// sqlAst template boundaries
// ---------------------------------------------------------------------------

test("sqlAst: values bind as parameters, schema objects splice structurally", () => {
  const frag = sqlAst`${users.email} = ${"a@x.com"} or ${users} match ${true}`;
  const q = compileStatement(selectStatement({ from: ident("users"), where: [frag] }));
  assert.equal(q.sql, 'select * from "users" where ("users"."email" = $1 or "users" match $2)');
  assert.deepEqual(q.params, ["a@x.com", true]);
});

test("sqlAst: legacy {sql, params} fragments are rejected, never renumbered", () => {
  assert.throws(() => sqlAst`${raw("x = $1")}` as never, /legacy SqlFragment/);
  const legacy = sql`count(*)`;
  assert.throws(() => sqlAst`${legacy}` as never, /legacy SqlFragment/);
});

test("sqlAst: unsupported interpolation shapes fail explicitly", () => {
  assert.throws(() => sqlAst`${[1, 2]}` as never, /cannot interpolate/);
  assert.throws(() => sqlAst`${new Map()}` as never, /cannot interpolate/);
  assert.throws(() => sqlAst`${undefined}` as never, /cannot interpolate/);
  assert.throws(() => sqlAst`${() => 1}` as never, /cannot interpolate/);
});

test("trustSql: brand cannot be produced without the acknowledgment", () => {
  assert.throws(() => trustSql("select 1", { iAcknowledgeThisIsTrustedSql: true } as never), /acknowledgment/);
  const trusted = trustSql("now()", TRUSTED_SQL_ACK);
  assert.equal(trusted.text, "now()");
  // A hand-built lookalike carries no runtime marker and is not spliced.
  assert.throws(() => sqlAst`${{ text: "select 1" } as never}` as never, /cannot interpolate/);
});

// ---------------------------------------------------------------------------
// Builder integration: the one representative path, against hand-written SQL
// ---------------------------------------------------------------------------

test("builder: mapped columns + bound where + aliased join + composed subquery", () => {
  // Mapped column via default projection (createdAt -> created_at) and a
  // bound where value.
  const plain = astSelect().from(users).where(sqlAst`${ref("users", users.active)} = ${true}`).orderBy(ref("users", users.id)).toSQL();
  assert.equal(
    plain.sql,
    'select "users"."id", "users"."email", "users"."active", "users"."created_at" as "createdAt" ' +
      'from "users" where ("users"."active" = $1) order by "users"."id" asc',
  );
  assert.deepEqual(plain.params, [true]);

  // One join with alias: the joined table's columns are referenced through
  // the alias; the from table's columns project directly.
  const joined = astSelect({ email: users.email, postTitle: sqlAst`${ref("p", posts.title)}` })
    .from(users)
    .innerJoin(posts, "p", sqlAst`${ref("p", posts.authorId)} = ${ref("users", users.id)}`)
    .where(sqlAst`${ref("p", posts.published)} = ${true}`)
    .orderBy(ref("p", posts.id), "desc")
    .limit(3)
    .toSQL();
  assert.equal(
    joined.sql,
    'select "users"."email", "p"."title" as "postTitle" from "users" ' +
      'inner join "posts" as "p" on "p"."author_id" = "users"."id" ' +
      'where ("p"."published" = $1) order by "p"."id" desc limit 3',
  );
  assert.deepEqual(joined.params, [true]);

  // A composable subquery built by another astSelect flows in structurally,
  // inheriting global parameter numbering.
  const authorIds = astSelect({ authorId: posts.authorId })
    .from(posts)
    .where(sqlAst`${ref("posts", posts.published)} = ${true}`)
    .subquery();
  const withSub = astSelect({ email: users.email })
    .from(users)
    .where(sqlAst`${ref("users", users.id)} in (${authorIds}) and ${ref("users", users.active)} = ${true}`)
    .toSQL();
  assert.equal(
    withSub.sql,
    'select "users"."email" from "users" where ("users"."id" in ' +
      '((select "posts"."author_id" as "authorId" from "posts" where ("posts"."published" = $1))) and "users"."active" = $2)',
  );
  assert.deepEqual(withSub.params, [true, true]);
});

test("builder: a CTE joins by name and carries its own parameters", () => {
  const recent = astSelect({ id: posts.id, authorId: posts.authorId })
    .from(posts)
    .where(sqlAst`${ref("posts", posts.published)} = ${true}`)
    .subquery();
  const q = astSelect({ email: users.email, postId: sqlAst`${ident("r")}."id"` })
    .from(users)
    .withCte("r", recent)
    .innerJoin(ident("r"), undefined, sqlAst`${ident("r")}."authorId" = ${ref("users", users.id)}`)
    .where(sqlAst`${ref("users", users.active)} = ${true}`)
    .toSQL();
  assert.equal(
    q.sql,
    'with "r" as (select "posts"."id", "posts"."author_id" as "authorId" from "posts" where ("posts"."published" = $1)) ' +
      'select "users"."email", "r"."id" as "postId" from "users" ' +
      'inner join "r" on "r"."authorId" = "users"."id" where ("users"."active" = $2)',
  );
  assert.deepEqual(q.params, [true, true]);
});

// ---------------------------------------------------------------------------
// Immutable reuse (V02's reuse case)
// ---------------------------------------------------------------------------

test("builder: fluent calls fork; mutating one branch cannot affect the other", () => {
  const base = astSelect({ email: users.email }).from(users);
  const activeOnly = base.where(sqlAst`${ref("users", users.active)} = ${true}`).limit(1);
  const bigIds = base.where(sqlAst`${ref("users", users.id)} > ${100}`).limit(2);

  const a = activeOnly.toSQL();
  const b = bigIds.toSQL();
  assert.equal(a.sql, 'select "users"."email" from "users" where ("users"."active" = $1) limit 1');
  assert.equal(b.sql, 'select "users"."email" from "users" where ("users"."id" > $1) limit 2');
  assert.deepEqual(a.params, [true]);
  assert.deepEqual(b.params, [100]);

  // The base is untouched and reusable for two "requests" with different
  // filters — neither contaminates the other.
  assert.equal(base.toSQL().sql, 'select "users"."email" from "users"');
  assert.deepEqual(base.toSQL(), base.toSQL());

  // Instances are frozen: a hostile write cannot smuggle state in.
  assert.equal(Reflect.set(activeOnly, "limitCount", 999), false);
  assert.equal(activeOnly.toSQL().sql, 'select "users"."email" from "users" where ("users"."active" = $1) limit 1');

  // Chained divergence AFTER forking stays independent.
  const a2 = activeOnly.offset(5).toSQL();
  const a3 = activeOnly.toSQL();
  assert.ok(a2.sql.endsWith("limit 1 offset 5"));
  assert.ok(a3.sql.endsWith("limit 1"));
});

test("builder: shared nodes are never mutated by compilation", () => {
  const cond = sqlAst`${ref("users", users.active)} = ${true}`;
  const stmt = astSelect().from(users).where(cond).toAST();
  compileStatement(stmt);
  compileStatement(stmt);
  assert.equal(Object.isFrozen(stmt), true);
  assert.equal(Object.isFrozen(cond.parts), true);
  const again = astSelect().from(users).where(cond).toSQL();
  assert.equal(again.sql, 'select "users"."id", "users"."email", "users"."active", "users"."created_at" as "createdAt" from "users" where ("users"."active" = $1)');
});

// ---------------------------------------------------------------------------
// Independent pre-order param walker — the oracle for one-traversal binding.
// Mirrors the emitter's visit order (ctes, projections, from, joins, where,
// order by), written independently of compile.ts's switch.
// ---------------------------------------------------------------------------

function collectParamsPreOrder(node: SqlNode, out: unknown[]): void {
  switch (node.kind) {
    case "identifier":
    case "trusted":
      return;
    case "qualified":
      return;
    case "param":
      out.push(node.value);
      return;
    case "fragment":
      for (const part of node.parts) if (typeof part !== "string") collectParamsPreOrder(part, out);
      return;
    case "expr":
      for (const arg of node.args) collectParamsPreOrder(arg, out);
      return;
    case "projection":
      collectParamsPreOrder(node.expr, out);
      return;
    case "join":
      collectParamsPreOrder(node.target, out);
      if (node.on) collectParamsPreOrder(node.on, out);
      return;
    case "subquery":
      collectParamsPreOrder(node.select, out);
      return;
    case "cte":
      collectParamsPreOrder(node.select, out);
      return;
    case "select":
      for (const c of node.ctes) collectParamsPreOrder(c, out);
      for (const p of node.projections) collectParamsPreOrder(p, out);
      if (node.from) collectParamsPreOrder(node.from, out);
      for (const j of node.joins) collectParamsPreOrder(j, out);
      for (const w of node.where) collectParamsPreOrder(w, out);
      for (const o of node.orderBy) collectParamsPreOrder(o.expr, out);
      return;
  }
}

// ---------------------------------------------------------------------------
// V07 fixed-seed fuzz: bounded random ASTs compile deterministically and
// round-trip their parameters.
// ---------------------------------------------------------------------------

function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test("fuzz: 200 seeded random ASTs compile deterministically with exact param round-trip", () => {
  const rng = mulberry32(0x20260922);
  const pick = <T>(arr: readonly T[]): T => arr[Math.floor(rng() * arr.length)];
  const int = (min: number, max: number): number => min + Math.floor(rng() * (max - min + 1));

  const tables = ["users", "orders", 'we"ird', "ta ble", "t$1", "select"] as const;
  const cols = ["id", "author_id", 'va"l', "col one", "c$2", "x"] as const;
  const trustedTexts = ["now()", "- 1", "%%", "$$raw $1 $$", "'literal '' quote'"] as const;
  const ops = ["=", "<>", "<", "<=", ">", ">=", "+", "||", "like"] as const;
  const calls = ["coalesce", "abs", "lower", "greatest"] as const;
  const params = [0, 1, -42, 3.5, "", "x", "$1", "$$d$$", 'quo"te', null, true, false, "9007199254740993"] as const;
  const aliases = ["a", "b", "p", 'al"ias', "t1"] as const;

  const genValue = (depth: number): ValueNode => {
    switch (int(0, depth <= 0 ? 1 : 5)) {
      case 0:
        return param(pick(params));
      case 1:
        return qual(pick(tables), pick(cols));
      case 2:
        return fragment(...Array.from({ length: int(1, 4) }, () => (rng() < 0.4 ? pick(trustedTexts) : genValue(depth - 1))));
      case 3:
        return sqlAst`${trustSql(pick(trustedTexts), TRUSTED_SQL_ACK)}`;
      case 4:
        return expr("binary", pick(ops), [genValue(depth - 1), genValue(depth - 1)]);
      default:
        return expr("call", pick(calls), Array.from({ length: int(0, 2) }, () => genValue(depth - 1)));
    }
  };

  const genStatement = (depth: number): StatementNode => {
    const joinCount = int(0, 2);
    const joins = Array.from({ length: joinCount }, () => {
      const useSub = depth > 0 && rng() < 0.3;
      const target = useSub ? subquery(genStatement(depth - 1)) : ident(pick(tables));
      const type = pick(["inner", "left", "right", "full"] as const);
      const alias = rng() < 0.7 ? pick(aliases) : undefined;
      return join(type, target, { alias, on: expr("binary", "=", [qual(alias ?? pick(tables), pick(cols)), param(pick(params))]) });
    });
    const ctes = depth > 0 && rng() < 0.3 ? [cte(pick(aliases), genStatement(depth - 1))] : [];
    return selectStatement({
      ctes,
      projections: Array.from({ length: int(1, 4) }, () => ({
        kind: "projection" as const,
        expr: qual(pick(tables), pick(cols)),
        alias: rng() < 0.5 ? pick(cols) : undefined,
      })),
      from: rng() < 0.85 ? ident(pick(tables)) : subquery(genStatement(depth - 1)),
      joins,
      where: Array.from({ length: int(0, 3) }, () => genValue(2)),
      orderBy: rng() < 0.5 ? [{ expr: qual(pick(tables), pick(cols)), direction: rng() < 0.5 ? ("asc" as const) : ("desc" as const) }] : [],
      limit: rng() < 0.3 ? int(0, 100) : undefined,
      offset: rng() < 0.15 ? int(0, 100) : undefined,
    });
  };

  let determinismChecks = 0;
  for (let i = 0; i < 200; i++) {
    const stmt = genStatement(2);
    const first = compileStatement(stmt);
    const second = compileStatement(stmt);
    assert.equal(second.sql, first.sql, `case ${i}: recompile differs`);
    assert.deepEqual(second.params, first.params, `case ${i}: params differ on recompile`);
    determinismChecks++;

    const expectedParams: unknown[] = [];
    collectParamsPreOrder(stmt, expectedParams);
    assert.deepEqual(
      first.params,
      expectedParams,
      `case ${i}: bound params must equal the pre-order param walk (one traversal)`,
    );
    assert.ok(first.sql.startsWith("select ") || first.sql.startsWith("with "), `case ${i}: statement shape`);
    assert.ok(!first.sql.includes("\0"), `case ${i}: NUL byte in output`);
    for (const v of first.params) {
      assert.ok(
        params.includes(v as (typeof params)[number]),
        `case ${i}: param ${JSON.stringify(v)} did not round-trip from the generator pool`,
      );
    }
  }
  assert.ok(determinismChecks === 200);
});

// ---------------------------------------------------------------------------
// F-R1 regression: forged text-bearing nodes fail closed. The reviewer's
// exact payloads (JSON.parse'd kind:"trusted" / kind:"fragment") plus spread
// copies and hand-built literals must never splice raw SQL — via sqlAst,
// builder slots, fragment() or direct compile.
// ---------------------------------------------------------------------------

test("F-R1: JSON.parse'd trusted/fragment nodes are rejected on every entry path", () => {
  // Reviewer payload (b'): forged marker + kind — previously injected verbatim.
  const forgedTrusted = JSON.parse('{"__trusted":true,"kind":"trusted","text":"1=1; drop table x;--"}');
  // Reviewer payload (b''): fragment with raw text parts.
  const forgedFragment = JSON.parse('{"kind":"fragment","parts":["1=1) or (pg_sleep(10))--"]}');
  const payload = JSON.parse('{"kind":"trusted","text":"1=1; drop table x;--"}');

  // via sqlAst interpolation
  assert.throws(() => sqlAst`${forgedTrusted}`, /forged "trusted"/);
  assert.throws(() => sqlAst`${forgedFragment}`, /forged "fragment"/);
  assert.throws(() => sqlAst`${payload}`, /forged "trusted"/);

  // via builder slots (any-typed JSON flows straight into where())
  assert.throws(() => astSelect().from(users).where(forgedTrusted).toSQL(), /forged "trusted"/);
  assert.throws(() => astSelect().from(users).where(forgedFragment).toSQL(), /forged "fragment"/);
  assert.throws(() => astSelect().from(users).where(payload).toSQL(), /forged "trusted"/);
  assert.throws(() => astSelect().from(users).orderBy(forgedFragment).toSQL(), /forged "fragment"/);

  // via fragment() parts
  assert.throws(() => fragment(forgedTrusted), /forged "trusted"/);
  assert.throws(() => fragment(forgedFragment), /forged "fragment"/);

  // via selectStatement + direct compile
  assert.throws(() => compileStatement(selectStatement({ where: [forgedTrusted] })), /forged "trusted"/);
  assert.throws(() => compileStatement(selectStatement({ where: [forgedFragment] })), /forged "fragment"/);

  // isValueNode itself refuses them
  assert.equal(isValueNode(forgedTrusted), false);
  assert.equal(isValueNode(forgedFragment), false);
  assert.equal(isValueNode(payload), false);
});

test("F-R1: spread copies and hand-built literals of text-bearing kinds are rejected", () => {
  const real = sqlAst`${ref("users", users.id)} = ${1}`;
  // The node brand is non-enumerable, so a spread copy loses it even when
  // every enumerable field (kind, parts) survives.
  const spread = { ...real, parts: ["1=1; drop table x;--"] };
  assert.throws(() => astSelect().from(users).where(spread).toSQL(), /forged "fragment"/);
  assert.throws(() => sqlAst`${spread}`, /forged "fragment"/);
  assert.throws(() => fragment(spread), /forged "fragment"/);
  const trustedNode = (sqlAst`${trustSql("now()", TRUSTED_SQL_ACK)}`).parts[0] as ValueNode;
  const spreadTrusted = { ...trustedNode, text: "1=1; drop table x;--" };
  assert.equal((spreadTrusted as { kind: string }).kind, "trusted");
  assert.throws(() => astSelect().from(users).where(spreadTrusted).toSQL(), /forged "trusted"/);

  // Hand-built literals (no JSON involved) fail the same way.
  assert.throws(() => astSelect().from(users).where({ kind: "trusted", text: "1=1" }).toSQL(), /forged "trusted"/);
  assert.throws(() => astSelect().from(users).where({ kind: "fragment", parts: ["1=1"] }).toSQL(), /forged "fragment"/);
});

test("F-R1: JSON.parse'd structural nodes are provably safe (quoted or parameterized)", () => {
  // Forged identifier: the name is QUOTED at emit, so hostile text becomes a
  // (nonexistent) quoted identifier — an error at the database, never SQL.
  const evilIdent = JSON.parse('{"kind":"identifier","name":"x; drop table y"}');
  const q = astSelect({ email: users.email }).from(users).where(sqlAst`${evilIdent} = ${1}`).toSQL();
  assert.equal(q.sql, 'select "users"."email" from "users" where ("x; drop table y" = $1)');

  // Forged param: the value is BOUND, never spliced.
  const evilParam = JSON.parse('{"kind":"param","value":"1=1; drop table x;--"}');
  const p = astSelect({ email: users.email }).from(users).where(sqlAst`${qual("users", "id")} = ${evilParam}`).toSQL();
  assert.equal(p.sql, 'select "users"."email" from "users" where ("users"."id" = $1)');
  assert.deepEqual(p.params, ["1=1; drop table x;--"]);

  // Forged qualified with hostile parts: every part is quoted.
  const evilQual = JSON.parse('{"kind":"qualified","parts":["a; drop table b","c\\"; --"]}');
  const qq = astSelect({ email: users.email }).from(users).where(sqlAst`${evilQual} = ${1}`).toSQL();
  assert.equal(qq.sql, 'select "users"."email" from "users" where ("a; drop table b"."c""; --" = $1)');

  // Non-string identifier text (e.g. an object with a hostile replace method
  // smuggled into qualified parts) fails closed in quoteIdent.
  const sneaky = { kind: "qualified", parts: [{ replace: () => '"; drop table y;--' }] };
  assert.throws(() => astSelect().from(users).where(sqlAst`${sneaky}`).toSQL(), /identifier text must be a string/);
  assert.throws(() => astSelect().from(users).where(sqlAst`${JSON.parse('{"kind":"identifier","name":123}')}`).toSQL(), /identifier text must be a string/);
  assert.throws(() => astSelect().from(users).where(sqlAst`${JSON.parse('{"kind":"identifier","name":"a\\u0000b"}')}`).toSQL(), /NUL/);
});

test("F-R1: forged expr operators, join types, limits and unknown kinds fail closed at compile", () => {
  // expr op is spliced verbatim when legitimate — so it is validated against
  // the same operator/function rules the constructor uses.
  const forgedExpr = JSON.parse(
    '{"kind":"expr","form":"binary","op":"0=1) or (1=1;--","args":[{"kind":"qualified","parts":["t","a"]},{"kind":"param","value":1}]}',
  );
  assert.throws(() => astSelect().from(users).where(forgedExpr).toSQL(), /not an allowed operator/);

  // join type is spliced into the join keyword — validated at compile.
  const forgedJoin = JSON.parse(
    '{"kind":"join","type":"left join evil on 1=1;--","target":{"kind":"identifier","name":"t"},"on":{"kind":"param","value":1}}',
  );
  assert.throws(() => compileStatement(selectStatement({ from: ident("t"), joins: [forgedJoin] })), /unknown join type/);

  // limit/offset are number-interpolated — validated at compile.
  const forgedLimit = JSON.parse(
    '{"kind":"select","ctes":[],"projections":[],"joins":[],"where":[],"orderBy":[],"from":{"kind":"identifier","name":"t"},"limit":"1; drop table x"}',
  );
  assert.throws(() => compileStatement(forgedLimit), /compile limit: must be a non-negative safe integer/);

  // Unknown kinds (and non-nodes) fail closed instead of compiling silently.
  const state = { parts: [] as string[], params: [] as unknown[], aliasCounter: 0 };
  assert.throws(() => compile({ kind: "bogus" } as never, state), /unknown node kind/);
  assert.throws(() => compile("free text" as never, state), /unknown node kind/);
  assert.throws(() => compile(null as never, state));
});

// ---------------------------------------------------------------------------
// F-R2 regression: the builder freeze is deep over builder-owned collections.
// The reviewer's attack — fork, wheres.pop() on one branch — must throw (or
// otherwise fail to mutate) and leave both branches' SQL untouched.
// ---------------------------------------------------------------------------

test("F-R2: the reviewer's wheres.pop() attack cannot corrupt a sibling fork", () => {
  const b1 = astSelect({ email: users.email }).from(users).where(sqlAst`${ref("users", users.active)} = ${true}`);
  const b2 = b1.limit(5); // shares b1's wheres array (copy-on-write)

  const wheres = (b2 as unknown as { wheres: readonly unknown[] }).wheres;
  assert.equal(Object.isFrozen(wheres), true, "wheres must be a frozen array");

  // The exact attack: pop() throws on a frozen array in strict mode (ESM);
  // in sloppy mode it would silently no-op — either way no mutation.
  let popThrew = false;
  try {
    (wheres as unknown as unknown[]).pop();
  } catch {
    popThrew = true;
  }
  assert.equal(popThrew, true, "wheres.pop() must throw in strict mode");

  // The push(null) poison variant is blocked the same way.
  let pushThrew = false;
  try {
    (wheres as unknown as unknown[]).push(null);
  } catch {
    pushThrew = true;
  }
  assert.equal(pushThrew, true, "wheres.push(null) must throw in strict mode");

  // Sibling and fork compile unaffected either way.
  assert.equal(b1.toSQL().sql, 'select "users"."email" from "users" where ("users"."active" = $1)');
  assert.equal(b2.toSQL().sql, 'select "users"."email" from "users" where ("users"."active" = $1) limit 5');
  assert.deepEqual(b1.toSQL().params, [true]);
});

test("F-R2: every builder-owned collection is frozen (joinSpecs, orders, cteSpecs, fromSpec)", () => {
  const sub = astSelect({ id: posts.id }).from(posts).subquery();
  const b = astSelect({ email: users.email })
    .from(users)
    .innerJoin(posts, "p", sqlAst`${ref("p", posts.authorId)} = ${ref("users", users.id)}`)
    .where(sqlAst`${ref("users", users.active)} = ${true}`)
    .orderBy(ref("users", users.id))
    .withCte("r", sub);

  const internals = b as unknown as Record<string, unknown>;
  for (const field of ["wheres", "joinSpecs", "orders", "cteSpecs"] as const) {
    assert.equal(Object.isFrozen(internals[field]), true, `${field} array must be frozen`);
    assert.throws(() => (internals[field] as unknown[]).pop(), TypeError, `${field}.pop() must throw`);
    assert.throws(() => (internals[field] as unknown[]).push(null), TypeError, `${field}.push() must throw`);
  }
  assert.equal(Object.isFrozen(internals.fromSpec), true, "fromSpec object must be frozen");
  for (const spec of internals.joinSpecs as object[]) assert.equal(Object.isFrozen(spec), true, "join spec elements must be frozen");
  for (const spec of internals.orders as object[]) assert.equal(Object.isFrozen(spec), true, "order spec elements must be frozen");

  // The builder still compiles exactly as before the attacks.
  const q = b.toSQL();
  assert.ok(q.sql.includes('inner join "posts" as "p" on "p"."author_id" = "users"."id"'));
  assert.ok(q.sql.includes('where ("users"."active" = $1)'));
  assert.deepEqual(q.params, [true]);
});



// @ts-expect-error trustSql demands the acknowledgment argument
void trustSql("select 1");
// @ts-expect-error a string is not the acknowledgment type
void trustSql("select 1", "i promise");
// @ts-expect-error TrustedSql cannot be assigned from a plain literal
const notTrusted: TrustedSql = { text: "select 1" };
void notTrusted;
// @ts-expect-error plain strings are not condition nodes
void astSelect().from(users).where("active = true");
// @ts-expect-error legacy fragments are not value nodes (and are rejected at runtime)
void astSelect().from(users).where(raw("id = $1"));

// Positive controls — must keep compiling:
const okTrusted = trustSql("now()", TRUSTED_SQL_ACK);
const okQuery = astSelect({ email: users.email, now: sqlAst`${okTrusted}` }).from(users).where(sqlAst`${ref("users", users.id)} = ${1}`);
void [okQuery.toSQL()];
