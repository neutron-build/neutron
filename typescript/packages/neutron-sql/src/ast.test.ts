import assert from "node:assert/strict";
import test from "node:test";
import {
  astSelect,
  compile,
  compileStatement,
  cte,
  defaultCell,
  deleteStatement,
  expr,
  fragment,
  ident,
  insertStatement,
  isValueNode,
  join,
  param,
  paramCast,
  qual,
  ref,
  selectStatement,
  sqlAst,
  subquery,
  TRUSTED_SQL_ACK,
  trustSql,
  updateStatement,
  type AnyStatementNode,
  type SqlNode,
  type StatementNode,
  type TrustedSql,
  type ValueNode,
} from "./index.js";
import { pgTable, serial, integer, text, boolean, raw } from "./index.js";

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
    'select * from "t" where ("t"."a" = $1) and (not "t"."b") and coalesce("t"."c", $2)',
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
  assert.throws(() => sqlAst`${raw("count(*)")}` as never, /legacy SqlFragment/);
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
    case "default":
      return;
    case "insert":
      for (const row of node.rows) for (const cell of row) collectParamsPreOrder(cell, out);
      if (node.returning) for (const p of node.returning) collectParamsPreOrder(p, out);
      return;
    case "update":
      for (const s of node.sets) collectParamsPreOrder(s.value, out);
      for (const w of node.where) collectParamsPreOrder(w, out);
      if (node.returning) for (const p of node.returning) collectParamsPreOrder(p, out);
      return;
    case "delete":
      for (const w of node.where) collectParamsPreOrder(w, out);
      if (node.returning) for (const p of node.returning) collectParamsPreOrder(p, out);
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
  // Sample WITHOUT replacement: duplicate column lists/assignments are
  // invalid statements by contract (Q03 order-independent duplicate
  // rejection), so the generator stays inside the valid input space.
  const pickDistinct = <T>(arr: readonly T[], count: number): T[] => {
    const pool = [...arr];
    const out: T[] = [];
    for (let i = 0; i < count && pool.length > 0; i++) {
      out.push(pool.splice(Math.floor(rng() * pool.length), 1)[0]);
    }
    return out;
  };

  const tables = ["users", "orders", 'we"ird', "ta ble", "t$1", "select"] as const;
  const cols = ["id", "author_id", 'va"l', "col one", "c$2", "x"] as const;
  const trustedTexts = ["now()", "- 1", "%%", "$$raw $1 $$", "'literal '' quote'", "b or c", "not b", "a and b or c"] as const;
  const ops = ["=", "<>", "<", "<=", ">", ">=", "+", "||", "like", "and", "or"] as const;
  const calls = ["coalesce", "abs", "lower", "greatest"] as const;
  const params = [0, 1, -42, 3.5, "", "x", "$1", "$$d$$", 'quo"te', null, true, false, "9007199254740993"] as const;
  const aliases = ["a", "b", "p", 'al"ias', "t1"] as const;
  const casts = [undefined, "timestamp", "timestamptz", "date", "json", "jsonb"] as const;

  const genValue = (depth: number): ValueNode => {
    switch (int(0, depth <= 0 ? 1 : 6)) {
      case 0: {
        const cast = pick(casts);
        const v = pick(params);
        return cast === undefined ? param(v) : paramCast(v, cast);
      }
      case 1:
        return qual(pick(tables), pick(cols));
      case 2:
        return fragment(...Array.from({ length: int(1, 4) }, () => (rng() < 0.4 ? pick(trustedTexts) : genValue(depth - 1))));
      case 3:
        return sqlAst`${trustSql(pick(trustedTexts), TRUSTED_SQL_ACK)}`;
      case 4:
        return expr("binary", pick(ops), [genValue(depth - 1), genValue(depth - 1)]);
      case 5:
        return expr("unary", "not", [genValue(depth - 1)]);
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
  let mutations = 0;
  const genAnyStatement = (): AnyStatementNode => {
    switch (int(0, 3)) {
      case 1: {
        // insert: schema-ordered columns, per-row param/default/cast cells
        const columnCount = int(1, 4);
        const columnNames = pickDistinct(cols, columnCount);
        const rows = Array.from({ length: int(1, 3) }, () =>
          Array.from({ length: columnCount }, () => {
            if (rng() < 0.25) return defaultCell();
            const cast = pick(casts);
            const v = pick(params);
            return cast === undefined ? param(v) : paramCast(v, cast);
          }),
        );
        return insertStatement({ table: ident(pick(tables)), columns: columnNames, rows });
      }
      case 2:
        return updateStatement({
          table: qual("sche\"ma", pick(tables)),
          sets: pickDistinct(cols, int(1, 3)).map((column) => ({ column, value: genValue(1) })),
          where: Array.from({ length: int(1, 2) }, () => genValue(2)),
        });
      case 3:
        return deleteStatement({
          table: ident(pick(tables)),
          where: Array.from({ length: int(1, 2) }, () => genValue(2)),
        });
      default:
        return genStatement(2);
    }
  };
  for (let i = 0; i < 200; i++) {
    const stmt = genAnyStatement();
    if (stmt.kind !== "select") mutations++;
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
    assert.ok(
      first.sql.startsWith("select ") || first.sql.startsWith("with ") ||
        first.sql.startsWith("insert ") || first.sql.startsWith("update ") || first.sql.startsWith("delete "),
      `case ${i}: statement shape`,
    );
    assert.ok(!first.sql.includes("\0"), `case ${i}: NUL byte in output`);
    for (const v of first.params) {
      assert.ok(
        params.includes(v as (typeof params)[number]),
        `case ${i}: param ${JSON.stringify(v)} did not round-trip from the generator pool`,
      );
    }
  }
  assert.ok(determinismChecks === 200);
  assert.ok(mutations >= 100, `mutation statements must be well represented (got ${mutations})`);
});

// ---------------------------------------------------------------------------
// F04 rework (attempt 2) MAJOR-1: connective-grouping fuzz. The main fuzz
// above proves determinism and param order; this one pins the GROUPING SPEC
// — every fragment/trusted operand of a compiler-applied operator (binary,
// unary, where-list join) is paren-delimited — against an independent
// renderer derived from that spec, not from compile.ts. Attempt 1 shipped
// the where-list parens only; and()/not() spliced fragment text bare and
// silently returned wrong rows (reviewer live proof: 5 of 8 truth-table
// rows where 3 were intended).
// ---------------------------------------------------------------------------

test("fuzz: connectives over fragments group exactly (spec renderer oracle)", () => {
  const rng = mulberry32(0x20260923);
  const pick = <T>(arr: readonly T[]): T => arr[Math.floor(rng() * arr.length)];
  const int = (min: number, max: number): number => min + Math.floor(rng() * (max - min + 1));
  // Sample WITHOUT replacement: duplicate column lists/assignments are
  // invalid statements by contract (Q03 order-independent duplicate
  // rejection), so the generator stays inside the valid input space.
  const pickDistinct = <T>(arr: readonly T[], count: number): T[] => {
    const pool = [...arr];
    const out: T[] = [];
    for (let i = 0; i < count && pool.length > 0; i++) {
      out.push(pool.splice(Math.floor(rng() * pool.length), 1)[0]);
    }
    return out;
  };

  const cols = ["a", "b", "c", 'we"ird'] as const;
  const values = [0, 1, 2, true, false, "x", null] as const;
  const connectiveTexts = ["b or c", "not b", "a and b or c", "b is null or c", "x"] as const;

  type Tree =
    | { k: "eq"; col: string; v: (typeof values)[number] }
    | { k: "frag"; parts: Array<string | Tree> }
    | { k: "and" | "or"; l: Tree; r: Tree }
    | { k: "not"; a: Tree };

  const genLeaf = (): Tree => (rng() < 0.3 ? { k: "frag", parts: [pick(connectiveTexts)] } : { k: "eq", col: pick(cols), v: pick(values) });
  const genFrag = (): Tree => ({
    k: "frag",
    parts: Array.from({ length: int(1, 3) }, () => (rng() < 0.5 ? pick(connectiveTexts) : { k: "eq", col: pick(cols), v: pick(values) })),
  });
  const genTree = (depth: number): Tree => {
    if (depth <= 0) return rng() < 0.35 ? genFrag() : genLeaf();
    switch (int(0, 3)) {
      case 0:
        return { k: "and", l: genTree(depth - 1), r: genTree(depth - 1) };
      case 1:
        return { k: "or", l: genTree(depth - 1), r: genTree(depth - 1) };
      case 2:
        return { k: "not", a: genTree(depth - 1) };
      default:
        return rng() < 0.5 ? genFrag() : genLeaf();
    }
  };

  const toNode = (t: Tree): ValueNode => {
    switch (t.k) {
      case "eq":
        return expr("binary", "=", [qual("tt", t.col), param(t.v)]);
      case "frag":
        return fragment(...t.parts.map((p) => (typeof p === "string" ? p : toNode(p))));
      case "not":
        return expr("unary", "not", [toNode(t.a)]);
      default:
        return expr("binary", t.k, [toNode(t.l), toNode(t.r)]);
    }
  };

  // Spec renderer (independent of compile.ts): where items join with "and"
  // and every item is delimited when text-bearing; binary/unary operators
  // delimit fragment operands; params number in emission order.
  const expectedParams: unknown[] = [];
  const render = (t: Tree): string => {
    switch (t.k) {
      case "eq":
        expectedParams.push(t.v);
        return `("tt"."${t.col.replace(/"/g, '""')}" = $${expectedParams.length})`;
      case "frag":
        return t.parts.map((p) => (typeof p === "string" ? p : render(p))).join("");
      case "not":
        return `(not ${operand(t.a)})`;
      default:
        return `(${operand(t.l)} ${t.k} ${operand(t.r)})`;
    }
  };
  const operand = (t: Tree): string => (t.k === "frag" ? `(${render(t)})` : render(t));

  let fragmentOperands = 0;
  const countFrags = (t: Tree, under: boolean): void => {
    if (t.k === "frag" && under) fragmentOperands++;
    if (t.k === "and" || t.k === "or") {
      countFrags(t.l, true);
      countFrags(t.r, true);
    }
    if (t.k === "not") countFrags(t.a, true);
    if (t.k === "frag") for (const p of t.parts) if (typeof p !== "string") countFrags(p, false);
  };

  for (let i = 0; i < 300; i++) {
    const trees = Array.from({ length: int(1, 3) }, () => genTree(3));
    for (const t of trees) countFrags(t, false);
    const stmt = selectStatement({
      projections: [projectionForTest()],
      from: ident("tt"),
      where: trees.map(toNode),
    });
    const compiled = compileStatement(stmt);
    expectedParams.length = 0;
    const expectedWhere = trees.map((t) => (t.k === "frag" ? `(${render(t)})` : render(t))).join(" and ");
    const expectedSql = `select "x" from "tt" where ${expectedWhere}`;
    assert.equal(compiled.sql, expectedSql, `case ${i}: grouping differs from spec`);
    assert.deepEqual(compiled.params, expectedParams, `case ${i}: params differ from spec order`);
  }
  assert.ok(fragmentOperands >= 100, `fragments as connective operands must dominate the corpus (got ${fragmentOperands})`);

  function projectionForTest() {
    return { kind: "projection" as const, expr: ident("x") };
  }
});

test("F04 rework MAJOR-2: astSelect projectionSpec is copied on fork, caller object untouched", () => {
  // F01 review-2 carry-forward, AST builder side: a post-fork `evil` key
  // reached BOTH sibling forks' compiled SQL when projectionSpec was shared
  // by reference (review-1 verified empirically).
  const projection: Record<string, unknown> = { email: users.email };
  const base = astSelect(projection as never).from(users);
  const b1 = base.where(qual("users", "id"));
  const b2 = base.limit(3);
  const before = b1.toSQL().sql;
  projection.evil = sqlAst`${ident("users")}."id"`;
  assert.equal(b1.toSQL().sql, before, "fork 1 must compile its construction-time snapshot");
  assert.ok(!b2.toSQL().sql.includes("evil"), "fork 2 must not see post-fork caller mutation");
  assert.ok(!base.offset(1).toSQL().sql.includes("evil"), "later forks of the base are isolated too");
  // The caller's object is copied, never frozen by the builder.
  assert.equal(Object.isFrozen(projection), false);
  delete projection.evil;
  assert.equal(b1.toSQL().sql, before);
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



// Compile-time-only fixtures (F03 pattern: never-called arrows so the
// intentional throws stay type-level and never abort module evaluation —
// code after a throwing fixture would silently never register).
// @ts-expect-error trustSql demands the acknowledgment argument
void (() => trustSql("select 1"));
// @ts-expect-error a string is not the acknowledgment type
void (() => trustSql("select 1", "i promise"));
// @ts-expect-error TrustedSql cannot be assigned from a plain literal
const notTrusted: TrustedSql = { text: "select 1" };
void notTrusted;
// @ts-expect-error plain strings are not condition nodes
void (() => astSelect().from(users).where("active = true"));
// @ts-expect-error legacy fragments are not value nodes (and are rejected at runtime)
void (() => astSelect().from(users).where(raw("id = $1")));

// Positive controls — must keep compiling:
const okTrusted = trustSql("now()", TRUSTED_SQL_ACK);
const okQuery = astSelect({ email: users.email, now: sqlAst`${okTrusted}` }).from(users).where(sqlAst`${ref("users", users.id)} = ${1}`);
void [okQuery.toSQL()];

// ---------------------------------------------------------------------------
// F04: mutation statements (insert/update/delete) compile through the same
// one-traversal compiler; param casts render `$n::text::<cast>`; the `__q`
// alias namespace is reserved; constructor validation fails closed.
// ---------------------------------------------------------------------------

test("F04: insert statements compile with default cells, casts and returning", () => {
  const stmt = insertStatement({
    table: ident("t"),
    columns: ["a", "b", "c"],
    rows: [
      [param(1), paramCast("2026-01-02T03:04:05.678Z", "timestamptz"), defaultCell()],
      [defaultCell(), param(null), defaultCell()],
    ],
    returning: [{ kind: "projection", expr: qual("t", "a"), alias: "a" }],
  });
  const q = compileStatement(stmt);
  assert.equal(q.sql, 'insert into "t" ("a", "b", "c") values ($1, $2::text::timestamptz, default), (default, $3, default) returning "t"."a" as "a"');
  assert.deepEqual(q.params, [1, "2026-01-02T03:04:05.678Z", null]);
  const again = compileStatement(stmt);
  assert.equal(again.sql, q.sql);
  assert.deepEqual(again.params, q.params);
});

test("F04: insert default values form and its mutual exclusivity", () => {
  const q = compileStatement(insertStatement({ table: ident("t"), defaultValues: true }));
  assert.equal(q.sql, "insert into \"t\" default values");
  assert.deepEqual(q.params, []);
  assert.throws(() => insertStatement({ table: ident("t"), defaultValues: true, columns: ["a"] }), /defaultValues takes no columns/);
  assert.throws(() => insertStatement({ table: ident("t"), columns: ["a"], rows: [] }), /need columns and rows/);
  assert.throws(() => insertStatement({ table: ident("t"), columns: ["a", "b"], rows: [[param(1)]] }), /row has 1 cells but 2 columns/);
  // Cells must be param/default nodes — arbitrary value nodes are rejected.
  assert.throws(
    () => insertStatement({ table: ident("t"), columns: ["a"], rows: [[qual("x", "y") as never]] }),
    /cells must be param\(\) or defaultCell\(\) nodes/,
  );
});

test("F04: update and delete compile with where and returning", () => {
  const u = compileStatement(
    updateStatement({
      table: ident("users"),
      sets: [
        { column: "name", value: param("B") },
        { column: "active", value: expr("unary", "not", [param(true)]) },
      ],
      where: [expr("binary", "=", [qual("users", "id"), param(7)])],
      returning: [{ kind: "projection", expr: qual("users", "id") }],
    }),
  );
  assert.equal(u.sql, 'update "users" set "name" = $1, "active" = (not $2) where ("users"."id" = $3) returning "users"."id"');
  assert.deepEqual(u.params, ["B", true, 7]);

  const d = compileStatement(
    deleteStatement({
      table: qual("sche\"ma", "logs"),
      where: [fragment(qual("sche\"ma", "logs"), ".ts < ", param(10))],
      returning: [{ kind: "projection", expr: qual("sche\"ma", "logs") }],
    }),
  );
  assert.equal(d.sql, 'delete from "sche""ma"."logs" where ("sche""ma"."logs".ts < $1) returning "sche""ma"."logs"');
  assert.deepEqual(d.params, [10]);
});

test("F04: mutation constructors refuse predicate-free statements", () => {
  assert.throws(() => updateStatement({ table: ident("t"), sets: [{ column: "a", value: param(1) }], where: [] }), /where predicate is required/);
  assert.throws(() => deleteStatement({ table: ident("t"), where: [] }), /where predicate is required/);
  assert.throws(() => updateStatement({ table: ident("t"), sets: [], where: [param(1)] }), /at least one assignment/);
});

test("F04: param casts are validated at construction and at the compile choke point", () => {
  assert.equal(compileStatement(insertStatement({ table: ident("t"), columns: ["a"], rows: [[paramCast("x", "jsonb")]] })).sql, 'insert into "t" ("a") values ($1::text::jsonb)');
  assert.throws(() => paramCast(1, "timestamp; drop table x"), /cast must be a plain lowercase type name/);
  assert.throws(() => paramCast(1, "Timestamp"), /cast must be a plain lowercase type name/);
  // A forged cast on a hand-built node fails at compile (the choke point).
  const forged = JSON.parse('{"kind":"param","value":1,"cast":"int; drop table x"}');
  assert.throws(() => compileStatement(selectStatement({ from: ident("t"), where: [forged] })), /cast must be a plain lowercase type name/);
});

test("F04: the __q alias namespace is reserved for compiler-generated aliases", () => {
  assert.throws(() => astSelect().from(users, "__q1"), /reserved for compiler-generated/);
  assert.throws(() => astSelect().from(users).innerJoin(posts, "__q2", sqlAst`${ref("p", posts.authorId)} = ${ref("users", users.id)}`), /reserved/);
  // Regular aliases keep working (column refs stay owner-qualified until Q01
  // adds alias-aware references).
  const q = astSelect({ email: users.email }).from(users, "u").toSQL();
  assert.equal(q.sql, 'select "users"."email" from "users" as "u"');
});
