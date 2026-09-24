// ORM capability probes (orm-program X00 / V18).
//
// Each probe states one PostgreSQL behaviour that @neutron-build/sql, the CLI
// introspector or Studio depends on, and checks the connected engine against
// it through the in-repo neutron-sql driver adapters. The expectation is
// always the PostgreSQL behaviour: the same probes run against PostgreSQL 17
// as a control and must all be `supported` there. A probe is never relaxed to
// fit an engine — an engine that behaves differently gets `unsupported`.
//
// Verdicts:
//   supported    behaved exactly as PostgreSQL does
//   unsupported  answered differently: a server error, a wrong result, or no
//                answer within the probe deadline
//   unknown      no verdict possible: a precondition (setup) failed or the
//                connection was lost, so the capability itself was not tested
//
// Probes run in order; each gets fresh connections and uniquely named objects.

import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// Probe vocabulary
// ---------------------------------------------------------------------------

export class Precondition extends Error {}

/** Run setup SQL. A failure here means the probe cannot reach its verdict. */
export async function setup(drv, statements) {
  for (const s of Array.isArray(statements) ? statements : [statements]) {
    try {
      await drv.execute(s);
    } catch (err) {
      throw new Precondition(`setup failed: ${describeError(err)} — in: ${s.slice(0, 160)}`, { cause: err });
    }
  }
}

export function describeError(err) {
  if (!err || typeof err !== "object") return String(err);
  const state = err.sqlstate ? ` [${err.sqlstate}]` : "";
  return `${err.name ?? "Error"}${state}: ${String(err.message ?? err).split("\n")[0].slice(0, 300)}`;
}

/** Expect `fn` to fail with the given SQLSTATE. */
async function expectState(fn, sqlstate, what) {
  let err = null;
  try {
    await fn();
  } catch (e) {
    err = e;
  }
  if (!err) throw new assert.AssertionError({ message: `${what}: expected SQLSTATE ${sqlstate}, statement succeeded` });
  if (err.sqlstate !== sqlstate) {
    if (err.sqlstate === undefined && !(err instanceof assert.AssertionError)) throw err;
    throw new assert.AssertionError({ message: `${what}: expected SQLSTATE ${sqlstate}, got ${describeError(err)}` });
  }
}

const rowsOf = async (drv, text, params) => drv.query(text, params);
const one = async (drv, text, params) => (await drv.query(text, params))[0];
const parse = (v) => (typeof v === "string" ? JSON.parse(v) : v);

// Relation fixture shared by the relation-SQL and DML probes. Users 1..3,
// posts: user 1 -> 10, 11; user 2 -> 12, 13 (13 has NULL score); user 3 none.
function relFixture(t) {
  return [
    `create table ${t}_u (id int primary key, name text not null)`,
    `create table ${t}_p (id int primary key, u_id int not null references ${t}_u (id), title text not null, score int)`,
    `insert into ${t}_u (id, name) values (1, 'ann'), (2, 'bob'), (3, 'cy')`,
    `insert into ${t}_p (id, u_id, title, score) values (10, 1, 'a', 5), (11, 1, 'b', 7), (12, 2, 'c', 1), (13, 2, 'd', null)`,
  ];
}

function relProbe(id, title, body) {
  return {
    id,
    area: "relation-sql",
    title,
    run: async (ctx) => {
      const t = ctx.name("r");
      const drv = await ctx.session();
      await setup(drv, relFixture(t));
      await body(drv, t);
    },
  };
}

function dmlProbe(id, title, body) {
  return { ...relProbe(id, title, body), area: "dml" };
}

// ---------------------------------------------------------------------------
// engine
// ---------------------------------------------------------------------------

const engineProbes = [
  {
    id: "engine.identity",
    area: "engine",
    title: "SELECT VERSION() identifies the engine (FRAMEWORK_CONTRACT §1)",
    run: async (ctx) => {
      const identity = await ctx.identity();
      assert.notEqual(identity.product, "unknown", `unrecognised version string: ${identity.raw}`);
      return `${identity.product} ${identity.version}`;
    },
  },
  {
    id: "engine.capability.jsonb-functions",
    area: "engine",
    title: "neutron-sql capability gate resolves jsonb-functions (relational reads require it)",
    run: async (ctx) => {
      const db = await ctx.orm({}, {});
      const ev = await db.capability("jsonb-functions");
      if (ev.status === "supported") return ev.evidence;
      return { status: ev.status, detail: ev.evidence };
    },
  },
];

// ---------------------------------------------------------------------------
// relation-sql: the primitives the relational compiler emits
// ---------------------------------------------------------------------------

const relationProbes = [
  relProbe("relation.correlated_scalar_subquery", "correlated scalar subquery in the select list", async (drv, t) => {
    const rows = await rowsOf(drv, `select u.id, (select count(*) from ${t}_p p where p.u_id = u.id)::int as n from ${t}_u u order by u.id`);
    assert.deepEqual(rows.map((r) => [Number(r.id), Number(r.n)]), [[1, 2], [2, 2], [3, 0]]);
  }),
  relProbe("relation.jsonb_build_object", "jsonb_build_object with mixed value types", async (drv) => {
    const r = await one(drv, `select jsonb_build_object('a', 1, 'b', 'x', 'c', null)::text as v`);
    assert.deepEqual(parse(r.v), { a: 1, b: "x", c: null });
  }),
  relProbe("relation.jsonb_agg_order_by", "jsonb_agg(expr ORDER BY ...) honours the aggregate ordering", async (drv, t) => {
    const r = await one(drv, `select jsonb_agg(p.id order by p.id desc)::text as v from ${t}_p p where p.u_id = 1`);
    assert.deepEqual(parse(r.v), [11, 10]);
  }),
  relProbe("relation.coalesce_empty_jsonb_array", "coalesce(jsonb_agg over no rows, '[]'::jsonb) yields []", async (drv, t) => {
    const r = await one(drv, `select coalesce((select jsonb_agg(p.id) from ${t}_p p where p.u_id = 3), '[]'::jsonb)::text as v`);
    assert.deepEqual(parse(r.v), []);
  }),
  relProbe("relation.correlated_derived_table_limit", "per-parent LIMIT: derived table in a scalar subquery correlated to the outer row", async (drv, t) => {
    const rows = await rowsOf(
      drv,
      `select u.id, (select coalesce(jsonb_agg(l.id order by l.id desc), '[]'::jsonb) from (select p.id from ${t}_p p where p.u_id = u.id order by p.id desc limit 1) as l)::text as ids from ${t}_u u order by u.id`,
    );
    assert.deepEqual(rows.map((r) => [Number(r.id), parse(r.ids)]), [[1, [11]], [2, [13]], [3, []]]);
  }),
  relProbe("relation.nested_correlation", "two-level nested jsonb aggregation correlated to the outermost row", async (drv, t) => {
    const r = await one(
      drv,
      `select (select jsonb_agg(jsonb_build_object('p', p.id, 'owner', (select x.name from ${t}_u x where x.id = p.u_id and x.id = u.id)) order by p.id) from ${t}_p p where p.u_id = u.id)::text as v from ${t}_u u where u.id = 1`,
    );
    assert.deepEqual(parse(r.v), [{ p: 10, owner: "ann" }, { p: 11, owner: "ann" }]);
  }),
  relProbe("relation.to_jsonb_text_leaf", "lossless leaf encoding: int8/numeric/timestamptz cast to text inside jsonb", async (drv) => {
    const r = await one(
      drv,
      `select jsonb_build_object('i', (9223372036854775807::int8)::text, 'n', (12345678901234567890.123456789::numeric)::text, 't', to_jsonb(('2026-01-02 03:04:05.123456'::timestamp)::text))::text as v`,
    );
    assert.deepEqual(parse(r.v), { i: "9223372036854775807", n: "12345678901234567890.123456789", t: "2026-01-02 03:04:05.123456" });
  }),
  relProbe("relation.lateral_join", "CROSS JOIN LATERAL", async (drv, t) => {
    const rows = await rowsOf(drv, `select u.id, l.n from ${t}_u u cross join lateral (select count(*)::int as n from ${t}_p p where p.u_id = u.id) l order by u.id`);
    assert.deepEqual(rows.map((r) => [Number(r.id), Number(r.n)]), [[1, 2], [2, 2], [3, 0]]);
  }),
  relProbe("relation.left_join_lateral", "LEFT JOIN LATERAL ... ON true keeps parents without children", async (drv, t) => {
    const rows = await rowsOf(
      drv,
      `select u.id, l.id as pid from ${t}_u u left join lateral (select p.id from ${t}_p p where p.u_id = u.id order by p.id desc limit 1) l on true order by u.id`,
    );
    assert.deepEqual(rows.map((r) => [Number(r.id), r.pid === null ? null : Number(r.pid)]), [[1, 11], [2, 13], [3, null]]);
  }),
  relProbe("relation.left_join_nulls", "LEFT JOIN null-extends unmatched rows", async (drv, t) => {
    const rows = await rowsOf(drv, `select u.id, p.id as pid from ${t}_u u left join ${t}_p p on p.u_id = u.id where u.id = 3`);
    assert.deepEqual(rows.map((r) => [Number(r.id), r.pid]), [[3, null]]);
  }),
  relProbe("relation.row_value_comparison", "row-value comparison (a, b) > (x, y) (keyset pagination)", async (drv, t) => {
    const rows = await rowsOf(drv, `select id from ${t}_p where (u_id, id) > ($1::int, $2::int) order by u_id, id`, [1, 10]);
    assert.deepEqual(rows.map((r) => Number(r.id)), [11, 12, 13]);
  }),
  relProbe("relation.cte", "WITH (non-recursive CTE)", async (drv, t) => {
    const rows = await rowsOf(drv, `with c as (select u_id, count(*)::int as n from ${t}_p group by u_id) select u.id, coalesce(c.n, 0) as n from ${t}_u u left join c on c.u_id = u.id order by u.id`);
    assert.deepEqual(rows.map((r) => [Number(r.id), Number(r.n)]), [[1, 2], [2, 2], [3, 0]]);
  }),
  relProbe("relation.cte_recursive", "WITH RECURSIVE", async (drv) => {
    const r = await one(drv, `with recursive r(n) as (select 1 union all select n + 1 from r where n < 5) select sum(n)::int as s from r`);
    assert.equal(Number(r.s), 15);
  }),
  relProbe("relation.set_operations", "UNION / UNION ALL / INTERSECT / EXCEPT", async (drv, t) => {
    const u = await rowsOf(drv, `select u_id as v from ${t}_p union select id from ${t}_u order by 1`);
    const ua = await rowsOf(drv, `select u_id as v from ${t}_p union all select id from ${t}_u order by 1`);
    const i = await rowsOf(drv, `select u_id as v from ${t}_p intersect select id from ${t}_u order by 1`);
    const e = await rowsOf(drv, `select id as v from ${t}_u except select u_id from ${t}_p order by 1`);
    assert.deepEqual(u.map((r) => Number(r.v)), [1, 2, 3], "union");
    assert.deepEqual(ua.map((r) => Number(r.v)), [1, 1, 1, 2, 2, 2, 3], "union all");
    assert.deepEqual(i.map((r) => Number(r.v)), [1, 2], "intersect");
    assert.deepEqual(e.map((r) => Number(r.v)), [3], "except");
  }),
  relProbe("relation.distinct_on", "DISTINCT ON", async (drv, t) => {
    const rows = await rowsOf(drv, `select distinct on (u_id) u_id, id from ${t}_p order by u_id, id desc`);
    assert.deepEqual(rows.map((r) => [Number(r.u_id), Number(r.id)]), [[1, 11], [2, 13]]);
  }),
  relProbe("relation.window_row_number", "row_number() OVER (PARTITION BY ... ORDER BY ...)", async (drv, t) => {
    const rows = await rowsOf(drv, `select id, row_number() over (partition by u_id order by id desc)::int as rn from ${t}_p order by id`);
    assert.deepEqual(rows.map((r) => [Number(r.id), Number(r.rn)]), [[10, 2], [11, 1], [12, 2], [13, 1]]);
  }),
  relProbe("relation.aggregate_filter", "aggregate FILTER (WHERE ...)", async (drv, t) => {
    const r = await one(drv, `select count(*) filter (where score > 4)::int as hi, count(score)::int as nn, count(*)::int as n from ${t}_p`);
    assert.deepEqual([Number(r.hi), Number(r.nn), Number(r.n)], [2, 3, 4]);
  }),
  relProbe("relation.group_by_having", "GROUP BY ... HAVING", async (drv, t) => {
    const rows = await rowsOf(drv, `select u_id, sum(score)::int as s from ${t}_p group by u_id having sum(score) > $1::int order by u_id`, [5]);
    assert.deepEqual(rows.map((r) => [Number(r.u_id), Number(r.s)]), [[1, 12]]);
  }),
  relProbe("relation.exists_subquery", "EXISTS / NOT EXISTS correlated subqueries", async (drv, t) => {
    const a = await rowsOf(drv, `select id from ${t}_u u where exists (select 1 from ${t}_p p where p.u_id = u.id) order by id`);
    const b = await rowsOf(drv, `select id from ${t}_u u where not exists (select 1 from ${t}_p p where p.u_id = u.id) order by id`);
    assert.deepEqual(a.map((r) => Number(r.id)), [1, 2]);
    assert.deepEqual(b.map((r) => Number(r.id)), [3]);
  }),
  relProbe("relation.in_subquery", "IN (subquery)", async (drv, t) => {
    const rows = await rowsOf(drv, `select id from ${t}_u where id in (select u_id from ${t}_p where score is null) order by id`);
    assert.deepEqual(rows.map((r) => Number(r.id)), [2]);
  }),
  relProbe("relation.any_array_param", "= ANY($1::int[]) with an array parameter", async (drv, t) => {
    const rows = await rowsOf(drv, `select id from ${t}_u where id = any($1::int[]) order by id`, [[1, 3]]);
    assert.deepEqual(rows.map((r) => Number(r.id)), [1, 3]);
  }),
  relProbe("relation.order_nulls_last", "ORDER BY ... DESC NULLS LAST / ASC NULLS FIRST", async (drv, t) => {
    const a = await rowsOf(drv, `select id from ${t}_p order by score desc nulls last, id`);
    const b = await rowsOf(drv, `select id from ${t}_p order by score asc nulls first, id`);
    assert.deepEqual(a.map((r) => Number(r.id)), [11, 10, 12, 13]);
    assert.deepEqual(b.map((r) => Number(r.id)), [13, 12, 10, 11]);
  }),
  relProbe("relation.limit_offset_params", "LIMIT $n OFFSET $m as bound parameters", async (drv, t) => {
    const rows = await rowsOf(drv, `select id from ${t}_p order by id limit $1 offset $2`, [2, 1]);
    assert.deepEqual(rows.map((r) => Number(r.id)), [11, 12]);
  }),
  relProbe("relation.schema_qualified", "CREATE SCHEMA and schema-qualified tables", async (drv, t) => {
    await drv.execute(`create schema ${t}_s`);
    await drv.execute(`create table ${t}_s.items (id int primary key, v text)`);
    await drv.execute(`insert into ${t}_s.items values (1, 'x')`);
    const r = await one(drv, `select v from ${t}_s.items where id = 1`);
    assert.equal(r.v, "x");
  }),
  relProbe("relation.quoted_mixed_case_identifiers", "quoted mixed-case identifiers keep their case", async (drv, t) => {
    await drv.execute(`create table "${t}_Mixed" ("Id" int primary key, "camelCol" text)`);
    await drv.execute(`insert into "${t}_Mixed" ("Id", "camelCol") values (1, 'v')`);
    const r = await one(drv, `select "camelCol" from "${t}_Mixed" where "Id" = 1`);
    assert.equal(r.camelCol, "v");
  }),
];

// ---------------------------------------------------------------------------
// dml
// ---------------------------------------------------------------------------

const dmlProbes = [
  dmlProbe("dml.insert_returning", "INSERT ... RETURNING", async (drv, t) => {
    const rows = await rowsOf(drv, `insert into ${t}_u (id, name) values (4, 'di'), (5, 'ed') returning id, name`);
    assert.deepEqual(rows.map((r) => [Number(r.id), r.name]), [[4, "di"], [5, "ed"]]);
  }),
  dmlProbe("dml.update_returning", "UPDATE ... RETURNING", async (drv, t) => {
    const rows = await rowsOf(drv, `update ${t}_p set score = score + 1 where u_id = $1 returning id, score`, [1]);
    assert.deepEqual(rows.map((r) => [Number(r.id), Number(r.score)]).sort((a, b) => a[0] - b[0]), [[10, 6], [11, 8]]);
  }),
  dmlProbe("dml.delete_returning", "DELETE ... RETURNING", async (drv, t) => {
    const rows = await rowsOf(drv, `delete from ${t}_p where id = $1 returning id, title`, [12]);
    assert.deepEqual(rows.map((r) => [Number(r.id), r.title]), [[12, "c"]]);
    const left = await one(drv, `select count(*)::int as n from ${t}_p`);
    assert.equal(Number(left.n), 3);
  }),
  dmlProbe("dml.multirow_values_default", "multi-row VALUES with DEFAULT in individual cells", async (drv, t) => {
    await setup(drv, `create table ${t}_d (k serial primary key, a int not null default 7, b int not null default 9)`);
    const rows = await rowsOf(drv, `insert into ${t}_d (a, b) values (1, default), (default, 2) returning a, b`);
    assert.deepEqual(rows.map((r) => [Number(r.a), Number(r.b)]), [[1, 9], [7, 2]]);
  }),
  dmlProbe("dml.default_values", "INSERT ... DEFAULT VALUES", async (drv, t) => {
    await setup(drv, `create table ${t}_d (k serial primary key, a int not null default 7)`);
    const r = await one(drv, `insert into ${t}_d default values returning k, a`);
    assert.deepEqual([Number(r.k), Number(r.a)], [1, 7]);
  }),
  dmlProbe("dml.on_conflict_do_update", "INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col", async (drv, t) => {
    const rows = await rowsOf(drv, `insert into ${t}_u (id, name) values (1, 'ann2'), (9, 'new') on conflict (id) do update set name = excluded.name returning id, name`);
    assert.deepEqual(rows.map((r) => [Number(r.id), r.name]).sort((a, b) => a[0] - b[0]), [[1, "ann2"], [9, "new"]]);
    const n = await one(drv, `select count(*)::int as n from ${t}_u`);
    assert.equal(Number(n.n), 4);
  }),
  dmlProbe("dml.on_conflict_do_nothing", "INSERT ... ON CONFLICT DO NOTHING affects zero rows on a duplicate", async (drv, t) => {
    const rows = await rowsOf(drv, `insert into ${t}_u (id, name) values (1, 'dup') on conflict do nothing returning id`);
    assert.equal(rows.length, 0);
    const r = await one(drv, `select name from ${t}_u where id = 1`);
    assert.equal(r.name, "ann");
  }),
  dmlProbe("dml.on_conflict_where", "ON CONFLICT DO UPDATE ... WHERE (conditional upsert)", async (drv, t) => {
    const a = await rowsOf(drv, `insert into ${t}_p (id, u_id, title, score) values (10, 1, 'x', 1) on conflict (id) do update set score = excluded.score where ${t}_p.score < excluded.score returning id`);
    const b = await rowsOf(drv, `insert into ${t}_p (id, u_id, title, score) values (10, 1, 'x', 99) on conflict (id) do update set score = excluded.score where ${t}_p.score < excluded.score returning id, score`);
    assert.equal(a.length, 0, "lower score must not update");
    assert.deepEqual(b.map((r) => [Number(r.id), Number(r.score)]), [[10, 99]]);
  }),
  dmlProbe("dml.on_conflict_unique_column", "ON CONFLICT on a non-PK unique column", async (drv, t) => {
    await setup(drv, `create table ${t}_e (id serial primary key, email text not null unique, hits int not null default 0)`);
    await drv.execute(`insert into ${t}_e (email) values ('a@x')`);
    const r = await one(drv, `insert into ${t}_e (email) values ('a@x') on conflict (email) do update set hits = ${t}_e.hits + 1 returning id, hits`);
    assert.deepEqual([Number(r.id), Number(r.hits)], [1, 1]);
  }),
  dmlProbe("dml.update_from", "UPDATE ... FROM (join update)", async (drv, t) => {
    const n = await drv.execute(`update ${t}_p p set title = u.name from ${t}_u u where u.id = p.u_id and u.id = 1`);
    assert.equal(n, 2);
    const rows = await rowsOf(drv, `select title from ${t}_p where u_id = 1 order by id`);
    assert.deepEqual(rows.map((r) => r.title), ["ann", "ann"]);
  }),
  dmlProbe("dml.delete_using", "DELETE ... USING (join delete)", async (drv, t) => {
    const n = await drv.execute(`delete from ${t}_p p using ${t}_u u where u.id = p.u_id and u.name = 'bob'`);
    assert.equal(n, 2);
  }),
  dmlProbe("dml.affected_row_counts", "UPDATE/DELETE report exact affected row counts", async (drv, t) => {
    assert.equal(await drv.execute(`update ${t}_p set score = 0 where u_id = 2`), 2);
    assert.equal(await drv.execute(`update ${t}_p set score = 0 where u_id = 3`), 0);
    assert.equal(await drv.execute(`delete from ${t}_p where id in (10, 11, 99)`), 2);
  }),
  dmlProbe("dml.identity_column", "GENERATED ALWAYS AS IDENTITY assigns values and rejects explicit ones (428C9)", async (drv, t) => {
    await setup(drv, `create table ${t}_i (id int generated always as identity primary key, v text)`);
    const r = await one(drv, `insert into ${t}_i (v) values ('a') returning id`);
    assert.equal(Number(r.id), 1);
    await expectState(() => drv.execute(`insert into ${t}_i (id, v) values (50, 'b')`), "428C9", "explicit identity value");
  }),
  dmlProbe("dml.generated_stored_column", "GENERATED ALWAYS AS (expr) STORED computes the column", async (drv, t) => {
    await setup(drv, `create table ${t}_g (a int not null, b int generated always as (a * 2) stored)`);
    const r = await one(drv, `insert into ${t}_g (a) values (3) returning b`);
    assert.equal(Number(r.b), 6);
  }),
];

// ---------------------------------------------------------------------------
// constraints
// ---------------------------------------------------------------------------

function conProbe(id, title, body) {
  return { ...relProbe(id, title, body), area: "constraints" };
}

const constraintProbes = [
  conProbe("constraint.not_null", "NOT NULL violation -> 23502", async (drv, t) => {
    await expectState(() => drv.execute(`insert into ${t}_u (id, name) values (7, null)`), "23502", "null into NOT NULL");
  }),
  conProbe("constraint.unique", "primary key / unique violation -> 23505", async (drv, t) => {
    await expectState(() => drv.execute(`insert into ${t}_u (id, name) values (1, 'again')`), "23505", "duplicate pk");
  }),
  conProbe("constraint.foreign_key_insert", "foreign key violation on insert -> 23503", async (drv, t) => {
    await expectState(() => drv.execute(`insert into ${t}_p (id, u_id, title) values (99, 42, 'orphan')`), "23503", "orphan child");
  }),
  conProbe("constraint.foreign_key_restrict_delete", "deleting a referenced parent (NO ACTION) -> 23503", async (drv, t) => {
    await expectState(() => drv.execute(`delete from ${t}_u where id = 1`), "23503", "delete referenced parent");
  }),
  conProbe("constraint.check", "CHECK violation -> 23514", async (drv, t) => {
    await setup(drv, `create table ${t}_c (id int primary key, qty int not null check (qty >= 0))`);
    await expectState(() => drv.execute(`insert into ${t}_c values (1, -1)`), "23514", "check violation");
  }),
  conProbe("constraint.fk_on_delete_cascade", "ON DELETE CASCADE removes children", async (drv, t) => {
    await setup(drv, [
      `create table ${t}_a (id int primary key)`,
      `create table ${t}_b (id int primary key, a_id int not null references ${t}_a (id) on delete cascade)`,
      `insert into ${t}_a values (1), (2)`,
      `insert into ${t}_b values (10, 1), (11, 1), (12, 2)`,
    ]);
    await drv.execute(`delete from ${t}_a where id = 1`);
    const r = await one(drv, `select count(*)::int as n from ${t}_b`);
    assert.equal(Number(r.n), 1);
  }),
  conProbe("constraint.composite_foreign_key", "composite (tenant, id) foreign key is enforced as a tuple", async (drv, t) => {
    await setup(drv, [
      `create table ${t}_inv (tenant int not null, id int not null, primary key (tenant, id))`,
      `create table ${t}_line (id int primary key, tenant int not null, inv_id int not null, foreign key (tenant, inv_id) references ${t}_inv (tenant, id))`,
      `insert into ${t}_inv values (1, 1), (2, 2)`,
      `insert into ${t}_line values (1, 1, 1)`,
    ]);
    await expectState(() => drv.execute(`insert into ${t}_line values (2, 1, 2)`), "23503", "tuple (1,2) does not exist although both halves do");
  }),
  conProbe("constraint.deferrable_fk", "DEFERRABLE INITIALLY DEFERRED foreign key checks at COMMIT", async (drv, t) => {
    // The DEFERRABLE clause is itself the capability under test, so its DDL
    // runs in the body: an engine that rejects the shape gets `unsupported`
    // with that error as evidence, not an `unknown` from failed scaffolding.
    await drv.execute(`create table ${t}_pa (id int primary key)`);
    await drv.execute(`create table ${t}_ch (id int primary key, pa int not null references ${t}_pa (id) deferrable initially deferred)`);
    await drv.begin(async (tx) => {
      await tx.execute(`insert into ${t}_ch values (1, 5)`);
      await tx.execute(`insert into ${t}_pa values (5)`);
    });
    await expectState(
      () => drv.begin(async (tx) => { await tx.execute(`insert into ${t}_ch values (2, 6)`); }),
      "23503",
      "deferred violation at commit",
    );
  }),
  conProbe("constraint.varchar_length", "varchar(n) overflow -> 22001", async (drv, t) => {
    await setup(drv, `create table ${t}_v (id int primary key, s varchar(3))`);
    await expectState(() => drv.execute(`insert into ${t}_v values (1, 'abcd')`), "22001", "varchar(3) overflow");
  }),
];

// ---------------------------------------------------------------------------
// codecs: parameters in, ::text oracle out
// ---------------------------------------------------------------------------

function codecProbe(id, title, columns, body) {
  return {
    id,
    area: "codec",
    title,
    run: async (ctx) => {
      const t = ctx.name("c");
      const drv = await ctx.session();
      await setup(drv, `create table ${t} (id int primary key, ${columns})`);
      await body(drv, t);
    },
  };
}

const INT8_MAX = "9223372036854775807";
const INT8_MIN = "-9223372036854775808";

const codecProbes = [
  codecProbe("codec.int8_extremes", "int8 min/max round-trip exactly", "v int8", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1), (2, $2)`, [INT8_MAX, INT8_MIN]);
    const rows = await rowsOf(drv, `select v::text as v from ${t} order by id`);
    assert.deepEqual(rows.map((r) => r.v), [INT8_MAX, INT8_MIN]);
  }),
  codecProbe("codec.numeric_precision", "numeric(40,20) keeps every digit", "v numeric(40,20)", async (drv, t) => {
    const v = "12345678901234567890.12345678901234567890";
    await drv.execute(`insert into ${t} values (1, $1)`, [v]);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, v);
  }),
  codecProbe("codec.numeric_unconstrained", "unconstrained numeric keeps scale as written", "v numeric", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1), (2, $2)`, ["1.50", "-0.000000000000000000001"]);
    const rows = await rowsOf(drv, `select v::text as v from ${t} order by id`);
    assert.deepEqual(rows.map((r) => r.v), ["1.50", "-0.000000000000000000001"]);
  }),
  codecProbe("codec.timestamp_microseconds", "timestamp keeps microseconds, no timezone shift", "v timestamp", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::text::timestamp)`, ["2026-01-02 03:04:05.123456"]);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, "2026-01-02 03:04:05.123456");
  }),
  codecProbe("codec.timestamptz_utc", "timestamptz normalises to UTC under TimeZone=UTC, microseconds kept", "v timestamptz", async (drv, t) => {
    await drv.execute(`set time zone 'UTC'`);
    await drv.execute(`insert into ${t} values (1, $1::text::timestamptz)`, ["2026-01-02 03:04:05.123456+02"]);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, "2026-01-02 01:04:05.123456+00");
  }),
  codecProbe("codec.timestamptz_session_timezone", "timestamptz text output follows the session TimeZone", "v timestamptz", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::text::timestamptz)`, ["2026-01-02 00:00:00+00"]);
    await drv.execute(`set time zone 'Asia/Tokyo'`);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, "2026-01-02 09:00:00+09");
  }),
  codecProbe("codec.date", "date round-trip incl. infinity", "v date", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::text::date), (2, 'infinity')`, ["2024-02-29"]);
    const rows = await rowsOf(drv, `select v::text as v from ${t} order by id`);
    assert.deepEqual(rows.map((r) => r.v), ["2024-02-29", "infinity"]);
  }),
  codecProbe("codec.bytea", "bytea round-trips every byte (incl. 0x00, 0x5c, 0xff)", "v bytea", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1)`, [Buffer.from([0x00, 0x01, 0x5c, 0xff, 0x27])]);
    const r = await one(drv, `select encode(v, 'hex') as h, v from ${t}`);
    assert.equal(r.h, "00015cff27");
    assert.deepEqual([...r.v], [0x00, 0x01, 0x5c, 0xff, 0x27]);
  }),
  codecProbe("codec.jsonb_roundtrip", "jsonb keeps structure and large numeric literals", "v jsonb", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::text::jsonb)`, [`{"a":[1,2,{"b":null}],"big":12345678901234567890,"s":"x"}`]);
    const r = await one(drv, `select v->>'big' as big, (v->'a'->2->'b') is not null as has_b, jsonb_typeof(v->'a'->2->'b') as tb, v->'a'->>0 as a0 from ${t}`);
    assert.deepEqual([r.big, r.has_b, r.tb, r.a0], ["12345678901234567890", true, "null", "1"]);
  }),
  codecProbe("codec.jsonb_null_vs_sql_null", "JSON null and SQL NULL stay distinct in jsonb", "v jsonb", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, 'null'::jsonb), (2, null)`);
    const rows = await rowsOf(drv, `select id, v is null as sqlnull, jsonb_typeof(v) as ty from ${t} order by id`);
    assert.deepEqual(rows.map((r) => [Number(r.id), r.sqlnull, r.ty]), [[1, false, "null"], [2, true, null]]);
  }),
  codecProbe("codec.uuid", "uuid round-trip and canonical lower-case text", "v uuid", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1)`, ["6F1C2B3A-4D5E-4F60-8A7B-9C0D1E2F3A4B"]);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, "6f1c2b3a-4d5e-4f60-8a7b-9c0d1e2f3a4b");
  }),
  codecProbe("codec.float8_special", "float8 NaN / Infinity / -Infinity", "v float8", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, $1)`, [0.1]);
    const rows = await rowsOf(drv, `select v::text as v from ${t} order by id`);
    assert.deepEqual(rows.map((r) => r.v), ["NaN", "Infinity", "-Infinity", "0.1"]);
  }),
  codecProbe("codec.boolean", "boolean round-trip", "v boolean", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1), (2, $2)`, [true, false]);
    const rows = await rowsOf(drv, `select v from ${t} order by id`);
    assert.deepEqual(rows.map((r) => r.v), [true, false]);
  }),
  codecProbe("codec.text_array_param", "text[] parameter keeps elements containing commas and quotes", "v text[]", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::text[])`, [["a", "b,c", 'd"e']]);
    const r = await one(drv, `select array_length(v, 1) as n, v[2] as second, v[3] as third from ${t}`);
    assert.deepEqual([Number(r.n), r.second, r.third], [3, "b,c", 'd"e']);
  }),
  codecProbe("codec.int_array_result", "int4[] result decodes to a JS array (array type OID on the wire)", "v int4[]", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, '{1,2,3}')`);
    const r = await one(drv, `select v from ${t}`);
    assert.deepEqual(r.v, [1, 2, 3]);
  }),
  codecProbe("codec.param_typed_int", "SELECT $1::int4 describes and returns an integer", "v int", async (drv) => {
    const r = await one(drv, `select $1::int4 as v`, [1]);
    assert.equal(r.v, 1);
  }),
  codecProbe("codec.param_typed_jsonb", "SELECT $1::jsonb describes its result column", "v int", async (drv) => {
    const r = await one(drv, `select $1::text::jsonb as v`, [`{"k":1}`]);
    assert.deepEqual(parse(r.v), { k: 1 });
  }),
  codecProbe("codec.enum", "CREATE TYPE ... AS ENUM: values, declared ordering, invalid label 22P02", "x int", async (drv, t) => {
    await setup(drv, [`create type ${t}_mood as enum ('sad', 'ok', 'happy')`, `create table ${t}_m (id int primary key, m ${t}_mood not null)`]);
    await drv.execute(`insert into ${t}_m values (1, 'happy'), (2, 'sad'), (3, 'ok')`);
    const rows = await rowsOf(drv, `select m::text as label from ${t}_m order by m`);
    assert.deepEqual(rows.map((r) => r.label), ["sad", "ok", "happy"]);
    await expectState(() => drv.execute(`insert into ${t}_m values (4, 'angry')`), "22P02", "invalid enum label");
  }),
  codecProbe("codec.interval", "interval text output", "v interval", async (drv, t) => {
    await drv.execute(`insert into ${t} values (1, $1::interval)`, ["1 day 02:03:04.5"]);
    const r = await one(drv, `select v::text as v from ${t}`);
    assert.equal(r.v, "1 day 02:03:04.5");
  }),
];

// ---------------------------------------------------------------------------
// catalog: the query shapes cli/internal/db/introspect_v2.go issues
// ---------------------------------------------------------------------------

function catalogProbe(id, title, body) {
  return {
    id,
    area: "catalog",
    title,
    run: async (ctx) => {
      const t = ctx.name("k");
      const drv = await ctx.session();
      // Plain index only: a partial index is its own capability (probed by
      // catalog.partial_index) and must not sink every introspection probe
      // to `unknown` on an engine that cannot build one.
      await setup(drv, [
        `create table ${t}_parent (id bigserial primary key, code varchar(20) not null unique, amount numeric(10,2) not null default 0, note text, created timestamptz not null default now())`,
        `create table ${t}_child (id serial primary key, parent_id bigint not null references ${t}_parent (id) on delete cascade, qty int not null check (qty > 0), tag text)`,
        `create index ${t}_child_tag_idx on ${t}_child (tag)`,
      ]);
      const oid = async (name) => {
        const r = await one(drv, `select c.oid::int8::text as oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = $1 and n.nspname = current_schema()`, [name]);
        if (!r) throw new assert.AssertionError({ message: `pg_class has no row for ${name}` });
        return Number(r.oid);
      };
      await body(drv, t, oid);
    },
  };
}

const catalogProbes = [
  catalogProbe("catalog.information_schema_columns", "information_schema.columns: order, types, nullability, defaults", async (drv, t) => {
    const rows = await rowsOf(
      drv,
      `select column_name, data_type, is_nullable, column_default is not null as has_default from information_schema.columns where table_name = $1 order by ordinal_position`,
      [`${t}_parent`],
    );
    assert.deepEqual(
      rows.map((r) => [r.column_name, r.data_type, r.is_nullable, r.has_default]),
      [
        ["id", "bigint", "NO", true],
        ["code", "character varying", "NO", false],
        ["amount", "numeric", "NO", true],
        ["note", "text", "YES", false],
        ["created", "timestamp with time zone", "NO", true],
      ],
    );
  }),
  catalogProbe("catalog.pg_class_relations", "pg_class/pg_namespace relation listing with relkind (introspectV2RelationsSQL core)", async (drv, t) => {
    const rows = await rowsOf(
      drv,
      `select c.relname, c.relkind::text as relkind, c.relrowsecurity from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r','p','v','m','f','S') and n.nspname <> 'information_schema' and n.nspname !~ '^pg_' and c.relname like $1 order by c.relname`,
      [`${t}\\_%`],
    );
    const tables = rows.filter((r) => r.relkind === "r").map((r) => r.relname);
    const seqs = rows.filter((r) => r.relkind === "S").map((r) => r.relname);
    assert.deepEqual(tables, [`${t}_child`, `${t}_parent`]);
    assert.deepEqual(seqs, [`${t}_child_id_seq`, `${t}_parent_id_seq`]);
  }),
  catalogProbe("catalog.introspect_relations_query", "the introspector's full relations query (pg_depend/pg_policy/pg_trigger/pg_inherits subqueries) executes", async (drv, t) => {
    const rows = await rowsOf(
      drv,
      `SELECT c.oid, n.nspname, c.relname, c.relkind::text, c.relpersistence::text, c.relrowsecurity, c.relforcerowsecurity, c.relispartition,
       COALESCE(c.reloptions, '{}'::name[])::text[] AS reloptions,
       EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e') AS extension_owned,
       (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS npolicies,
       (SELECT count(*) FROM pg_trigger g WHERE g.tgrelid = c.oid AND NOT g.tgisinternal) AS ntriggers,
       (SELECT count(*) FROM pg_inherits i WHERE i.inhrelid = c.oid) AS nparents,
       EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')) AS sequence_attached
       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S') AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relname = $1`,
      [`${t}_parent_id_seq`],
    );
    assert.equal(rows.length, 1, "sequence row");
    assert.equal(rows[0].sequence_attached, true, "serial sequence is attached to its column via pg_depend");
    assert.equal(rows[0].extension_owned, false);
  }),
  catalogProbe("catalog.pg_attribute_types", "pg_attribute + pg_type + atttypmod + pg_get_expr(adbin) (introspectV2ColumnsSQL)", async (drv, t, oid) => {
    const rows = await rowsOf(
      drv,
      `SELECT a.attnum, a.attname, t.typname, a.atttypmod, a.attnotnull, a.attidentity::text AS attidentity, a.attgenerated::text AS attgenerated, pg_get_expr(ad.adbin, a.attrelid) AS default_expr
       FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid JOIN pg_namespace tn ON tn.oid = t.typnamespace
       LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
       WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
      [await oid(`${t}_parent`)],
    );
    assert.deepEqual(
      rows.map((r) => [r.attname, r.typname, Number(r.atttypmod), r.attnotnull]),
      [
        ["id", "int8", -1, true],
        ["code", "varchar", 24, true],
        ["amount", "numeric", 655366, true],
        ["note", "text", -1, false],
        ["created", "timestamptz", -1, true],
      ],
    );
    const defaults = Object.fromEntries(rows.map((r) => [r.attname, r.default_expr]));
    assert.equal(defaults.id, `nextval('${t}_parent_id_seq'::regclass)`);
    assert.equal(defaults.amount, "0");
    assert.equal(defaults.created, "now()");
    assert.equal(defaults.note, null);
  }),
  catalogProbe("catalog.format_type", "format_type(atttypid, atttypmod) renders parameterised types", async (drv, t, oid) => {
    const rows = await rowsOf(drv, `select attname, format_type(atttypid, atttypmod) as ty from pg_attribute where attrelid = $1 and attnum > 0 and not attisdropped order by attnum`, [await oid(`${t}_parent`)]);
    assert.deepEqual(rows.map((r) => r.ty), ["bigint", "character varying(20)", "numeric(10,2)", "text", "timestamp with time zone"]);
  }),
  catalogProbe("catalog.pg_constraint", "pg_constraint: contype, conkey, confkey→names, confdeltype, pg_get_expr(conbin) (introspectV2ConstraintsSQL)", async (drv, t, oid) => {
    const rows = await rowsOf(
      drv,
      `SELECT rc.conname, rc.contype::text AS contype, rc.conkey::int2[] AS conkey, rt.relname AS ref_table,
       (SELECT array_agg(ta.attname ORDER BY k.ord) FROM unnest(rc.confkey) WITH ORDINALITY AS k(attnum, ord) JOIN pg_attribute ta ON ta.attrelid = rc.confrelid AND ta.attnum = k.attnum)::text[] AS ref_cols,
       rc.confdeltype::text AS confdeltype, pg_get_expr(rc.conbin, rc.conrelid) AS check_expr
       FROM pg_constraint rc LEFT JOIN pg_class rt ON rt.oid = rc.confrelid
       WHERE rc.conrelid = $1 AND rc.contype IN ('p', 'u', 'c', 'f', 'x') ORDER BY rc.contype`,
      [await oid(`${t}_child`)],
    );
    const byType = Object.fromEntries(rows.map((r) => [r.contype, r]));
    assert.deepEqual(Object.keys(byType).sort(), ["c", "f", "p"], "one pk, one fk, one check");
    assert.deepEqual(byType.f.conkey.map(Number), [2]);
    assert.equal(byType.f.ref_table, `${t}_parent`);
    assert.deepEqual(byType.f.ref_cols, ["id"]);
    assert.equal(byType.f.confdeltype, "c");
    assert.equal(byType.c.check_expr, "(qty > 0)");
    assert.deepEqual(byType.p.conkey.map(Number), [1]);
  }),
  catalogProbe("catalog.pg_get_constraintdef", "pg_get_constraintdef renders FK/check definitions", async (drv, t, oid) => {
    const rows = await rowsOf(drv, `select contype::text as contype, pg_get_constraintdef(oid) as def from pg_constraint where conrelid = $1 and contype in ('f', 'c') order by contype`, [await oid(`${t}_child`)]);
    assert.deepEqual(rows.map((r) => r.def), ["CHECK ((qty > 0))", `FOREIGN KEY (parent_id) REFERENCES ${t}_parent(id) ON DELETE CASCADE`]);
  }),
  catalogProbe("catalog.pg_index", "pg_index + pg_am, excluding constraint-backed indexes (introspectV2IndexesSQL)", async (drv, t, oid) => {
    const rows = await rowsOf(
      drv,
      `SELECT ic.relname, i.indisunique, am.amname, i.indnkeyatts, i.indkey::int2[] AS indkey, pg_get_expr(i.indpred, i.indrelid) AS predicate
       FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_am am ON am.oid = ic.relam
       WHERE i.indrelid = $1 AND i.indisvalid AND i.indisready AND NOT EXISTS (SELECT 1 FROM pg_constraint pc WHERE pc.conindid = i.indexrelid)
       ORDER BY ic.relname`,
      [await oid(`${t}_child`)],
    );
    assert.deepEqual(
      rows.map((r) => [r.relname, r.indisunique, r.amname, Number(r.indnkeyatts), r.indkey.map(Number), r.predicate]),
      [[`${t}_child_tag_idx`, false, "btree", 1, [4], null]],
    );
  }),
  catalogProbe("catalog.partial_index", "CREATE INDEX ... WHERE: partial index builds and introspects its predicate", async (drv, t) => {
    await drv.execute(`create index ${t}_child_qty_idx on ${t}_child (qty) where qty > 0`);
    const r = await one(drv, `select pg_get_expr(i.indpred, i.indrelid) as predicate from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = $1`, [`${t}_child_qty_idx`]);
    assert.equal(r?.predicate, "(qty > 0)");
  }),
  catalogProbe("catalog.pg_get_indexdef", "pg_get_indexdef renders the index definition", async (drv, t) => {
    const r = await one(drv, `select pg_get_indexdef(c.oid) as def from pg_class c where c.relname = $1`, [`${t}_child_tag_idx`]);
    assert.equal(r.def, `CREATE INDEX ${t}_child_tag_idx ON public.${t}_child USING btree (tag)`);
  }),
  catalogProbe("catalog.regclass_cast", "'name'::regclass resolves both directions", async (drv, t) => {
    const r = await one(drv, `select ($1::text)::regclass::oid::int8 = (select oid::int8 from pg_class where relname = $1) as same, ($1::text)::regclass::text as back`, [`${t}_parent`]);
    assert.deepEqual([r.same, r.back], [true, `${t}_parent`]);
  }),
  catalogProbe("catalog.pg_enum", "pg_type/pg_enum enum listing in declared order (introspectV2EnumsSQL)", async (drv, t) => {
    await setup(drv, `create type ${t}_state as enum ('draft', 'live', 'gone')`);
    const rows = await rowsOf(
      drv,
      `SELECT t.typname, e.enumlabel FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_enum e ON e.enumtypid = t.oid WHERE t.typtype = 'e' AND t.typname = $1 ORDER BY n.nspname, t.typname, e.enumsortorder`,
      [`${t}_state`],
    );
    assert.deepEqual(rows.map((r) => r.enumlabel), ["draft", "live", "gone"]);
  }),
  catalogProbe("catalog.views", "CREATE VIEW + pg_get_viewdef", async (drv, t) => {
    await drv.execute(`create view ${t}_v as select id, code from ${t}_parent where note is null`);
    const r = await one(drv, `select c.relkind::text as k, pg_get_viewdef(c.oid) as def from pg_class c where c.relname = $1`, [`${t}_v`]);
    assert.equal(r.k, "v");
    assert.match(String(r.def), /select/i);
    const rows = await rowsOf(drv, `select * from ${t}_v`);
    assert.equal(rows.length, 0);
  }),
  catalogProbe("catalog.current_schema_and_search_path", "current_schema() / search_path defaults to public", async (drv) => {
    const r = await one(drv, `select current_schema() as s, current_setting('search_path') as p`);
    assert.equal(r.s, "public");
    assert.match(String(r.p), /public/);
  }),
];

// ---------------------------------------------------------------------------
// ddl atomicity
// ---------------------------------------------------------------------------

const tableExists = async (drv, name) =>
  (await one(drv, `select count(*)::int as n from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = $1 and n.nspname = current_schema()`, [name])).n > 0;
const columnExists = async (drv, table, col) =>
  (await one(drv, `select count(*)::int as n from information_schema.columns where table_name = $1 and column_name = $2`, [table, col])).n > 0;

function ddlProbe(id, title, body) {
  return {
    id,
    area: "ddl",
    title,
    run: async (ctx) => {
      const t = ctx.name("d");
      const drv = await ctx.session();
      await setup(drv, [`create table ${t}_base (id int primary key, v text)`, `insert into ${t}_base values (1, 'keep')`]);
      await body(drv, t, ctx);
    },
  };
}

class Rollback extends Error {}
async function inRolledBackTx(drv, fn) {
  try {
    await drv.begin(async (tx) => {
      await fn(tx);
      throw new Rollback("rollback");
    });
  } catch (err) {
    if (!(err instanceof Rollback)) throw err;
  }
}

const ddlProbes = [
  ddlProbe("ddl.create_table_rollback", "CREATE TABLE inside a rolled-back transaction leaves no table", async (drv, t) => {
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`create table ${t}_new (id int primary key)`);
      await tx.execute(`insert into ${t}_new values (1)`);
    });
    assert.equal(await tableExists(drv, `${t}_new`), false, "table must not survive ROLLBACK");
  }),
  ddlProbe("ddl.alter_add_column_rollback", "ALTER TABLE ADD COLUMN rolls back", async (drv, t) => {
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`alter table ${t}_base add column extra int`);
    });
    assert.equal(await columnExists(drv, `${t}_base`, "extra"), false, "column must not survive ROLLBACK");
  }),
  ddlProbe("ddl.drop_table_rollback", "DROP TABLE rolls back with its data intact", async (drv, t) => {
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`drop table ${t}_base`);
    });
    const r = await one(drv, `select v from ${t}_base where id = 1`);
    assert.equal(r?.v, "keep");
  }),
  ddlProbe("ddl.create_index_rollback", "CREATE INDEX rolls back", async (drv, t) => {
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`create index ${t}_v_idx on ${t}_base (v)`);
    });
    assert.equal(await tableExists(drv, `${t}_v_idx`), false, "index relation must not survive ROLLBACK");
  }),
  ddlProbe("ddl.rename_column_rollback", "ALTER TABLE RENAME COLUMN rolls back", async (drv, t) => {
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`alter table ${t}_base rename column v to w`);
    });
    assert.equal(await columnExists(drv, `${t}_base`, "v"), true, "original column name must be back");
  }),
  ddlProbe("ddl.failed_migration_all_or_nothing", "a failing statement mid-migration rolls back earlier DDL in the same transaction", async (drv, t) => {
    let failed = false;
    try {
      await drv.begin(async (tx) => {
        await tx.execute(`create table ${t}_m1 (id int primary key)`);
        await tx.execute(`alter table ${t}_base add column m int`);
        await tx.execute(`create table ${t}_m1 (id int primary key)`);
      });
    } catch (err) {
      if (err?.sqlstate !== "42P07") throw new assert.AssertionError({ message: `duplicate CREATE TABLE should fail with 42P07, got ${describeError(err)}` });
      failed = true;
    }
    assert.equal(failed, true, "duplicate CREATE TABLE must fail");
    assert.equal(await tableExists(drv, `${t}_m1`), false, "first CREATE TABLE must be rolled back");
    assert.equal(await columnExists(drv, `${t}_base`, "m"), false, "ALTER must be rolled back");
  }),
  ddlProbe("ddl.ddl_and_history_commit_together", "DDL and a history-row insert commit atomically", async (drv, t) => {
    await setup(drv, `create table ${t}_hist (version text primary key)`);
    await drv.begin(async (tx) => {
      await tx.execute(`create table ${t}_c1 (id int primary key)`);
      await tx.execute(`insert into ${t}_hist values ('001')`);
    });
    assert.equal(await tableExists(drv, `${t}_c1`), true);
    const r = await one(drv, `select count(*)::int as n from ${t}_hist`);
    assert.equal(Number(r.n), 1);
  }),
  ddlProbe("ddl.error_aborts_transaction", "after an error, the transaction rejects further statements (25P02)", async (drv, t) => {
    let inner = null;
    try {
      await drv.begin(async (tx) => {
        await tx.execute(`insert into ${t}_base values (1, 'dup')`).catch(() => {});
        try {
          await tx.execute(`select 1`);
        } catch (err) {
          inner = err;
        }
      });
    } catch {
      // COMMIT of an aborted transaction reports ROLLBACK; either way is fine.
    }
    if (!inner) throw new assert.AssertionError({ message: "statement after an error inside the transaction succeeded (expected 25P02)" });
    assert.equal(inner.sqlstate, "25P02", describeError(inner));
  }),
  ddlProbe("ddl.uncommitted_ddl_invisible", "another session cannot see a table created in an uncommitted transaction", async (drv, t, ctx) => {
    const other = await ctx.session();
    let seen = null;
    await inRolledBackTx(drv, async (tx) => {
      await tx.execute(`create table ${t}_hidden (id int)`);
      seen = await tableExists(other, `${t}_hidden`);
    });
    assert.equal(seen, false, "uncommitted table visible to another session");
  }),
  ddlProbe("ddl.create_index_concurrently_in_tx_rejected", "CREATE INDEX CONCURRENTLY inside a transaction block -> 25001", async (drv, t) => {
    await expectState(
      () => drv.begin(async (tx) => { await tx.execute(`create index concurrently ${t}_cc on ${t}_base (v)`); }),
      "25001",
      "CIC in a transaction block",
    );
  }),
  ddlProbe("ddl.create_index_concurrently", "CREATE INDEX CONCURRENTLY outside a transaction builds a valid index", async (drv, t) => {
    await drv.execute(`create index concurrently ${t}_cc on ${t}_base (v)`);
    const r = await one(drv, `select i.indisvalid from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = $1`, [`${t}_cc`]);
    assert.equal(r?.indisvalid, true);
  }),
];

// ---------------------------------------------------------------------------
// rls
// ---------------------------------------------------------------------------

function rlsProbe(id, title, body) {
  return {
    id,
    area: "rls",
    title,
    run: async (ctx) => {
      const t = ctx.name("s");
      const role = ctx.role(`${t}_app`);
      const drv = await ctx.session();
      await setup(drv, [
        `create role ${role} nologin`,
        `create table ${t}_docs (id int primary key, tenant int not null, body text not null)`,
        `insert into ${t}_docs values (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')`,
        `grant select, insert, update, delete on ${t}_docs to ${role}`,
      ]);
      await body(drv, t, role, ctx);
    },
  };
}

async function asRole(drv, role, tenant, fn) {
  return drv.begin(async (tx) => {
    await tx.execute(`set local role ${role}`);
    if (tenant !== undefined) await tx.query(`select set_config('app.tenant', $1, true)`, [String(tenant)]);
    return fn(tx);
  });
}

const rlsProbes = [
  rlsProbe("rls.policy_filters_rows", "ENABLE ROW LEVEL SECURITY + USING policy filters a non-owner role's reads", async (drv, t, role) => {
    await setup(drv, `alter table ${t}_docs enable row level security`);
    // The PostgreSQL idiom under test: a custom-GUC tenant predicate. The
    // CREATE POLICY is the capability, so it runs in the body — an engine
    // with a restricted predicate language gets `unsupported` carrying the
    // restriction, not an `unknown` from failed setup.
    await drv.execute(`create policy ${t}_tenant on ${t}_docs using (tenant = current_setting('app.tenant')::int)`);
    const rows = await asRole(drv, role, 1, (tx) => tx.query(`select id from ${t}_docs order by id`));
    assert.deepEqual(rows.map((r) => Number(r.id)), [1, 2]);
  }),
  rlsProbe("rls.current_user_policy", "USING (col = current_user) policy filters rows for the assumed role", async (drv, t, role) => {
    await setup(drv, [`alter table ${t}_docs enable row level security`, `update ${t}_docs set body = '${role}' where id = 1`]);
    await drv.execute(`create policy ${t}_owner on ${t}_docs using (body = current_user)`);
    const mine = await asRole(drv, role, undefined, (tx) => tx.query(`select id from ${t}_docs order by id`));
    const owner = await drv.query(`select id from ${t}_docs order by id`);
    assert.deepEqual(mine.map((r) => Number(r.id)), [1], "role sees only its own row");
    assert.equal(owner.length, 3, "superuser creator still sees all rows");
  }),
  rlsProbe("rls.with_check_blocks_insert", "WITH CHECK rejects a write outside the policy (42501)", async (drv, t, role) => {
    await setup(drv, `alter table ${t}_docs enable row level security`);
    await drv.execute(`create policy ${t}_tenant on ${t}_docs using (tenant = current_setting('app.tenant')::int) with check (tenant = current_setting('app.tenant')::int)`);
    await expectState(() => asRole(drv, role, 1, (tx) => tx.execute(`insert into ${t}_docs values (9, 2, 'x')`)), "42501", "insert for another tenant");
  }),
  rlsProbe("rls.no_policy_default_deny", "RLS enabled with no policy denies every row to a non-owner", async (drv, t, role) => {
    await setup(drv, `alter table ${t}_docs enable row level security`);
    const rows = await asRole(drv, role, undefined, (tx) => tx.query(`select id from ${t}_docs`));
    assert.equal(rows.length, 0);
  }),
  rlsProbe("rls.owner_bypass_unless_forced", "the table owner bypasses RLS unless FORCE ROW LEVEL SECURITY", async (drv, t, role) => {
    // The owner must be an ordinary role: superusers bypass RLS even when forced.
    await setup(drv, [
      `alter table ${t}_docs owner to ${role}`,
      `alter table ${t}_docs enable row level security`,
      `create policy ${t}_none on ${t}_docs using (false)`,
    ]);
    const before = await asRole(drv, role, undefined, (tx) => tx.query(`select id from ${t}_docs`));
    assert.equal(before.length, 3, "owner sees all rows without FORCE");
    await setup(drv, `alter table ${t}_docs force row level security`);
    const after = await asRole(drv, role, undefined, (tx) => tx.query(`select id from ${t}_docs`));
    assert.equal(after.length, 0, "owner is filtered with FORCE");
  }),
  rlsProbe("rls.set_config_transaction_local", "set_config(name, value, true) is transaction-local", async (drv) => {
    const inside = await drv.begin(async (tx) => {
      await tx.query(`select set_config('app.tenant', '7', true)`);
      return (await tx.query(`select current_setting('app.tenant', true) as v`))[0].v;
    });
    const after = (await drv.query(`select coalesce(current_setting('app.tenant', true), '') as v`))[0].v;
    assert.equal(inside, "7");
    assert.equal(after, "");
  }),
  rlsProbe("rls.pg_policies_introspection", "pg_policies / relrowsecurity expose policies for introspection", async (drv, t) => {
    await setup(drv, [`alter table ${t}_docs enable row level security`, `create policy ${t}_tenant on ${t}_docs using (tenant = 1)`]);
    const pol = await rowsOf(drv, `select policyname, cmd from pg_policies where tablename = $1`, [`${t}_docs`]);
    const cls = await one(drv, `select relrowsecurity from pg_class where relname = $1`, [`${t}_docs`]);
    assert.deepEqual(pol.map((r) => [r.policyname, r.cmd]), [[`${t}_tenant`, "ALL"]]);
    assert.equal(cls.relrowsecurity, true);
  }),
  rlsProbe("rls.set_local_role_transaction_local", "SET LOCAL ROLE reverts at transaction end (pooled connections resume their login role)", async (drv, t, role) => {
    const before = (await drv.query(`select current_user as u`))[0].u;
    await drv.begin(async (tx) => {
      await tx.execute(`set local role ${role}`);
      const inside = (await tx.query(`select current_user as u`))[0].u;
      assert.equal(inside, role, "role assumed inside the transaction");
    });
    const after = (await drv.query(`select current_user as u`))[0].u;
    assert.equal(after, before, `current_user after COMMIT is "${after}", expected "${before}"`);
  }),
  rlsProbe("rls.privilege_denied_without_grant", "a role without a grant gets 42501 on read", async (drv, t, role) => {
    await setup(drv, [`create table ${t}_secret (id int primary key)`]);
    await expectState(() => asRole(drv, role, undefined, (tx) => tx.query(`select * from ${t}_secret`)), "42501", "select without grant");
  }),
];

// ---------------------------------------------------------------------------
// locks, isolation, cancellation
// ---------------------------------------------------------------------------

function lockProbe(id, title, body) {
  return {
    id,
    area: "locks",
    title,
    run: async (ctx) => {
      const t = ctx.name("l");
      const a = await ctx.session();
      const b = await ctx.session();
      await setup(a, [`create table ${t} (id int primary key, n int not null)`, `insert into ${t} values (1, 0), (2, 0), (3, 0)`]);
      await body(a, b, t, ctx);
    },
  };
}

const lockKey = (t) => Number.parseInt(t.replace(/[^0-9a-f]/g, "").slice(-7), 16) || 4242;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const lockProbes = [
  lockProbe("lock.advisory_session", "pg_advisory_lock excludes other sessions until pg_advisory_unlock", async (a, b, t) => {
    const k = lockKey(t);
    await a.query(`select pg_advisory_lock($1::int8)`, [k]);
    const held = (await b.query(`select pg_try_advisory_lock($1::int8) as ok`, [k]))[0].ok;
    const unlocked = (await a.query(`select pg_advisory_unlock($1::int8) as ok`, [k]))[0].ok;
    const later = (await b.query(`select pg_try_advisory_lock($1::int8) as ok`, [k]))[0].ok;
    await b.query(`select pg_advisory_unlock($1::int8)`, [k]);
    assert.deepEqual([held, unlocked, later], [false, true, true]);
  }),
  lockProbe("lock.advisory_released_on_disconnect", "a session advisory lock is released when its connection closes", async (a, b, t, ctx) => {
    const k = lockKey(t);
    const c = await ctx.session();
    await c.query(`select pg_advisory_lock($1::int8)`, [k]);
    const held = (await b.query(`select pg_try_advisory_lock($1::int8) as ok`, [k]))[0].ok;
    await c.close();
    let later = false;
    for (let i = 0; i < 20 && !later; i++) {
      await sleep(100);
      later = (await b.query(`select pg_try_advisory_lock($1::int8) as ok`, [k]))[0].ok;
    }
    assert.deepEqual([held, later], [false, true]);
  }),
  lockProbe("lock.advisory_xact", "pg_advisory_xact_lock is held until COMMIT", async (a, b, t) => {
    const k = lockKey(t);
    let during = null;
    await a.begin(async (tx) => {
      await tx.query(`select pg_advisory_xact_lock($1::int8)`, [k]);
      during = (await b.query(`select pg_try_advisory_xact_lock($1::int8) as ok`, [k]))[0].ok;
    });
    const after = (await b.query(`select pg_try_advisory_lock($1::int8) as ok`, [k]))[0].ok;
    await b.query(`select pg_advisory_unlock($1::int8)`, [k]);
    assert.deepEqual([during, after], [false, true]);
  }),
  lockProbe("lock.select_for_update_nowait", "SELECT ... FOR UPDATE NOWAIT on a row locked by another transaction -> 55P03", async (a, b, t) => {
    await a.begin(async (tx) => {
      await tx.query(`select id from ${t} where id = 1 for update`);
      await expectState(() => b.query(`select id from ${t} where id = 1 for update nowait`), "55P03", "NOWAIT on a locked row");
    });
  }),
  lockProbe("lock.select_for_update_skip_locked", "FOR UPDATE SKIP LOCKED skips rows locked by another transaction", async (a, b, t) => {
    let got = null;
    await a.begin(async (tx) => {
      await tx.query(`select id from ${t} where id = 1 for update`);
      got = await b.query(`select id from ${t} order by id for update skip locked`);
    });
    assert.deepEqual(got.map((r) => Number(r.id)), [2, 3]);
  }),
  lockProbe("lock.row_lock_timeout", "an UPDATE blocked by a row lock honours lock_timeout -> 55P03", async (a, b, t) => {
    await a.begin(async (tx) => {
      await tx.execute(`update ${t} set n = n + 1 where id = 1`);
      await expectState(
        () => b.begin(async (tb) => {
          await tb.execute(`set local lock_timeout = '300ms'`);
          await tb.execute(`update ${t} set n = n + 10 where id = 1`);
        }),
        "55P03",
        "update of a row locked by an open transaction",
      );
    });
  }),
  lockProbe("lock.blocked_update_waits_then_applies", "a blocked UPDATE waits for the holder, then applies on the committed row (no lost update)", async (a, b, t) => {
    let release;
    const gate = new Promise((r) => (release = r));
    let bDone = false;
    const holder = a.begin(async (tx) => {
      await tx.execute(`update ${t} set n = n + 1 where id = 2`);
      await gate;
    });
    await sleep(100);
    const waiter = b.execute(`update ${t} set n = n + 10 where id = 2`).then((n) => {
      bDone = true;
      return n;
    });
    await sleep(400);
    const waitedWhileHeld = !bDone;
    release();
    await holder;
    await waiter;
    const r = await one(a, `select n from ${t} where id = 2`);
    assert.equal(waitedWhileHeld, true, "second UPDATE completed while the first transaction still held the row");
    assert.equal(Number(r.n), 11, "both increments must apply");
  }),
  lockProbe("lock.lock_table_nowait", "LOCK TABLE ... NOWAIT against an ACCESS EXCLUSIVE holder -> 55P03", async (a, b, t) => {
    await a.begin(async (tx) => {
      await tx.execute(`lock table ${t} in access exclusive mode`);
      await expectState(() => b.begin(async (tb) => { await tb.execute(`lock table ${t} in access share mode nowait`); }), "55P03", "LOCK TABLE NOWAIT");
    });
  }),
  lockProbe("lock.statement_timeout", "statement_timeout cancels a long statement -> 57014", async (a) => {
    await a.execute(`set statement_timeout = '200ms'`);
    await expectState(() => a.query(`select pg_sleep(3)`), "57014", "pg_sleep past statement_timeout");
  }),
  lockProbe("lock.cancel_request", "driver cancel (neutron-sql deadlineMs) stops a running statement -> 57014 and the connection stays usable", async (_a, _b, _t, ctx) => {
    // pg cancels through pg_cancel_backend on a second pooled connection (README: a max: 1 pool cannot).
    const a = await ctx.session({ max: 2 });
    const started = Date.now();
    await expectState(() => a.query(`select 1 as x from pg_sleep(4)`, [], { deadlineMs: 200 }), "57014", "deadline on pg_sleep(4)");
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 3000, `cancel took ${elapsed} ms — the statement ran to completion`);
    const r = await one(a, `select 1 as x`);
    assert.equal(Number(r.x), 1);
  }),
  lockProbe("lock.pg_cancel_backend", "pg_cancel_backend(pid) from another session cancels its statement -> 57014", async (a, b) => {
    const pid = (await a.query(`select pg_backend_pid() as p`))[0].p;
    const running = a.query(`select pg_sleep(4)`).then(() => null, (e) => e);
    await sleep(300);
    const ok = (await b.query(`select pg_cancel_backend($1::int) as ok`, [pid]))[0].ok;
    const err = await running;
    assert.equal(ok, true, "pg_cancel_backend returned false");
    assert.equal(err?.sqlstate, "57014", err ? describeError(err) : "statement completed without cancellation");
  }),
];

const isolationProbes = [
  {
    ...lockProbe("txn.isolation_levels_applied", "BEGIN ISOLATION LEVEL ... is reflected by transaction_isolation", async (a) => {
      const seen = [];
      for (const level of ["read-committed", "repeatable-read", "serializable"]) {
        seen.push(await a.begin(async (tx) => (await tx.query(`select current_setting('transaction_isolation') as v`))[0].v, { isolation: level }));
      }
      assert.deepEqual(seen, ["read committed", "repeatable read", "serializable"]);
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.read_only_rejects_writes", "READ ONLY transactions reject writes -> 25006", async (a, _b, t) => {
      await expectState(() => a.begin(async (tx) => { await tx.execute(`update ${t} set n = 1`); }, { readOnly: true }), "25006", "write in READ ONLY");
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.repeatable_read_snapshot", "REPEATABLE READ keeps its snapshot across another session's commit", async (a, b, t) => {
      const counts = await a.begin(async (tx) => {
        const first = Number((await tx.query(`select count(*)::int as n from ${t}`))[0].n);
        await b.execute(`insert into ${t} values (9, 0)`);
        const second = Number((await tx.query(`select count(*)::int as n from ${t}`))[0].n);
        return [first, second];
      }, { isolation: "repeatable-read" });
      assert.deepEqual(counts, [3, 3]);
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.read_committed_sees_commits", "READ COMMITTED sees another session's commit on the next statement", async (a, b, t) => {
      const counts = await a.begin(async (tx) => {
        const first = Number((await tx.query(`select count(*)::int as n from ${t}`))[0].n);
        await b.execute(`insert into ${t} values (9, 0)`);
        const second = Number((await tx.query(`select count(*)::int as n from ${t}`))[0].n);
        return [first, second];
      });
      assert.deepEqual(counts, [3, 4]);
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.serializable_write_skew", "SERIALIZABLE rejects write skew with 40001", async (a, b, t) => {
      let release;
      const gate = new Promise((r) => (release = r));
      const outcomes = await Promise.allSettled([
        a.begin(async (tx) => {
          await tx.query(`select sum(n) from ${t} where id in (1, 2)`);
          await gate;
          await tx.execute(`update ${t} set n = 1 where id = 1`);
        }, { isolation: "serializable" }),
        b.begin(async (tx) => {
          await tx.query(`select sum(n) from ${t} where id in (1, 2)`);
          release();
          await sleep(50);
          await tx.execute(`update ${t} set n = 1 where id = 2`);
        }, { isolation: "serializable" }),
      ]);
      const failures = outcomes.filter((o) => o.status === "rejected");
      assert.equal(failures.length, 1, `expected exactly one serialization failure, got ${failures.length}`);
      assert.equal(failures[0].reason?.sqlstate, "40001", describeError(failures[0].reason));
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.savepoint_rollback", "SAVEPOINT / ROLLBACK TO SAVEPOINT undoes only the inner work", async (a, _b, t) => {
      await a.begin(async (tx) => {
        await tx.execute(`update ${t} set n = 1 where id = 1`);
        await tx.execute(`savepoint sp1`);
        await tx.execute(`update ${t} set n = 2 where id = 2`);
        await tx.execute(`rollback to savepoint sp1`);
        await tx.execute(`update ${t} set n = 3 where id = 3`);
        await tx.execute(`release savepoint sp1`);
      });
      const rows = await a.query(`select n from ${t} order by id`);
      assert.deepEqual(rows.map((r) => Number(r.n)), [1, 0, 3]);
    }),
    area: "transactions",
  },
  {
    ...lockProbe("txn.savepoint_recovers_error", "ROLLBACK TO SAVEPOINT recovers a transaction after a statement error", async (a, _b, t) => {
      await a.begin(async (tx) => {
        await tx.execute(`savepoint sp`);
        await tx.execute(`insert into ${t} values (1, 0)`).catch(() => {});
        await tx.execute(`rollback to savepoint sp`);
        await tx.execute(`insert into ${t} values (4, 4)`);
      });
      const r = await one(a, `select count(*)::int as n from ${t}`);
      assert.equal(Number(r.n), 4);
    }),
    area: "transactions",
  },
];

// ---------------------------------------------------------------------------
// orm: @neutron-build/sql end to end
// ---------------------------------------------------------------------------

function ormFixture(sqlmod, t) {
  const { pgTable, serial, integer, text, bigint, numeric, timestamp, timestamptz, date, bytea, jsonb, relations } = sqlmod;
  const users = pgTable(`${t}_users`, {
    id: serial("id").primaryKey(),
    email: text("email").notNull().unique(),
    createdAt: timestamptz("created_at").notNull().defaultNow(),
  });
  const posts = pgTable(`${t}_posts`, {
    id: serial("id").primaryKey(),
    userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
    title: text("title").notNull(),
    score: integer("score"),
  });
  const comments = pgTable(`${t}_comments`, {
    id: serial("id").primaryKey(),
    postId: integer("post_id").notNull().references(() => posts.id, { onDelete: "cascade" }),
    body: text("body").notNull(),
  });
  const ledger = pgTable(`${t}_ledger`, {
    id: bigint("id").primaryKey(),
    amount: numeric("amount").notNull(),
    at: timestamp("at").notNull(),
    atz: timestamptz("atz").notNull(),
    d: date("d").notNull(),
    bin: bytea("bin"),
    doc: jsonb("doc"),
  });
  const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }));
  const postsRelations = relations(posts, ({ one, many }) => ({
    author: one(users, { fields: [posts.userId], references: [users.id] }),
    comments: many(comments),
  }));
  const commentsRelations = relations(comments, ({ one }) => ({
    post: one(posts, { fields: [comments.postId], references: [posts.id] }),
  }));
  return {
    tables: { users, posts, comments, ledger },
    relations: { users: usersRelations, posts: postsRelations, comments: commentsRelations },
  };
}

function ormProbe(id, title, body, { seed = true } = {}) {
  return {
    id,
    area: "orm",
    title,
    run: async (ctx) => {
      const t = ctx.name("o");
      const fx = ormFixture(ctx.sql, t);
      const db = await ctx.orm(fx.tables, fx.relations);
      const ddl = ctx.sql.schemaToDDL(Object.values(fx.tables));
      if (id === "orm.schema_ddl") {
        for (const s of ddl) await db.driver.execute(s);
        return `${ddl.length} statements`;
      }
      try {
        for (const s of ddl) await db.driver.execute(s);
      } catch (err) {
        throw new Precondition(`schemaToDDL fixture failed: ${describeError(err)}`, { cause: err });
      }
      const { users, posts, comments } = fx.tables;
      if (seed) {
        try {
          await db.insert(users).values([{ email: "a@x" }, { email: "b@x" }, { email: "c@x" }]);
          await db.insert(posts).values([
            { userId: 1, title: "p1", score: 5 },
            { userId: 1, title: "p2", score: 7 },
            { userId: 2, title: "p3" },
          ]);
          await db.insert(comments).values([{ postId: 1, body: "c1" }, { postId: 1, body: "c2" }, { postId: 3, body: "c3" }]);
        } catch (err) {
          throw new Precondition(`ORM seed insert failed: ${describeError(err)}`, { cause: err });
        }
      }
      return body(db, fx.tables, ctx);
    },
  };
}

const ormProbes = [
  ormProbe("orm.schema_ddl", "schemaToDDL output (serial PK, unique, FK ON DELETE CASCADE, bigint/numeric/temporal/bytea/jsonb) executes", async () => {}),
  ormProbe("orm.insert_returning", "insert().values().returning() with server defaults", async (db, { users }) => {
    const [row] = await db.insert(users).values({ email: "d@x" }).returning();
    assert.equal(row.id, 4);
    assert.equal(row.email, "d@x");
    assert.match(row.createdAt, /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}(:\d{2})?)$/);
  }),
  ormProbe("orm.batch_insert_defaults", "batch insert with per-row omitted keys (DEFAULT cells)", async (db, { posts }) => {
    const rows = await db.insert(posts).values([{ userId: 3, title: "x", score: 1 }, { userId: 3, title: "y" }]).returning();
    assert.deepEqual(rows.map((r) => [r.title, r.score]), [["x", 1], ["y", null]]);
  }),
  ormProbe("orm.select_where_order_limit", "select().from().where().orderBy().limit()", async (db, { posts }, ctx) => {
    const { eq, desc } = ctx.sql;
    const rows = await db.select().from(posts).where(eq(posts.userId, 1)).orderBy(desc(posts.id)).limit(1);
    assert.deepEqual(rows.map((r) => [r.id, r.title]), [[2, "p2"]]);
  }),
  ormProbe("orm.update_returning", "update().set().where().returning()", async (db, { posts }, ctx) => {
    const rows = await db.update(posts).set({ score: 9 }).where(ctx.sql.eq(posts.id, 3)).returning();
    assert.deepEqual(rows.map((r) => [r.id, r.score]), [[3, 9]]);
  }),
  ormProbe("orm.delete_returning", "delete().where().returning() with ON DELETE CASCADE children", async (db, { users, comments }, ctx) => {
    const rows = await db.delete(users).where(ctx.sql.eq(users.id, 1)).returning();
    assert.deepEqual(rows.map((r) => r.id), [1]);
    const left = await db.select().from(comments);
    assert.deepEqual(left.map((r) => r.body), ["c3"]);
  }),
  ormProbe("orm.upsert", "onConflictUpdate (EXCLUDED) / onConflictDoNothing", async (db, { users }, ctx) => {
    const { excluded } = ctx.sql;
    const up = await db.insert(users).values({ email: "a@x" }).onConflictUpdate({ target: users.email, set: { email: ctx.sql.sql`${excluded(users.email)}` } }).returning();
    assert.deepEqual(up.map((r) => r.id), [1]);
    const none = await db.insert(users).values({ email: "b@x" }).onConflictDoNothing();
    assert.equal(none, 0);
  }),
  ormProbe("orm.left_join", "leftJoin with null-extended rows", async (db, { users, posts }, ctx) => {
    const { sql, alias } = ctx.sql;
    const p = alias(posts, "p");
    const rows = await db.select({ uid: users.id, pid: p.id }).from(users).leftJoin(p, sql`${p.userId} = ${users.id}`).orderBy(sql`${users.id}`, sql`${p.id}`);
    assert.deepEqual(rows.map((r) => [r.uid, r.pid]), [[1, 1], [1, 2], [2, 3], [3, null]]);
  }),
  ormProbe("orm.aggregate_group_by", "count() with groupBy / having", async (db, { posts }, ctx) => {
    const { count, sql } = ctx.sql;
    const rows = await db.select({ userId: posts.userId, n: count() }).from(posts).groupBy(posts.userId).having(sql`${count()} >= ${2}`);
    assert.deepEqual(rows.map((r) => [r.userId, Number(r.n)]), [[1, 2]]);
  }),
  ormProbe("orm.cte", "cteTable over a grouped select", async (db, { posts }, ctx) => {
    const { count, cteTable, desc } = ctx.sql;
    const agg = cteTable("agg", db.select({ userId: posts.userId, n: count() }).from(posts).groupBy(posts.userId));
    const rows = await db.select({ userId: agg.userId, n: agg.n }).from(agg).orderBy(desc(agg.n));
    assert.deepEqual(rows.map((r) => [r.userId, Number(r.n)]), [[1, 2], [2, 1]]);
  }),
  ormProbe("orm.set_operation", "union() of two selects", async (db, { users, posts }, ctx) => {
    const { sql, eq } = ctx.sql;
    const rows = await db.select({ id: users.id }).from(users).where(eq(users.id, 3)).union(db.select({ id: posts.userId }).from(posts)).orderBy(sql`id`);
    assert.deepEqual(rows.map((r) => r.id), [1, 2, 3]);
  }),
  ormProbe("orm.relations_nested", "query.findMany with nested with + per-parent limit/orderBy (jsonb aggregation)", async (db, { posts }, ctx) => {
    const { desc } = ctx.sql;
    const rows = await db.query.users.findMany({
      with: { posts: { limit: 1, orderBy: [desc(posts.id)], with: { comments: true } } },
    });
    const shape = rows.map((u) => [u.id, u.posts.map((p) => [p.id, p.comments.map((c) => c.body)])]).sort((x, y) => x[0] - y[0]);
    assert.deepEqual(shape, [[1, [[2, []]]], [2, [[3, ["c3"]]]], [3, []]]);
  }),
  ormProbe("orm.relations_to_one", "query.findFirst with a to-one relation", async (db, { posts }, ctx) => {
    const row = await db.query.posts.findFirst({ where: ctx.sql.eq(posts.id, 3), with: { author: true } });
    assert.equal(row?.author?.email, "b@x");
  }),
  ormProbe("orm.keyset_pagination", "keyset() pager walks all rows in order", async (db, { posts }, ctx) => {
    const { keyset, ascNullsLast } = ctx.sql;
    const pager = keyset(posts, [ascNullsLast(posts.id)], { perPage: 2 });
    const seen = [];
    let cursor;
    for (let i = 0; i < 5; i++) {
      const page = await pager.page(db.select().from(posts), cursor, 2);
      seen.push(...page.rows.map((r) => r.id));
      cursor = page.nextCursor;
      if (!cursor) break;
    }
    assert.deepEqual(seen, [1, 2, 3]);
  }),
  ormProbe("orm.prepared_statement", "driver.prepare() executes a named/prepared statement repeatedly", async (db, { users }, ctx) => {
    const stmt = db.driver.prepare(`select email from "${ctx.sql.getTableName(users)}" where id = $1`);
    const a = await stmt.query([1]);
    const b = await stmt.query([2]);
    assert.deepEqual([a[0]?.email, b[0]?.email], ["a@x", "b@x"]);
  }),
  ormProbe("orm.transaction_commit_rollback", "db.transaction commits, and rolls back on throw", async (db, { users }) => {
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ email: "t1@x" });
    });
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ email: "t2@x" });
      throw new Error("abort");
    }).catch(() => {});
    const emails = (await db.select().from(users)).map((r) => r.email).sort();
    assert.deepEqual(emails, ["a@x", "b@x", "c@x", "t1@x"]);
  }),
  ormProbe("orm.transaction_nested_savepoint", "nested db.transaction is a real savepoint", async (db, { users }) => {
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ email: "outer@x" });
      await tx.transaction(async (inner) => {
        await inner.insert(users).values({ email: "inner@x" });
        throw new Error("inner abort");
      }).catch(() => {});
    });
    const emails = (await db.select().from(users)).map((r) => r.email);
    assert.ok(emails.includes("outer@x") && !emails.includes("inner@x"), `got ${emails.join(",")}`);
  }),
  ormProbe("orm.codec_values", "ORM codecs: int8 max as bigint, exact numeric, microsecond timestamp/timestamptz, date, bytea, jsonb", async (db, { ledger }) => {
    const bin = new Uint8Array([0, 255, 92, 39]);
    await db.insert(ledger).values({
      id: 9223372036854775807n,
      amount: "12345678901234567890.123456789",
      at: "2026-01-02 03:04:05.123456",
      atz: "2026-01-02T03:04:05.123456Z",
      d: "2024-02-29",
      bin,
      doc: { a: [1, { b: null }], s: "x" },
    });
    const [row] = await db.select().from(ledger);
    assert.equal(row.id, 9223372036854775807n);
    assert.equal(row.amount, "12345678901234567890.123456789");
    assert.equal(row.at.replace("T", " "), "2026-01-02 03:04:05.123456");
    assert.match(row.atz, /^2026-01-02[T ]03:04:05\.123456(Z|\+00(:00)?)$/);
    assert.equal(row.d, "2024-02-29");
    assert.deepEqual([...row.bin], [...bin]);
    assert.deepEqual(row.doc, { a: [1, { b: null }], s: "x" });
  }, { seed: false }),
  ormProbe("orm.jsonb_null_distinct", "ORM writes jsonNull and SQL NULL distinctly", async (db, { ledger }, ctx) => {
    const base = { amount: "1", at: "2026-01-01 00:00:00", atz: "2026-01-01T00:00:00Z", d: "2026-01-01" };
    await db.insert(ledger).values([{ id: 1n, ...base, doc: ctx.sql.jsonNull }, { id: 2n, ...base, doc: null }]);
    const rows = await db.driver.query(`select id::text as id, doc is null as sqlnull, jsonb_typeof(doc) as ty from "${ctx.sql.getTableName(ledger)}" order by id`);
    assert.deepEqual(rows.map((r) => [r.id, r.sqlnull, r.ty]), [["1", false, "null"], ["2", true, null]]);
  }, { seed: false }),
];

export const PROBES = [
  ...engineProbes,
  ...relationProbes,
  ...dmlProbes,
  ...constraintProbes,
  ...codecProbes,
  ...catalogProbes,
  ...ddlProbes,
  ...rlsProbes,
  ...lockProbes,
  ...isolationProbes,
  ...ormProbes,
];
