// ---------------------------------------------------------------------------
// Q07 unit leg — array literal text, enum/array typed surfaces, schema API
// guards, view read surface, custom codecs, generated/identity write
// rejection. Live V10/V08 oracles live in live.q07.postgres.test.ts and the
// Go round-trip suite.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import {
  type IndexMethod,
  ascNullsFirst,
  bigint,
  boolean,
  bytea,
  check,
  createDatabase,
  desc,
  descNullsLast,
  eq,
  exportSchemaV2,
  foreignKey,
  ident,
  index,
  integer,
  json,
  jsonb,
  numeric,
  pgEnum,
  pgSchema,
  pgTable,
  pgView,
  primaryKey,
  serial,
  sql,
  text,
  timestamp,
  unique,
  uuid,
  varchar,
} from "./index.js";

const require = createRequire(import.meta.url);
const tone3 = pgEnum("tone3", ["low", "high"]);
const { parseArrayLiteral, formatArrayLiteral } = require("./pg-array.js") as {
  parseArrayLiteral: (text: string, describe: string) => Array<string | null>;
  formatArrayLiteral: (elements: ReadonlyArray<string | null>) => string;
};

// ---------------------------------------------------------------------------
// Array literal text (pg-array.ts)
// ---------------------------------------------------------------------------

test("q07 array literal: parse round-trips quoting, escapes, NULL and empty", () => {
  assert.deepEqual(parseArrayLiteral("{}", "t"), []);
  assert.deepEqual(parseArrayLiteral("{1,2}", "t"), ["1", "2"]);
  assert.deepEqual(parseArrayLiteral("{hello,\"two words\",NULL}", "t"), ["hello", "two words", null]);
  assert.deepEqual(parseArrayLiteral("{\"a\\\"b\",\"c\\\\d\"}", "t"), ['a"b', "c\\d"]);
  assert.deepEqual(parseArrayLiteral("{NULL,null}", "t"), [null, null]);
  assert.deepEqual(parseArrayLiteral("{\"\"}", "t"), [""]);
  assert.throws(() => parseArrayLiteral("[0:1]={1,2}", "t"), /non-default lower bounds/);
  assert.throws(() => parseArrayLiteral("{{1},{2}}", "t"), /multi-dimensional/);
  assert.throws(() => parseArrayLiteral("{1", "t"), /expected a PostgreSQL array literal/);
  assert.throws(() => parseArrayLiteral("{a,b", "t"), /expected a PostgreSQL array literal/);
});

test("q07 array literal: format mirrors PostgreSQL deparse (minimal quoting)", () => {
  assert.equal(formatArrayLiteral(["1", "2"]), "{1,2}");
  assert.equal(formatArrayLiteral(["hello", "two words", null]), '{hello,"two words",NULL}');
  assert.equal(formatArrayLiteral([""]), '{""}');
  assert.equal(formatArrayLiteral(["null"]), '{"null"}');
  assert.equal(formatArrayLiteral(["a,b", 'q"t', "c\\d"]), '{"a,b","q\\"t","c\\\\d"}');
  assert.equal(formatArrayLiteral([null]), "{NULL}");
});

// ---------------------------------------------------------------------------
// Schema API guards
// ---------------------------------------------------------------------------

test("q07 schema api: identity guards", () => {
  assert.throws(() => pgTable("t", { id: text("id").generatedAlwaysAsIdentity() }), /identity columns must be integer/);
  assert.throws(() => pgTable("t", { id: serial("id").generatedAlwaysAsIdentity() }), /serial already implies/);
  assert.throws(
    () => pgTable("t", { id: integer("id").default(1).generatedAlwaysAsIdentity() }),
    /cannot combine identity with a default/,
  );
  assert.throws(
    () => pgTable("t", { id: integer("id"), d: integer("d").generatedAlwaysAs(sql`${ident("id")} + 1`, { mode: "virtual" as never }) }),
    /virtual generated columns require PostgreSQL 18 and are explicitly unsupported/,
  );
  assert.throws(
    () => pgTable("t", { id: integer("id"), d: integer("d").generatedAlwaysAs(sql`${ident("id")} + 1`).default(1 as never) }),
    /cannot combine a default/,
  );
});

test("q07 schema api: array and codec guards", () => {
  assert.throws(() => pgTable("t", { id: serial("id"), ts: text("ts").array().array() }), /already an array/);
  assert.throws(() => pgTable("t", { id: serial("id"), ts: timestamp("ts").array() }), /temporal\/bytea/);
  assert.throws(
    () =>
      pgTable("t", {
        id: serial("id"),
        m: pgEnum("m", ["a"])("m")
          .array()
          .codec({ decode: (v: string) => v, encode: (v: string) => v } as never),
      }),
    /custom codecs apply to the column's own value/,
  );
  assert.throws(
    () => pgTable("t", { id: serial("id"), n: integer("n").codec({ decode: (v: number) => v, encode: 5 as never }) }),
    /must be \{ decode, encode \} functions/,
  );
});

// ---------------------------------------------------------------------------
// Typed surfaces: insert/update types (compile-time), runtime write rejection
// ---------------------------------------------------------------------------

const mood = pgEnum("mood", ["sad", "ok", "glad"]);
const items = pgTable("items", {
  id: integer("id").generatedAlwaysAsIdentity().primaryKey(),
  name: text("name").notNull(),
  tone: mood("tone").notNull(),
  tags: text("tags").array().notNull(),
  scores: bigint("scores").array(),
  gross: numeric("gross").generatedAlwaysAs(sql`${ident("name")} <> ''`),
});

const db = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { items },
});

test("q07 query layer: enum and array values bind and validate", () => {
  const ins = db.insert(items).values({ name: "a", tone: "ok", tags: ["x", "y", null] }).returning().toSQL();
  assert.deepEqual(ins.params, ["a", "ok", "{x,y,NULL}"]);
  assert.match(ins.sql, /\$3::text::text\[\]/i);
  // Invalid enum membership is rejected before any SQL runs.
  assert.throws(() => db.insert(items).values({ name: "a", tone: "angry" as never, tags: [] }).toSQL(), /not a declared value of enum "mood"/);
  assert.throws(() => db.insert(items).values({ name: "a", tone: "ok", tags: [["nested"]] as never }).toSQL(), /one-dimensional/);
  assert.throws(() => db.insert(items).values({ name: "a", tone: "ok", tags: [undefined as never] }).toSQL(), /use null for SQL NULL/);
});

test("q07 query layer: writes to generated/identity-always columns are rejected", () => {
  assert.throws(() => db.insert(items).values({ id: 1, name: "a", tone: "ok", tags: [] } as never).toSQL(), /GENERATED ALWAYS AS IDENTITY/);
  assert.throws(
    () =>
      db
        .insert(items)
        .values({ name: "a", tone: "ok", tags: [], gross: "1" as never })
        .toSQL(),
    /GENERATED ALWAYS AS \(stored\)/,
  );
  assert.throws(() => db.update(items).set({ id: 2 } as never).where(eq(items.name, "a")).toSQL(), /GENERATED ALWAYS AS IDENTITY/);
  assert.throws(() => db.update(items).set({ gross: "2" as never }).where(eq(items.name, "a")).toSQL(), /GENERATED ALWAYS AS \(stored\)/);
});

// ---------------------------------------------------------------------------
// Views: read surface, mutation rejection
// ---------------------------------------------------------------------------

const users2 = pgTable("users2", {
  id: serial("id").primaryKey(),
  email: text("email").notNull(),
  active: text("active").notNull().default("y"),
});
const activeUsers = pgView("active_users", { id: users2.id, email: users2.email }, { definition: sql`select ${users2.id}, ${users2.email} from ${users2} where ${users2.active} = 'y'` });

const vdb = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { users2 },
});

test("q07 views: select reads like a table (view-qualified references)", () => {
  const sel = vdb.select().from(activeUsers).where(eq(activeUsers.email, "a@b.c")).toSQL();
  assert.equal(sel.sql, 'select "active_users"."id", "active_users"."email" from "active_users" where ("active_users"."email" = $1)');
});

test("q07 views: mutations are rejected", () => {
  const dyn = vdb as unknown as {
    insert: (t: unknown) => { values: (v: unknown) => { toSQL: () => unknown } };
    update: (t: unknown) => { set: (v: unknown) => { toSQL: () => unknown } };
    delete: (t: unknown) => { where: (c: unknown) => { toSQL: () => unknown } };
  };
  assert.throws(() => dyn.insert(activeUsers).values({ email: "x" }).toSQL(), /is a view — views are read-only/);
  assert.throws(() => dyn.update(activeUsers).set({ email: "x" }).toSQL(), /is a view — views are read-only/);
  assert.throws(() => dyn.delete(activeUsers).where(eq(activeUsers.email, "x")).toSQL(), /is a view — views are read-only/);
});

// ---------------------------------------------------------------------------
// Custom codecs (Q07e)
// ---------------------------------------------------------------------------

interface Cents {
  cents: number;
}
const money = pgTable("money", {
  id: serial("id").primaryKey(),
  amount: numeric("amount").codec({
    decode: (v: string): Cents => ({ cents: Math.round(Number(v) * 100) }),
    encode: (v: Cents): string => (v.cents / 100).toFixed(2),
  }),
});

const mdb = await createDatabase({
  url: "postgres://snapshot:nouser@127.0.0.1:1/none",
  driverOptions: { driver: "postgres" },
  tables: { money },
});

test("q07 custom codecs: writes encode, defaults encode, codec errors carry context", () => {
  const ins = mdb.insert(money).values({ amount: { cents: 101 } }).toSQL();
  assert.deepEqual(ins.params, ["1.01"]);
  const upd = mdb.update(money).set({ amount: { cents: 5 } }).where(eq(money.amount, { cents: 5 } as never)).toSQL();
  assert.deepEqual(upd.params, ["0.05", "0.05"]);
  assert.throws(
    () =>
      mdb
        .update(money)
        .set({ amount: { cents: Number.NaN } })
        .where(eq(money.id, 1))
        .toSQL(),
    /not a decimal string|is not a plain numeric literal|numeric/,
  );
});

// ---------------------------------------------------------------------------
// Composite/rich index/constraint API shapes (export-side covered in
// export.test.ts; here: builder accepts them, DDL fails closed)
// ---------------------------------------------------------------------------

test("q07 extras: table constraints and rich indexes record and reject precisely", () => {
  const t = pgTable(
    "rich",
    {
      a: integer("a").notNull(),
      b: integer("b"),
      c: text("c"),
    },
    (x) => [
      primaryKey({ columns: [x.a] }),
      unique({ name: "rich_ab_key", columns: [x.a, x.b] }),
      check("rich_c_check", sql`${x.c} is null or ${x.c} <> ''`),
      foreignKey({ name: "rich_a_fkey", columns: [x.a] }).references(users2, [users2.id], { onDelete: "cascade", deferrable: true }),
      index("rich_bc_idx").using("gin").on(sql`lower(${x.c})`).where(sql`${x.c} is not null`),
      index("rich_ab_order").on(desc(x.a), ascNullsFirst(x.b), descNullsLast(x.b)),
    ],
  );
  const { getTableConstraints, getTableIndexes } = require("./schema.js") as {
    getTableConstraints: (t2: unknown) => Array<{ kind: string; name?: string }>;
    getTableIndexes: (t2: unknown) => Array<{ indexName: string; keyParts: Array<{ column?: string; expression?: unknown; order?: string; nulls?: string }>; whereExpr?: unknown; includeCols: string[] }>;
  };
  assert.equal(getTableConstraints(t).length, 4);
  const idxs = getTableIndexes(t);
  const order = idxs.find((i) => i.indexName === "rich_ab_order")!;
  assert.deepEqual(
    order.keyParts.map((p) => ({ order: p.order, nulls: p.nulls })),
    [
      { order: "desc", nulls: undefined },
      { order: undefined, nulls: "first" },
      { order: "desc", nulls: "last" },
    ],
  );
  assert.throws(() => require("./ddl.js").schemaToDDL([t]), /legacy DDL emitter/);
});

test("q07 pgSchema: enum factory, schema validation", () => {
  const alt = pgSchema("alt");
  const e = alt.enum("m", ["a", "b"]);
  assert.equal(e.enumName, "m");
  assert.equal(e.schema, "alt");
  assert.throws(() => pgSchema(""), /non-empty/);
  assert.throws(() => pgEnum("m2", [] as never), /non-empty array/);
  assert.throws(() => pgEnum("m3", ["a", "a"]), /declared twice/);
  assert.throws(() => pgEnum("m4", ["a\nb"] as never), /control characters/);
});

// ---------------------------------------------------------------------------
// Review fixes: decode through a fake pg pool (driver boundary, no database)
// — custom codecs apply on every read path; generated columns stay
// comparable in predicates while writes are rejected before SQL runs.
// ---------------------------------------------------------------------------

import { wrapPgPool } from "./drivers.js";

function fakePool(rows: Array<Record<string, unknown>>, seen: string[]) {
  const query = async (text: string | { text: string }) => {
    seen.push(typeof text === "string" ? text : text.text);
    return { rows: rows.map((r) => ({ ...r })), rowCount: rows.length };
  };
  return {
    query,
    connect: async () => ({ query, release: () => {} }),
    end: async () => {},
  };
}

const tone = pgEnum("tone2", ["low", "high"]);
const decoded = pgTable("decoded", {
  id: bigint("id").generatedAlwaysAsIdentity().primaryKey(),
  label: text("label").notNull().codec({ decode: (v: string) => v.toUpperCase(), encode: (v: string) => v.toLowerCase() }),
  at: timestamp("at").codec({ decode: (v: string) => `@${v}`, encode: (v: string) => v.slice(1) }),
  level: tone("level").notNull(),
  counts: bigint("counts").array(),
  total: integer("total").generatedAlwaysAs(sql`1 + 1`),
});

test("q07 decode: custom codecs on flat text and temporal text-wire reads, enums and int8 arrays exact", async () => {
  const seen: string[] = [];
  const driver = wrapPgPool(fakePool([
    { id: "9007199254740993", label: "abc", at: '"2024-01-02T03:04:05.123456"', level: "high", counts: '{1,NULL,-9223372036854775808}', total: 2 },
  ], seen));
  const ddb = await createDatabase({ driver, tables: { decoded } });
  const rows = await ddb.select().from(decoded).where(eq(decoded.id, 5n));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].id, 9007199254740993n);
  assert.equal(rows[0].label, "ABC");
  assert.equal(rows[0].at, "@2024-01-02T03:04:05.123456");
  assert.equal(rows[0].level, "high");
  assert.deepEqual(rows[0].counts, [1n, null, -9223372036854775808n]);
  const selectSql = seen.find((s) => s.includes(`from "decoded"`));
  assert.ok(selectSql !== undefined && selectSql.includes('"decoded"."counts"::text'), selectSql);

  const drifted = wrapPgPool(fakePool([{ id: "1", label: "a", at: null, level: "medium", counts: null, total: 2 }], []));
  const ddb2 = await createDatabase({ driver: drifted, tables: { decoded } });
  await assert.rejects(async () => { await ddb2.select().from(decoded); }, /not a declared value of enum "tone2".*drifted/);
});

test("q07 generated columns: comparable in predicates, rejected for every write shape", async () => {
  const driver = wrapPgPool(fakePool([], []));
  const ddb = await createDatabase({ driver, tables: { decoded } });
  // Predicates over identity-always / generated columns compile normally.
  const sel = ddb.select().from(decoded).where(eq(decoded.id, 1n)).where(eq(decoded.total, 2)).toSQL();
  assert.deepEqual(sel.params, [1n, 2]);
  // Writes: value, null and sql-fragment assignments all reject before SQL.
  assert.throws(() => ddb.insert(decoded).values({ label: "a", level: "low", total: null } as never).toSQL(), /GENERATED ALWAYS AS \(stored\)/);
  assert.throws(() => ddb.update(decoded).set({ total: sql`3` } as never).where(eq(decoded.id, 1n)).toSQL(), /GENERATED ALWAYS AS \(stored\)/);
  assert.throws(() => ddb.update(decoded).set({ id: null } as never).where(eq(decoded.id, 1n)).toSQL(), /GENERATED ALWAYS AS IDENTITY/);
  // The custom codec encodes predicate values too.
  const byLabel = ddb.select().from(decoded).where(eq(decoded.label, "ABC")).toSQL();
  assert.deepEqual(byLabel.params, ["abc"]);
});

// ---------------------------------------------------------------------------
// Index method applicability (Q07 rework F1): a method/key-type combination
// without a default operator class must be refused at definition time on
// both sides — the pinned DDL would fail at apply with SQLSTATE 42704 on
// every server. The facts are the PostgreSQL 15-17 built-in default
// opclasses (pg_opclass), probed live on PostgreSQL 17 and verified against
// the REL_15_STABLE catalog source.
// ---------------------------------------------------------------------------

test("q07 index methods: refuse method/type combos without a default operator class at export", () => {
  const probe = (
    method: IndexMethod,
    key: (t: Record<string, any>) => any,
    column: () => any,
    enums: Record<string, any> = {},
  ) => {
    const t = pgTable("probe", { id: serial("id").primaryKey(), k: column() }, (tbl) => [
      index("probe_k_idx").using(method).on(key(tbl as Record<string, any>)),
    ]);
    return exportSchemaV2({ probe: t, ...enums });
  };
  const refuse = (label: string, fn: () => unknown) =>
    assert.throws(fn, (err: Error) => /\[invalid-index\]/.test(err.message) && /operator-class slot/.test(err.message), label);
  const accept = (label: string, fn: () => unknown) => {
    const doc = fn() as { tables: Array<{ indexes: unknown[] }> };
    assert.equal(doc.tables[0].indexes.length, 1, label);
  };

  const c = { text: () => text("k"), varchar: () => varchar("k", 80), bool: () => boolean("k"), int: () => integer("k") };

  refuse("gin over text", () => probe("gin", (t) => t.k, c.text));
  refuse("gin over varchar", () => probe("gin", (t) => t.k, c.varchar));
  refuse("gin over int4", () => probe("gin", (t) => t.k, c.int));
  refuse("gin over bool", () => probe("gin", (t) => t.k, c.bool));
  refuse("gin over json scalar", () => probe("gin", (t) => t.k, () => json("k")));
  refuse("hash over json scalar", () => probe("hash", (t) => t.k, () => json("k")));
  refuse("btree over json scalar", () => probe("btree", (t) => t.k, () => json("k")));
  refuse("brin over jsonb", () => probe("brin", (t) => t.k, () => jsonb("k")));
  refuse("brin over bool", () => probe("brin", (t) => t.k, c.bool));
  refuse("spgist over int4", () => probe("spgist", (t) => t.k, c.int));
  refuse("gist over text", () => probe("gist", (t) => t.k, c.text));
  refuse("gist over text[]", () => probe("gist", (t) => t.k, () => text("k").array()));
  refuse("brin over enum", () => probe("brin", (t) => t.k, () => tone3("k"), { tone3 }));
  refuse("brin over text[]", () => probe("brin", (t) => t.k, () => text("k").array()));
  refuse("spgist over text[]", () => probe("spgist", (t) => t.k, () => text("k").array()));

  accept("gin over text[]", () => probe("gin", (t) => t.k, () => text("k").array()));
  accept("gin over jsonb", () => probe("gin", (t) => t.k, () => jsonb("k")));
  accept("gin over enum[]", () => probe("gin", (t) => t.k, () => tone3("k").array(), { tone3 }));
  accept("spgist over text", () => probe("spgist", (t) => t.k, c.text));
  accept("brin over text", () => probe("brin", (t) => t.k, c.text));
  accept("brin over uuid", () => probe("brin", (t) => t.k, () => uuid("k")));
  accept("hash over text", () => probe("hash", (t) => t.k, c.text));
  accept("hash over jsonb", () => probe("hash", (t) => t.k, () => jsonb("k")));
  accept("btree over jsonb", () => probe("btree", (t) => t.k, () => jsonb("k")));
  accept("btree over enum", () => probe("btree", (t) => t.k, () => tone3("k"), { tone3 }));
});

test("q07 index methods: expression keys are only definable with btree (opclass of the result type is unverifiable)", () => {
  const expr = (method: IndexMethod) =>
    exportSchemaV2({
      probe: pgTable("probe", { id: serial("id").primaryKey(), k: text("k") }, (t) => [
        index("probe_k_idx").using(method).on(sql`lower(${t.k})`),
      ]),
    });
  assert.throws(() => expr("gin"), (err: Error) => /\[invalid-index\]/.test(err.message) && /expression key/.test(err.message));
  for (const method of ["hash", "gist", "spgist", "brin"] as const) {
    assert.throws(() => expr(method), /\[invalid-index\]/, method);
  }
  const ok = expr("btree");
  assert.equal(ok.tables[0].indexes[0].key.length, 1);
});

