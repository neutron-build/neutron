import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  pgView,
  numeric,
  serial,
  integer,
  text,
  sql,
  createTableSQL,
  sqlTypeOf,
  exportSchema,
  exportSchemaV2,
  getTableColumns,
  eq,
} from "./index.js";

// Gap 1.4: declarable numeric precision/scale on the named-column factory
// numeric(name, options). The shared contract subset (Go schema_v2.go): the
// emitted SQL type is NUMERIC(p,s) with precision 1..1000 and scale
// 0..precision; scale requires precision; precision-only normalizes to scale
// 0. Value semantics are unchanged — reads stay exact decimal strings.
// Unit-only: live PostgreSQL typmod/readback behavior (coercion of 1.235 to
// 1.24, overflow rejection) is certified by the wave-closure live oracle,
// not here.

function precisionScale(col: unknown): { precision: number | undefined; scale: number | undefined } {
  const c = col as { numericPrecision?: number; numericScale?: number };
  return { precision: c.numericPrecision, scale: c.numericScale };
}

test("numeric typmod: valid declarations store paired metadata", () => {
  assert.deepEqual(precisionScale(numeric("amount")), { precision: undefined, scale: undefined });
  assert.deepEqual(precisionScale(numeric("amount", {})), { precision: undefined, scale: undefined });
  assert.deepEqual(precisionScale(numeric("amount", { precision: 10, scale: 2 })), { precision: 10, scale: 2 });
  assert.deepEqual(precisionScale(numeric("amount", { precision: 1, scale: 0 })), { precision: 1, scale: 0 });
  assert.deepEqual(precisionScale(numeric("amount", { precision: 1000, scale: 1000 })), { precision: 1000, scale: 1000 });
  // precision-only normalizes to scale 0 (exported as both p and 0)
  assert.deepEqual(precisionScale(numeric("amount", { precision: 10 })), { precision: 10, scale: 0 });
});

test("numeric typmod: invalid options are rejected at declaration, before any SQL", () => {
  for (const bad of [0, -1, 1001, 1.5, NaN, Infinity, -Infinity]) {
    assert.throws(() => numeric("amount", { precision: bad }), /precision must be an integer within 1\.\.1000/, `precision ${String(bad)}`);
  }
  assert.throws(() => numeric("amount", { precision: 10, scale: -1 }), /scale must be an integer within 0\.\.precision \(10\)/);
  assert.throws(() => numeric("amount", { precision: 10, scale: 11 }), /scale must be an integer within 0\.\.precision \(10\)/);
  assert.throws(() => numeric("amount", { precision: 10, scale: 0.5 }), /scale must be an integer within 0\.\.precision \(10\)/);
  assert.throws(() => numeric("amount", { precision: 10, scale: NaN }), /scale must be an integer within 0\.\.precision \(10\)/);
  // scale without precision has no SQL spelling — refused, not defaulted
  assert.throws(() => numeric("amount", { scale: 2 }), /scale requires precision/);
});

test("numeric typmod: decoder overload coexists with the typmod", () => {
  const dec = (raw: string) => Number(raw);
  const withBoth = numeric("rate", { precision: 8, scale: 4, decoder: dec });
  assert.deepEqual(precisionScale(withBoth), { precision: 8, scale: 4 });
  assert.equal((withBoth as unknown as { valueDecoder?: unknown }).valueDecoder, dec);
  const decoderOnly = numeric("rate", { decoder: dec });
  assert.deepEqual(precisionScale(decoderOnly), { precision: undefined, scale: undefined });
  const plain = numeric("amount");
  assert.equal((plain as unknown as { valueDecoder?: unknown }).valueDecoder, undefined);
  assert.throws(() => numeric("rate", { precision: 8, decoder: "not-a-function" as unknown as (raw: string) => unknown }), /decoder must be a function/);
});

test("numeric typmod: legacy DDL renders NUMERIC(p,s); unconstrained numeric unchanged", () => {
  const ledger = pgTable("ledger_typmod", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }).notNull(),
    whole: numeric("whole", { precision: 10 }),
    plain: numeric("plain"),
  });
  assert.equal(sqlTypeOf(getTableColumns(ledger).amount), "numeric(10,2)");
  assert.equal(sqlTypeOf(getTableColumns(ledger).whole), "numeric(10,0)");
  assert.equal(sqlTypeOf(getTableColumns(ledger).plain), "numeric");
  const ddl = createTableSQL(ledger);
  assert.ok(ddl.includes('"amount" numeric(10,2) not null'), ddl);
  assert.ok(ddl.includes('"whole" numeric(10,0)'), ddl);
  assert.ok(ddl.includes('"plain" numeric'), ddl);
  assert.ok(!ddl.includes("numeric(,"), ddl);
});

test("numeric typmod: declared defaults keep their literal spelling", () => {
  const t = pgTable("defaults_typmod", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }).notNull().default("1.50"),
  });
  const ddl = createTableSQL(t);
  assert.ok(ddl.includes('"amount" numeric(10,2) not null default 1.50'), ddl);
  const doc = exportSchemaV2({ t });
  assert.deepEqual(doc.tables[0].columns[1].default, { kind: "literal", sql: "1.50" });
});

test("numeric typmod: view clones keep the declared typmod", () => {
  const base = pgTable("clone_base", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 12, scale: 4 }),
    plain: numeric("plain"),
  });
  const view = pgView("clone_view", { id: base.id, amount: base.amount, plain: base.plain }, { definition: sql`select ${base.id}, ${base.amount}, ${base.plain} from ${base}` });
  const viewCols = getTableColumns(view);
  assert.deepEqual(precisionScale(viewCols.amount), { precision: 12, scale: 4 });
  assert.deepEqual(precisionScale(viewCols.plain), { precision: undefined, scale: undefined });
  assert.equal(sqlTypeOf(viewCols.amount), "numeric(12,4)");
});

test("numeric typmod: v2 export emits precision/scale params (scalars and arrays)", () => {
  const t = pgTable("v2_typmod", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }),
    whole: numeric("whole", { precision: 10 }),
    plain: numeric("plain"),
    tags: numeric("tags", { precision: 6, scale: 3 }).array(),
  });
  const doc = exportSchemaV2({ t });
  const typeOf = (name: string) => doc.tables[0].columns.find((c) => c.name === name)!.type;
  assert.deepEqual(typeOf("amount"), { name: "numeric", codec: "decimal-string", params: { precision: 10, scale: 2 } });
  // precision-only exports BOTH p and the normalized 0 (never relies on
  // downstream defaults)
  assert.deepEqual(typeOf("whole"), { name: "numeric", codec: "decimal-string", params: { precision: 10, scale: 0 } });
  // unconstrained numeric exports exactly as before — no params key
  assert.deepEqual(typeOf("plain"), { name: "numeric", codec: "decimal-string" });
  assert.equal("params" in typeOf("plain"), false);
  // numeric arrays preserve the modifiers in v2
  assert.deepEqual(typeOf("tags"), { name: "numeric", codec: "array", array: true, params: { precision: 6, scale: 3 } });
});

test("numeric typmod: exported v2 JSON passes the CLI schema-v2 contract rules", () => {
  // Mirrors the Go validator (cli/internal/db/schema_v2.go): numeric accepts
  // exactly params.precision (integer 1..1000) and params.scale (integer
  // 0..precision); scale requires precision; scalar numeric requires codec
  // "decimal-string", numeric arrays codec "array" with array=true; params
  // values must be safe integers. Run over the actual exported JSON.
  const t = pgTable("contract_typmod", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }),
    whole: numeric("whole", { precision: 10 }),
    maxed: numeric("maxed", { precision: 1000, scale: 1000 }),
    minned: numeric("minned", { precision: 1, scale: 0 }),
    plain: numeric("plain"),
    tags: numeric("tags", { precision: 6, scale: 3 }).array(),
    label: text("label"),
    count: integer("count"),
  });
  const view = pgView("contract_view", { amount: t.amount }, { definition: sql`select ${t.amount} from ${t}` });
  const parsed = JSON.parse(JSON.stringify(exportSchemaV2({ t, view }))) as {
    tables: Array<{ columns: Array<{ name: string; type: { name: string; codec: string; array?: boolean; params?: Record<string, unknown> } }> }>;
  };
  assert.equal(parsed.tables.length, 1);
  for (const col of parsed.tables[0].columns) {
    const ty = col.type;
    if (ty.name !== "numeric") continue;
    const isArray = ty.array === true;
    assert.equal(ty.codec, isArray ? "array" : "decimal-string", `${col.name}: codec`);
    const params = ty.params;
    if (params === undefined) continue; // unconstrained numeric: no params
    assert.deepEqual(Object.keys(params).sort(), ["precision", "scale"], `${col.name}: numeric accepts exactly precision/scale params`);
    for (const v of Object.values(params)) {
      assert.equal(typeof v, "number", `${col.name}: param must serialize as a JSON number`);
      assert.ok(Number.isSafeInteger(v), `${col.name}: param must be a safe integer`);
    }
    const p = params.precision as number;
    const s = params.scale as number;
    assert.ok(p >= 1 && p <= 1000, `${col.name}: precision must be within 1..1000`);
    assert.ok(s >= 0 && s <= p, `${col.name}: scale must be within 0..precision`);
  }
  const amount = parsed.tables[0].columns.find((c) => c.name === "amount")!;
  assert.deepEqual(amount.type.params, { precision: 10, scale: 2 });
});

test("numeric typmod: legacy v1 export refuses modifier-bearing columns; unconstrained still exports", () => {
  const plain = pgTable("v1_plain", {
    id: serial("id").primaryKey(),
    amount: numeric("amount"),
  });
  const exported = exportSchema({ plain });
  assert.equal(exported.tables[0].columns[1].type, "numeric");

  const constrained = pgTable("v1_constrained", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }),
  });
  assert.throws(() => exportSchema({ constrained }), /v1 export shape cannot represent numeric precision\/scale; use exportSchemaV2/);
  assert.throws(() => exportSchema({ constrained }), /numeric\(10,2\)/);
});

test("numeric typmod: legacy array refusal stays intact with modifiers present", () => {
  const t = pgTable("array_typmod", {
    id: serial("id").primaryKey(),
    tags: numeric("tags", { precision: 6, scale: 3 }).array(),
  });
  assert.throws(() => createTableSQL(t), /is an array column — the legacy DDL emitter cannot emit array types; use schema export v2/);
  assert.throws(() => exportSchema({ t }), /is an array column — the v1 export shape cannot represent arrays; use exportSchemaV2/);
});

test("numeric typmod: query layer is unchanged — constrained columns read and compare like plain numerics", () => {
  const t = pgTable("query_typmod", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }).notNull(),
  });
  assert.doesNotThrow(() => void eq(t.amount, "12.34"));
});
