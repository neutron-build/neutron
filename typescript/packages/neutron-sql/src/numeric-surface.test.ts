import assert from "node:assert/strict";
import test from "node:test";
import {
  pgTable,
  serial,
  numeric,
  getTableColumns,
  sqlTypeOf,
  createTableSQL,
  type NumericColumnOptions,
} from "./index.js";

// NUMERIC_SURFACE_HARDENING: the two surface-hygiene residuals from the
// NEUTRON_SQL_FIXES review — (a) NumericColumnOptions is importable from the
// package entry (this file is type-checked by the `pnpm test` build step, so
// the type import above fails compilation if the re-export is missing);
// (b) the legacy DDL renderer never emits numeric(p,undefined) when internal
// numeric metadata is half-set (precision present, scale undefined).

test("numeric surface: NumericColumnOptions is importable from the package entry", () => {
  const plain: NumericColumnOptions = { precision: 10, scale: 2 };
  assert.equal(plain.precision, 10);
  assert.equal(plain.scale, 2);
  const withDecoder: NumericColumnOptions<(raw: string) => number> = {
    precision: 8,
    scale: 4,
    decoder: (raw) => Number(raw),
  };
  assert.equal(typeof withDecoder.decoder, "function");
});

test("numeric surface: factory-created columns render exactly as before (no behavior change)", () => {
  const t = pgTable("surface_factory_unchanged", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }),
    whole: numeric("whole", { precision: 10 }),
    plain: numeric("plain"),
  });
  assert.equal(sqlTypeOf(getTableColumns(t).amount), "numeric(10,2)");
  assert.equal(sqlTypeOf(getTableColumns(t).whole), "numeric(10,0)");
  assert.equal(sqlTypeOf(getTableColumns(t).plain), "numeric");
  assert.equal(
    createTableSQL(t),
    'create table "surface_factory_unchanged" (\n  "id" serial primary key,\n  "amount" numeric(10,2),\n  "whole" numeric(10,0),\n  "plain" numeric\n)',
  );
});

test("numeric surface: DDL renderer never emits numeric(p,undefined) on half-set metadata (internal-invariant)", () => {
  // INTERNAL-INVARIANT TEST — not a public-API path. The numeric() factory
  // always writes numericPrecision/numericScale as a pair (precision-only
  // normalizes scale to 0) and pgView clones copy both fields, so
  // precision-set/scale-undefined metadata is reachable only by direct
  // property assignment on a column builder (a future factory regression or
  // hand-assembled metadata). Construct it exactly that way.
  const t = pgTable("surface_half_set", {
    id: serial("id").primaryKey(),
    amount: numeric("amount", { precision: 10, scale: 2 }),
  });
  const amount = getTableColumns(t).amount as unknown as { numericPrecision?: number; numericScale?: number };
  assert.deepEqual(
    { precision: amount.numericPrecision, scale: amount.numericScale },
    { precision: 10, scale: 2 },
  );
  amount.numericScale = undefined; // half-set: precision 10 present, scale undefined
  assert.equal(sqlTypeOf(getTableColumns(t).amount), "numeric(10,0)");
  const ddl = createTableSQL(t);
  assert.ok(!ddl.includes("undefined"), ddl);
  assert.ok(ddl.includes('"amount" numeric(10,0)'), ddl);
});
