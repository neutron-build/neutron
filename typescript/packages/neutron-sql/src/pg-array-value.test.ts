import test from "node:test";
import assert from "node:assert/strict";
import { PgArray, bigint, jsonb, jsonNull, pgTable, text } from "./index.js";
import { parsePgArray, formatPgArray } from "./pg-array-value.js";
import { decodeArrayText, encodeWriteValue } from "./codecs.js";

const ctx = { tableName: "native_arrays", propertyKey: "values", columnName: "values" };

test("native array shape retains signed lower bounds and nullable escaped elements", () => {
  const value = new PgArray([{ length: 2, lowerBound: -3 }, { length: 3, lowerBound: 0 }], ["NULL", null, "", 'a"b', "c\\d", "{comma,}"]);
  const decoded = parsePgArray(formatPgArray(value));
  assert.deepEqual(decoded.dimensions, value.dimensions);
  assert.deepEqual(decoded.elements, value.elements);
  assert.deepEqual(parsePgArray("{}").dimensions, []);
  assert.deepEqual(parsePgArray("{{1,2},{3,4}}").dimensions, [{ length: 2, lowerBound: 1 }, { length: 2, lowerBound: 1 }]);
});

test("native arrays reject invalid cardinality, bounds, shape and sparse or recursive JSON", () => {
  for (const input of ["{{1},{2,3}}", "{{1},2}", "[0:2]={1,2}", "{1}trailing", "{\"unterminated}", "{{}}", "[2147483647:2147483648]={1,2}"]) assert.throws(() => parsePgArray(input));
  assert.throws(() => new PgArray([{ length: 2, lowerBound: 1 }], [1]), /cardinality/);
  assert.throws(() => new PgArray([{ length: 1000001, lowerBound: 1 }], []), /budget/);
  assert.throws(() => new PgArray([{ length: 1, lowerBound: 1 }], new Array(1)), /sparse/);
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  assert.throws(() => new PgArray([{ length: 1, lowerBound: 1 }], [cyclic]), /cycles/);
  const accessor = Object.defineProperty({}, "bad", { enumerable: true, get() { throw new Error("getter must not execute"); } });
  assert.throws(() => new PgArray([{ length: 1, lowerBound: 1 }], [accessor]), /accessors/);
});

test("native array snapshots isolate input and detached JSON/binary outputs", () => {
  const dimensions = [{ length: 2, lowerBound: 0 }];
  const json = { nested: [1, { value: 2 }] };
  const binary = new Uint8Array([1, 2]);
  const value = new PgArray<unknown>(dimensions, [json, binary]);
  dimensions[0].lowerBound = 100; json.nested[0] = 9; binary[0] = 9;
  assert.equal(value.dimensions[0].lowerBound, 0);
  assert.deepEqual(value.elements[0], { nested: [1, { value: 2 }] });
  (value.elements[1] as Uint8Array)[0] = 8;
  assert.deepEqual(value.elements[1], new Uint8Array([1, 2]));
});

test("native array codec preserves int8 extremes and JSON null distinct from SQL NULL", () => {
  const table = pgTable("native_arrays", { values: bigint("values").nativeArray(), docs: jsonb("docs").nativeArray(), legacy: text("legacy").array() });
  const integers = new PgArray([{ length: 3, lowerBound: -2 }], [-9223372036854775808n, null, 9223372036854775807n]);
  const encoded = encodeWriteValue(table.values, ctx, integers);
  const decoded = decodeArrayText(table.values, ctx, encoded.bind) as PgArray<bigint>;
  assert.deepEqual(decoded.elements, integers.elements);
  assert.deepEqual(decoded.dimensions, integers.dimensions);
  const docs = new PgArray<unknown>([{ length: 3, lowerBound: 4 }], [jsonNull, null, [1, null]]);
  const jsonEncoded = encodeWriteValue(table.docs, ctx, docs);
  const jsonDecoded = decodeArrayText(table.docs, ctx, jsonEncoded.bind) as PgArray<unknown>;
  assert.deepEqual(jsonDecoded.elements, docs.elements);
  assert.throws(() => encodeWriteValue(table.values, ctx, [1n]), /require PgArray/);
  assert.throws(() => decodeArrayText(table.legacy, ctx, "{{a},{b}}"), /multi-dimensional/);
});
