import assert from "node:assert/strict";
import { test } from "node:test";

import { parsePartialJson } from "./partial-json.js";

test("parses complete JSON as-is", () => {
  assert.deepEqual(parsePartialJson('{"a":1}'), { a: 1 });
});

test("completes an object cut mid-string", () => {
  assert.deepEqual(parsePartialJson('{"title":"Neut'), { title: "Neut" });
});

test("completes nested structures", () => {
  assert.deepEqual(parsePartialJson('{"items":[{"name":"a"},{"name":"b'), {
    items: [{ name: "a" }, { name: "b" }],
  });
});

test("drops dangling fragments so the last complete snapshot parses", () => {
  assert.deepEqual(parsePartialJson('{"done":tru'), {});
  assert.deepEqual(parsePartialJson('{"a":1,'), { a: 1 });
  assert.deepEqual(parsePartialJson('{"a":1,"key"'), { a: 1 });
  assert.deepEqual(parsePartialJson('{"title":"Neutron","stars":'), { title: "Neutron" });
});

test("returns undefined when nothing parses", () => {
  assert.equal(parsePartialJson(""), undefined);
  assert.equal(parsePartialJson("not json"), undefined);
});

test("handles escaped quotes inside strings", () => {
  assert.deepEqual(parsePartialJson('{"quote":"say \\"hi'), { quote: 'say "hi' });
});

test("drops a trailing backslash before completing", () => {
  assert.deepEqual(parsePartialJson('{"path":"C:\\\\'), { path: "C:\\" });
});
