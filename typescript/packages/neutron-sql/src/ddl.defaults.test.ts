import assert from "node:assert/strict";
import test from "node:test";
import { schemaToDDL } from "./ddl.js";
import { pgTable, text, jsonb } from "./schema.js";
import { jsonNull } from "./codecs.js";

test("legacy DDL distinguishes JSON null, SQL null and codec-transformed defaults", () => {
  const table = pgTable("oracle_defaults", {
    nil: jsonb("nil").default(jsonNull), absent: jsonb("absent").default(null),
    encoded: text("encoded").codec({ encode: (value: string) => `wire:${value}`, decode: (value: unknown) => String(value) }).default("quote'"),
  });
  const [ddl] = schemaToDDL([table]);
  assert.match(ddl, /"nil" jsonb default 'null'/);
  assert.match(ddl, /"absent" jsonb default null/);
  assert.match(ddl, /"encoded" text default 'wire:quote'''/);
});
