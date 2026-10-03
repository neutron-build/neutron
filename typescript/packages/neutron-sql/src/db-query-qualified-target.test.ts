import assert from "node:assert/strict";
import test from "node:test";
import { createDatabase, integer, pgSchema, pgTable, relations, type Driver } from "./index.js";
function driver(): Driver { return { query: async () => [], execute: async () => 0, begin: async fn => fn(driver()), close: async () => {}, lifecycle: { ownership: "borrowed", terminated: false, terminate: async () => {} } }; }
const users = pgTable("users", { id: integer("id").primaryKey(), ownerId: integer("owner_id") });
const target = pgSchema("other").table("users", { id: integer("id").primaryKey() });
test("unqualified owner can reference an explicitly qualified same-named target", async () => {
  const own = relations(users, ({ one }) => ({ owner: one(target, { fields: [users.ownerId], references: [target.id] }) }));
  const db = await createDatabase({ driver: driver(), tables: { users }, relations: { own } });
  const plan = db.query.users.toSQL({ with: { owner: true } });
  assert.match(plan.sql, /from "other"\."users"/i);
});
test("relation columns from a foreign same-named table are refused", async () => {
  const own = relations(users, ({ one }) => ({ owner: one(target, { fields: [users.ownerId], references: [users.id] }) }));
  await assert.rejects(createDatabase({ driver: driver(), tables: { users }, relations: { own } }), /own columns/);
});
