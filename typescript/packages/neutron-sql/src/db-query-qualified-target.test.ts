import assert from "node:assert/strict";
import test from "node:test";
import { createDatabase, integer, pgSchema, pgTable, pgView, sql, relations, type Driver } from "./index.js";
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

// Even auto-updatable PostgreSQL views remain read-only in this ORM profile.
test("nested writes to a view fail before any SQL or transaction", async () => {
  const view = pgView("user_view", { id: users.id, ownerId: users.ownerId }, { definition: sql`select ${users.id}, ${users.ownerId} from ${users}` });
  let touched = false;
  const fake = driver();
  fake.query = async () => { touched = true; return []; };
  fake.execute = async () => { touched = true; return 0; };
  fake.begin = async () => { touched = true; throw new Error("must not begin"); };
  const db = await createDatabase({ driver: fake, tables: { view } });
  await assert.rejects(db.query.view.create({ data: { id: 1, ownerId: 2 } }), /views are read-only/);
  await assert.rejects(db.query.view.update({ where: { id: 1 }, data: { ownerId: 3 } }), /views are read-only/);
  await assert.rejects(db.query.view.delete({ where: { id: 1 } }), /views are read-only/);
  assert.equal(touched, false);
});
