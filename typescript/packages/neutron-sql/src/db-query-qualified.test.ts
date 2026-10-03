import assert from "node:assert/strict";
import test from "node:test";
import { createDatabase, eq, asc, pgSchema, integer, text, relations, type Driver } from "./index.js";

function recordingDriver(): Driver & { statements: string[] } {
  const statements: string[] = [];
  const driver: Driver = {
    async query<T>(sql: string): Promise<T[]> { statements.push(sql); return []; },
    async execute(sql: string) { statements.push(sql); return 0; },
    async begin<T>(fn: (tx: Driver) => Promise<T>) { return fn(driver); },
    async close() {},
    lifecycle: { ownership: "borrowed", terminated: false, async terminate() {} },
  };
  return Object.assign(driver, { statements });
}
const left = pgSchema("tenant.left");
const right = pgSchema("tenant.right");
const leftUsers = left.table("users", { id: integer("id").primaryKey(), name: text("name") });
const rightUsers = right.table("users", { id: integer("id").primaryKey(), name: text("name") });
const leftPosts = left.table("posts", { id: integer("id").primaryKey(), authorId: integer("author_id"), title: text("title") });
const rightPosts = right.table("posts", { id: integer("id").primaryKey(), authorId: integer("author_id"), title: text("title") });
const leftUsersRelations = relations(leftUsers, ({ many }) => ({ posts: many(leftPosts) }));
const leftPostsRelations = relations(leftPosts, ({ one }) => ({ author: one(leftUsers, { fields: [leftPosts.authorId], references: [leftUsers.id] }) }));
const rightUsersRelations = relations(rightUsers, ({ many }) => ({ posts: many(rightPosts) }));
const rightPostsRelations = relations(rightPosts, ({ one }) => ({ author: one(rightUsers, { fields: [rightPosts.authorId], references: [rightUsers.id] }) }));

test("same-named tables retain qualified identity through nested reads and writes", async () => {
  const driver = recordingDriver();
  const db = await createDatabase({ driver, tables: { leftUsers, rightUsers, leftPosts, rightPosts },
    relations: { leftUsersRelations, rightUsersRelations, leftPostsRelations, rightPostsRelations } });
  const plan = db.query.leftUsers.toSQL({ with: { posts: { where: eq(leftPosts.title, "left"), orderBy: [asc(leftPosts.id)], limit: 1, with: { author: true } } } });
  assert.match(plan.sql, /"tenant\.left"\."users"/);
  assert.match(plan.sql, /"tenant\.left"\."posts"/);
  assert.equal(plan.sql.includes('"tenant.right"'), false);
  assert.deepEqual(plan.params, ["left"]);
  const other = db.query.rightUsers.toSQL({ with: { posts: true } });
  assert.match(other.sql, /"tenant\.right"\."posts"/);
  assert.equal(other.sql.includes('"tenant.left"'), false);
  const write = db.query.leftUsers.explainCreate({ data: { id: 1, name: "left", posts: { create: [{ id: 2, title: "child" }] } } });
  assert.match(JSON.stringify(write), /tenant\.left/);
  assert.equal(JSON.stringify(write).includes('tenant.right'), false);
  assert.equal(driver.statements.length, 0);
});

test("qualified relation filters cannot bind to a same-named foreign schema", async () => {
  const db = await createDatabase({ driver: recordingDriver(), tables: { leftUsers, leftPosts }, relations: { leftUsersRelations, leftPostsRelations } });
  assert.throws(() => db.query.leftUsers.toSQL({ with: { posts: { where: eq(rightPosts.title, "other tenant") } } }), /own table/);
});

test("duplicate physical registration is refused before SQL", async () => {
  const driver = recordingDriver();
  await assert.rejects(createDatabase({ driver, tables: { first: leftUsers, second: leftUsers } }), /registered more than once/);
  assert.equal(driver.statements.length, 0);
});
