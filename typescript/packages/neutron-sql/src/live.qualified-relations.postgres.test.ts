import assert from "node:assert/strict";
import test from "node:test";
import { Pool } from "pg";
import { createDatabase, eq, asc, integer, text, pgSchema, relations } from "./index.js";
import { TEST_URL, ensureLive, uniqueDbName } from "./live-harness.js";

for (const driver of ["pg", "postgres"] as const) {
  test(`qualified relational reads/writes ignore public twins (${driver})`, async () => {
    if (!(await ensureLive(`qualified relational identity (${driver})`))) return;
    const name = uniqueDbName(`npg_qualified_${driver}`);
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    await admin.query(`CREATE DATABASE "${name}"`);
    const url = new URL(TEST_URL); url.pathname = `/${name}`;
    const native = new Pool({ connectionString: url.toString(), max: 1 });
    const a = pgSchema("tenant.left"); const b = pgSchema("tenant.right");
    const aUsers = a.table("users", { id: integer("id").primaryKey(), name: text("name").notNull() });
    const bUsers = b.table("users", { id: integer("id").primaryKey(), name: text("name").notNull() });
    const aPosts = a.table("posts", { id: integer("id").primaryKey(), authorId: integer("author_id").notNull(), title: text("title").notNull() });
    const bPosts = b.table("posts", { id: integer("id").primaryKey(), authorId: integer("author_id").notNull(), title: text("title").notNull() });
    const ar = relations(aUsers, ({ many }) => ({ posts: many(aPosts) }));
    const ap = relations(aPosts, ({ one }) => ({ author: one(aUsers, { fields: [aPosts.authorId], references: [aUsers.id] }) }));
    const br = relations(bUsers, ({ many }) => ({ posts: many(bPosts) }));
    const bp = relations(bPosts, ({ one }) => ({ author: one(bUsers, { fields: [bPosts.authorId], references: [bUsers.id] }) }));
    const db = await createDatabase({ url: url.toString(), driverOptions: { driver }, tables: { aUsers, bUsers, aPosts, bPosts }, relations: { aUsers: ar, aPosts: ap, bUsers: br, bPosts: bp } });
    try {
      for (const schema of ["public", "tenant.left", "tenant.right"]) {
        if (schema !== "public") await native.query(`CREATE SCHEMA "${schema}"`);
        await native.query(`CREATE TABLE "${schema}".users (id integer PRIMARY KEY, name text NOT NULL)`);
        await native.query(`CREATE TABLE "${schema}".posts (id integer PRIMARY KEY, author_id integer NOT NULL REFERENCES "${schema}".users(id), title text NOT NULL)`);
        await native.query(`INSERT INTO "${schema}".users VALUES (1, $1)`, [schema]);
        await native.query(`INSERT INTO "${schema}".posts VALUES (1, 1, $1), (2, 1, 'second')`, [schema]);
      }
      const left = await db.query.aUsers.findMany({ with: { posts: { where: eq(aPosts.title, "tenant.left"), orderBy: [asc(aPosts.id)], limit: 1, with: { author: true } } } });
      assert.equal(left[0].name, "tenant.left");
      assert.equal(left[0].posts.length, 1);
      assert.equal(left[0].posts[0].author?.name, "tenant.left");
      const right = await db.query.bUsers.findMany({ with: { posts: true } });
      assert.equal(right[0].name, "tenant.right");
      assert.deepEqual(right[0].posts.map(post => post.title), ["tenant.right", "second"]);
      await db.query.aUsers.create({ data: { id: 3, name: "created", posts: { create: [{ id: 3, title: "new child" }] } } });
      const rows = await native.query('SELECT author_id, title FROM "tenant.left".posts WHERE id = 3');
      assert.deepEqual(rows.rows, [{ author_id: 3, title: "new child" }]);
      for (const schema of ["public", "tenant.right"]) {
        assert.equal((await native.query(`SELECT count(*)::integer AS n FROM "${schema}".users WHERE id=3`)).rows[0].n, 0);
      }
      await assert.rejects(db.query.aUsers.create({ data: { id: 4, name: "must rollback", posts: { create: [{ id: 1, title: "duplicate" }] } } }));
      assert.equal((await native.query('SELECT count(*)::integer AS n FROM "tenant.left".users WHERE id=4')).rows[0].n, 0);
      await db.query.aPosts.update({ where: { id: 3 }, data: { title: "updated" } });
      assert.equal((await native.query('SELECT title FROM "tenant.left".posts WHERE id=3')).rows[0].title, "updated");
      await db.query.aUsers.delete({ where: { id: 3 }, cascade: { posts: "delete" } });
      assert.equal((await native.query('SELECT count(*)::integer AS n FROM "tenant.left".posts WHERE id=3')).rows[0].n, 0);
    } finally {
      await db.close(); await native.end();
      await admin.query(`DROP DATABASE "${name}"`); await admin.end();
    }
  });
}
