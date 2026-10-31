import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { createDatabase, pgTable, primaryKey, integer, text, relations, eq, and, asc } from "./index.js";
import { ensureLive, TEST_URL } from "./live-harness.js";

for (const driver of ["pg", "postgres"] as const) {
  test(`live table-level composite PK (${driver}): tenant-local nested reads and child pagination`, async () => {
    if (!(await ensureLive(`composite PK ${driver}`))) return;
    const name = `v10_orm_composite_${randomUUID().replaceAll("-", "")}`;
    const admin = new Pool({ connectionString: TEST_URL, max: 1 });
    let db: Awaited<ReturnType<typeof createDatabase>> | undefined;
    let oracle: Pool | undefined;
    try {
      await admin.query(`CREATE DATABASE "${name}"`);
      const url = new URL(TEST_URL); url.pathname = `/${name}`;
      oracle = new Pool({ connectionString: url.href, max: 1 });
      await oracle.query(`
        CREATE TABLE projects(tenant_key TEXT, local_id INT, PRIMARY KEY(tenant_key,local_id));
        CREATE TABLE documents(tenant_key TEXT, local_id INT, project_id INT, sort_key INT, PRIMARY KEY(local_id,tenant_key), FOREIGN KEY(tenant_key,project_id) REFERENCES projects(tenant_key,local_id));
        CREATE TABLE leaves(tenant_key TEXT, local_id INT, document_id INT, label TEXT, PRIMARY KEY(tenant_key,local_id), FOREIGN KEY(document_id,tenant_key) REFERENCES documents(local_id,tenant_key));
        INSERT INTO projects VALUES('a',1),('b',1),('a',2);
        INSERT INTO documents VALUES('a',3,1,0),('a',1,1,0),('a',2,1,0),('b',1,1,0);
        INSERT INTO leaves VALUES('a',2,1,'second'),('a',1,1,'first'),('b',1,1,'other');
      `);
      const projects = pgTable("projects", { tenant: text("tenant_key").notNull(), id: integer("local_id").notNull() }, t => [primaryKey({ columns: [t.tenant, t.id] })]);
      const documents = pgTable("documents", { tenant: text("tenant_key").notNull(), id: integer("local_id").notNull(), projectId: integer("project_id"), sort: integer("sort_key") }, t => [primaryKey({ columns: [t.id, t.tenant] })]);
      const leaves = pgTable("leaves", { tenant: text("tenant_key").notNull(), id: integer("local_id").notNull(), documentId: integer("document_id"), label: text("label") }, t => [primaryKey({ columns: [t.tenant, t.id] })]);
      const pr = relations(projects, ({ many }) => ({ documents: many(documents) }));
      const dr = relations(documents, ({ one, many }) => ({ project: one(projects, { fields: [documents.projectId, documents.tenant], references: [projects.id, projects.tenant] }), leaves: many(leaves) }));
      const lr = relations(leaves, ({ one }) => ({ document: one(documents, { fields: [leaves.documentId, leaves.tenant], references: [documents.id, documents.tenant] }) }));
      const actual = await createDatabase({ url: url.href, driverOptions: { driver, max: 2 }, tables: { projects, documents, leaves }, relations: { projects: pr, documents: dr, leaves: lr } });
      db = actual;
      const nested = await actual.query.projects.findMany({ orderBy: [asc(projects.tenant), asc(projects.id)], with: { documents: { with: { leaves: true } } } });
      assert.deepEqual(nested.map(p => [p.tenant, p.id, p.documents.map(d => [d.tenant, d.id, d.leaves.map(l => [l.tenant, l.id, l.label])])]), [
        ["a", 1, [["a", 1, [["a", 1, "first"], ["a", 2, "second"]]], ["a", 2, []], ["a", 3, []]]], ["a", 2, []], ["b", 1, [["b", 1, [["b", 1, "other"]]]]],
      ]);
      for (const offset of [0, 1, 2]) {
        const page = await actual.query.projects.findFirst({ where: and(eq(projects.tenant, "a"), eq(projects.id, 1)), with: { documents: { orderBy: [asc(documents.sort)], limit: 1, offset, columns: ["projectId"], with: { leaves: true } } } });
        const expected: { rows: { local_id: number }[] } = await oracle.query('SELECT local_id FROM documents WHERE tenant_key=$1 AND project_id=$2 ORDER BY sort_key,local_id,tenant_key LIMIT 1 OFFSET $3', ["a", 1, offset]);
        assert.equal(expected.rows[0].local_id, offset + 1);
        assert.deepEqual(page?.documents.map(d => [d.projectId, d.leaves.map(l => l.label)]), [[1, offset === 0 ? ["first", "second"] : []]]);
      }
      const reverse = await actual.query.documents.findFirst({ where: and(eq(documents.tenant, "b"), eq(documents.id, 1)), with: { project: true } });
      assert.deepEqual(reverse?.project, { tenant: "b", id: 1 });
      assert.equal((await oracle.query('SELECT count(*)::int AS n FROM documents')).rows[0].n, 4);
    } finally {
      if (db) await db.close();
      if (oracle) await oracle.end();
      await admin.query(`DROP DATABASE IF EXISTS "${name}"`);
      await admin.end();
    }
  });
}
