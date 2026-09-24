import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { sqliteTable, integer, text } from "drizzle-orm/sqlite-core";
import { eq, sql } from "drizzle-orm";
import { createDrizzleDatabase } from "./drizzle.js";

// I03 live SQLite fixture — the minimal honest one. The wrapper's SQLite leg
// (drizzle-orm/libsql over @libsql/client) executes real statements against
// a real database file in a temp directory: typed inserts/reads through
// Drizzle's own API (no casts to Neutron types anywhere), a Drizzle
// transaction (pass-through — the wrapper adds no transaction layer), and a
// clean close. Runs everywhere: no server, no env vars.

const todos = sqliteTable("i03_todos", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  label: text("label").notNull(),
  done: integer("done", { mode: "boolean" }).notNull().default(false),
});

test("sqlite: genuine Drizzle typed round-trip through the wrapper", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "i03-sqlite-"));
  try {
    const database = await createDrizzleDatabase({
      profile: { provider: "sqlite", connectionString: path.join(dir, "fixture.db") },
      schema: { todos },
    });

    assert.equal(database.profile.provider, "sqlite");
    assert.equal(database.nucleus, null);

    await database.db.run(sql`CREATE TABLE i03_todos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      label TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    )`);

    await database.db.insert(todos).values({ label: "write the fixture", done: true });
    await database.db.insert(todos).values({ label: "verify typed reads", done: false });

    const rows = await database.db.select().from(todos).orderBy(todos.id);
    assert.deepEqual(
      rows.map((r) => ({ label: r.label, done: r.done })),
      [
        { label: "write the fixture", done: true },
        { label: "verify typed reads", done: false },
      ],
    );

    const one = await database.db.select().from(todos).where(eq(todos.id, 2));
    assert.equal(one.length, 1);
    assert.equal(one[0]!.label, "verify typed reads");

    await database.db.update(todos).set({ done: true }).where(eq(todos.id, 2));
    const flipped = await database.db.select().from(todos).where(eq(todos.id, 2));
    assert.equal(flipped[0]!.done, true);

    await database.db.delete(todos).where(eq(todos.id, 1));
    const remaining = await database.db.select().from(todos);
    assert.equal(remaining.length, 1);

    await database.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("sqlite: Drizzle transactions pass through the wrapper untouched", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "i03-sqlite-tx-"));
  try {
    const database = await createDrizzleDatabase({
      profile: { provider: "sqlite", connectionString: path.join(dir, "tx.db") },
      schema: { todos },
    });

    await database.db.run(sql`CREATE TABLE i03_todos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      label TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    )`);

    // Commit path: both rows land.
    await database.db.transaction(async (tx) => {
      await tx.insert(todos).values({ label: "tx-a" });
      await tx.insert(todos).values({ label: "tx-b" });
    });
    let rows = await database.db.select().from(todos);
    assert.equal(rows.length, 2);

    // Rollback path: an inner failure rolls the whole Drizzle transaction
    // back — the wrapper neither intercepts nor emulates it.
    await assert.rejects(
      () =>
        database.db.transaction(async (tx) => {
          await tx.insert(todos).values({ label: "tx-c" });
          throw new Error("boom-mid-tx");
        }),
      /boom-mid-tx/,
    );
    rows = await database.db.select().from(todos);
    assert.equal(rows.length, 2);

    await database.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
