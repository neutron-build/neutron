import assert from "node:assert/strict";
import test from "node:test";
import { pgTable, serial, text, integer } from "drizzle-orm/pg-core";
import { eq, sql } from "drizzle-orm";
import { createDrizzleDatabase } from "./drizzle.js";
import type { Sql } from "postgres";

// I03 live Postgres fixture for the Drizzle wrapper (V14 integration
// subset): genuine Drizzle statements through the wrapper's postgres.js leg,
// Drizzle's own transaction API passing through untouched (commit + rollback
// observable), and a typed round-trip whose values are verified against the
// raw driver (independent of Drizzle's result mapping).
//
// Target: NEUTRON_TEST_PG_URL (package convention; defaults to the container
// used by the queue suite). Every run creates and drops its own uniquely
// named i03_-prefixed database on that server — never a shared database, and
// never the admin database's migration metadata. Skips with a visible reason
// when the server is not reachable.

const PG_URL = process.env.NEUTRON_TEST_PG_URL || "postgres://postgres:test@127.0.0.1:5434/postgres";

const people = pgTable("i03_people", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  score: integer("score").notNull().default(0),
});

let reachableProbe: Promise<boolean> | null = null;
function pgReachable(): Promise<boolean> {
  if (!reachableProbe) {
    reachableProbe = (async () => {
      try {
        const mod = (await import("postgres")) as { default: (url: string, opts?: object) => Sql };
        const sql = mod.default(PG_URL, { connect_timeout: 2, max: 1 });
        await sql.unsafe("select 1");
        await sql.end();
        return true;
      } catch {
        return false;
      }
    })();
  }
  return reachableProbe;
}

function uniqueDbName(): string {
  return `i03_drizzle_${process.pid}_${Math.floor(Date.now() / 1000)}`;
}

test("postgres: genuine Drizzle typed round-trip and transaction pass-through", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start a server and set NEUTRON_TEST_PG_URL to run the live Drizzle wrapper suite`);
  }

  const admin = ((await import("postgres")) as { default: (url: string, opts?: object) => Sql }).default(PG_URL, {
    max: 1,
    connect_timeout: 2,
  });
  const dbName = uniqueDbName();
  await admin.unsafe(`CREATE DATABASE "${dbName}"`);

  try {
    const database = await createDrizzleDatabase({
      profile: {
        provider: "postgres",
        connectionString: adminUrlForDb(dbName),
      },
      schema: { people },
    });
    assert.equal(database.profile.provider, "postgres");
    assert.equal(database.nucleus, null);

    await database.db.execute(sql`CREATE TABLE i03_people (
      id serial PRIMARY KEY,
      email text NOT NULL UNIQUE,
      score integer NOT NULL DEFAULT 0
    )`);

    await database.db.insert(people).values([
      { email: "a@i03.test", score: 1 },
      { email: "b@i03.test", score: 2 },
    ]);

    const rows = await database.db.select().from(people).orderBy(people.id);
    assert.deepEqual(
      rows.map((r) => ({ email: r.email, score: r.score })),
      [
        { email: "a@i03.test", score: 1 },
        { email: "b@i03.test", score: 2 },
      ],
    );

    const one = await database.db.select().from(people).where(eq(people.email, "b@i03.test"));
    assert.equal(one.length, 1);
    assert.equal(one[0]!.score, 2);

    // Drizzle transaction, commit path — the wrapper adds no layer between
    // Drizzle and postgres.js.
    await database.db.transaction(async (tx) => {
      await tx.update(people).set({ score: 5 }).where(eq(people.email, "a@i03.test"));
      await tx.insert(people).values({ email: "c@i03.test", score: 3 });
    });

    // Rollback path — the mid-transaction failure discards the write.
    await assert.rejects(
      () =>
        database.db.transaction(async (tx) => {
          await tx.insert(people).values({ email: "d@i03.test" });
          throw new Error("boom-mid-pg-tx");
        }),
      /boom-mid-pg-tx/,
    );

    // Independent oracle: raw driver read, no Drizzle in the path. (The
    // spread also strips postgres.js's Result array subclass so the strict
    // deep-equal compares plain rows.)
    const raw = Array.from(
      await database.client.unsafe<{ email: string; score: number }[]>(
        "SELECT email, score FROM i03_people ORDER BY id",
      ),
    );
    assert.deepEqual(raw, [
      { email: "a@i03.test", score: 5 },
      { email: "b@i03.test", score: 2 },
      { email: "c@i03.test", score: 3 },
    ]);

    await database.close();
  } finally {
    // The wrapper's pooled sessions can outlive sql.end() by a moment;
    // terminate stragglers and force-drop so the disposable database never
    // survives the run. Admin teardown always runs, even if the drop fails.
    try {
      await admin.unsafe(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
      );
      await admin.unsafe(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
    } catch {
      // last resort without FORCE (PG < 13)
      try {
        await admin.unsafe(`DROP DATABASE IF EXISTS "${dbName}"`);
      } catch {
        // leave cleanup to the operator; the name is i03_-prefixed and disposable
      }
    } finally {
      await admin.end();
    }
  }
});

function adminUrlForDb(dbName: string): string {
  const url = new URL(PG_URL);
  url.pathname = `/${dbName}`;
  return url.toString();
}
