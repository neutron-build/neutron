// ---------------------------------------------------------------------------
// @neutron-build/nucleus — live-engine migration protocol tests
//
// These run only against a real Nucleus server (NEUTRON_TEST_DATABASE_URL);
// without it they skip. They mirror go/nucleus/migrate_integration_test.go:
// protocol-v2 history lifecycle, legacy refusal + adoption, cross-runner
// serialization with NO automatic takeover, and mixed-runner refusal.
// ---------------------------------------------------------------------------

import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";

import { PgTransport } from "./transport.js";
import {
  migrate,
  migrateDown,
  adoptMigrations,
  forceUnlockMigrations,
  migrationLockInfo,
  migrationChecksum,
  legacyGoSdkChecksum,
} from "./migrate.js";
import type { Transport } from "./types.js";
import type { Migration } from "./migrate.js";

const url = process.env.NEUTRON_TEST_DATABASE_URL ?? "";

function live(): boolean {
  return url !== "";
}

async function reset(t: Transport): Promise<void> {
  for (const ddl of [
    "DROP TABLE IF EXISTS _neutron_migration_lock",
    "DROP TABLE IF EXISTS _neutron_migrations",
    "DROP TABLE IF EXISTS ts_a",
    "DROP TABLE IF EXISTS ts_b",
    "DROP TABLE IF EXISTS ts_c",
  ]) {
    await t.execute(ddl);
  }
}

describe("Integration: migration protocol v2 (live engine)", { skip: !live() && "NEUTRON_TEST_DATABASE_URL not set" }, () => {
  let t: Transport;

  before(async () => {
    t = new PgTransport(url);
  });

  after(async () => {
    await t.close().catch(() => {});
  });

  const plan: Migration[] = [
    { version: 1, name: "first", up: "CREATE TABLE ts_a (id INT)", down: "DROP TABLE ts_a" },
    { version: 2, name: "second", up: "CREATE TABLE ts_b (id INT)", down: "DROP TABLE ts_b" },
  ];

  it("applies, records v2 metadata, enforces checksums, rolls back", async () => {
    await reset(t);

    const ran = await migrate(t, plan);
    assert.deepEqual(ran, ["first", "second"]);

    const rows = await t.query<{ version: number; checksum: string | null; owner: string | null; format: string | null }>(
      "SELECT version, checksum, owner, format FROM _neutron_migrations");
    assert.equal(rows.rows.length, 2);
    for (const row of rows.rows) {
      assert.equal(row.checksum, migrationChecksum(plan[Number(row.version) - 1].up));
      assert.equal(row.format, "v2");
      assert.ok(String(row.owner).startsWith("nucleus-ts-sdk@"));
    }

    // No-op re-run still verifies.
    assert.deepEqual(await migrate(t, plan), []);

    // Tampering is refused.
    await assert.rejects(
      () => migrate(t, [{ ...plan[0], up: "CREATE TABLE ts_a (id BIGINT)" }, plan[1]]),
      (err: Error) => err.message.includes("modified since it was applied"),
    );

    // Down/up cycle.
    await migrateDown(t, plan, 1);
    let after = await t.query<{ version: number }>("SELECT version FROM _neutron_migrations");
    assert.equal(after.rows.length, 1);
    await migrate(t, plan);
    after = await t.query<{ version: number }>("SELECT version FROM _neutron_migrations");
    assert.equal(after.rows.length, 2);
  });

  it("refuses legacy history until adoptMigrations graduates it (unverified, never baselined)", async () => {
    await reset(t);
    await t.execute(
      "CREATE TABLE _neutron_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    await t.execute("INSERT INTO _neutron_migrations (version, name) VALUES (1, 'first')");

    // Refused BEFORE mutations: version 2 must not run.
    await assert.rejects(() => migrate(t, plan), (err: Error) => err.message.includes("adopt"));
    await assert.rejects(
      () => t.query("SELECT 1 FROM ts_b"),
      () => true, // table must not exist
    );

    const report = await adoptMigrations(t, plan);
    assert.deepEqual(report.verified, []);
    assert.deepEqual(report.unverified, [1]);
    const row = await t.query<{ checksum: string | null; format: string | null }>(
      "SELECT checksum, format FROM _neutron_migrations WHERE version = 1");
    assert.equal(row.rows[0].checksum, null);
    assert.equal(row.rows[0].format, "v2");

    // Now the run proceeds, applying only version 2.
    assert.deepEqual(await migrate(t, plan), ["second"]);
  });

  it("verifies Go-SDK legacy digests during adoption and enforces them after", async () => {
    await reset(t);
    const up1 = "CREATE TABLE ts_a (id INT)";
    await t.execute(
      "CREATE TABLE _neutron_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT)");
    await t.execute(
      "INSERT INTO _neutron_migrations (version, name, checksum) VALUES (1, 'first', $1)",
      [legacyGoSdkChecksum(1, "first", up1)]);

    const report = await adoptMigrations(t, [
      { version: 1, name: "first", up: up1, down: "DROP TABLE ts_a" },
      plan[1],
    ]);
    assert.deepEqual(report.verified, [1]);

    const row = await t.query<{ checksum: string | null }>(
      "SELECT checksum FROM _neutron_migrations WHERE version = 1");
    assert.equal(row.rows[0].checksum, migrationChecksum(up1));

    // Enforced from now on.
    await assert.rejects(
      () => migrate(t, [{ version: 1, name: "first", up: up1 + " -- tampered" }, plan[1]]),
      (err: Error) => err.message.includes("modified since it was applied"),
    );
  });

  it("serializes two runners (separate pools) with exactly-once effects", async () => {
    await reset(t);
    // Four runners, not two: the bootstrap DDL (CREATE TABLE IF NOT EXISTS)
    // races in Postgres's catalog when the tables were just dropped — the
    // loser historically took a pg_type unique violation (23505) — and the
    // wider the field, the harder that race is pinned.
    const extra = [new PgTransport(url), new PgTransport(url), new PgTransport(url)];

    const results = await Promise.allSettled([
      migrate(t, plan),
      ...extra.map((tx) => migrate(tx, plan)),
    ]);
    for (const tx of extra) {
      await tx.close().catch(() => {});
    }
    for (const r of results) {
      assert.equal(r.status, "fulfilled");
    }
    const rows = await t.query<{ version: number }>("SELECT version FROM _neutron_migrations");
    assert.equal(rows.rows.length, 2);
  });

  it("never steals a claim: stale and dead holders block until explicit force-unlock", async () => {
    await reset(t);

    // A claim left behind by a holder that died without releasing — the
    // durable aftermath of process death (the Go suite kills a real client
    // to produce this same state).
    await t.execute(
      "CREATE TABLE IF NOT EXISTS _neutron_migration_lock (id INTEGER PRIMARY KEY, token BIGINT NOT NULL, locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), owner TEXT)");
    await t.execute(
      "INSERT INTO _neutron_migration_lock (id, token, owner) VALUES (1, 424242, 'nucleus-go-sdk@ghost:1')");

    // Diagnostics see it.
    const info = await migrationLockInfo(t);
    assert.equal(info.held, true);
    assert.equal(info.owner, "nucleus-go-sdk@ghost:1");

    // Even aged far beyond any historical threshold, no runner takes over.
    await t.execute("UPDATE _neutron_migration_lock SET locked_at = NOW() - make_interval(secs => 86400) WHERE id = 1");
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort(), 500);
    await assert.rejects(
      () => migrate(t, plan, { signal: controller.signal }),
      () => true,
    );
    clearTimeout(abortTimer);
    // The claim survived untouched: same token, same ghost.
    const held = await t.query<{ token: string }>("SELECT token FROM _neutron_migration_lock WHERE id = 1");
    assert.equal(String(held.rows[0].token), "424242");

    // Explicit unlock — the only recovery path — lets the runner proceed.
    await forceUnlockMigrations(t);
    assert.deepEqual(await migrate(t, plan), ["first", "second"]);
  });

  it("refuses a CLI-owned text history before any mutation", async () => {
    await reset(t);
    await t.execute(
      "CREATE TABLE _neutron_migrations (version TEXT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT, owner TEXT, format TEXT)");

    await assert.rejects(
      () => migrate(t, plan),
      (err: Error) => err.message.includes("text"),
    );
    await assert.rejects(() => t.query("SELECT 1 FROM ts_a"), () => true);
  });
});
