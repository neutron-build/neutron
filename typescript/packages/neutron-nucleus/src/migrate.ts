// ---------------------------------------------------------------------------
// Nucleus client — migration system
//
// Protocol v2 (contracts/data/MIGRATIONS.md): canonical checksums over the
// up SQL, owner/format metadata on every row, legacy histories refused until
// explicit adoption, and cross-process serialization via the
// _neutron_migration_lock claim with NO automatic time-based takeover — the
// heartbeat is diagnostic only, and recovery from a dead holder is the
// explicit forceUnlockMigrations call.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import { hostname } from "node:os";

import type { Transport } from './types.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A single migration definition. */
export interface Migration {
  /** Monotonically increasing version number. */
  version: number;
  /** Human-readable name. */
  name: string;
  /** SQL to apply the migration. */
  up: string;
  /** SQL to revert the migration (optional). */
  down?: string;
}

/** A migration that has already been applied. */
export interface MigrationRecord {
  version: number;
  name: string;
  appliedAt: Date;
}

/** Protocol marker recorded on history rows this module writes. */
export const MIGRATION_HISTORY_FORMAT = 'v2';

/** Options shared by the migration runners. */
export interface MigrateOptions {
  /** AbortSignal: aborts lock waits and the run. */
  signal?: AbortSignal;
  /** Override the diagnostic owner identity recorded on claims/rows. */
  owner?: string;
}

/** What one explicit adoption decided, per row. */
export interface MigrationAdoptionReport {
  /** Versions whose recorded legacy digest reproduced from the supplied
   * plan: content proven identical, re-recorded under the v2 checksum. */
  verified: number[];
  /** Versions with nothing to verify against (no recorded checksum, or no
   * matching plan entry): checksum stays NULL, reported honestly. */
  unverified: number[];
}

/** Diagnostic view of the migration claim. Informational only. */
export interface MigrationLockInfo {
  held: boolean;
  owner: string | null;
  heartbeat: string | null;
}

// ---------------------------------------------------------------------------
// Canonical digests
// ---------------------------------------------------------------------------

/** Canonical v2 checksum: lowercase-hex SHA-256 over the up SQL exactly as
 * supplied, UTF-8 encoded. Identical inputs produce identical digests in
 * the CLI, the Go SDK and this SDK. */
export function migrationChecksum(up: string): string {
  return createHash('sha256').update(up, 'utf8').digest('hex');
}

/** Pre-M04 Go SDK digest (GO-30 era): SHA-256 over NUL-separated
 * version/name/up. Rows from those clients carry it; adoption re-verifies
 * it against supplied plans. Never written by this SDK. */
export function legacyGoSdkChecksum(version: number, name: string, up: string): string {
  return createHash('sha256').update(`${version}\x00${name}\x00${up}`, 'utf8').digest('hex');
}

function defaultOwner(): string {
  return `nucleus-ts-sdk@${hostname()}:${process.pid}`;
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

const MIGRATIONS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS _neutron_migrations (
  version     INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  checksum    TEXT,
  owner       TEXT,
  format      TEXT
)`;

const MIGRATIONS_ADD_CHECKSUM = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS checksum TEXT`;

const MIGRATIONS_ADD_OWNER = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS owner TEXT`;

const MIGRATIONS_ADD_FORMAT = `
ALTER TABLE _neutron_migrations ADD COLUMN IF NOT EXISTS format TEXT`;

const MIGRATION_LOCK_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS _neutron_migration_lock (
  id        INTEGER PRIMARY KEY,
  token     BIGINT NOT NULL,
  locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  owner     TEXT
)`;

const MIGRATION_LOCK_ADD_OWNER = `
ALTER TABLE _neutron_migration_lock ADD COLUMN IF NOT EXISTS owner TEXT`;

async function ensureTable(transport: Transport): Promise<void> {
  await transport.execute(MIGRATIONS_TABLE_SQL);
  await transport.execute(MIGRATIONS_ADD_CHECKSUM);
  await transport.execute(MIGRATIONS_ADD_OWNER);
  await transport.execute(MIGRATIONS_ADD_FORMAT);
}

interface AppliedRow {
  version: number;
  checksum: string | null;
  format: string | null;
}

async function appliedRows(transport: Transport): Promise<Map<number, AppliedRow>> {
  const result = await transport.query<{ version: number; checksum: string | null; format: string | null }>(
    'SELECT version, checksum, format FROM _neutron_migrations');
  const map = new Map<number, AppliedRow>();
  for (const row of result.rows) {
    map.set(Number(row.version), {
      version: Number(row.version),
      checksum: row.checksum ?? null,
      format: row.format ?? null,
    });
  }
  return map;
}

/** Sort a copy ascending by version and validate the plan before any SQL
 * runs: duplicates and empty names/up are refused up front. */
function prepareMigrations(migrations: Migration[]): Migration[] {
  const sorted = [...migrations].sort((a, b) => a.version - b.version);
  const seen = new Set<number>();
  for (const m of sorted) {
    if (!Number.isInteger(m.version) || m.version <= 0) {
      throw new Error(`nucleus: invalid migration version ${m.version} (must be a positive integer)`);
    }
    if (!m.name?.trim()) throw new Error(`nucleus: migration ${m.version} has an empty name`);
    if (!m.up?.trim()) throw new Error(`nucleus: migration ${m.version} (${m.name}) has empty up SQL`);
    if (seen.has(m.version)) throw new Error(`nucleus: duplicate migration version ${m.version}`);
    seen.add(m.version);
  }
  return sorted;
}

/** Refuse a history this runner must not touch, before any mutation: a TEXT
 * version column means the canonical CLI protocol owns the database. */
async function checkHistoryShape(transport: Transport): Promise<void> {
  const result = await transport.query<{ data_type: string }>(`
    SELECT data_type FROM information_schema.columns
    WHERE table_name = '_neutron_migrations' AND column_name = 'version'`);
  if (result.rows.length === 0) return; // table absent: fresh database
  const t = String(result.rows[0].data_type).toLowerCase();
  if (t === 'integer' || t === 'smallint' || t === 'bigint') return;
  if (t === 'text' || t === 'character varying' || t === 'character' || t === 'varchar') {
    throw new Error(
      'nucleus: _neutron_migrations.version is a text column — this history belongs to the ' +
        'canonical CLI protocol (text IDs); the SDK runner refuses rather than mix formats. ' +
        'Use `neutron migrate`, or re-adopt the history with the CLI to move it back to integer versions');
  }
  throw new Error(`nucleus: _neutron_migrations.version has unsupported type "${t}"`);
}

/** Refuse rows this runner cannot trust, before any mutation: every row
 * must be protocol v2. NULL-format rows are legacy (TS history, or pre-M04
 * Go history) and graduate only through adoptMigrations. v2 rows with
 * checksums are enforced; adopted-unverified rows (NULL checksum) are
 * exempt — never silently baselined. */
function verifyHistory(plan: Migration[], applied: Map<number, AppliedRow>): void {
  for (const m of plan) {
    const rec = applied.get(m.version);
    if (!rec) continue;
    if (rec.format !== MIGRATION_HISTORY_FORMAT) {
      throw new Error(
        `nucleus: migration ${m.version} (${m.name}) is recorded in a legacy history format and must be ` +
          'adopted once before this runner continues — call adoptMigrations (explicit, transactional; ' +
          'unprovable rows stay unverified)');
    }
    if (rec.checksum == null) continue; // adopted-unverified: exempt
    const want = migrationChecksum(m.up);
    if (rec.checksum !== want) {
      throw new Error(
        `nucleus: migration ${m.version} (${m.name}) has been modified since it was applied: ` +
          `recorded checksum sha256:${rec.checksum} does not match the current script (${want}) — ` +
          'restore the applied script or write a new migration');
    }
  }
}

// ---------------------------------------------------------------------------
// Ledger claim (no automatic takeover)
// ---------------------------------------------------------------------------

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason ?? new Error('aborted'));
    const t = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(t);
      reject(signal.reason ?? new Error('aborted'));
    }, { once: true });
  });
}

/**
 * Claim the database's migration runner slot, waiting until any other
 * holder RELEASES. There is no time-based takeover: a claim held by a
 * crashed runner blocks until forceUnlockMigrations is called with
 * evidence the holder cannot execute. The heartbeat written here is
 * diagnostic only.
 */
async function acquireMigrationLock(
  transport: Transport,
  options?: MigrateOptions,
): Promise<string> {
  await transport.execute(MIGRATION_LOCK_TABLE_SQL);
  await transport.execute(MIGRATION_LOCK_ADD_OWNER);

  // The claim table is shared with the Go SDK, whose schema predates this
  // module: token is BIGINT. A 15-hex-digit slice (60 bits, always positive
  // int64) keeps the token numeric on the wire for both SDKs.
  const token = BigInt(
    '0x' + createHash('sha256')
      .update(`${Date.now()}-${Math.random()}-${process.pid}`, 'utf8')
      .digest('hex')
      .slice(0, 15),
  ).toString();
  const owner = options?.owner ?? defaultOwner();

  let delay = 25;
  const maxDelay = 2000;
  for (;;) {
    options?.signal?.throwIfAborted();
    const inserted = await transport.execute(
      'INSERT INTO _neutron_migration_lock (id, token, owner) VALUES (1, $1, $2) ON CONFLICT (id) DO NOTHING',
      [token, owner],
    );
    if (inserted === 1) return token;
    await sleep(delay, options?.signal);
    delay = Math.min(delay * 2, maxDelay);
  }
}

async function releaseMigrationLock(transport: Transport, token: string): Promise<void> {
  await transport.execute('DELETE FROM _neutron_migration_lock WHERE id = 1 AND token = $1', [token]);
}

/** Read the current migration claim for diagnostics: who holds it and how
 * fresh the heartbeat is. Informational only — nothing here acts on
 * staleness. */
export async function migrationLockInfo(transport: Transport): Promise<MigrationLockInfo> {
  const result = await transport.query<{ owner: string | null; heartbeat: string | null }>(
    'SELECT owner, locked_at::text AS heartbeat FROM _neutron_migration_lock WHERE id = 1');
  if (result.rows.length === 0) return { held: false, owner: null, heartbeat: null };
  return {
    held: true,
    owner: result.rows[0].owner ?? null,
    heartbeat: result.rows[0].heartbeat ?? null,
  };
}

/**
 * Delete the migration claim unconditionally — the explicit crash-recovery
 * escape hatch for a holder that died without releasing. Call it ONLY with
 * evidence the previous holder cannot execute (a dead process, a retired
 * deployment); the SDK cannot manufacture that evidence, which is exactly
 * why no automatic takeover exists.
 */
export async function forceUnlockMigrations(transport: Transport): Promise<void> {
  await transport.execute('DELETE FROM _neutron_migration_lock WHERE id = 1');
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Run all pending migrations in ascending version order.
 *
 * Each migration runs inside its own transaction with its DDL, checksum,
 * owner and format committed atomically. Serialized across runners by the
 * ledger claim; a legacy history is refused until adoptMigrations graduates
 * it. Returns the names of the migrations that were applied.
 */
export async function migrate(
  transport: Transport,
  migrations: Migration[],
  options?: MigrateOptions,
): Promise<string[]> {
  const plan = prepareMigrations(migrations);
  const token = await acquireMigrationLock(transport, options);
  const owner = options?.owner ?? defaultOwner();

  try {
    await checkHistoryShape(transport);
    await ensureTable(transport);
    const applied = await appliedRows(transport);
    verifyHistory(plan, applied);

    const ran: string[] = [];
    for (const m of plan) {
      if (applied.has(m.version)) continue;

      const tx = await transport.beginTransaction();
      try {
        await tx.execute(m.up);
        await tx.execute(
          'INSERT INTO _neutron_migrations (version, name, checksum, owner, format) VALUES ($1, $2, $3, $4, $5)',
          [m.version, m.name, migrationChecksum(m.up), owner, MIGRATION_HISTORY_FORMAT],
        );
        await tx.commit();
        ran.push(m.name);
        // Heartbeat refresh: diagnostic only, never a lease.
        await transport
          .execute('UPDATE _neutron_migration_lock SET locked_at = NOW() WHERE id = 1 AND token = $1', [token])
          .catch(() => {});
      } catch (err) {
        await tx.rollback().catch(() => {});
        throw err;
      }
    }

    return ran;
  } finally {
    await releaseMigrationLock(transport, token).catch(() => {});
  }
}

/**
 * Roll back the most recently applied migrations.
 *
 * @param steps Number of migrations to roll back (default 1).
 * @returns Names of the migrations that were rolled back.
 */
export async function migrateDown(
  transport: Transport,
  migrations: Migration[],
  steps = 1,
  options?: MigrateOptions,
): Promise<string[]> {
  const plan = [...prepareMigrations(migrations)].reverse();
  const token = await acquireMigrationLock(transport, options);

  try {
    await checkHistoryShape(transport);
    await ensureTable(transport);
    const applied = await appliedRows(transport);
    verifyHistory(plan, applied);

    const rolled: string[] = [];
    for (const m of plan) {
      if (rolled.length >= steps) break;
      if (!applied.has(m.version)) continue;
      if (!m.down) {
        throw new Error(`Migration ${m.version} (${m.name}) has no down SQL`);
      }

      const tx = await transport.beginTransaction();
      try {
        await tx.execute(m.down);
        await tx.execute('DELETE FROM _neutron_migrations WHERE version = $1', [m.version]);
        await tx.commit();
        rolled.push(m.name);
      } catch (err) {
        await tx.rollback().catch(() => {});
        throw err;
      }
    }

    return rolled;
  } finally {
    await releaseMigrationLock(transport, token).catch(() => {});
  }
}

/**
 * Explicitly graduate a legacy history into protocol v2, in one
 * transaction (contracts/data/MIGRATIONS.md §6). Never fabricates trust:
 * a recorded legacy Go SDK digest that reproduces from the supplied plan
 * adopts the row as verified; everything else adopts as unverified with a
 * NULL checksum; a recorded checksum matching neither digest aborts the
 * adoption. Supersedes the pre-v2 silent baselining.
 */
export async function adoptMigrations(
  transport: Transport,
  migrations: Migration[],
  options?: MigrateOptions,
): Promise<MigrationAdoptionReport> {
  const plan = prepareMigrations(migrations);
  const token = await acquireMigrationLock(transport, options);
  const owner = options?.owner ?? defaultOwner();

  try {
    await checkHistoryShape(transport);

    const exists = await transport.fetchval<number>(
      'SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = \'_neutron_migrations\')');
    if (!exists) throw new Error('nucleus: nothing to adopt: no migration history exists');
    await ensureTable(transport);

    const byVersion = new Map(plan.map((m) => [m.version, m]));
    const history = await transport.query<{ version: number; name: string; checksum: string | null }>(
      'SELECT version, name, checksum FROM _neutron_migrations');

    const report: MigrationAdoptionReport = { verified: [], unverified: [] };
    const tx = await transport.beginTransaction();
    try {
      for (const row of history.rows) {
        const version = Number(row.version);
        const m = byVersion.get(version);
        const newDigest = m ? migrationChecksum(m.up) : '';
        const legacyDigest = m ? legacyGoSdkChecksum(version, row.name, m.up) : '';

        if (m && row.checksum != null && row.checksum === legacyDigest) {
          await tx.execute(
            'UPDATE _neutron_migrations SET checksum = $1, owner = $2, format = $3 WHERE version = $4',
            [newDigest, owner, MIGRATION_HISTORY_FORMAT, version]);
          report.verified.push(version);
        } else if (m && row.checksum != null && row.checksum === newDigest) {
          await tx.execute(
            'UPDATE _neutron_migrations SET owner = $1, format = $2 WHERE version = $3',
            [owner, MIGRATION_HISTORY_FORMAT, version]);
          report.verified.push(version);
        } else if (m && row.checksum != null) {
          throw new Error(
            `nucleus: adoption refused: migration ${version} (${row.name}) has a recorded checksum that ` +
              'matches neither the supplied plan nor the legacy Go SDK digest — restore the applied SQL ' +
              'or reconcile manually');
        } else {
          // No recorded checksum or no matching plan entry: honest NULL,
          // reported — never baselined.
          await tx.execute(
            'UPDATE _neutron_migrations SET checksum = NULL, owner = $1, format = $2 WHERE version = $3',
            [owner, MIGRATION_HISTORY_FORMAT, version]);
          report.unverified.push(version);
        }
      }
      await tx.commit();
    } catch (err) {
      await tx.rollback().catch(() => {});
      throw err;
    }

    return report;
  } finally {
    await releaseMigrationLock(transport, token).catch(() => {});
  }
}

/**
 * Return all previously applied migrations, ordered by version ascending.
 */
export async function migrationStatus(transport: Transport): Promise<MigrationRecord[]> {
  await ensureTable(transport);
  const result = await transport.query<{ version: number; name: string; applied_at: string }>(
    'SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version',
  );
  return result.rows.map((r) => ({
    version: r.version,
    name: r.name,
    appliedAt: new Date(r.applied_at),
  }));
}
