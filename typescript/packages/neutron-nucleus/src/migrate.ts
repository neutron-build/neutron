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
import { PgTransport } from './transport.js';
import { sqlState } from './retry.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface MigrationNamespace { schema: string; sql(statement: string): string }

async function captureMigrationNamespace(transport: Transport): Promise<MigrationNamespace> {
  if (transport instanceof PgTransport && transport.valueProfile === 'lossless-read-v1') throw new Error('lossless-read-v1 is a SQL read profile; use a separate default migration transport');
  let result;
  try {
    result = await transport.query<{ intended_schema: string | null; schema_oid: string | null; catalog_oid: string | null; name: string; relation_oid: string | null; resolved_schema: string | null; kind: string | null; persistence: string | null; version_oid: string | null; version_kind: string | null; version_namespace: string | null; version_name: string | null }>(`
      SELECT pg_catalog.current_schema() AS intended_schema, ns.oid::text AS schema_oid, pg_catalog.to_regclass('pg_catalog.pg_class')::oid::text AS catalog_oid, names.name,
        c.oid::text AS relation_oid, rn.nspname AS resolved_schema, c.relkind::text AS kind, c.relpersistence::text AS persistence,
        vt.oid::text AS version_oid, vt.typtype::text AS version_kind, vn.nspname AS version_namespace, vt.typname AS version_name
      FROM (VALUES ('_neutron_migrations'), ('_neutron_migration_lock')) AS names(name)
      LEFT JOIN pg_catalog.pg_namespace ns ON ns.nspname = pg_catalog.current_schema()
      LEFT JOIN pg_catalog.pg_class c ON c.oid = pg_catalog.to_regclass(names.name)
      LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = c.relnamespace
      LEFT JOIN pg_catalog.pg_attribute va ON names.name = '_neutron_migrations'
        AND va.attrelid = c.oid AND va.attname = 'version' AND va.attnum > 0 AND NOT va.attisdropped
      LEFT JOIN pg_catalog.pg_type vt ON vt.oid = va.atttypid
      LEFT JOIN pg_catalog.pg_namespace vn ON vn.oid = vt.typnamespace`);
  } catch (error) {
    throw new Error('nucleus: unsupported migration namespace profile: actual persistent catalog identity required', { cause: error });
  }
  let schema = '';
  const seen = new Set<string>();
  for (const row of result.rows) {
    if (typeof row.catalog_oid !== 'string' || !row.catalog_oid) throw new Error('nucleus: unsupported migration namespace profile: catalog lookup identity required');
    if (typeof row.intended_schema !== 'string' || !row.intended_schema || typeof row.schema_oid !== 'string' || !row.schema_oid || (row.intended_schema.startsWith('pg_') || row.intended_schema === 'information_schema')) throw new Error('nucleus: unsupported migration namespace profile: persistent current_schema required');
    if (schema && schema !== row.intended_schema) throw new Error('nucleus: inconsistent migration namespace identity');
    schema = row.intended_schema;
    if (!['_neutron_migrations', '_neutron_migration_lock'].includes(row.name) || seen.has(row.name)) throw new Error('nucleus: unsupported migration namespace profile: incomplete catalog identity');
    seen.add(row.name);
    if (row.relation_oid != null) {
      if (typeof row.relation_oid !== 'string' || !row.relation_oid || typeof row.resolved_schema !== 'string' || typeof row.kind !== 'string' || typeof row.persistence !== 'string') throw new Error('nucleus: unsupported migration namespace profile: incomplete relation identity');
      if (row.resolved_schema !== schema || row.resolved_schema.startsWith('pg_temp_')) throw new Error(`nucleus: migration namespace ambiguity: ${JSON.stringify(row.name)} resolves in ${JSON.stringify(row.resolved_schema)} instead of intended schema ${JSON.stringify(schema)}; remove temporary shadows or configure the intended schema first`);
      if (row.kind !== 'r' || row.persistence !== 'p') throw new Error(`nucleus: migration metadata ${JSON.stringify(schema)}.${JSON.stringify(row.name)} is not an ordinary persistent table`);
      if (row.name === '_neutron_migrations') {
        if (typeof row.version_oid !== 'string' || typeof row.version_kind !== 'string' || typeof row.version_namespace !== 'string' || typeof row.version_name !== 'string') throw new Error('nucleus: unsupported migration history version identity: version column required');
        const integers: Record<string, string> = { '21': 'int2', '23': 'int4', '20': 'int8' };
        const texts: Record<string, string> = { '25': 'text', '1043': 'varchar', '1042': 'bpchar' };
        if (row.version_kind !== 'b' || row.version_namespace !== 'pg_catalog' || integers[row.version_oid] !== row.version_name) {
          if (row.version_kind === 'b' && row.version_namespace === 'pg_catalog' && texts[row.version_oid] === row.version_name) throw new Error('nucleus: _neutron_migrations.version is a text column — this history belongs to the canonical CLI protocol (text IDs); the SDK runner refuses rather than mix formats. Use `neutron migrate` for this text-ID history; moving it to SDK integer IDs requires explicit reconciliation');
          throw new Error(`nucleus: unsupported migration history version identity ${JSON.stringify(row.version_namespace)}.${JSON.stringify(row.version_name)} (OID ${JSON.stringify(row.version_oid)}, kind ${JSON.stringify(row.version_kind)}); actual pg_catalog int2/int4/int8 required`);
        }
      }
    } else if (row.resolved_schema != null || row.kind != null || row.persistence != null) throw new Error('nucleus: unsupported migration namespace profile: inconsistent relation identity');
  }
  if (seen.size !== 2) throw new Error('nucleus: unsupported migration namespace profile: both metadata identities required');
  const prefix = '"' + schema.replaceAll('"', '""') + '".';
  return { schema, sql: (statement) => statement.replace(/_neutron_migration_lock|_neutron_migrations/g, (name) => prefix + '"' + name + '"') };
}

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
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
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
  locked_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
  owner     TEXT
)`;

const MIGRATION_LOCK_ADD_OWNER = `
ALTER TABLE _neutron_migration_lock ADD COLUMN IF NOT EXISTS owner TEXT`;

/** Bootstrap DDL is written IF NOT EXISTS, but two cold runners racing the
 * same statement can both decide to create: Postgres breaks the tie with a
 * catalog unique violation (23505) or duplicate table (42P07), and the
 * loser's statement fails despite IF NOT EXISTS. Retry with
 * backoff — the winner's create commits and the re-run is a no-op. */
async function executeBootstrapDdl(transport: Transport, sql: string): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await transport.execute(sql);
      return;
    } catch (err) {
      const code = sqlState(err);
      const coldCreateRace = code === '42P07' && /^\s*CREATE TABLE IF NOT EXISTS\b/i.test(sql);
      if (attempt >= 5 || (code !== '23505' && !coldCreateRace)) throw err;
      await sleep(25 * (attempt + 1));
    }
  }
}

async function ensureTable(transport: Transport, namespace: MigrationNamespace): Promise<void> {
  await executeBootstrapDdl(transport, namespace.sql(MIGRATIONS_TABLE_SQL));
  await executeBootstrapDdl(transport, namespace.sql(MIGRATIONS_ADD_CHECKSUM));
  await executeBootstrapDdl(transport, namespace.sql(MIGRATIONS_ADD_OWNER));
  await executeBootstrapDdl(transport, namespace.sql(MIGRATIONS_ADD_FORMAT));
}

/** Ordinary runs create a fresh v2 table but never add columns to legacy history. */
async function prepareMigrationHistory(transport: Transport, namespace: MigrationNamespace): Promise<void> {
  await checkHistoryShape(transport, namespace);
  const result = await transport.query<{ column_name: string }>(`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = '_neutron_migrations'`, [namespace.schema]);
  const columns = new Set(result.rows.map((row) => row.column_name));
  if (columns.size > 0) {
    for (const column of ['checksum', 'owner', 'format']) {
      if (!columns.has(column)) throw new Error(`nucleus: legacy migration history lacks ${column}; call adoptMigrations explicitly before continuing`);
    }
  }
  await executeBootstrapDdl(transport, namespace.sql(MIGRATIONS_TABLE_SQL));
}

interface AppliedRow {
  version: number;
  checksum: string | null;
  format: string | null;
}

async function appliedRows(transport: Transport, namespace: MigrationNamespace): Promise<Map<number, AppliedRow>> {
  const result = await transport.query<{ version: number; checksum: string | null; format: string | null }>(
    namespace.sql('SELECT version, checksum, format FROM _neutron_migrations'));
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
  // Copy primitive fields before the first await: callers retain their array
  // and records while a claim wait or statement execution is in progress.
  const sorted = migrations.map(({ version, name, up, down }) => ({ version, name, up, down }))
    .sort((a, b) => a.version - b.version);
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

/** Refuse a history before history mutation (claim metadata may already exist): a TEXT
 * version column means the canonical CLI protocol owns the database. */
async function checkHistoryShape(transport: Transport, namespace: MigrationNamespace): Promise<void> {
  const result = await transport.query<{ data_type: string }>(`
    SELECT data_type FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = '_neutron_migrations' AND column_name = 'version'`, [namespace.schema]);
  if (result.rows.length === 0) return; // table absent: fresh database
  const t = String(result.rows[0].data_type).toLowerCase();
  if (t === 'integer' || t === 'smallint' || t === 'bigint') return;
  if (t === 'text' || t === 'character varying' || t === 'character' || t === 'varchar') {
    throw new Error(
      'nucleus: _neutron_migrations.version is a text column — this history belongs to the ' +
        'canonical CLI protocol (text IDs); the SDK runner refuses rather than mix formats. ' +
        'Use `neutron migrate` for this text-ID history; moving it to SDK integer IDs requires explicit reconciliation');
  }
  throw new Error(`nucleus: _neutron_migrations.version has unsupported type "${t}"`);
}

/** Refuse rows this runner cannot trust, before business-schema mutation: every row
 * must be protocol v2. NULL-format rows are legacy (TS history, or pre-M04
 * Go history) and graduate only through adoptMigrations. v2 rows with
 * checksums are enforced; adopted-unverified rows (NULL checksum) are
 * exempt — never silently baselined. */
function verifyHistory(plan: Migration[], applied: Map<number, AppliedRow>): void {
  for (const [version, rec] of [...applied].sort(([a], [b]) => a - b)) {
    if (rec.format !== MIGRATION_HISTORY_FORMAT) {
      throw new Error(`nucleus: migration ${version} is recorded without the supported v2 history format; reconcile and call adoptMigrations explicitly before continuing (unprovable rows stay unverified)`);
    }
  }
  for (const m of plan) {
    const rec = applied.get(m.version);
    if (!rec) continue;
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
  namespace: MigrationNamespace,
  options?: MigrateOptions,
): Promise<string> {
  await executeBootstrapDdl(transport, namespace.sql(MIGRATION_LOCK_TABLE_SQL));
  await executeBootstrapDdl(transport, namespace.sql(MIGRATION_LOCK_ADD_OWNER));

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
      namespace.sql('INSERT INTO _neutron_migration_lock (id, token, owner) VALUES (1, $1, $2) ON CONFLICT (id) DO NOTHING'),
      [token, owner],
    );
    if (inserted === 1) return token;
    await sleep(delay, options?.signal);
    delay = Math.min(delay * 2, maxDelay);
  }
}

async function releaseMigrationLock(transport: Transport, token: string, namespace: MigrationNamespace): Promise<void> {
  await transport.execute(namespace.sql('DELETE FROM _neutron_migration_lock WHERE id = 1 AND token = $1'), [token]);
}

/** Read the current migration claim for diagnostics: who holds it and how
 * fresh the heartbeat is. Informational only — nothing here acts on
 * staleness. */
export async function migrationLockInfo(transport: Transport): Promise<MigrationLockInfo> {
  const namespace = await captureMigrationNamespace(transport);
  const result = await transport.query<{ owner: string | null; heartbeat: string | null }>(
    namespace.sql('SELECT owner, locked_at::text AS heartbeat FROM _neutron_migration_lock WHERE id = 1'));
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
  const namespace = await captureMigrationNamespace(transport);
  await transport.execute(namespace.sql('DELETE FROM _neutron_migration_lock WHERE id = 1'));
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Run all pending migrations in ascending version order.
 *
 * Each migration runs inside its own transaction, including checksum,
 * owner and format history updates. PostgreSQL rolls back migration DDL;
 * Nucleus catalog DDL can remain after failure and requires reconciliation
 * before retry. Serialized across runners by the ledger claim; a legacy
 * history is refused until adoptMigrations graduates
 * it. Returns the names of the migrations that were applied.
 */
export async function migrate(
  transport: Transport,
  migrations: Migration[],
  options?: MigrateOptions,
): Promise<string[]> {
  const plan = prepareMigrations(migrations);
  const namespace = await captureMigrationNamespace(transport);
  const token = await acquireMigrationLock(transport, namespace, options);
  const owner = options?.owner ?? defaultOwner();

  try {
    await prepareMigrationHistory(transport, namespace);
    const applied = await appliedRows(transport, namespace);
    verifyHistory(plan, applied);

    const ran: string[] = [];
    for (const m of plan) {
      if (applied.has(m.version)) continue;

      const tx = await transport.beginTransaction();
      try {
        await tx.execute(m.up);
        await tx.execute(
          namespace.sql('INSERT INTO _neutron_migrations (version, name, checksum, owner, format) VALUES ($1, $2, $3, $4, $5)'),
          [m.version, m.name, migrationChecksum(m.up), owner, MIGRATION_HISTORY_FORMAT],
        );
        await tx.commit();
        ran.push(m.name);
        // Heartbeat refresh: diagnostic only, never a lease.
        await transport
          .execute(namespace.sql('UPDATE _neutron_migration_lock SET locked_at = pg_catalog.now() WHERE id = 1 AND token = $1'), [token])
          .catch(() => {});
      } catch (err) {
        await tx.rollback().catch(() => {});
        throw err;
      }
    }

    return ran;
  } finally {
    await releaseMigrationLock(transport, token, namespace).catch(() => {});
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
  if (!Number.isSafeInteger(steps) || steps < 0) throw new RangeError('steps must be a finite non-negative integer');
  if (steps === 0) return [];
  const plan = prepareMigrations(migrations);
  const namespace = await captureMigrationNamespace(transport);
  const token = await acquireMigrationLock(transport, namespace, options);

  try {
    await prepareMigrationHistory(transport, namespace);
    const applied = await appliedRows(transport, namespace);
    verifyHistory(plan, applied);

    const byVersion = new Map(plan.map((m) => [m.version, m]));
    const frontier = [...applied.keys()].sort((a, b) => b - a).slice(0, steps).map((version) => {
      const m = byVersion.get(version);
      if (!m) throw new Error(`Cannot roll back migration ${version}: missing local migration`);
      if (!m.down?.trim()) throw new Error(`Migration ${m.version} (${m.name}) has no down SQL`);
      const recorded = applied.get(version)!;
      if (recorded.checksum !== migrationChecksum(m.up)) {
        throw new Error(`Cannot roll back migration ${version}: matching verified up checksum required`);
      }
      return m;
    });
    // Preflight the whole persisted frontier before the first business DDL.
    const rolled: string[] = [];
    for (const m of frontier) {
      const tx = await transport.beginTransaction();
      try {
        await tx.execute(m.down!);
        await tx.execute(namespace.sql('DELETE FROM _neutron_migrations WHERE version = $1'), [m.version]);
        await tx.commit();
        rolled.push(m.name);
      } catch (err) {
        await tx.rollback().catch(() => {});
        throw err;
      }
    }

    return rolled;
  } finally {
    await releaseMigrationLock(transport, token, namespace).catch(() => {});
  }
}

/**
 * Explicitly graduate a legacy history into protocol v2, in one
 * transaction (contracts/data/MIGRATIONS.md §6). PostgreSQL also rolls back
 * metadata-column DDL; Nucleus may retain nullable columns after a later
 * failure. Digest mismatches are refused before that DDL. Never fabricates trust:
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
  const namespace = await captureMigrationNamespace(transport);
  const token = await acquireMigrationLock(transport, namespace, options);
  const owner = options?.owner ?? defaultOwner();

  try {
    await checkHistoryShape(transport, namespace);

    const exists = await transport.fetchval<number>(
      'SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = \'_neutron_migrations\')', [namespace.schema]);
    if (!exists) throw new Error('nucleus: nothing to adopt: no migration history exists');

    const byVersion = new Map(plan.map((m) => [m.version, m]));
    const report: MigrationAdoptionReport = { verified: [], unverified: [] };
    const isNucleus = String(await transport.fetchval('SELECT pg_catalog.version()')).includes('Nucleus');
    let tx = await transport.beginTransaction();
    try {
      const hasChecksum = await tx.fetchval<boolean>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = '_neutron_migrations' AND column_name = 'checksum')", [namespace.schema]);
      const history = await tx.query<{ version: number; name: string; checksum: string | null }>(
        hasChecksum ? namespace.sql('SELECT version, name, checksum FROM _neutron_migrations') :
          namespace.sql('SELECT version, name, NULL AS checksum FROM _neutron_migrations'));
      // Validate every digest before nullable-column DDL: catalog DDL is not
      // rolled back by Nucleus, whereas PostgreSQL rolls it back with this tx.
      for (const row of history.rows) {
        const version = Number(row.version);
        const m = byVersion.get(version);
        if (m && row.checksum != null && row.checksum !== migrationChecksum(m.up) &&
            row.checksum !== legacyGoSdkChecksum(version, row.name, m.up)) {
          throw new Error(`nucleus: adoption refused: migration ${version} (${row.name}) has a recorded checksum that matches neither the supplied plan nor the legacy Go SDK digest — restore the applied SQL or reconcile manually`);
        }
      }
      if (isNucleus) {
        // Finish the old tuple-layout snapshot before nontransactional DDL.
        // The same ledger claim covers preflight, upgrade, and graduation.
        await tx.rollback();
        await ensureTable(transport, namespace);
        tx = await transport.beginTransaction();
      } else {
        // Do not retry a DDL error inside an aborted PostgreSQL transaction.
        for (const ddl of [MIGRATIONS_ADD_CHECKSUM, MIGRATIONS_ADD_OWNER, MIGRATIONS_ADD_FORMAT]) {
          await tx.execute(namespace.sql(ddl));
        }
      }
      for (const row of history.rows) {
        const version = Number(row.version);
        const m = byVersion.get(version);
        const newDigest = m ? migrationChecksum(m.up) : '';
        const legacyDigest = m ? legacyGoSdkChecksum(version, row.name, m.up) : '';

        if (m && row.checksum != null && row.checksum === legacyDigest) {
          await tx.execute(
            namespace.sql('UPDATE _neutron_migrations SET checksum = $1, owner = $2, format = $3 WHERE version = $4'),
            [newDigest, owner, MIGRATION_HISTORY_FORMAT, version]);
          report.verified.push(version);
        } else if (m && row.checksum != null && row.checksum === newDigest) {
          await tx.execute(
            namespace.sql('UPDATE _neutron_migrations SET owner = $1, format = $2 WHERE version = $3'),
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
            namespace.sql('UPDATE _neutron_migrations SET checksum = NULL, owner = $1, format = $2 WHERE version = $3'),
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
    await releaseMigrationLock(transport, token, namespace).catch(() => {});
  }
}

/**
 * Return all previously applied migrations, ordered by version ascending.
 */
export async function migrationStatus(transport: Transport): Promise<MigrationRecord[]> {
  const namespace = await captureMigrationNamespace(transport);
  await prepareMigrationHistory(transport, namespace);
  verifyHistory([], await appliedRows(transport, namespace));
  const result = await transport.query<{ version: number; name: string; applied_at: string }>(
    namespace.sql('SELECT version, name, applied_at FROM _neutron_migrations ORDER BY version'),
  );
  return result.rows.map((r) => ({
    version: r.version,
    name: r.name,
    appliedAt: new Date(r.applied_at),
  }));
}
