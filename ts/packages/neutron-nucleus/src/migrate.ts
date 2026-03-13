// ---------------------------------------------------------------------------
// Nucleus client — migration system
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const MIGRATIONS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS _neutron_migrations (
  version     INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`;

async function ensureTable(transport: Transport): Promise<void> {
  await transport.execute(MIGRATIONS_TABLE_SQL);
}

async function appliedVersions(transport: Transport): Promise<Set<number>> {
  await ensureTable(transport);
  const result = await transport.query<{ version: number }>('SELECT version FROM _neutron_migrations');
  return new Set(result.rows.map((r) => r.version));
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Run all pending migrations in ascending version order.
 *
 * Each migration runs inside its own transaction. Returns the names of the
 * migrations that were applied.
 */
export async function migrate(transport: Transport, migrations: Migration[]): Promise<string[]> {
  const applied = await appliedVersions(transport);
  const sorted = [...migrations].sort((a, b) => a.version - b.version);
  const ran: string[] = [];

  for (const m of sorted) {
    if (applied.has(m.version)) continue;

    const tx = await transport.beginTransaction();
    try {
      await tx.execute(m.up);
      await tx.execute(
        'INSERT INTO _neutron_migrations (version, name) VALUES ($1, $2)',
        [m.version, m.name],
      );
      await tx.commit();
      ran.push(m.name);
    } catch (err) {
      await tx.rollback();
      throw err;
    }
  }

  return ran;
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
): Promise<string[]> {
  const applied = await appliedVersions(transport);
  const sorted = [...migrations].sort((a, b) => b.version - a.version); // descending
  const rolled: string[] = [];

  for (const m of sorted) {
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
      await tx.rollback();
      throw err;
    }
  }

  return rolled;
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
