// ---------------------------------------------------------------------------
// @neutron/nucleus — main entry point
// ---------------------------------------------------------------------------

export { createClient } from './client.js';
export type { NucleusClientConfig, NucleusClientBase, NucleusClientBuilder } from './client.js';

export type {
  Transport,
  TransactionTransport,
  QueryResult,
  IsolationLevel,
  NucleusFeatures,
  NucleusPlugin,
} from './types.js';

export {
  NucleusError,
  NucleusConnectionError,
  NucleusQueryError,
  NucleusNotFoundError,
  NucleusConflictError,
  NucleusTransactionError,
  NucleusFeatureError,
  NucleusAuthError,
} from './errors.js';

export { HttpTransport } from './transport.js';
export { detectFeatures } from './features.js';
export { migrate, migrateDown, migrationStatus } from './migrate.js';
export type { Migration, MigrationRecord } from './migrate.js';
