// @neutron-build/data/drizzle — the typed Drizzle interop entry (I03).
//
// This subpath carries the wrapper's genuine drizzle-orm types, so importing
// it type-checks against drizzle-orm/postgres-js and drizzle-orm/libsql
// (optional peers — installed wherever Drizzle is actually used). The root
// `@neutron-build/data` export re-exports only the loosely typed alias and
// stays free of drizzle-orm type references.

export { createDrizzleDatabase } from "./db/drizzle.js";
export type {
  DrizzleDatabase,
  DrizzleDatabaseOptions,
  PostgresDrizzleDatabase,
  SqliteDrizzleDatabase,
  TypedDrizzleOptions,
  ProfileOf,
} from "./db/drizzle.js";
export { resolveDatabaseProfile, type DatabaseProfile } from "./db/index.js";
export {
  resolveDataConfig,
  type DataConfigInput,
  type ResolvedDataConfig,
  type DatabaseProvider,
} from "./config.js";
