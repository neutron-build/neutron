import type { DataConfigInput } from "../config.js";
import type { DatabaseProfile } from "./index.js";

// Loose wrapper types, kept free of drizzle-orm imports on purpose: the root
// `@neutron-build/data` entry re-exports these, and consumers that never use
// the Drizzle integration must be able to type-check against the package
// without drizzle-orm installed. The genuinely typed surface (real
// drizzle-orm result types via Postgres/SQLite overloads) lives in
// ./drizzle.ts behind the `@neutron-build/data/drizzle` subpath export.

export interface DrizzleDatabaseOptions<TSchema extends Record<string, unknown> = Record<string, unknown>> {
  profile?: DatabaseProfile;
  config?: DataConfigInput;
  schema?: TSchema;
}

export interface DrizzleDatabase {
  profile: DatabaseProfile;
  client: unknown;
  db: unknown;
  /**
   * When connected to Nucleus, this holds the `@neutron-build/nucleus` client
   * builder return value after `.connect()`. You can use it to access
   * non-relational models (KV, Vector, Graph, etc.).
   *
   * `null` when connected to plain Postgres or SQLite.
   */
  nucleus: unknown | null;
  close: () => Promise<void>;
}
