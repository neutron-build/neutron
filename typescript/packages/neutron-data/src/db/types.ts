import type { DataConfigInput } from "../config.js";
import type { DatabaseProfile } from "./index.js";

// Loose wrapper types, kept free of drizzle-orm imports on purpose: the root
// `@neutron-build/data` entry re-exports these, and consumers that never use
// the Drizzle integration must be able to type-check against the package
// without drizzle-orm installed. The genuinely typed surface (real
// drizzle-orm result types via Postgres/SQLite overloads) lives in
// ./drizzle.ts behind the `@neutron-build/data/drizzle` subpath export.

/** A caller-configured, already connected companion. No plugins are added. */
export interface NucleusCompanion {
  client: { close: () => Promise<void> };
  /** Borrowed clients are never closed; owned clients transfer cleanup to this factory. */
  ownership: "borrowed" | "owned";
}

export interface DrizzleDatabaseOptions<TSchema extends Record<string, unknown> = Record<string, unknown>> {
  profile?: DatabaseProfile;
  config?: DataConfigInput;
  schema?: TSchema;
  /** Nucleus profiles only. null opts out of automatic plugin-free allocation. */
  nucleusCompanion?: NucleusCompanion | null;
  requiredCapabilities?: Partial<import("@neutron-build/sql/lifecycle").ConnectionCapabilities>;
}

export interface DrizzleDatabase {
  readonly capabilities: Readonly<import('@neutron-build/sql/lifecycle').ConnectionCapabilities>;
  readonly lifecycle: ReturnType<typeof import('@neutron-build/sql/lifecycle').resourceLifecycle>;
  profile: DatabaseProfile;
  client: unknown;
  db: unknown;
  /** Configured companion, or the legacy plugin-free client. Model properties
   * exist only if the caller configured plugins. null for Postgres/SQLite. */
  nucleus: unknown | null;
  close: () => Promise<void>;
}
