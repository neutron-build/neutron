import type { DataConfigInput, DatabaseProvider } from "../config.js";
import { resolveDatabaseProfile, type DatabaseProfile } from "./index.js";
import type { DrizzleDatabase, DrizzleDatabaseOptions } from "./types.js";
export type { DrizzleDatabase, DrizzleDatabaseOptions } from "./types.js";
import { lazyImport } from "../internal/lazy-import.js";
import { assertNodeRuntime } from "../internal/node-runtime.js";

// Typed Drizzle interop (I03): this module returns REAL drizzle-orm objects —
// `db` is a genuine `PostgresJsDatabase` (postgres.js leg, also used for
// Nucleus over the pg wire protocol) or `LibSQLDatabase` (SQLite leg), with
// the caller's schema threaded through so `db.select()`/`db.query` carry
// Drizzle's own result types. Nothing is re-typed or cast to a Neutron type.
// The type imports below are erased at compile time; the runtime dependency
// stays lazy (dynamic import in the provider branches), so importing this
// module never loads a driver.
//
// This surface is exported through the `@neutron-build/data/drizzle`
// subpath. The root export keeps a loosely typed alias (db: unknown) so
// consumers without drizzle-orm installed never need its types.

import type { Sql } from "postgres";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { Client as LibSqlClient } from "@libsql/client";
import type { LibSQLDatabase } from "drizzle-orm/libsql";

/** A `DatabaseProfile` narrowed to one provider — the overload key that
 *  selects the genuine Drizzle result type. */
export type ProfileOf<P extends DatabaseProvider> = Omit<DatabaseProfile, "provider"> & {
  provider: P;
};

/** Options for the typed overloads: an explicit profile (no env/config
 *  auto-detection, which cannot be resolved statically). */
export interface TypedDrizzleOptions<
  P extends DatabaseProvider,
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends Omit<DrizzleDatabaseOptions<TSchema>, "profile"> {
  profile: ProfileOf<P>;
}

/** Result for the Postgres and Nucleus providers: `db` is what
 *  `drizzle-orm/postgres-js`'s `drizzle()` actually returns — a genuine
 *  `PostgresJsDatabase` carrying the caller's schema (relational
 *  `db.query.<table>` typing included) plus drizzle's `$client` handle. */
export interface PostgresDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> {
  profile: ProfileOf<"postgres" | "nucleus">;
  /** The postgres.js `Sql` client driving Drizzle. */
  client: Sql;
  db: PostgresJsDatabase<TSchema> & { $client: Sql };
  /** `@neutron-build/nucleus` client after `.connect()` when the provider is
   *  `nucleus` and the package is installed; `null` otherwise. */
  nucleus: unknown | null;
  close: () => Promise<void>;
}

/** Result for the SQLite provider: `db` is what `drizzle-orm/libsql`'s
 *  `drizzle()` actually returns — a genuine `LibSQLDatabase` carrying the
 *  caller's schema, plus drizzle's `$client` handle. */
export interface SqliteDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> {
  profile: ProfileOf<"sqlite">;
  /** The `@libsql/client` `Client` driving Drizzle. */
  client: LibSqlClient;
  db: LibSQLDatabase<TSchema> & { $client: LibSqlClient };
  nucleus: null;
  close: () => Promise<void>;
}

export async function createDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(options: TypedDrizzleOptions<"postgres" | "nucleus", TSchema>): Promise<PostgresDrizzleDatabase<TSchema>>;
export async function createDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(options: TypedDrizzleOptions<"sqlite", TSchema>): Promise<SqliteDrizzleDatabase<TSchema>>;
/**
 * Provider resolved at runtime (env auto-detection or `config`) — the Drizzle
 * flavor cannot be known statically, so `db` is `unknown`. For genuine typed
 * results pass an explicit `profile` (matched by the overloads above) or
 * import through `@neutron-build/data/drizzle` and narrow with
 * `result.profile.provider`.
 */
export async function createDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, unknown>,
>(options?: DrizzleDatabaseOptions<TSchema>): Promise<DrizzleDatabase>;
export async function createDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, unknown>,
>(options: DrizzleDatabaseOptions<TSchema> = {}): Promise<DrizzleDatabase> {
  assertNodeRuntime("createDrizzleDatabase");

  const profile = options.profile || resolveDatabaseProfile(options.config);

  if (profile.provider === "nucleus") {
    return await createNucleusDrizzle(profile, options.schema);
  }

  if (profile.provider === "postgres") {
    return await createPostgresDrizzle(profile, options.schema);
  }

  return await createSqliteDrizzle(profile, options.schema);
}

/**
 * Create a Drizzle database backed by Nucleus.
 *
 * Since Nucleus speaks the PostgreSQL wire protocol, we reuse the same
 * `postgres` driver for Drizzle ORM. In addition, we create a Nucleus
 * client to provide access to non-relational data models.
 */
async function createNucleusDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>
): Promise<DrizzleDatabase> {
  // Use the same postgres driver — Nucleus speaks pgwire
  const postgresModule = await lazyImport<{ default: typeof import("postgres") }>(
    "postgres",
    "Install with `pnpm add postgres drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<typeof import("drizzle-orm/postgres-js")>(
    "drizzle-orm/postgres-js",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  const sqlClient: Sql = postgresModule.default(profile.connectionString, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
  });

  const db = schema
    ? drizzleModule.drizzle(sqlClient, { schema })
    : drizzleModule.drizzle(sqlClient);

  // Optionally create the Nucleus multi-model client.
  // This uses `@neutron-build/nucleus` which may not be installed in every project.
  let nucleus: unknown | null = null;
  type NucleusFactory = (config: { url: string }) => {
    use: (plugin: unknown) => unknown;
    connect: () => Promise<unknown>;
  };
  let createNucleusClient: NucleusFactory | undefined;
  try {
    const nucleusModule = await lazyImport<{ createClient?: NucleusFactory }>(
      "@neutron-build/nucleus",
      "@neutron-build/nucleus is optional for multi-model features"
    );
    createNucleusClient = nucleusModule.createClient;
  } catch {
    // Only the import belongs in the try: a missing module legitimately means
    // Drizzle-only mode. A failed connect() (server down, auth rejected) must
    // surface — swallowing it here silently degraded Nucleus profiles to
    // `nucleus: null` with no error and no log.
    nucleus = null;
  }
  if (createNucleusClient) {
    nucleus = await createNucleusClient({
      url: profile.connectionString,
    }).connect();
  }

  return {
    profile,
    client: sqlClient,
    db,
    nucleus,
    close: async () => {
      if (nucleus && typeof (nucleus as { close?: () => Promise<void> }).close === "function") {
        await (nucleus as { close: () => Promise<void> }).close();
      }
      await sqlClient.end();
    },
  };
}

async function createPostgresDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>
): Promise<DrizzleDatabase> {
  const postgresModule = await lazyImport<{ default: typeof import("postgres") }>(
    "postgres",
    "Install with `pnpm add postgres drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<typeof import("drizzle-orm/postgres-js")>(
    "drizzle-orm/postgres-js",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  const sqlClient: Sql = postgresModule.default(profile.connectionString, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
  });

  const db = schema
    ? drizzleModule.drizzle(sqlClient, { schema })
    : drizzleModule.drizzle(sqlClient);

  return {
    profile,
    client: sqlClient,
    db,
    nucleus: null,
    close: async () => {
      await sqlClient.end();
    },
  };
}

async function createSqliteDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>
): Promise<DrizzleDatabase> {
  const libsqlModule = await lazyImport<typeof import("@libsql/client")>(
    "@libsql/client",
    "Install with `pnpm add @libsql/client drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<typeof import("drizzle-orm/libsql")>(
    "drizzle-orm/libsql",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  // node:path is imported lazily so evaluating this module (and the
  // `@neutron-build/data/drizzle` entry) never requires a Node builtin —
  // non-Node runtimes fail at createDrizzleDatabase's runtime guard with a
  // precise error instead of a module-resolution crash.
  const pathModule = await import("node:path");
  const url = normalizeSqliteConnection(profile.connectionString, pathModule.resolve);
  const client: LibSqlClient = libsqlModule.createClient({ url });
  const db = schema ? drizzleModule.drizzle(client, { schema }) : drizzleModule.drizzle(client);

  return {
    profile,
    client,
    db,
    nucleus: null,
    close: async () => {
      await client.close();
    },
  };
}

function normalizeSqliteConnection(connectionString: string, resolve: (p: string) => string): string {
  if (
    connectionString.startsWith("file:") ||
    connectionString.startsWith("libsql:") ||
    connectionString.startsWith("http://") ||
    connectionString.startsWith("https://")
  ) {
    return connectionString;
  }

  return `file:${resolve(connectionString)}`;
}
