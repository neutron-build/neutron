import type { DataConfigInput, DatabaseProvider } from "../config.js";
import { resolveDatabaseProfile, type DatabaseProfile } from "./index.js";
import type { DrizzleDatabase, DrizzleDatabaseOptions } from "./types.js";
export type { DrizzleDatabase, DrizzleDatabaseOptions, NucleusCompanion } from "./types.js";
import { lazyImport } from "../internal/lazy-import.js";
import { cleanupAfterFailure, ownedClose } from "../internal/resources.js";
import { admitConnection, resourceLifecycle } from "@neutron-build/sql/lifecycle";
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
// Derive the SQLite client from Drizzle's own return type instead of
// importing an optional driver directly into this shared declaration.
type LibSqlClient = ReturnType<typeof import("drizzle-orm/libsql").drizzle>["$client"];
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
const drizzleCapabilities = Object.freeze({ cancellation: 'unsupported', mutationOutcome: 'driver-error', automaticMutationReplay: false } as const);

function withOwnership<T extends object>(value: T, resources: (() => unknown | Promise<unknown>)[]) {
  const lifecycle = resourceLifecycle('owned', ownedClose(resources));
  return { ...value, capabilities: drizzleCapabilities, lifecycle, close: () => lifecycle.terminate() };
}

export interface PostgresDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> {
  readonly capabilities: DrizzleDatabase["capabilities"];
  readonly lifecycle: DrizzleDatabase["lifecycle"];
  profile: ProfileOf<"postgres" | "nucleus">;
  /** The postgres.js `Sql` client driving Drizzle. */
  client: Sql;
  db: PostgresJsDatabase<TSchema> & { $client: Sql };
  /** Configured companion or legacy plugin-free client; null when opted out
   * or when the optional peer is absent. Plugins are caller-owned configuration. */
  nucleus: unknown | null;
  close: () => Promise<void>;
}

/** Result for the SQLite provider: `db` is what `drizzle-orm/libsql`'s
 *  `drizzle()` actually returns — a genuine `LibSQLDatabase` carrying the
 *  caller's schema, plus drizzle's `$client` handle. */
export interface SqliteDrizzleDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> {
  readonly capabilities: DrizzleDatabase["capabilities"];
  readonly lifecycle: DrizzleDatabase["lifecycle"];
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

  admitConnection({ capabilities: drizzleCapabilities }, options.requiredCapabilities ?? {});
  const profile = options.profile || resolveDatabaseProfile(options.config);
  if (options.nucleusCompanion !== undefined && options.nucleusCompanion !== null) {
    if (profile.provider !== "nucleus") throw new Error("nucleusCompanion requires a Nucleus profile");
    if (!["borrowed", "owned"].includes(options.nucleusCompanion.ownership) || typeof options.nucleusCompanion.client?.close !== "function") {
      throw new Error("nucleusCompanion requires a closeable client and explicit borrowed/owned ownership");
    }
  }

  if (profile.provider === "nucleus") {
    return await createNucleusDrizzle(profile, options.schema, options.nucleusCompanion);
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
 * plugin-free companion for legacy callers. Inject a configured companion for model access.
 */
async function createNucleusDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>,
  companion?: DrizzleDatabaseOptions["nucleusCompanion"],
): Promise<DrizzleDatabase> {
  const resources: (() => unknown | Promise<unknown>)[] = [];
  // Ownership transfers at factory entry, before any later startup await.
  if (companion?.ownership === "owned") resources.push(() => companion.client.close());
  try {
    const postgresModule = await lazyImport<{ default: typeof import("postgres") }>("postgres", "Install postgres drizzle-orm");
    const drizzleModule = await lazyImport<typeof import("drizzle-orm/postgres-js")>("drizzle-orm/postgres-js", "Install drizzle-orm");
    const sqlClient: Sql = postgresModule.default(profile.connectionString, { max: 10, idle_timeout: 20, connect_timeout: 10 });
    resources.push(() => sqlClient.end());
    const db = schema ? drizzleModule.drizzle(sqlClient, { schema }) : drizzleModule.drizzle(sqlClient);
    let nucleus: unknown | null = companion?.client ?? null;
    if (companion === undefined) {
      type Connected = { close: () => Promise<void> };
      let factory: ((config: { url: string }) => { connect: () => Promise<Connected> }) | undefined;
      try {
        const module = await lazyImport<{ createClient?: typeof factory }>("@neutron-build/nucleus", "Optional Nucleus companion");
        factory = module.createClient;
      } catch { /* Optional peer absent: Drizzle-only mode. */ }
      if (factory) {
        // The builder owns failed connection startup; once connected we own close.
        const connected = await factory({ url: profile.connectionString }).connect();
        resources.push(() => connected.close());
        nucleus = connected;
      }
    }
    return withOwnership({ profile, client: sqlClient, db, nucleus }, resources);
  } catch (error) {
    return cleanupAfterFailure(error, resources);
  }
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

  const resources = [() => sqlClient.end()];
  try {
    const db = schema ? drizzleModule.drizzle(sqlClient, { schema }) : drizzleModule.drizzle(sqlClient);
    return withOwnership({ profile, client: sqlClient, db, nucleus: null }, resources);
  } catch (error) { return cleanupAfterFailure(error, resources); }
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
  const resources = [() => client.close()];
  try {
    const db = schema ? drizzleModule.drizzle(client, { schema }) : drizzleModule.drizzle(client);
    return withOwnership({ profile, client, db, nucleus: null }, resources);
  } catch (error) { return cleanupAfterFailure(error, resources); }
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
