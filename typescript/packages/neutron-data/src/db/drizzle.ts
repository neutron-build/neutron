import * as path from "node:path";
import type { DataConfigInput } from "../config.js";
import { resolveDatabaseProfile, type DatabaseProfile } from "./index.js";
import { lazyImport } from "../internal/lazy-import.js";

export interface DrizzleDatabaseOptions {
  profile?: DatabaseProfile;
  config?: DataConfigInput;
  schema?: Record<string, unknown>;
}

export interface DrizzleDatabase {
  profile: DatabaseProfile;
  client: unknown;
  db: unknown;
  /**
   * When connected to Nucleus, this holds the `@neutron/nucleus` client
   * builder return value after `.connect()`. You can use it to access
   * non-relational models (KV, Vector, Graph, etc.).
   *
   * `null` when connected to plain Postgres or SQLite.
   */
  nucleus: unknown | null;
  close: () => Promise<void>;
}

export async function createDrizzleDatabase(
  options: DrizzleDatabaseOptions = {}
): Promise<DrizzleDatabase> {
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
  const postgresModule = await lazyImport<{ default?: (...args: unknown[]) => any }>(
    "postgres",
    "Install with `pnpm add postgres drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<{ drizzle?: (...args: unknown[]) => unknown }>(
    "drizzle-orm/postgres-js",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  if (!postgresModule.default || !drizzleModule.drizzle) {
    throw new Error("Failed to initialize Nucleus Drizzle client.");
  }

  const sqlClient = postgresModule.default(profile.connectionString, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
  });

  const db = schema
    ? drizzleModule.drizzle(sqlClient, { schema })
    : drizzleModule.drizzle(sqlClient);

  // Optionally create the Nucleus multi-model client.
  // This uses `@neutron/nucleus` which may not be installed in every project.
  let nucleus: unknown | null = null;
  try {
    const nucleusModule = await lazyImport<{
      createClient?: (config: { url: string }) => {
        use: (plugin: unknown) => unknown;
        connect: () => Promise<unknown>;
      };
    }>(
      "@neutron/nucleus",
      "@neutron/nucleus is optional for multi-model features"
    );

    if (nucleusModule.createClient) {
      nucleus = await nucleusModule.createClient({
        url: profile.connectionString,
      }).connect();
    }
  } catch {
    // @neutron/nucleus not installed — Drizzle-only mode is fine.
    nucleus = null;
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
      if (typeof sqlClient.end === "function") {
        await sqlClient.end();
      }
    },
  };
}

async function createPostgresDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>
): Promise<DrizzleDatabase> {
  const postgresModule = await lazyImport<{ default?: (...args: unknown[]) => any }>(
    "postgres",
    "Install with `pnpm add postgres drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<{ drizzle?: (...args: unknown[]) => unknown }>(
    "drizzle-orm/postgres-js",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  if (!postgresModule.default || !drizzleModule.drizzle) {
    throw new Error("Failed to initialize Postgres Drizzle client.");
  }

  const sqlClient = postgresModule.default(profile.connectionString, {
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
      if (typeof sqlClient.end === "function") {
        await sqlClient.end();
      }
    },
  };
}

async function createSqliteDrizzle(
  profile: DatabaseProfile,
  schema?: Record<string, unknown>
): Promise<DrizzleDatabase> {
  const libsqlModule = await lazyImport<{ createClient?: (options: { url: string }) => unknown }>(
    "@libsql/client",
    "Install with `pnpm add @libsql/client drizzle-orm` (or npm/yarn equivalent)"
  );
  const drizzleModule = await lazyImport<{ drizzle?: (...args: unknown[]) => unknown }>(
    "drizzle-orm/libsql",
    "Install with `pnpm add drizzle-orm` (or npm/yarn equivalent)"
  );

  if (!libsqlModule.createClient || !drizzleModule.drizzle) {
    throw new Error("Failed to initialize SQLite Drizzle client.");
  }

  const url = normalizeSqliteConnection(profile.connectionString);
  const client = libsqlModule.createClient({ url });
  const db = schema ? drizzleModule.drizzle(client, { schema }) : drizzleModule.drizzle(client);

  return {
    profile,
    client,
    db,
    nucleus: null,
    close: async () => {
      const maybeClose = (client as { close?: () => Promise<void> | void }).close;
      if (typeof maybeClose === "function") {
        await maybeClose.call(client);
      }
    },
  };
}

function normalizeSqliteConnection(connectionString: string): string {
  if (
    connectionString.startsWith("file:") ||
    connectionString.startsWith("libsql:") ||
    connectionString.startsWith("http://") ||
    connectionString.startsWith("https://")
  ) {
    return connectionString;
  }

  const absolute = path.resolve(connectionString);
  return `file:${absolute}`;
}

