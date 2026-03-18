import { resolveDataConfig, type DataConfigInput, type DatabaseProvider } from "../config.js";

export interface DatabaseProfile {
  provider: DatabaseProvider;
  connectionString: string;
}

export function resolveDatabaseProfile(input: DataConfigInput = {}): DatabaseProfile {
  const resolved = resolveDataConfig(input);

  if (resolved.database === "sqlite") {
    return {
      provider: "sqlite",
      connectionString: resolved.sqlitePath,
    };
  }

  if (resolved.database === "nucleus") {
    // Nucleus uses its own URL env var. Falls back to the Postgres URL if
    // the Nucleus-specific var is not set (Nucleus speaks pgwire).
    const connectionString =
      process.env[resolved.nucleusUrlEnv] ?? process.env[resolved.postgresUrlEnv];
    if (!connectionString) {
      throw new Error(
        `Nucleus profile selected but neither ${resolved.nucleusUrlEnv} nor ${resolved.postgresUrlEnv} is set.`
      );
    }
    return {
      provider: "nucleus",
      connectionString,
    };
  }

  // postgres
  const connectionString = process.env[resolved.postgresUrlEnv];
  if (!connectionString) {
    throw new Error(
      `Postgres profile selected but ${resolved.postgresUrlEnv} is not set.`
    );
  }

  return {
    provider: "postgres",
    connectionString,
  };
}

