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

