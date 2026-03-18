export type DatabaseProvider = "sqlite" | "postgres" | "nucleus";

export interface DataConfigInput {
  database?: DatabaseProvider;
  sqlitePath?: string;
  postgresUrlEnv?: string;
  /**
   * Environment variable holding the Nucleus server URL.
   * When `database` is `"nucleus"` (or auto-detected), this is used to
   * configure the `@neutron/nucleus` client transport.
   * Defaults to `"NUCLEUS_URL"`, falling back to the Postgres URL env var.
   */
  nucleusUrlEnv?: string;
}

export interface ResolvedDataConfig {
  database: DatabaseProvider;
  sqlitePath: string;
  postgresUrlEnv: string;
  nucleusUrlEnv: string;
}

const DEFAULT_SQLITE_PATH = ".neutron/dev.db";
const DEFAULT_POSTGRES_URL_ENV = "DATABASE_URL";
const DEFAULT_NUCLEUS_URL_ENV = "NUCLEUS_URL";

export function resolveDataConfig(input: DataConfigInput = {}): ResolvedDataConfig {
  const postgresUrlEnv = input.postgresUrlEnv || DEFAULT_POSTGRES_URL_ENV;
  const sqlitePath = input.sqlitePath || DEFAULT_SQLITE_PATH;
  const nucleusUrlEnv = input.nucleusUrlEnv || DEFAULT_NUCLEUS_URL_ENV;

  if (input.database) {
    return {
      database: input.database,
      sqlitePath,
      postgresUrlEnv,
      nucleusUrlEnv,
    };
  }

  // Auto-detect: check for Nucleus URL first, then Postgres, then SQLite.
  const hasNucleusUrl = Boolean(process.env[nucleusUrlEnv]);
  const hasPostgresUrl = Boolean(process.env[postgresUrlEnv]);

  let database: DatabaseProvider;
  if (hasNucleusUrl) {
    database = "nucleus";
  } else if (hasPostgresUrl) {
    database = "postgres";
  } else {
    database = "sqlite";
  }

  return {
    database,
    sqlitePath,
    postgresUrlEnv,
    nucleusUrlEnv,
  };
}

