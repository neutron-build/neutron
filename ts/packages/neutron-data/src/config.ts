export type DatabaseProvider = "sqlite" | "postgres";

export interface DataConfigInput {
  database?: DatabaseProvider;
  sqlitePath?: string;
  postgresUrlEnv?: string;
}

export interface ResolvedDataConfig {
  database: DatabaseProvider;
  sqlitePath: string;
  postgresUrlEnv: string;
}

const DEFAULT_SQLITE_PATH = ".neutron/dev.db";
const DEFAULT_POSTGRES_URL_ENV = "DATABASE_URL";

export function resolveDataConfig(input: DataConfigInput = {}): ResolvedDataConfig {
  const postgresUrlEnv = input.postgresUrlEnv || DEFAULT_POSTGRES_URL_ENV;
  const sqlitePath = input.sqlitePath || DEFAULT_SQLITE_PATH;

  if (input.database) {
    return {
      database: input.database,
      sqlitePath,
      postgresUrlEnv,
    };
  }

  const hasPostgresUrl = Boolean(process.env[postgresUrlEnv]);
  return {
    database: hasPostgresUrl ? "postgres" : "sqlite",
    sqlitePath,
    postgresUrlEnv,
  };
}

