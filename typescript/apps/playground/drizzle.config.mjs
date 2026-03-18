const isPostgres = Boolean(process.env.DATABASE_URL);
const sqlitePath = process.env.SQLITE_PATH || ".neutron/dev.db";

/** @type {import("drizzle-kit").Config} */
const config = {
  schema: isPostgres
    ? "./src/data/db/schema.pg.ts"
    : "./src/data/db/schema.sqlite.ts",
  out: isPostgres ? "./drizzle/postgres" : "./drizzle/sqlite",
  dialect: isPostgres ? "postgresql" : "sqlite",
  dbCredentials: isPostgres
    ? { url: process.env.DATABASE_URL }
    : { url: sqlitePath },
  verbose: true,
  strict: true,
};

export default config;
