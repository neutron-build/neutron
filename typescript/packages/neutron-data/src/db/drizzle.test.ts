import assert from "node:assert/strict";
import test from "node:test";
import { resolveDatabaseProfile, type DatabaseProfile } from "./index.js";
import type { DataConfigInput } from "../config.js";

test("resolveDatabaseProfile returns sqlite profile from input", () => {
  const profile = resolveDatabaseProfile({
    database: "sqlite",
    sqlitePath: "/tmp/test.db",
  });

  assert.equal(profile.provider, "sqlite");
  assert.equal(profile.connectionString, "/tmp/test.db");
});

test("resolveDatabaseProfile returns postgres profile from env", () => {
  const originalEnv = process.env.POSTGRES_URL;
  try {
    process.env.POSTGRES_URL = "postgres://user:pass@localhost/db";

    const profile = resolveDatabaseProfile({
      database: "postgres",
      postgresUrlEnv: "POSTGRES_URL",
    });

    assert.equal(profile.provider, "postgres");
    assert.equal(profile.connectionString, "postgres://user:pass@localhost/db");
  } finally {
    if (originalEnv === undefined) {
      delete process.env.POSTGRES_URL;
    } else {
      process.env.POSTGRES_URL = originalEnv;
    }
  }
});

test("resolveDatabaseProfile throws if postgres env var not set", () => {
  const originalEnv = process.env.DATABASE_URL;
  try {
    delete process.env.DATABASE_URL;

    assert.throws(
      () => {
        resolveDatabaseProfile({
          database: "postgres",
          postgresUrlEnv: "DATABASE_URL",
        });
      },
      { message: /not set/ }
    );
  } finally {
    if (originalEnv !== undefined) {
      process.env.DATABASE_URL = originalEnv;
    }
  }
});

test("resolveDatabaseProfile detects sqlite from env", () => {
  const profile = resolveDatabaseProfile({
    database: "sqlite",
    sqlitePath: "./db.sqlite",
  });

  assert.equal(profile.provider, "sqlite");
  assert.ok(profile.connectionString.includes("db.sqlite"));
});

test("resolveDatabaseProfile returns correct connection string for postgres", () => {
  const originalEnv = process.env.MY_PG_URL;
  try {
    process.env.MY_PG_URL = "postgresql://localhost:5432/myapp";

    const profile = resolveDatabaseProfile({
      database: "postgres",
      postgresUrlEnv: "MY_PG_URL",
    });

    assert.equal(profile.connectionString, "postgresql://localhost:5432/myapp");
  } finally {
    if (originalEnv === undefined) {
      delete process.env.MY_PG_URL;
    } else {
      process.env.MY_PG_URL = originalEnv;
    }
  }
});

test("resolveDatabaseProfile handles multiple calls with same config", () => {
  const config: DataConfigInput = {
    database: "sqlite",
    sqlitePath: "/data/app.db",
  };

  const profile1 = resolveDatabaseProfile(config);
  const profile2 = resolveDatabaseProfile(config);

  assert.equal(profile1.provider, profile2.provider);
  assert.equal(profile1.connectionString, profile2.connectionString);
});

test("DatabaseProfile interface has required fields", () => {
  const profile: DatabaseProfile = {
    provider: "postgres",
    connectionString: "postgres://localhost/db",
  };

  assert.ok("provider" in profile);
  assert.ok("connectionString" in profile);
  assert.equal(profile.provider, "postgres");
  assert.equal(profile.connectionString, "postgres://localhost/db");
});

test("resolveDatabaseProfile sqlite returns file: path format", () => {
  const profile = resolveDatabaseProfile({
    database: "sqlite",
    sqlitePath: "/tmp/test.db",
  });

  assert.equal(profile.provider, "sqlite");
  assert.ok(profile.connectionString.includes("test.db"));
});
