import assert from "node:assert/strict";
import { test } from "node:test";
import { NeutronSqlError } from "./errors.js";
import { wrapPgPool, wrapPostgresJs, type PgPoolLike, type PostgresJsClient } from "./drivers.js";

const INVALID_MODES = [
  { isolation: "bogus" as never },
  { deferrable: true },
];

test("pg adapter: invalid transaction modes refuse before a connection is pinned", async () => {
  let connects = 0;
  const pool = {
    connect: async () => {
      connects += 1;
      throw new Error("a connection must not be acquired for invalid modes");
    },
    query: async () => ({ rows: [], rowCount: 0 }),
    end: async () => undefined,
  } as unknown as PgPoolLike;
  const driver = wrapPgPool(pool);
  for (const modes of INVALID_MODES) {
    await assert.rejects(driver.begin(async () => 1, modes), NeutronSqlError);
  }
  assert.equal(connects, 0);
});

test("postgres.js adapter: invalid transaction modes refuse before a connection is reserved", async () => {
  let reserves = 0;
  const client = {
    reserve: async () => {
      reserves += 1;
      throw new Error("a connection must not be reserved for invalid modes");
    },
    end: async () => undefined,
  } as unknown as PostgresJsClient;
  const driver = wrapPostgresJs(client);
  for (const modes of INVALID_MODES) {
    await assert.rejects(driver.begin(async () => 1, modes), NeutronSqlError);
  }
  assert.equal(reserves, 0);
});
