import assert from "node:assert/strict";
import test from "node:test";
import {
  NeutronSqlError,
  createDatabase,
  assertNodeRuntime,
  type Driver,
  type PinnedExecutor,
} from "./index.js";

// I03 unit battery for the database-level transaction scope and the
// runtime-support guard. The fake Driver/PinnedExecutor doubles below are
// pure in-memory controls used ONLY for scope semantics (argument handling,
// guard firing); every database-facing behavior stays in the live suites.

interface Recording {
  statements: string[];
  releasedWith: unknown[];
}

function fakePinnedDriver(): Driver & { recording: Recording } {
  const recording: Recording = { statements: [], releasedWith: [] };
  const pin: PinnedExecutor = {
    async query<T>(sqlText: string): Promise<T[]> {
      recording.statements.push(sqlText);
      return [] as T[];
    },
    async execute(sqlText: string): Promise<number> {
      recording.statements.push(sqlText);
      return 0;
    },
    release(err?: unknown): void {
      recording.releasedWith.push(err);
    },
  };
  const driver: Driver = {
    async query<T>(sqlText: string): Promise<T[]> {
      recording.statements.push(sqlText);
      return [] as T[];
    },
    async execute(sqlText: string): Promise<number> {
      recording.statements.push(sqlText);
      return 0;
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>): Promise<T> {
      return fn(driver);
    },
    async close(): Promise<void> {},
    lifecycle: { ownership: "borrowed", terminated: false, terminate: () => Promise.resolve() },
    pin: () => Promise.resolve(pin),
  };
  return Object.assign(driver, { recording });
}

test("db-level nested transaction rejects modes at runtime (JS-only reach)", async () => {
  const driver = fakePinnedDriver();
  const db = await createDatabase({ driver });

  // The declared TS surface takes one parameter; a JS caller passing an
  // options object previously had it silently ignored (I02 review LOW-1).
  // It must now reject precisely, pointing at where modes belong.
  await assert.rejects(
    () =>
      db.transaction(async (tx) => {
        await (tx.transaction as (fn: (tx: unknown) => Promise<void>, modes?: unknown) => Promise<void>)(
          async () => {},
          { isolation: "serializable" },
        );
      }),
    (err: unknown) => {
      assert.ok(err instanceof NeutronSqlError);
      assert.match(
        err.message,
        /properties of the outer BEGIN.*db\.transaction\(fn, options\).*driver-scoped begin\(fn, modes\)/s,
      );
      return true;
    },
  );

  // A nested call WITHOUT modes still works (real savepoint path).
  await db.transaction(async (tx) => {
    await tx.transaction(async () => {});
  });
  // undefined modes (explicit) are tolerated like an absent argument.
  await db.transaction(async (tx) => {
    await (tx.transaction as (fn: (tx: unknown) => Promise<void>, modes?: unknown) => Promise<void>)(
      async () => {},
      undefined,
    );
  });
  // An options object with no mode keys is a no-op object, not a rejection.
  await db.transaction(async (tx) => {
    await (tx.transaction as (fn: (tx: unknown) => Promise<void>, modes?: unknown) => Promise<void>)(
      async () => {},
      {},
    );
  });
});

test("runtime guard: createDatabase and loadDriver reject non-Node runtimes precisely", async () => {
  const versions = process.versions as { node?: string };
  const hadNode = versions.node;
  try {
    delete versions.node;
    assert.throws(
      () => assertNodeRuntime("loadDriver"),
      (err: unknown) => {
        assert.ok(err instanceof NeutronSqlError);
        assert.match(String(err.message), /requires a Node\.js runtime/);
        assert.match(String(err.message), /no edge\/browser transport adapter/);
        return true;
      },
    );
    await assert.rejects(
      () => createDatabase({ driver: fakePinnedDriver() }),
      /requires a Node\.js runtime/,
    );
  } finally {
    if (hadNode) versions.node = hadNode;
  }
});

test("runtime guard: a Node process passes untouched", () => {
  assert.doesNotThrow(() => assertNodeRuntime("loadDriver"));
});
