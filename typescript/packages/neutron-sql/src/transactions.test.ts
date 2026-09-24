import assert from "node:assert/strict";
import test from "node:test";
import {
  CommitAmbiguityError,
  ConnectionFailedError,
  ServerSqlError,
  isRetriableTransactionError,
} from "./errors.js";
import {
  hasModes,
  renderBeginSql,
  runRetriedTransaction,
  runTransaction,
  validateRetryOptions,
  type PinnedExecutor,
  type TransactionRetryOptions,
} from "./transactions.js";
import type { SqlEvent } from "./logger.js";

// Unit battery for the shared transaction runner. The PinnedExecutor below
// is a pure in-memory double used ONLY for control-flow assertions (which
// SQL the runner emits, how failures classify, how retry bounds behave);
// every database-facing behavior is verified live in
// live.i02.postgres.test.ts against real Postgres with both drivers.

interface ScriptedFailure {
  match: RegExp;
  error: unknown;
}

function fakePin(failures: ScriptedFailure[] = []): PinnedExecutor & { statements: string[]; releasedWith: unknown[] } {
  const statements: string[] = [];
  const releasedWith: unknown[] = [];
  return {
    statements,
    releasedWith,
    async query<T>(sqlText: string): Promise<T[]> {
      statements.push(sqlText);
      return [] as T[];
    },
    async execute(sqlText: string): Promise<number> {
      statements.push(sqlText);
      for (const f of failures) {
        if (f.match.test(sqlText)) throw f.error;
      }
      return 0;
    },
    release(err?: unknown): void {
      releasedWith.push(err);
    },
  };
}

test("I02: renderBeginSql maps the three real isolation levels", () => {
  assert.equal(renderBeginSql({}), "begin");
  assert.equal(renderBeginSql({ isolation: "read-committed" }), "begin isolation level read committed");
  assert.equal(renderBeginSql({ isolation: "repeatable-read" }), "begin isolation level repeatable read");
  assert.equal(renderBeginSql({ isolation: "serializable" }), "begin isolation level serializable");
  assert.equal(renderBeginSql({ isolation: "serializable", readOnly: true }), "begin isolation level serializable read only");
  assert.equal(renderBeginSql({ isolation: "serializable", readOnly: true, deferrable: true }), "begin isolation level serializable read only deferrable");
  assert.equal(renderBeginSql({ readOnly: true }), "begin read only");
});

test("I02: renderBeginSql rejects unknown isolation pre-SQL (no READ UNCOMMITTED)", () => {
  assert.throws(() => renderBeginSql({ isolation: "read-uncommitted" as never }), /not supported/);
  assert.throws(() => renderBeginSql({ isolation: "snapshot" as never }), /not supported/);
});

test("I02: deferrable is refused where it cannot be real", () => {
  // PostgreSQL accepts DEFERRABLE with any mode but it has no effect unless
  // SERIALIZABLE + READ ONLY — the silent no-op is rejected instead.
  assert.throws(() => renderBeginSql({ deferrable: true }), /deferrable transactions require/);
  assert.throws(() => renderBeginSql({ isolation: "serializable", deferrable: true }), /readOnly/);
  assert.throws(() => renderBeginSql({ isolation: "read-committed", readOnly: true, deferrable: true }), /serializable/);
  // sanity: the real combination renders
  assert.equal(renderBeginSql({ isolation: "serializable", readOnly: true, deferrable: true }), "begin isolation level serializable read only deferrable");
});

test("I02: hasModes detects non-default modes", () => {
  assert.equal(hasModes({}), false);
  assert.equal(hasModes({ isolation: "serializable" }), true);
  assert.equal(hasModes({ readOnly: true }), true);
  assert.equal(hasModes({ deferrable: true }), true);
});

test("I02: runTransaction emits begin/commit and releases the pin", async () => {
  const pin = fakePin();
  const events: SqlEvent[] = [];
  const out = await runTransaction(pin, async (tx) => {
    await tx.query("select 1");
    return "ok";
  }, {}, { onEvent: (e) => events.push(e) });
  assert.equal(out, "ok");
  assert.deepEqual(pin.statements, ["begin", "select 1", "commit"]);
  assert.equal(pin.releasedWith.length, 1);
  assert.equal(pin.releasedWith[0], undefined);
  assert.deepEqual(
    events.map((e) => e.kind),
    ["tx-begin", "tx-commit"],
  );
  assert.equal(events[0].isolation, undefined);
  assert.ok(events[0].statementId.length === 16);
});

test("I02: runTransaction renders modes into BEGIN and reports them on the event", async () => {
  const pin = fakePin();
  const events: SqlEvent[] = [];
  await runTransaction(pin, async () => 0, { isolation: "serializable", readOnly: true, deferrable: true }, { onEvent: (e) => events.push(e) });
  assert.equal(pin.statements[0], "begin isolation level serializable read only deferrable");
  assert.equal(events[0].isolation, "serializable");
  assert.equal(events[0].readOnly, true);
  assert.equal(events[0].deferrable, true);
});

test("I02: callback error rolls back and rethrows the original error; pin returns clean", async () => {
  const pin = fakePin();
  const events: SqlEvent[] = [];
  const boom = new Error("user failure");
  const err = await runTransaction(pin, async () => {
    throw boom;
  }, {}, { onEvent: (e) => events.push(e) }).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(err, boom);
  assert.deepEqual(pin.statements, ["begin", "rollback"]);
  assert.deepEqual(pin.releasedWith, [undefined]);
  const rollbackEvent = events.find((e) => e.kind === "tx-rollback");
  assert.ok(rollbackEvent);
  assert.equal(rollbackEvent.error?.name, "Error");
  assert.equal(rollbackEvent.error?.message, "user failure");
});

test("I02: failed rollback marks the pin suspect (released with the error)", async () => {
  const deadConn = new ConnectionFailedError("pg: socket gone", { code: "ECONNRESET" });
  const pin = fakePin([{ match: /rollback/, error: deadConn }]);
  const boom = new Error("original user failure");
  const err = await runTransaction(pin, async () => {
    throw boom;
  }).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(err, boom, "the ORIGINAL error propagates, not the rollback error");
  assert.equal(pin.releasedWith[0], deadConn);
});

test("I02: connection death during COMMIT is a distinct, never-retried ambiguity state", async () => {
  const dead = new ServerSqlError("pg: terminating connection due to administrator command", { sqlstate: "57P01" });
  const pin = fakePin([{ match: /^commit$/, error: dead }]);
  const events: SqlEvent[] = [];
  const err = await runTransaction(pin, async () => 1, {}, { onEvent: (e) => events.push(e) }).then(
    () => null,
    (e: unknown) => e,
  );
  assert.ok(err instanceof CommitAmbiguityError, `expected CommitAmbiguityError, got ${String(err)}`);
  assert.match(err.message, /outcome unknown/);
  assert.match(err.message, /never retried automatically/);
  assert.equal(err.cause, dead);
  // no rollback was attempted on a dead connection, and the pin is dropped
  assert.deepEqual(pin.statements, ["begin", "commit"]);
  assert.equal(pin.releasedWith[0], dead);
  assert.equal(isRetriableTransactionError(err), false);
  const ev = events.find((e) => e.kind === "tx-rollback");
  assert.equal(ev?.error?.name, "CommitAmbiguityError");
});

test("I02: 57014 cancellation arriving at COMMIT is ambiguous too", async () => {
  const canceled = new ServerSqlError("pg: canceling statement due to user request", { sqlstate: "57014" });
  const pin = fakePin([{ match: /^commit$/, error: canceled }]);
  const err = await runTransaction(pin, async () => 1).then(
    () => null,
    (e: unknown) => e,
  );
  assert.ok(err instanceof CommitAmbiguityError);
});

test("I02: an SQL error at COMMIT is a definite rollback answer, not ambiguity", async () => {
  const serialization = new ServerSqlError("pg: could not serialize access due to read/write dependencies", { sqlstate: "40001" });
  const pin = fakePin([{ match: /^commit$/, error: serialization }]);
  const err = await runTransaction(pin, async () => 1).then(
    () => null,
    (e: unknown) => e,
  );
  assert.ok(err instanceof ServerSqlError);
  assert.ok(!(err instanceof CommitAmbiguityError));
  assert.equal(isRetriableTransactionError(err), true);
});

test("I02: transport failure during BEGIN propagates and drops the pin", async () => {
  const dead = new ConnectionFailedError("pg: connection refused", { code: "ECONNREFUSED" });
  const pin = fakePin([{ match: /^begin/, error: dead }]);
  const err = await runTransaction(pin, async () => 1).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(err, dead);
  assert.equal(pin.releasedWith[0], dead);
});

// --- savepoints ------------------------------------------------------------

test("I02: nested transaction creates, and on success releases, a savepoint", async () => {
  const pin = fakePin();
  const events: SqlEvent[] = [];
  const out = await runTransaction(pin, async (tx) => {
    return tx.transaction(async () => "inner");
  }, {}, { onEvent: (e) => events.push(e) });
  assert.equal(out, "inner");
  const order = pin.statements;
  assert.ok(order.indexOf("begin") < order.indexOf('savepoint "neutron_sp_1"'));
  assert.ok(order.indexOf('savepoint "neutron_sp_1"') < order.indexOf('release savepoint "neutron_sp_1"'));
  assert.ok(order.indexOf('release savepoint "neutron_sp_1"') < order.indexOf("commit"));
  const spEvents = events.filter((e) => e.kind === "savepoint");
  assert.deepEqual(
    spEvents.map((e) => e.savepointAction),
    ["create", "release"],
  );
});

test("I02: nested transaction failure rolls back to the savepoint, releases it, rethrows; outer survives", async () => {
  const pin = fakePin();
  const boom = new Error("inner failure");
  const out = await runTransaction(pin, async (tx) => {
    const inner = await tx.transaction(async () => {
      throw boom;
    }).then(
      () => "no",
      (e: unknown) => String((e as Error).message),
    );
    await tx.query("select 1");
    return inner;
  });
  assert.equal(out, "inner failure");
  const order = pin.statements;
  assert.ok(order.indexOf('savepoint "neutron_sp_1"') < order.indexOf("rollback to savepoint \"neutron_sp_1\""));
  assert.ok(order.indexOf('rollback to savepoint "neutron_sp_1"') < order.indexOf('release savepoint "neutron_sp_1"'));
  assert.equal(order[order.length - 1], "commit");
});

test("I02: explicit savepoint with rollbackTo keeps it usable; release then rejects stale use", async () => {
  const pin = fakePin();
  const out = await runTransaction(pin, async (tx) => {
    await tx.query("insert into t values (1)");
    const sp = await tx.savepoint("middle");
    await tx.query("insert into t values (2)");
    await sp.rollbackTo();
    await tx.query("insert into t values (3)");
    await sp.rollbackTo(); // still active after rollbackTo
    await sp.release();
    await assert.rejects(sp.rollbackTo(), /already released/);
    await assert.rejects(sp.release(), /already released/);
    return "done";
  });
  assert.equal(out, "done");
  assert.deepEqual(
    pin.statements.filter((s) => s.includes("savepoint")),
    ['savepoint "middle"', "rollback to savepoint \"middle\"", "rollback to savepoint \"middle\"", 'release savepoint "middle"'],
  );
});

test("I02: savepoint names are validated as plain identifiers", async () => {
  const pin = fakePin();
  await assert.rejects(
    runTransaction(pin, async (tx) => tx.savepoint('bad "name"')),
    /not a plain identifier/,
  );
  await assert.rejects(
    runTransaction(pin, async (tx) => tx.savepoint("1starts-with-digit")),
    /not a plain identifier/,
  );
});

test("I02: modes on a nested transaction are rejected (savepoints take none)", async () => {
  const pin = fakePin();
  const err = await runTransaction(pin, async (tx) => {
    const scope = tx as unknown as { begin(fn: (s: unknown) => Promise<unknown>, modes?: unknown): Promise<unknown> };
    return scope.begin(async () => 1, { isolation: "serializable" }).then(
      () => null,
      (e: unknown) => e,
    );
  });
  assert.match(String((err as Error)?.message), /properties of the outer BEGIN/);
});

// --- retry -----------------------------------------------------------------

const RETRY_OK: TransactionRetryOptions = { maxAttempts: 3, idempotent: true };

test("I02: retry options require the idempotency assertion", () => {
  assert.throws(() => validateRetryOptions({ maxAttempts: 3 } as TransactionRetryOptions), /idempotent: true/);
  assert.throws(() => validateRetryOptions({ maxAttempts: 3, idempotent: false } as unknown as TransactionRetryOptions), /idempotent/);
  assert.throws(() => validateRetryOptions({ maxAttempts: 0, idempotent: true }), /maxAttempts/);
  assert.throws(() => validateRetryOptions({ maxAttempts: 1.5, idempotent: true }), /maxAttempts/);
  assert.throws(() => validateRetryOptions({ ...RETRY_OK, backoffMs: -5 }), /backoffMs/);
  assert.doesNotThrow(() => validateRetryOptions(RETRY_OK));
  assert.doesNotThrow(() => validateRetryOptions({ ...RETRY_OK, backoffMs: 0 }));
  assert.doesNotThrow(() => validateRetryOptions({ ...RETRY_OK, backoffMs: (a: number) => a * 10 }));
});

test("I02: retry retries 40001 up to the bound, then propagates the last error", async () => {
  let attempts = 0;
  const serialization = new ServerSqlError("pg: could not serialize access", { sqlstate: "40001" });
  const err = await runRetriedTransaction(
    () => {
      attempts++;
      const pin = fakePin([{ match: /^update/, error: serialization }]);
      return Promise.resolve(pin);
    },
    async (tx) => {
      await tx.execute("update t set v = 1");
      return "done";
    },
    {},
    { ...RETRY_OK, backoffMs: 0 },
  ).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(attempts, 3, "maxAttempts bounds total attempts");
  assert.equal(err, serialization);
});

test("I02: retry recovers when the conflict clears; deadlock 40P01 is also retriable", async () => {
  let attempts = 0;
  const deadlock = new ServerSqlError("pg: deadlock detected", { sqlstate: "40P01" });
  const out = await runRetriedTransaction(
    () => {
      attempts++;
      const failures = attempts === 1 ? [{ match: /^update/, error: deadlock }] : [];
      return Promise.resolve(fakePin(failures));
    },
    async (tx) => {
      await tx.execute("update t set v = 1");
      return attempts;
    },
    {},
    { ...RETRY_OK, backoffMs: 0 },
  );
  assert.equal(out, 2);
  assert.equal(attempts, 2);
});

test("I02: maxAttempts 1 disables retry entirely", async () => {
  let attempts = 0;
  const serialization = new ServerSqlError("pg: could not serialize access", { sqlstate: "40001" });
  await runRetriedTransaction(
    () => {
      attempts++;
      return Promise.resolve(fakePin([{ match: /^update/, error: serialization }]));
    },
    async (tx) => tx.execute("update t set v = 1"),
    {},
    { maxAttempts: 1, idempotent: true, backoffMs: 0 },
  ).then(
    () => null,
    () => null,
  );
  assert.equal(attempts, 1);
});

test("I02: non-retriable SQL errors surface after one attempt", async () => {
  let attempts = 0;
  const unique = new ServerSqlError("pg: duplicate key", { sqlstate: "23505" });
  const err = await runRetriedTransaction(
    () => {
      attempts++;
      return Promise.resolve(fakePin([{ match: /^insert/, error: unique }]));
    },
    async (tx) => tx.execute("insert into t values (1)"),
    {},
    { ...RETRY_OK, backoffMs: 0 },
  ).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(attempts, 1);
  assert.equal(err, unique);
});

test("I02: CommitAmbiguityError is never retried, even with retry armed", async () => {
  let attempts = 0;
  const dead = new ConnectionFailedError("pg: socket died at commit", { code: "ECONNRESET" });
  const err = await runRetriedTransaction(
    () => {
      attempts++;
      return Promise.resolve(fakePin([{ match: /^commit$/, error: dead }]));
    },
    async () => 1,
    {},
    { ...RETRY_OK, backoffMs: 0 },
  ).then(
    () => null,
    (e: unknown) => e,
  );
  assert.equal(attempts, 1, "an unknown commit outcome must never be replayed");
  assert.ok(err instanceof CommitAmbiguityError);
});

test("I02: retry annotates tx events with the attempt number", async () => {
  const serialization = new ServerSqlError("pg: could not serialize access", { sqlstate: "40001" });
  let attempts = 0;
  const events: SqlEvent[] = [];
  await runRetriedTransaction(
    () => {
      attempts++;
      const failures = attempts === 1 ? [{ match: /^update/, error: serialization }] : [];
      return Promise.resolve(fakePin(failures));
    },
    async (tx) => tx.execute("update t set v = 1"),
    {},
    { ...RETRY_OK, backoffMs: 0 },
    { onEvent: (e) => events.push(e) },
  );
  const begins = events.filter((e) => e.kind === "tx-begin");
  assert.deepEqual(begins.map((e) => e.attempt), [1, 2]);
});
