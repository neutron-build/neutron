import assert from "node:assert/strict";
import test from "node:test";
import { errorSummary, paramsLoggingEnabled, resolveLogger, statementIdOf, type SqlEvent } from "./logger.js";

// I02 observability: structured events are redacted by default — parameter
// values never appear in events (or in the default JSON-lines logger) unless
// the process explicitly opts in via NEUTRON_SQL_LOG_PARAMS=1. Live
// grep/canary assertions against a real database run in
// live.i02.postgres.test.ts; this file pins the pure logic.

function captureConsole(fn: () => void): string[] {
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: unknown[]) => {
    lines.push(args.map(String).join(" "));
  };
  try {
    fn();
  } finally {
    console.log = original;
  }
  return lines;
}

test("I02: default logger prints one JSON line per event, without params", () => {
  const logger = resolveLogger(true);
  assert.ok(logger !== null);
  const lines = captureConsole(() => {
    logger({ kind: "query-begin", statementId: statementIdOf("select 1"), sql: "select $1", params: undefined });
    logger({ kind: "query-end", statementId: statementIdOf("select 1"), sql: "select $1", durationMs: 1.5 });
  });
  assert.equal(lines.length, 2);
  for (const line of lines) {
    assert.ok(line.startsWith("[neutron-sql] {"));
    const parsed = JSON.parse(line.slice("[neutron-sql] ".length)) as Record<string, unknown>;
    assert.equal("params" in parsed && parsed.params !== undefined, false, "params must be absent by default");
  }
  const end = JSON.parse(lines[1].slice("[neutron-sql] ".length)) as Record<string, unknown>;
  assert.equal(end.kind, "query-end");
  assert.equal(typeof end.durationMs, "number");
  assert.equal(end.sql, undefined);
});

test("I02: params appear only under the explicit NEUTRON_SQL_LOG_PARAMS opt-in", () => {
  const previous = process.env.NEUTRON_SQL_LOG_PARAMS;
  try {
    process.env.NEUTRON_SQL_LOG_PARAMS = "1";
    assert.equal(paramsLoggingEnabled(), true);
    const logger = resolveLogger(true);
    assert.ok(logger);
    const lines = captureConsole(() => {
      logger({ kind: "query-begin", statementId: "x", sql: "select $1", params: ["secret-value"] });
    });
    const parsed = JSON.parse(lines[0].slice("[neutron-sql] ".length)) as Record<string, unknown>;
    assert.deepEqual(parsed.params, ["secret-value"], "opt-in mode includes parameter values");

    process.env.NEUTRON_SQL_LOG_PARAMS = undefined;
    assert.equal(paramsLoggingEnabled(), false);
    const lines2 = captureConsole(() => {
      logger({ kind: "query-begin", statementId: "x", sql: "select $1", params: ["secret-value"] });
    });
    const parsed2 = JSON.parse(lines2[0].slice("[neutron-sql] ".length)) as Record<string, unknown>;
    assert.equal("params" in parsed2, false);
  } finally {
    if (previous === undefined) delete process.env.NEUTRON_SQL_LOG_PARAMS;
    else process.env.NEUTRON_SQL_LOG_PARAMS = previous;
  }
});

test("I02: false/undefined loggers resolve to null; custom sinks receive redacted events", () => {
  assert.equal(resolveLogger(undefined), null);
  assert.equal(resolveLogger(false), null);
  const custom = (event: SqlEvent): void => void event;
  assert.equal(typeof resolveLogger(custom), "function");
});

test("I02: statementIdOf is deterministic, 16 hex chars, distinct per SQL", () => {
  const a = statementIdOf("select 1");
  assert.equal(a, statementIdOf("select 1"));
  assert.match(a, /^[0-9a-f]{16}$/);
  assert.notEqual(a, statementIdOf("select 2"));
});

test("I02: errorSummary carries classification and SQLSTATE without native values", () => {
  const err = new Error("boom") as Error & { code: string };
  err.code = "23505";
  const summary = errorSummary(err);
  assert.deepEqual(summary, { name: "Error", message: "SQL request failed (23505)", sqlstate: "23505" });
  const plain = errorSummary(new Error("no code"));
  assert.equal(plain.sqlstate, undefined);
  assert.equal(plain.name, "Error");
  const notError = errorSummary("string failure");
  assert.equal(notError.name, "string");
  assert.equal(notError.message, "SQL request failed");
});


test("redacted sinks omit literal secrets and native duplicate-key values", () => {
  const previous = process.env.NEUTRON_SQL_LOG_PARAMS;
  delete process.env.NEUTRON_SQL_LOG_PARAMS;
  try {
    const events: SqlEvent[] = [];
    const sink = resolveLogger(event => events.push(event))!;
    const secret = "postgresql://private-user:private-password@private-host/db";
    const error = new Error(`duplicate key: ${secret}`) as Error & { code: string };
    error.code = "23505";
    sink({ kind: "query-error", statementId: "fixed", sql: `select '${secret}'`, params: [secret], error: errorSummary(error) });
    assert.equal(JSON.stringify(events).includes(secret), false);
    assert.equal(events[0].error?.sqlstate, "23505");
    assert.doesNotThrow(() => resolveLogger(() => { throw new Error(secret); })!(events[0]));
  } finally {
    if (previous === undefined) delete process.env.NEUTRON_SQL_LOG_PARAMS;
    else process.env.NEUTRON_SQL_LOG_PARAMS = previous;
  }
});

test("async observer rejection is drained without process failure", async () => {
  resolveLogger(async () => { throw new Error("async observer unavailable"); })!({ kind: "query-end", statementId: "fixed" });
  await new Promise<void>(resolve => setImmediate(resolve));
});

test("opaque application error accessors cannot change observation outcome", () => {
  const error = new Error("private native detail");
  for (const key of ["sqlstate", "code", "constructor"]) Object.defineProperty(error, key, { get() { throw new Error("private accessor detail"); } });
  assert.deepEqual(errorSummary(error), { name: "Error", message: "SQL request failed", sqlstate: undefined });
});
