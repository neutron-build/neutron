import test from "node:test";
import assert from "node:assert/strict";
import { createQueryTelemetry } from "./telemetry.js";
import { SqlRequestLifecycle, RequestShutdownTimeoutError } from "./request-lifecycle.js";

const statementId = "0123456789abcdef";
test("query telemetry isolates overlapping and nested request contexts and strips diagnostic values", async () => {
  const emitted: unknown[] = [];
  const telemetry = createQueryTelemetry(() => ({ addEvent: (name, attributes) => { emitted.push({ name, attributes }); } }));
  const results = await Promise.all([1, 2].map(count => telemetry.run(async () => {
    for (let i = 0; i < count; i++) {
      telemetry.logger({ kind: "query-begin", statementId, sql: "literal-secret", params: ["bound-secret"] });
      await new Promise(resolve => setImmediate(resolve));
      telemetry.logger({ kind: "query-end", statementId, durationMs: 2 });
    }
    const nested = await telemetry.run(async () => { telemetry.logger({ kind: "query-error", statementId, error: { name: "secret", message: "echo-secret", sqlstate: "22P02" } }); });
    assert.equal(nested.metrics.failed, 1);
    return count;
  })));
  assert.deepEqual(results.map(result => result.metrics.started), [1, 2]);
  assert.deepEqual(results.map(result => result.metrics.succeeded), [1, 2]);
  assert.deepEqual(results.map(result => result.metrics.failed), [0, 0]);
  assert.equal(telemetry.current(), undefined);
  assert.doesNotMatch(JSON.stringify(emitted), /secret/);
});

test("query telemetry exporter failure does not change callback outcome", async () => {
  const telemetry = createQueryTelemetry(() => ({ addEvent: async () => { throw new Error("exporter failed"); } }));
  assert.equal((await telemetry.run(async () => { telemetry.logger({ kind: "query-end", statementId }); return 7; })).value, 7);
  await new Promise(resolve => setImmediate(resolve));
});

test("request lifecycle cancels and drains concurrent requests before exactly one close", async () => {
  let closed = 0;
  const lifecycle = new SqlRequestLifecycle({ async close() { closed++; } });
  const request = lifecycle.run(async (_database, signal) => new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true })));
  assert.equal(lifecycle.activeRequests, 1);
  const shutdown = lifecycle.shutdown({ graceMs: 0, cancelMs: 1000 });
  await assert.rejects(() => lifecycle.run(async () => 1), /admission closed/);
  await Promise.all([request, shutdown, lifecycle.shutdown({ graceMs: 0, cancelMs: 1000 })]);
  assert.equal(lifecycle.activeRequests, 0); assert.equal(closed, 1);
  await lifecycle.shutdown({ graceMs: 0, cancelMs: 0 }); assert.equal(closed, 1);
});

test("request lifecycle refuses to close beneath work ignoring cancellation and permits later drain retry", async () => {
  let release!: () => void; let closed = 0;
  const lifecycle = new SqlRequestLifecycle({ async close() { closed++; } });
  const request = lifecycle.run(async () => new Promise<void>(resolve => { release = resolve; }));
  await assert.rejects(() => lifecycle.shutdown({ graceMs: 0, cancelMs: 0 }), RequestShutdownTimeoutError);
  assert.equal(closed, 0); release(); await request;
  await lifecycle.shutdown({ graceMs: 0, cancelMs: 0 }); assert.equal(closed, 1);
});
