import assert from "node:assert/strict";
import test from "node:test";
import { InMemoryQueueDriver } from "./index.js";

// The InMemoryQueueDriver contract: a throwing handler must not drop the
// job, reject the producer's add(), or stall jobs behind it. The job is
// retried up to MAX_ATTEMPTS times with a short backoff, then parked on
// the driver's dead-letter list for inspection.

test("a failing handler is retried then dead-lettered, and add() resolves", async () => {
  const queue = new InMemoryQueueDriver();
  const attempts: string[] = [];
  await queue.process("boom", async (job) => {
    attempts.push(job.id);
    throw new Error("handler exploded");
  });

  const job = await queue.add("boom", { n: 1 });

  assert.equal(attempts.length, 3);
  assert.deepEqual(attempts, [job.id, job.id, job.id]);
  assert.equal(queue.deadLetters.length, 1);
  const dead = queue.deadLetters[0];
  assert.equal(dead.job.id, job.id);
  assert.deepEqual(dead.job.payload, { n: 1 });
  assert.equal(dead.attempts, 3);
  assert.equal((dead.error as Error).message, "handler exploded");
});

test("a failing handler does not reject a concurrent add() nor stall the next job", async () => {
  const queue = new InMemoryQueueDriver();
  const handled: unknown[] = [];
  await queue.process("task", async (job) => {
    if (job.payload === "bad") {
      throw new Error("nope");
    }
    handled.push(job.payload);
  });

  const [bad, good] = await Promise.all([
    queue.add("task", "bad"),
    queue.add("task", "good"),
  ]);

  assert.equal(bad.id, "1");
  assert.equal(good.id, "2");
  assert.deepEqual(handled, ["good"]);
  assert.equal(queue.deadLetters.length, 1);
  assert.equal(queue.deadLetters[0].job.payload, "bad");
});

test("a handler that succeeds within the retry budget is not dead-lettered", async () => {
  const queue = new InMemoryQueueDriver();
  let calls = 0;
  await queue.process("flaky", async () => {
    calls += 1;
    if (calls < 3) {
      throw new Error("transient");
    }
  });

  await queue.add("flaky", null);

  assert.equal(calls, 3);
  assert.equal(queue.deadLetters.length, 0);
});

test("jobs with no registered handler stay queued until one is registered", async () => {
  const queue = new InMemoryQueueDriver();
  const job = await queue.add("orphan", { a: 1 });

  assert.equal(queue.deadLetters.length, 0);

  const seen: unknown[] = [];
  await queue.process("orphan", (j) => {
    seen.push(j.payload);
  });

  assert.deepEqual(seen, [{ a: 1 }]);
  assert.equal(job.id, "1");
});
