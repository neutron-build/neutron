import assert from "node:assert/strict";
import test from "node:test";
import { BullMqQueueDriver } from "./bullmq.js";

// Mock job interface
interface MockJob {
  id?: string | number;
  name: string;
  data: unknown;
  timestamp?: number;
}

// Mock Queue
class MockBullMqQueue {
  private jobs: Map<string, MockJob> = new Map();
  private idCounter = 1;
  readonly addCalls: Array<{ name: string; payload: unknown; opts?: unknown }> = [];
  readonly removeRepeatableCalls: Array<{ name: string; repeatOpts: unknown }> = [];

  async add(
    name: string,
    payload: unknown,
    opts?: { repeat?: unknown }
  ): Promise<{ id: string | number | undefined }> {
    const id = String(this.idCounter++);
    this.jobs.set(id, { id, name, data: payload, timestamp: Date.now() });
    this.addCalls.push({ name, payload, opts });
    return { id };
  }

  async removeRepeatable(name: string, repeatOpts: unknown): Promise<boolean> {
    this.removeRepeatableCalls.push({ name, repeatOpts });
    return true;
  }

  async close(): Promise<void> {
    this.jobs.clear();
  }

  getJobs() {
    return Array.from(this.jobs.values());
  }
}

// Mock Worker
let capturedProcessor: ((job: MockJob) => Promise<void>) | null = null;

class MockBullMqWorker {
  constructor(
    queueName: string,
    processor: (job: MockJob) => Promise<void>,
    options?: Record<string, unknown>
  ) {
    capturedProcessor = processor;
  }

  async close(): Promise<void> {
    capturedProcessor = null;
  }
}

// Mock Redis connection
class MockRedisConnection {
  async quit(): Promise<void> {}
}

test("BullMqQueueDriver.add creates job", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  const job = await driver.add("send-email", { email: "user@example.com" });

  assert.ok(job.id);
  assert.equal(job.name, "send-email");
  assert.deepEqual(job.payload, { email: "user@example.com" });
  assert.ok(job.createdAt);
});

test("BullMqQueueDriver.process registers handler", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  let handlerCalled = false;
  await driver.process("send-email", async (job) => {
    handlerCalled = true;
    assert.equal(job.name, "send-email");
  });

  // Simulate job execution through captured processor
  if (capturedProcessor) {
    await capturedProcessor({
      id: "1",
      name: "send-email",
      data: { email: "user@example.com" },
    });
    assert.ok(handlerCalled);
  }

  await driver.close();
});

test("BullMqQueueDriver creates worker once", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  let workerCreationCount = 0;

  class CountingWorker {
    constructor(
      queueName: string,
      processor: (job: MockJob) => Promise<void>,
      options?: Record<string, unknown>
    ) {
      workerCreationCount++;
      capturedProcessor = processor;
    }

    async close(): Promise<void> {
      capturedProcessor = null;
    }
  }

  const driver = new BullMqQueueDriver(
    queue as any,
    CountingWorker as any,
    {},
    "test-queue",
    connection as any
  );

  // Register multiple handlers - should only create one worker
  await driver.process("send-email", async () => {});
  await driver.process("log-event", async () => {});
  await driver.process("send-sms", async () => {});

  assert.equal(workerCreationCount, 1);

  await driver.close();
});

test("BullMqQueueDriver.close cleans up resources", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  let connectionQuit = false;

  const mockConnection = {
    quit: async () => {
      connectionQuit = true;
    },
  };

  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    mockConnection as any
  );

  await driver.close();

  assert.ok(connectionQuit);
  assert.equal(queue.getJobs().length, 0);
});

test("BullMqQueueDriver handles multiple job types", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  const handlers: Map<string, any> = new Map();
  await driver.process("email", async (job) => {
    handlers.set("email-called", job);
  });
  await driver.process("sms", async (job) => {
    handlers.set("sms-called", job);
  });

  // Simulate job executions
  if (capturedProcessor) {
    await capturedProcessor({
      id: "1",
      name: "email",
      data: { to: "user@example.com" },
      timestamp: Date.now(),
    });
    await capturedProcessor({
      id: "2",
      name: "sms",
      data: { to: "+1234567890" },
      timestamp: Date.now(),
    });
  }

  assert.ok(handlers.has("email-called"));
  assert.ok(handlers.has("sms-called"));

  await driver.close();
});

test("BullMqQueueDriver.schedule maps to a native repeatable keyed by id", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  await driver.schedule("nightly-report", "0 3 * * *", { kind: "report" });

  assert.equal(queue.addCalls.length, 1);
  assert.equal(queue.addCalls[0].name, "nightly-report");
  assert.deepEqual(queue.addCalls[0].opts, {
    repeat: { pattern: "0 3 * * *", key: "nightly-report" },
  });
  assert.deepEqual(queue.addCalls[0].payload, { kind: "report" });
});

test("BullMqQueueDriver.unschedule removes the repeatable with the same opts used to add it", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  await driver.schedule("nightly-report", "0 3 * * *", null);
  await driver.unschedule("nightly-report");

  assert.equal(queue.removeRepeatableCalls.length, 1);
  assert.equal(queue.removeRepeatableCalls[0].name, "nightly-report");
  assert.deepEqual(queue.removeRepeatableCalls[0].repeatOpts, {
    pattern: "0 3 * * *",
    key: "nightly-report",
  });
});

test("BullMqQueueDriver.unschedule is a no-op for an unknown id", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  await driver.unschedule("never-scheduled");

  assert.equal(queue.removeRepeatableCalls.length, 0);
});

test("BullMqQueueDriver.schedule twice replaces the remembered pattern", async () => {
  const queue = new MockBullMqQueue();
  const connection = new MockRedisConnection();
  const driver = new BullMqQueueDriver(
    queue as any,
    MockBullMqWorker as any,
    {},
    "test-queue",
    connection as any
  );

  await driver.schedule("ticker", "*/5 * * * *", null);
  await driver.schedule("ticker", "0 * * * *", null);
  await driver.unschedule("ticker");

  assert.equal(queue.removeRepeatableCalls.length, 1);
  assert.deepEqual(queue.removeRepeatableCalls[0].repeatOpts, {
    pattern: "0 * * * *",
    key: "ticker",
  });
});

test("TSD-04: unknown job is delayed without successful acknowledgement, then handled after registration", async () => {
  class DelayedError extends Error {}
  const driver = new BullMqQueueDriver(new MockBullMqQueue(), MockBullMqWorker as any, {}, "mixed", new MockRedisConnection(), DelayedError);
  await driver.process("email", () => {});
  let delayed = 0;
  const unknown = {
    name: "video", data: { id: 1 },
    moveToDelayed: async (when: number) => { assert.ok(when > Date.now()); delayed++; },
  };
  await assert.rejects(() => capturedProcessor!(unknown), DelayedError);
  assert.equal(delayed, 1);
  let handled = false;
  await driver.process("video", () => { handled = true; });
  await capturedProcessor!(unknown);
  assert.equal(handled, true);
  await driver.close();
});

test("TSD-04: custom worker without native delay fails observably for unknown names", async () => {
  const driver = new BullMqQueueDriver(new MockBullMqQueue(), MockBullMqWorker as any, {}, "mixed", new MockRedisConnection());
  await driver.process("email", () => {});
  await assert.rejects(() => capturedProcessor!({ name: "video", data: null }), /no handler registered/);
  await driver.close();
});

test("BullMQ close preserves worker-before-connection order and drains every failed owner", async () => {
  const calls: string[] = [];
  const errors = [new Error("worker close"), new Error("queue close"), new Error("connection quit")];
  class Worker extends MockBullMqWorker {
    async close(): Promise<void> { calls.push("worker"); throw errors[0]; }
  }
  const queue = new MockBullMqQueue();
  queue.close = async () => { calls.push("queue"); throw errors[1]; };
  const connection = new MockRedisConnection();
  connection.quit = async () => { calls.push("connection"); throw errors[2]; };
  const driver = new BullMqQueueDriver(queue, Worker, {}, "test", connection);
  await driver.process("task", () => {});
  await assert.rejects(() => driver.close(), (error: any) => { assert.deepEqual(error.errors, errors); return true; });
  await assert.rejects(() => driver.close());
  assert.deepEqual(calls, ["worker", "queue", "connection"]);
  await assert.rejects(() => driver.process("task", () => {}), /closed/);
});
