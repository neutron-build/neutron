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

  async add(name: string, payload: unknown): Promise<{ id: string | number | undefined }> {
    const id = String(this.idCounter++);
    this.jobs.set(id, { id, name, data: payload, timestamp: Date.now() });
    return { id };
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
