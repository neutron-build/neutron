import assert from "node:assert/strict";
import test from "node:test";
import { RedisRealtimeBus } from "./redis.js";

// Mock subscriber
class MockSubscriber {
  private listeners: Map<string, Set<(...args: unknown[]) => void>> = new Map();

  async subscribe(channel: string): Promise<void> {
    // Mock implementation
  }

  async unsubscribe(channel: string): Promise<void> {
    // Mock implementation
  }

  on(event: string, listener: (...args: unknown[]) => void): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
    return this;
  }

  removeAllListeners(event?: string): this {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
    return this;
  }

  async quit(): Promise<void> {
    this.listeners.clear();
  }

  getListener(event: string) {
    return this.listeners.get(event);
  }

  get status(): string {
    return "ready";
  }
}

// Mock publisher
class MockPublisher {
  private subscriber: MockSubscriber | null = null;
  private messages: Map<string, any[]> = new Map();

  async publish(channel: string, message: string): Promise<number> {
    if (!this.messages.has(channel)) {
      this.messages.set(channel, []);
    }
    this.messages.get(channel)!.push(message);

    // Deliver to subscriber if it exists
    if (this.subscriber) {
      const listeners = this.subscriber.getListener("message");
      if (listeners) {
        for (const listener of listeners) {
          listener(channel, message);
        }
      }
    }
    return 1;
  }

  duplicate(): MockSubscriber {
    this.subscriber = new MockSubscriber();
    return this.subscriber;
  }

  async quit(): Promise<void> {
    this.messages.clear();
    if (this.subscriber) {
      await this.subscriber.quit();
      this.subscriber = null;
    }
  }

  get status(): string {
    return "ready";
  }

  getMessages(channel: string): string[] {
    return this.messages.get(channel) || [];
  }
}

test("RedisRealtimeBus.publish sends message", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  await bus.publish("test-channel", { data: "hello" });

  const messages = publisher.getMessages("test-channel");
  assert.equal(messages.length, 1);
  assert.deepEqual(JSON.parse(messages[0]), { data: "hello" });
});

test("RedisRealtimeBus.subscribe registers handler", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  let receivedPayload: unknown;
  bus.subscribe("test-channel", (payload) => {
    receivedPayload = payload;
  });

  await bus.publish("test-channel", { data: "hello" });

  assert.deepEqual(receivedPayload, { data: "hello" });
});

test("RedisRealtimeBus.subscribe returns unsubscribe function", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  let callCount = 0;
  const unsubscribe = bus.subscribe("test-channel", () => {
    callCount++;
  });

  await bus.publish("test-channel", { msg: "1" });
  assert.equal(callCount, 1);

  unsubscribe();

  await bus.publish("test-channel", { msg: "2" });
  assert.equal(callCount, 1); // Should not increase after unsubscribe
});

test("RedisRealtimeBus handles multiple subscribers", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  const results: unknown[] = [];

  bus.subscribe("channel", (payload) => {
    results.push(payload);
  });
  bus.subscribe("channel", (payload) => {
    results.push(payload);
  });

  await bus.publish("channel", { data: "test" });

  assert.equal(results.length, 2);
  assert.deepEqual(results[0], { data: "test" });
  assert.deepEqual(results[1], { data: "test" });
});

test("RedisRealtimeBus duplicates publisher for subscriber", async () => {
  let duplicateCalled = false;
  const mockPublisher: any = {
    publish: async () => 1,
    duplicate: () => {
      duplicateCalled = true;
      return new MockSubscriber();
    },
    quit: async () => {},
    status: "ready",
  };

  const bus = new RedisRealtimeBus(mockPublisher);

  bus.subscribe("channel", () => {});

  assert.ok(duplicateCalled);
});

test("RedisRealtimeBus applies channel prefix", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any, "app:");

  await bus.publish("events", { data: "test" });

  const messages = publisher.getMessages("app:events");
  assert.ok(messages.length > 0);
});

test("RedisRealtimeBus.close cleans up resources", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  bus.subscribe("channel", () => {});
  await bus.close();

  // Should throw after close
  assert.rejects(
    async () => {
      await bus.publish("channel", { data: "test" });
    },
    { message: /closed/i }
  );
});

test("RedisRealtimeBus parses JSON messages", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  let receivedPayload: unknown;
  bus.subscribe("channel", (payload) => {
    receivedPayload = payload;
  });

  await bus.publish("channel", { nested: { value: 42 } });

  assert.deepEqual(receivedPayload, { nested: { value: 42 } });
});

test("RedisRealtimeBus handles different channels independently", async () => {
  const publisher = new MockPublisher();
  const bus = new RedisRealtimeBus(publisher as any);

  const ch1Results: unknown[] = [];
  const ch2Results: unknown[] = [];

  bus.subscribe("channel1", (p) => ch1Results.push(p));
  bus.subscribe("channel2", (p) => ch2Results.push(p));

  await bus.publish("channel1", { id: 1 });
  await bus.publish("channel2", { id: 2 });

  assert.deepEqual(ch1Results, [{ id: 1 }]);
  assert.deepEqual(ch2Results, [{ id: 2 }]);
});
