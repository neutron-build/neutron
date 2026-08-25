import assert from "node:assert/strict";
import test from "node:test";
import { InMemoryRealtimeBus } from "./index.js";

test("a throwing subscriber does not block delivery to its siblings or reject publish", async () => {
  // Same contract as the Redis bus: each handler is isolated — one bad
  // subscriber logs and the rest still receive the payload. The in-memory
  // variant used to let the throw escape publish() and skip every subscriber
  // registered after the failing one.
  const bus = new InMemoryRealtimeBus();
  const received: string[] = [];

  bus.subscribe("orders", () => {
    throw new Error("subscriber blew up");
  });
  bus.subscribe("orders", (payload) => {
    received.push(`second:${String((payload as { id: string }).id)}`);
  });
  bus.subscribe("orders", () => {
    received.push("third");
  });

  await bus.publish("orders", { id: "o-1" });

  assert.deepEqual(received, ["second:o-1", "third"]);
});
