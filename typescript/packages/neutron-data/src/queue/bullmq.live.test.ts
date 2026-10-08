import assert from "node:assert/strict";
import test from "node:test";
import { Queue } from "bullmq";
import { createBullMqQueueDriver } from "./bullmq.js";

test("TSD-04 live BullMQ: unknown jobs survive worker restart and later registration", { timeout: 10000 }, async (t) => {
  const url = process.env.NEUTRON_TEST_REDIS_URL;
  if (!url) return t.skip("NEUTRON_TEST_REDIS_URL is not set");
  const name = `tsd04-${process.pid}-${Date.now()}`;
  const endpoint = new URL(url);
  const connection = { host: endpoint.hostname, port: Number(endpoint.port || 6379), db: Number(endpoint.pathname.slice(1) || 0), username: decodeURIComponent(endpoint.username) || undefined, password: decodeURIComponent(endpoint.password) || undefined };
  const queue = new Queue(name, { connection, prefix: "neutron" });
  let driver = await createBullMqQueueDriver({ url, queueName: name, concurrency: 1 });
  const wait = async (predicate: () => Promise<boolean>) => {
    const end = Date.now() + 4000;
    while (Date.now() < end) { if (await predicate()) return; await new Promise((r) => setTimeout(r, 20)); }
    assert.fail("BullMQ disposition timed out");
  };
  try {
    await driver.process("email", () => {});
    const job = await driver.add("video", { video: 1 });
    await wait(async () => (await queue.getJob(job.id))?.getState().then((state) => state === "delayed") ?? false);
    assert.equal((await queue.getJob(job.id))?.attemptsMade, 0);
    await driver.close();
    driver = await createBullMqQueueDriver({ url, queueName: name, concurrency: 1 });
    let handled = 0;
    await driver.process("video", () => { handled++; });
    await wait(async () => (await queue.getJob(job.id))?.getState().then((state) => state === "completed") ?? false);
    assert.equal(handled, 1);
  } finally {
    await driver.close();
    try { await queue.obliterate({ force: true }); } finally { await queue.close(); }
  }
});
