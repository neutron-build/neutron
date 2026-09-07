// SIGKILL-crash helper for postgres.live.test.ts. Forked as a child process:
// claims the "hang" job, signals the parent, then blocks forever while its
// heartbeat keeps the lease alive. The parent SIGKILLs it mid-job.
import { createPostgresQueueDriver } from "./postgres.js";

const url = process.env.NEUTRON_CRASH_HELPER_PG_URL;
const queue = process.env.NEUTRON_CRASH_HELPER_QUEUE;

if (!url || !queue) {
  throw new Error("NEUTRON_CRASH_HELPER_PG_URL and NEUTRON_CRASH_HELPER_QUEUE must be set");
}

const driver = await createPostgresQueueDriver({
  url,
  queueName: queue,
  pollIntervalMs: 50,
  batchSize: 1,
  leaseMs: 1000,
});

process.on("message", (message: unknown) => {
  if (message === "shutdown") {
    void driver.close();
  }
});

await driver.process("hang", (job) => {
  if (process.send) {
    process.send({ claimed: job.id });
  }
  return new Promise(() => {});
});
