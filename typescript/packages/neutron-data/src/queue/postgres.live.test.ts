import assert from "node:assert/strict";
import test from "node:test";
import { fork, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { createPostgresQueueDriver, type PostgresQueueDriver } from "./postgres.js";

// Live conformance suite for the Postgres queue driver, per the validation
// checklist in docs/design/DURABLE_EXECUTION_DESIGN.md. Runs against real
// Postgres when reachable and skips otherwise. Start one with:
//   podman run -d --name neutron-test-pg -e POSTGRES_PASSWORD=test \
//     -p 5434:5432 postgres:16-alpine
// Override the target with NEUTRON_TEST_PG_URL (defaults to that container).

const PG_URL = process.env.NEUTRON_TEST_PG_URL || "postgres://postgres:test@127.0.0.1:5434/postgres";
const HELPER_PATH = fileURLToPath(new URL("./postgres.test.helper.js", import.meta.url));

type RawSql = {
  unsafe(query: string, params?: unknown[]): Promise<any[]>;
  end(): Promise<void>;
};

let reachableProbe: Promise<boolean> | null = null;

function pgReachable(): Promise<boolean> {
  if (!reachableProbe) {
    reachableProbe = (async () => {
      try {
        const mod = await import("postgres");
        const sql = mod.default(PG_URL, { connect_timeout: 2, max: 1 });
        await sql.unsafe("select 1");
        await sql.end();
        return true;
      } catch {
        return false;
      }
    })();
  }
  return reachableProbe;
}

async function openRaw(): Promise<RawSql> {
  const mod = await import("postgres");
  return mod.default(PG_URL, { max: 2 }) as unknown as RawSql;
}

function uniqueQueue(): string {
  return `live_${randomUUID().slice(0, 8)}`;
}

async function waitFor(
  check: () => Promise<boolean>,
  timeoutMs: number,
  what: string
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.fail(`timed out after ${timeoutMs}ms waiting for ${what}`);
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

test("conformance 1: two concurrent workers drain N jobs exactly once", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start the test container to run live conformance`);
  }

  const queue = uniqueQueue();
  const N = 50;
  const workerA = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 50,
    batchSize: 5,
  });
  const workerB = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 50,
    batchSize: 5,
  });

  const counts = new Map<string, number>();
  const record = (id: string): void => {
    counts.set(id, (counts.get(id) ?? 0) + 1);
  };
  await workerA.process("race", (job) => {
    record(job.id);
  });
  await workerB.process("race", (job) => {
    record(job.id);
  });

  for (let i = 0; i < N; i += 1) {
    await workerA.add("race", { i });
  }

  await waitFor(async () => counts.size === N, 30_000, `all ${N} jobs to be processed`);
  await sleep(1500);

  const duplicates = [...counts.entries()].filter(([, count]) => count !== 1);
  const total = [...counts.values()].reduce((sum, count) => sum + count, 0);
  assert.equal(counts.size, N, "every enqueued job must have been processed");
  assert.equal(total, N, "total deliveries must equal N (exactly-once)");
  assert.deepEqual(duplicates, [], "no job may be delivered more than once");

  const raw = await openRaw();
  const unfinished = await raw.unsafe(
    `SELECT count(*)::int AS n FROM neutron_jobs WHERE queue = $1 AND status IN ('pending', 'active')`,
    [queue]
  );
  assert.equal(unfinished[0].n, 0, "no job may be lost");
  await raw.end();

  await workerA.close();
  await workerB.close();
});

test("conformance 2: a SIGKILLed worker's job is redelivered after lease expiry", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start the test container to run live conformance`);
  }

  const queue = uniqueQueue();
  const producer = await createPostgresQueueDriver({ url: PG_URL, queueName: queue });
  const job = await producer.add("hang", { stuck: true });
  await producer.close();

  const child = fork(HELPER_PATH, [], {
    env: {
      ...process.env,
      NEUTRON_CRASH_HELPER_PG_URL: PG_URL,
      NEUTRON_CRASH_HELPER_QUEUE: queue,
    },
    stdio: ["inherit", "inherit", "inherit", "ipc"],
  });

  const claimedId = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("helper never claimed the job")), 20_000);
    child.on("message", (message: { claimed?: string }) => {
      if (message?.claimed) {
        clearTimeout(timer);
        resolve(message.claimed);
      }
    });
    child.on("exit", (code, signal) => {
      clearTimeout(timer);
      reject(new Error(`helper exited before claiming: code=${code} signal=${signal}`));
    });
  });
  assert.equal(claimedId, job.id);

  child.kill("SIGKILL");
  await new Promise((resolve) => child.once("exit", resolve));

  const worker = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 50,
    batchSize: 1,
    leaseMs: 1000,
  });
  const delivered: string[] = [];
  await worker.process("hang", (deliveredJob) => {
    delivered.push(deliveredJob.id);
  });

  await waitFor(async () => delivered.includes(job.id), 30_000, "redelivery after lease expiry");
  assert.equal(delivered.filter((id) => id === job.id).length, 1, "exactly one redelivery");

  const raw = await openRaw();
  const row = await raw.unsafe(
    `SELECT status FROM neutron_jobs WHERE id = $1::uuid`,
    [job.id]
  );
  assert.equal(row[0].status, "done");
  await raw.end();

  await worker.close();
});

test("conformance 3: enqueued jobs survive a full client restart", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start the test container to run live conformance`);
  }

  const queue = uniqueQueue();
  const producer = await createPostgresQueueDriver({ url: PG_URL, queueName: queue });
  const job = await producer.add("survive", { ok: true });
  await producer.close();

  const raw = await openRaw();
  const parked = await raw.unsafe(
    `SELECT status FROM neutron_jobs WHERE id = $1::uuid`,
    [job.id]
  );
  assert.equal(parked[0].status, "pending", "job must be parked pending across the restart");
  await raw.end();

  const consumer = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 50,
  });
  const got: string[] = [];
  await consumer.process("survive", (deliveredJob) => {
    got.push(deliveredJob.id);
  });

  await waitFor(async () => got.length === 1, 15_000, "the restarted worker to pick the job up");
  assert.deepEqual(got, [job.id]);
  await consumer.close();
});

test("conformance 4: schedules fire per pattern, missed windows do not multi-fire, unschedule stops", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start the test container to run live conformance`);
  }

  const queue = uniqueQueue();
  const driver = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 100,
  });
  const raw = await openRaw();
  const firedCount = async (): Promise<number> => {
    const rows = await raw.unsafe(
      `SELECT count(*)::int AS n FROM neutron_jobs WHERE queue = $1 AND name = 'tick'`,
      [queue]
    );
    return rows[0].n;
  };

  try {
    await driver.process("tick", () => {});
    await driver.schedule("tick", "*/2 * * * * *", { seq: true });

    await waitFor(async () => (await firedCount()) >= 2, 15_000, "the cron to fire across boundaries");
    const beforeDrift = await firedCount();
    assert.ok(beforeDrift >= 2, `expected >= 2 fires, got ${beforeDrift}`);

    // Simulate 15 missed 2s windows: exactly one catch-up run may materialize.
    await raw.unsafe(
      `UPDATE neutron_schedules SET next_run_at = now() - interval '30 seconds'
       WHERE queue = $1 AND name = 'tick'`,
      [queue]
    );
    await sleep(1000);
    const afterDrift = await firedCount();
    assert.equal(afterDrift, beforeDrift + 1, "a missed window must produce one run, not N catch-ups");

    const schedule = await raw.unsafe(
      `SELECT next_run_at FROM neutron_schedules WHERE queue = $1 AND name = 'tick'`,
      [queue]
    );
    assert.ok(
      schedule[0].next_run_at instanceof Date && schedule[0].next_run_at.getTime() > Date.now() - 100,
      "next_run_at must have been advanced to a future occurrence"
    );

    await driver.unschedule("tick");
    await sleep(5000);
    assert.equal(await firedCount(), afterDrift, "unschedule must stop all further fires");

    const remaining = await raw.unsafe(
      `SELECT count(*)::int AS n FROM neutron_schedules WHERE queue = $1`,
      [queue]
    );
    assert.equal(remaining[0].n, 0);
  } finally {
    await driver.close();
    await raw.end();
  }
});

test("conformance 5: an always-failing job dead-letters at max_attempts with last_error", async (t) => {
  if (!(await pgReachable())) {
    return t.skip(`Postgres not reachable at ${PG_URL} — start the test container to run live conformance`);
  }

  const queue = uniqueQueue();
  const driver = await createPostgresQueueDriver({
    url: PG_URL,
    queueName: queue,
    pollIntervalMs: 50,
    maxAttempts: 3,
    backoffBaseMs: 10,
    leaseMs: 5000,
  });
  let calls = 0;
  await driver.process("boom", () => {
    calls += 1;
    throw new Error("kapow");
  });
  const job = await driver.add("boom", { x: 1 });

  const raw = await openRaw();
  await waitFor(
    async () => {
      const rows = await raw.unsafe(`SELECT status FROM neutron_jobs WHERE id = $1::uuid`, [job.id]);
      return rows[0].status === "dead";
    },
    20_000,
    "the job to reach dead"
  );

  await sleep(1500);
  assert.equal(calls, 3, "handler must run exactly max_attempts times");

  const row = await raw.unsafe(
    `SELECT status, attempts, max_attempts, last_error FROM neutron_jobs WHERE id = $1::uuid`,
    [job.id]
  );
  assert.equal(row[0].status, "dead");
  assert.equal(row[0].attempts, 3);
  assert.equal(row[0].max_attempts, 3);
  assert.equal(row[0].last_error, "kapow");
  assert.equal(calls, 3, "a dead job must not be requeued after dead-lettering");

  await raw.end();
  await driver.close();
});
