// Live integration against a real Nucleus: set NUCLEUS_TEST_URL (or
// DATABASE_URL) to run; skipped otherwise. Exercises the exact structural
// seams the fakes stand in for: NucleusEventStore over Streams, leases
// over the atomic KV primitives, RunIndex over Document.
import assert from "node:assert/strict";
import { test } from "node:test";

import { LeaseManager, executeRunExclusive } from "./lease.js";
import { NucleusEventStore } from "./nucleus-store.js";
import { completeSleep, executeRun } from "./run.js";
import { RunIndex, Scheduler } from "./scheduler.js";
import { workflow } from "./workflow.js";

const url = process.env.NUCLEUS_TEST_URL ?? process.env.DATABASE_URL;
const skip = url === undefined ? "set NUCLEUS_TEST_URL to run live Nucleus tests" : false;

test("workflow suspends and resumes on a real Nucleus", { skip }, async () => {
  const { createClient, withDocument, withKV, withStreams } = await import("@neutron-build/nucleus");
  const db = await createClient({ url: url! }).use(withStreams).use(withKV).use(withDocument).connect();
  try {
    const runId = `it-${crypto.randomUUID()}`;
    const store = new NucleusEventStore(db.streams, { prefix: `wf_it` });
    const leases = new LeaseManager(db.kv, { prefix: "wf_it:lease", ttlSeconds: 30 });
    const index = new RunIndex(db.document, { collection: "wf_it_runs" });

    let executions = 0;
    const wf = workflow("it-order", async (ctx) => {
      const value = await ctx.step("work", () => {
        executions += 1;
        return { n: 42 };
      });
      await ctx.sleep("1h");
      return ctx.step("finish", () => `done:${value.n}`);
    });

    // first pass under a lease: runs the step, parks on the sleep
    const first = await executeRunExclusive({ workflow: wf, runId, store, input: null, leases, owner: "it-a" });
    assert.equal(first?.status, "sleeping");
    await index.record(runId, wf.name, first!);

    // a contender while held is skipped
    const held = await leases.acquire(runId, "it-b");
    assert.ok(held);
    assert.equal(await executeRunExclusive({ workflow: wf, runId, store, leases, owner: "it-c" }), null);
    assert.equal(await held!.release(), true);

    // wake and finish via the scheduler path
    await completeSleep(store, runId);
    const scheduler = new Scheduler({ workflows: [wf], store, leases, index, owner: "it-d" });
    await index.markWake(runId);
    await scheduler.tick();

    const outcome = await executeRun({ workflow: wf, runId, store });
    assert.equal(outcome.status, "completed");
    assert.equal(outcome.output, "done:42");
    assert.equal(executions, 1); // replay never re-ran the step
  } finally {
    await db.close();
  }
});
