# @neutron-build/workflow

Durable workflow engine for Neutron. Event-sourced replay: workflows that
suspend mid-run, survive deploys and crashes, and resume exactly where
they left off — days or weeks later, with no process alive in between.

```ts
import { workflow, executeRun, deliverEvent, MemoryEventStore } from "@neutron-build/workflow";

const analyze = workflow("analyze-dataset", async (ctx, input: { url: string }) => {
  const data = await ctx.step("fetch", () => fetchDataset(input.url));
  const model = await ctx.step("train", () => train(data));

  await ctx.sleep("7d"); // nothing runs for 7 days; nothing is lost

  const approval = await ctx.waitForEvent<{ ok: boolean }>("approved");
  return ctx.step("publish", () => publish(model, approval));
});

const outcome = await executeRun({ workflow: analyze, runId, store, input: { url } });
// outcome.status: "completed" | "failed" | "sleeping" | "waiting"

// later, from a webhook or UI:
await deliverEvent(store, runId, "approved", { ok: true });
```

## How it works

A workflow function re-executes from the top on every resume. Completed
steps don't re-run — their recorded results replay from the event log.
The log is the only state; an executor that dies mid-run leaves a log the
next execution replays and continues. Two event families:

- **Cursor events** record the workflow's own operations in order
  (`step-completed`, `now`, `random`, `sleep-started`, `event-waiting`).
  Replay walks them one-by-one; any mismatch with the code is a
  `NondeterminismError` — thrown, not recorded, so a bad deploy never
  destroys a run (fix the code and execution continues).
- **External events** arrive while a run is suspended (`sleep-completed`
  from the scheduler, `event-received` from `deliverEvent`) and are
  buffered — an early signal waits for its `waitForEvent`.

## The determinism rule

Code between context calls must be deterministic: same log in, same path
out. All I/O and randomness go inside `ctx.step()`; use `ctx.now()` /
`ctx.random()` instead of `Date.now()` / `Math.random()`. Step results
must be JSON-serializable (live execution observes the post-JSON value,
so replay can never diverge). v1 workflows are sequential — no
`Promise.all` over context operations.

## Guarantees

- Log consistency and replay determinism: exactly-once.
- Step side effects: **at-least-once** (the industry-standard contract —
  a crashed executor's step may re-run if it died before the completion
  was durable; make steps idempotent where it matters).

## Running on Nucleus

```ts
import { createClient, withDocument, withKV, withStreams } from "@neutron-build/nucleus";
import { LeaseManager, NucleusEventStore, RunIndex, Scheduler, createEventsHandler } from "@neutron-build/workflow";

const db = await createClient({ url }).use(withStreams).use(withKV).use(withDocument).connect();
const store = new NucleusEventStore(db.streams);        // event log: one stream per run
const leases = new LeaseManager(db.kv);                 // atomic KV_SETNX ttl / KV_CDEL / KV_CEXPIRE
const index = new RunIndex(db.document);                // queryable run metadata

const scheduler = new Scheduler({ workflows: [analyze], store, leases, index, owner: hostname() });
scheduler.start();                                      // or call scheduler.tick() from a cron

// webhook surface for waitForEvent (any mode:"api" route):
export const POST = createEventsHandler({ store, index });
```

All three integrations are structurally typed — the package has zero
dependencies; a Nucleus client just fits. Leases make ticks idempotent,
so run as many scheduler processes as you like. Crash recovery is free:
a dead executor's lease expires and the next claimer replays the log.

## Agent approvals that survive weeks

With `@neutron-build/ai` installed (optional peer):

```ts
import { agentStep, approvalEventName } from "@neutron-build/workflow/ai";

const wf = workflow("release", async (ctx) => {
  const result = await agentStep(ctx, "agent", {
    model, prompt: "ship the release", tools: [deploy], maxSteps: 8,
  });
  return result.text;
});
// A needsApproval tool parks the run (status "waiting"). Days later:
// deliverEvent(store, runId, approvalEventName("agent", 0), [{ toolCallId, approved: true }])
// resumes the loop with the decision — no process ran in between.
```

Steps take retry and timeout budgets:

```ts
await ctx.step("charge", chargeCard, { retries: 3, retryDelay: "10m", timeout: "30s" });
// retryDelay parks the run durably between attempts; replay consumes
// recorded failed attempts and never re-runs what succeeded.
const wf = workflow("job", fn, { timeout: "30d" }); // total run budget
```

## Status

All five milestones complete: authoring API + replay engine (M1),
Nucleus event store + atomic executor leases (M2), wake scheduler +
events webhook + gated live suite (M3), retries/timeouts + the AI SDK
approval bridge (M4), and the language-neutral wire format published as
FRAMEWORK_CONTRACT.md §9 (M5).
