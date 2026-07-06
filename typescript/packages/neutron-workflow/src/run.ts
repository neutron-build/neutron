import { Cancellation, ReplayContext, Suspension } from "./context.js";
import { parseDuration } from "./duration.js";
import { NondeterminismError, WorkflowError, problemFromStatus } from "./errors.js";
import type { ProblemDetails } from "./errors.js";
import type {
  RunCancelledData,
  RunCompletedData,
  RunFailedData,
  RunStartedData,
  WorkflowEvent,
} from "./events.js";
import { WIRE_FORMAT_VERSION } from "./events.js";
import type { EventStore } from "./store.js";
import type { WorkflowDefinition } from "./workflow.js";

export type RunStatus = "completed" | "failed" | "cancelled" | "sleeping" | "waiting" | "retrying";

export interface RunOutcome {
  status: RunStatus;
  output?: unknown;
  error?: ProblemDetails;
  /** ISO-8601 wake time when sleeping. */
  wakeAt?: string;
  /** Awaited event name when waiting. */
  eventName?: string;
}

export interface ExecuteRunOptions<In> {
  workflow: WorkflowDefinition<In, unknown>;
  runId: string;
  store: EventStore;
  /** Used only when the run's log is empty (first execution). */
  input?: In;
}

/**
 * One execution pass: replay the log from the top, run live from where it
 * ends, and return how the run stands. Crash-safe by construction — a pass
 * that dies mid-step leaves a log that the next pass replays and continues.
 * Terminal runs short-circuit idempotently. NondeterminismError is thrown
 * (not recorded): the run stays intact for re-execution once the code is
 * fixed, instead of being marked failed by a bad deploy.
 */
export async function executeRun<In>(options: ExecuteRunOptions<In>): Promise<RunOutcome> {
  const events = await options.store.load(options.runId);

  let input: In;
  if (events.length === 0) {
    const started: WorkflowEvent = {
      v: WIRE_FORMAT_VERSION,
      seq: 0,
      type: "run-started",
      at: new Date().toISOString(),
      data: {
        workflow: options.workflow.name,
        input: options.input === undefined ? null : options.input,
      } satisfies RunStartedData,
    };
    const stored = JSON.parse(JSON.stringify(started)) as WorkflowEvent;
    await options.store.append(options.runId, stored);
    events.push(stored);
    input = (stored.data as RunStartedData).input as In;
  } else {
    // Identity check comes before the terminal short-circuit: a completed
    // run queried under the wrong workflow must not leak another
    // workflow's output.
    const started = events.find((event) => event.type === "run-started");
    if (started === undefined) {
      throw new WorkflowError(problemFromStatus(500, `Run ${options.runId} has a log without run-started.`));
    }
    const data = started.data as RunStartedData;
    if (data.workflow !== options.workflow.name) {
      throw new NondeterminismError(
        `Run ${options.runId} belongs to workflow "${data.workflow}", not "${options.workflow.name}".`,
      );
    }
    input = data.input as In;
  }

  const terminal = events.find(
    (event) => event.type === "run-completed" || event.type === "run-failed" || event.type === "run-cancelled",
  );
  if (terminal !== undefined) return outcomeFromTerminal(terminal);

  const ctx = new ReplayContext(options.runId, options.store, events);

  // Total run budget: measured against the recorded start time, so it
  // holds across any number of suspensions, executors, and restarts.
  const budget = options.workflow.options?.timeout;
  if (budget !== undefined) {
    const started = events.find((event) => event.type === "run-started")!;
    if (Date.parse(started.at) + parseDuration(budget) <= Date.now()) {
      const detail = `Run exceeded its ${String(budget)} budget.`;
      await ctx.append("run-failed", undefined, { error: { message: detail } } satisfies RunFailedData);
      return { status: "failed", error: problemFromStatus(500, detail) };
    }
  }

  try {
    const output = await options.workflow.run(ctx, input);
    const leftover = ctx.leftoverCursorEvent();
    if (leftover !== null) {
      throw new NondeterminismError(
        `Workflow returned but the log still has a recorded "${leftover.type}"` +
          `${leftover.name !== undefined ? ` "${leftover.name}"` : ""} (seq ${leftover.seq}) — code removed operations.`,
      );
    }
    const stored = await ctx.append("run-completed", undefined, {
      output: output === undefined ? null : output,
    });
    return { status: "completed", output: (stored.data as RunCompletedData).output };
  } catch (thrown) {
    if (thrown instanceof Cancellation) {
      // run-cancelled is already in the log (cancelRun wrote it); this
      // pass just stops cleanly. The next pass short-circuits terminally.
      const outcome: RunOutcome = { status: "cancelled" };
      if (thrown.reason !== null) outcome.error = problemFromStatus(499, thrown.reason);
      return outcome;
    }
    if (thrown instanceof Suspension) {
      if (thrown.reason === "sleeping" || thrown.reason === "retrying") {
        const outcome: RunOutcome = { status: thrown.reason };
        if (thrown.wakeAt !== undefined) outcome.wakeAt = thrown.wakeAt;
        return outcome;
      }
      const outcome: RunOutcome = { status: "waiting" };
      if (thrown.eventName !== undefined) outcome.eventName = thrown.eventName;
      return outcome;
    }
    if (thrown instanceof NondeterminismError) throw thrown;

    const problem =
      thrown instanceof WorkflowError
        ? thrown.problem
        : problemFromStatus(500, thrown instanceof Error ? thrown.message : String(thrown));
    await ctx.append("run-failed", undefined, { error: { message: problem.detail } } satisfies RunFailedData);
    return { status: "failed", error: problem };
  }
}

/**
 * Cancel a run: appends the terminal run-cancelled event. Suspended runs
 * settle on their next execution pass; a run mid-execution stops at its
 * next live step (the pass's cancellation point). Idempotent — cancelling
 * a cancelled run is a no-op; completed/failed runs 409.
 */
export async function cancelRun(store: EventStore, runId: string, reason?: string): Promise<void> {
  const events = await store.load(runId);
  if (events.length === 0) {
    throw new WorkflowError(problemFromStatus(404, `Unknown run: ${runId}.`));
  }
  if (events.some((event) => event.type === "run-cancelled")) return;
  if (events.some((event) => event.type === "run-completed" || event.type === "run-failed")) {
    throw new WorkflowError(problemFromStatus(409, `Run ${runId} already finished.`));
  }
  const raw: WorkflowEvent = {
    v: WIRE_FORMAT_VERSION,
    seq: events.reduce((max, event) => Math.max(max, event.seq), -1) + 1,
    type: "run-cancelled",
    at: new Date().toISOString(),
    data: { reason: reason ?? null } satisfies RunCancelledData,
  };
  await store.append(runId, JSON.parse(JSON.stringify(raw)) as WorkflowEvent);
}

/** Signal a suspended run: buffered until its waitForEvent(name) consumes it. */
export async function deliverEvent(
  store: EventStore,
  runId: string,
  name: string,
  payload?: unknown,
): Promise<void> {
  const events = await requireLiveRun(store, runId);
  await appendExternal(store, runId, events, "event-received", name, {
    payload: payload === undefined ? null : payload,
  });
}

/** Wake the run's oldest pending sleep — the scheduler's primitive. */
export async function completeSleep(store: EventStore, runId: string): Promise<void> {
  const events = await requireLiveRun(store, runId);
  const started = events.filter((event) => event.type === "sleep-started").length;
  const completed = events.filter((event) => event.type === "sleep-completed").length;
  if (completed >= started) {
    throw new WorkflowError(problemFromStatus(409, `Run ${runId} has no pending sleep.`));
  }
  await appendExternal(store, runId, events, "sleep-completed", undefined, undefined);
}

async function requireLiveRun(store: EventStore, runId: string): Promise<WorkflowEvent[]> {
  const events = await store.load(runId);
  if (events.length === 0) {
    throw new WorkflowError(problemFromStatus(404, `Unknown run: ${runId}.`));
  }
  if (
    events.some(
      (event) =>
        event.type === "run-completed" || event.type === "run-failed" || event.type === "run-cancelled",
    )
  ) {
    throw new WorkflowError(problemFromStatus(409, `Run ${runId} already finished.`));
  }
  return events;
}

async function appendExternal(
  store: EventStore,
  runId: string,
  events: WorkflowEvent[],
  type: "event-received" | "sleep-completed",
  name: string | undefined,
  data: unknown,
): Promise<void> {
  const raw: WorkflowEvent = {
    v: WIRE_FORMAT_VERSION,
    seq: events.reduce((max, event) => Math.max(max, event.seq), -1) + 1,
    type,
    at: new Date().toISOString(),
  };
  if (name !== undefined) raw.name = name;
  if (data !== undefined) raw.data = data;
  await store.append(runId, JSON.parse(JSON.stringify(raw)) as WorkflowEvent);
}

function outcomeFromTerminal(event: WorkflowEvent): RunOutcome {
  if (event.type === "run-completed") {
    return { status: "completed", output: (event.data as RunCompletedData).output };
  }
  if (event.type === "run-cancelled") {
    const reason = (event.data as RunCancelledData | undefined)?.reason ?? null;
    const outcome: RunOutcome = { status: "cancelled" };
    if (reason !== null) outcome.error = problemFromStatus(499, reason);
    return outcome;
  }
  return {
    status: "failed",
    error: problemFromStatus(500, (event.data as RunFailedData).error.message),
  };
}
