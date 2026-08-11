import { parseDuration } from "./duration.js";
import { NondeterminismError, WorkflowError, problemFromStatus } from "./errors.js";
import type {
  EventReceivedData,
  SleepStartedData,
  StepCompletedData,
  StepFailedData,
  WorkflowEvent,
  WorkflowEventType,
} from "./events.js";
import { WIRE_FORMAT_VERSION, isCursorEvent } from "./events.js";
import type { EventStore } from "./store.js";

/**
 * What workflow code programs against. Every method is recorded to the
 * event log on first (live) execution and replayed from it thereafter —
 * code between calls must be deterministic; all I/O belongs inside step().
 *
 * v1 workflows are sequential: do not run context operations concurrently
 * (no Promise.all over steps). Parallel steps are a later, explicit feature.
 */
export interface StepOptions {
  /** Re-attempts after the first failure (default 0 — fail immediately). */
  retries?: number;
  /**
   * Delay before each retry ("30s" or ms). 0 (default) retries in-process;
   * anything longer parks the run durably and the scheduler resumes it —
   * a retry hours later costs nothing while parked.
   */
  retryDelay?: string | number;
  /** Per-attempt time budget; a timeout counts as a failed attempt. */
  timeout?: string | number;
}

export interface WorkflowContext {
  readonly runId: string;
  /** Run once, record the JSON-serializable result, replay it forever after. */
  step<T>(name: string, fn: () => T | Promise<T>, options?: StepOptions): Promise<T>;
  /** Suspend for a duration ("30s", "2h", "7d", or ms). No process stays alive. */
  sleep(duration: string | number): Promise<void>;
  /** Suspend until deliverEvent(runId, name) supplies a payload. Early deliveries are buffered. */
  waitForEvent<T = unknown>(name: string): Promise<T>;
  /** Deterministic Date.now(). */
  now(): Promise<number>;
  /** Deterministic Math.random(). */
  random(): Promise<number>;
}

/**
 * Control-flow signal thrown when a live step is about to execute on a
 * run that has been cancelled (a run-cancelled event landed in the log
 * while this pass was executing). Same non-Error contract as Suspension.
 */
export class Cancellation {
  readonly reason: string | null;

  constructor(reason: string | null) {
    this.reason = reason;
  }
}

/**
 * Control-flow signal, not an error: the run parks until the scheduler or
 * an event wakes it. Deliberately does NOT extend Error — never swallow
 * unknown throws in workflow code (rethrow anything you didn't expect).
 */
export class Suspension {
  readonly reason: "sleeping" | "waiting" | "retrying";
  readonly wakeAt?: string;
  readonly eventName?: string;

  constructor(reason: "sleeping" | "waiting" | "retrying", detail: { wakeAt?: string; eventName?: string }) {
    this.reason = reason;
    if (detail.wakeAt !== undefined) this.wakeAt = detail.wakeAt;
    if (detail.eventName !== undefined) this.eventName = detail.eventName;
  }
}

export function isSuspension(value: unknown): value is Suspension {
  return value instanceof Suspension;
}

/** A step's recorded failure, thrown identically on live execution and every replay. */
export class StepError extends WorkflowError {
  readonly stepName: string;

  constructor(stepName: string, message: string, options?: { cause?: unknown }) {
    super(
      {
        type: "https://neutron.dev/errors/workflow-step-failed",
        title: "Workflow Step Failed",
        status: 500,
        detail: `Step "${stepName}" failed: ${message}`,
      },
      options,
    );
    this.name = "StepError";
    this.stepName = stepName;
  }
}

export class ReplayContext implements WorkflowContext {
  readonly runId: string;
  #store: EventStore;
  #cursor: WorkflowEvent[];
  #cursorIndex = 0;
  #sleepCompletions: number;
  #sleepsConsumed = 0;
  #eventBuffers = new Map<string, unknown[]>();
  #nextSeq: number;

  constructor(runId: string, store: EventStore, events: WorkflowEvent[]) {
    this.runId = runId;
    this.#store = store;
    this.#cursor = events.filter(isCursorEvent);
    this.#sleepCompletions = events.filter((event) => event.type === "sleep-completed").length;
    for (const event of events) {
      if (event.type === "event-received" && event.name !== undefined) {
        const buffer = this.#eventBuffers.get(event.name) ?? [];
        buffer.push((event.data as EventReceivedData).payload);
        this.#eventBuffers.set(event.name, buffer);
      }
    }
    this.#nextSeq = events.reduce((max, event) => Math.max(max, event.seq), -1) + 1;
  }

  async step<T>(name: string, fn: () => T | Promise<T>, options: StepOptions = {}): Promise<T> {
    const retries = options.retries ?? 0;
    let attempt = 1;

    // Replay: consume this step's recorded attempts. Failed attempts within
    // the retry budget are swallowed (they were retried); a recorded failure
    // beyond the budget replays as the step's terminal error.
    for (;;) {
      const recorded = this.#nextCursor();
      if (recorded === null) break; // end of log — execute live from `attempt`
      if (recorded.type === "step-completed") {
        this.#checkName(recorded, name, "step");
        return (recorded.data as StepCompletedData).result as T;
      }
      if (recorded.type === "step-failed") {
        this.#checkName(recorded, name, "step");
        const data = recorded.data as StepFailedData;
        if (data.attempt > retries) {
          throw new StepError(name, data.error.message);
        }
        attempt = data.attempt + 1;
        continue;
      }
      throw this.#mismatch(`step "${name}"`, recorded);
    }

    // The live boundary is the cancellation point: a cancel that lands
    // mid-pass takes effect before the next side-effecting step runs,
    // never mid-step. Replayed steps above never touch the store.
    await this.#throwIfCancelled();

    for (;;) {
      try {
        const result = await this.#attemptWithTimeout(fn, options.timeout);
        const stored = await this.append("step-completed", name, {
          result: result === undefined ? null : result,
        });
        // Return the post-JSON value so live execution and replay see the same thing.
        return (stored.data as StepCompletedData).result as T;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        await this.append("step-failed", name, { error: { message }, attempt });
        if (attempt > retries) {
          throw new StepError(name, message, { cause: error });
        }
        attempt += 1;
        const delayMs = parseDuration(options.retryDelay ?? 0);
        if (delayMs === 0) continue; // in-process retry
        // Durable retry: park the run; the scheduler re-executes it after
        // the delay, replay consumes the failed attempts, and execution
        // resumes here at the next attempt.
        throw new Suspension("retrying", {
          wakeAt: new Date(Date.now() + delayMs).toISOString(),
        });
      }
    }
  }

  async #throwIfCancelled(): Promise<void> {
    const events = await this.#store.load(this.runId);
    const cancelled = events.find((event) => event.type === "run-cancelled");
    if (cancelled !== undefined) {
      throw new Cancellation((cancelled.data as { reason?: string | null } | undefined)?.reason ?? null);
    }
  }

  async #attemptWithTimeout<T>(fn: () => T | Promise<T>, timeout: string | number | undefined): Promise<T> {
    if (timeout === undefined) return fn();
    const ms = parseDuration(timeout);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        Promise.resolve().then(fn),
        new Promise<never>((_resolve, reject) => {
          // Deliberately NOT unref'd. An unref'd timer does not keep the event
          // loop alive, and a hung step is the case where this timer is the
          // only thing left pending — so the loop drained and the timeout could
          // never fire, leaving the run suspended forever instead of failing.
          // That is precisely the situation a step timeout exists for.
          // The `finally` below clears the timer as soon as the race settles,
          // so a normal step never holds the loop open either way.
          timer = setTimeout(
            () => reject(new WorkflowError(problemFromStatus(500, `Timed out after ${String(timeout)}.`))),
            ms,
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  async sleep(duration: string | number): Promise<void> {
    const ms = parseDuration(duration);
    const recorded = this.#nextCursor();
    let until: string;
    if (recorded !== null) {
      if (recorded.type !== "sleep-started") throw this.#mismatch("sleep", recorded);
      until = (recorded.data as SleepStartedData).until;
    } else {
      until = new Date(Date.now() + ms).toISOString();
      await this.append("sleep-started", undefined, { until });
    }
    if (this.#sleepsConsumed < this.#sleepCompletions) {
      this.#sleepsConsumed += 1;
      return;
    }
    throw new Suspension("sleeping", { wakeAt: until });
  }

  async waitForEvent<T = unknown>(name: string): Promise<T> {
    const recorded = this.#nextCursor();
    if (recorded !== null) {
      if (recorded.type !== "event-waiting") throw this.#mismatch(`waitForEvent "${name}"`, recorded);
      this.#checkName(recorded, name, "waitForEvent");
    } else {
      await this.append("event-waiting", name, undefined);
    }
    const buffer = this.#eventBuffers.get(name);
    if (buffer !== undefined && buffer.length > 0) {
      return buffer.shift() as T;
    }
    throw new Suspension("waiting", { eventName: name });
  }

  async now(): Promise<number> {
    const recorded = this.#nextCursor();
    if (recorded !== null) {
      if (recorded.type !== "now") throw this.#mismatch("now()", recorded);
      return (recorded.data as { value: number }).value;
    }
    const value = Date.now();
    await this.append("now", undefined, { value });
    return value;
  }

  async random(): Promise<number> {
    const recorded = this.#nextCursor();
    if (recorded !== null) {
      if (recorded.type !== "random") throw this.#mismatch("random()", recorded);
      return (recorded.data as { value: number }).value;
    }
    const value = Math.random();
    await this.append("random", undefined, { value });
    return value;
  }

  /** Non-null when the workflow returned while recorded operations remain — code removed ops. */
  leftoverCursorEvent(): WorkflowEvent | null {
    return this.#cursorIndex < this.#cursor.length ? this.#cursor[this.#cursorIndex]! : null;
  }

  /** Append with the next seq; returns the stored (JSON-normalized) event. */
  async append(type: WorkflowEventType, name: string | undefined, data: unknown): Promise<WorkflowEvent> {
    const raw: WorkflowEvent = {
      v: WIRE_FORMAT_VERSION,
      seq: this.#nextSeq,
      type,
      at: new Date().toISOString(),
    };
    if (name !== undefined) raw.name = name;
    if (data !== undefined) raw.data = data;
    // JSON round-trip up front: the durable store is JSON, so live
    // execution must observe exactly what replay will read back.
    const event = JSON.parse(JSON.stringify(raw)) as WorkflowEvent;
    await this.#store.append(this.runId, event);
    this.#nextSeq += 1;
    return event;
  }

  #nextCursor(): WorkflowEvent | null {
    if (this.#cursorIndex >= this.#cursor.length) return null;
    const event = this.#cursor[this.#cursorIndex]!;
    this.#cursorIndex += 1;
    return event;
  }

  #checkName(recorded: WorkflowEvent, expected: string, kind: string): void {
    if (recorded.name !== expected) {
      throw new NondeterminismError(
        `Replay mismatch: code ran ${kind} "${expected}" but the log recorded "${recorded.name ?? ""}" (seq ${recorded.seq}). ` +
          `Workflow code changed between deploys; restore the old shape or migrate the run.`,
      );
    }
  }

  #mismatch(operation: string, recorded: WorkflowEvent): NondeterminismError {
    return new NondeterminismError(
      `Replay mismatch: code ran ${operation} but the log recorded "${recorded.type}"` +
        `${recorded.name !== undefined ? ` "${recorded.name}"` : ""} (seq ${recorded.seq}). ` +
        `Workflow code changed between deploys; restore the old shape or migrate the run.`,
    );
  }
}
