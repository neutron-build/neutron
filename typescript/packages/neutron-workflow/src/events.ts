/**
 * The event-log wire format — versioned and language-neutral from day one
 * (the multi-language SDK contract artifact). Every field is JSON; nothing
 * in replay branches on `at` (append wall-clock, informational only).
 *
 * Two families:
 * - CURSOR events record the workflow's own deterministic operations in
 *   execution order: step-completed/step-failed, now, random,
 *   sleep-started, event-waiting. Replay walks them one-by-one and any
 *   mismatch with the code is nondeterminism.
 * - EXTERNAL events are appended from outside a suspended run:
 *   sleep-completed (scheduler) and event-received (deliverEvent). Replay
 *   rebuilds them into FIFO buffers (per name for events), so an early
 *   signal is buffered until the workflow reaches its waitForEvent.
 *
 * `seq` is per-run, strictly increasing, assigned at append. Storage may
 * deliver duplicates under executor races (KV setNX has no atomic TTL, so
 * leases have a bounded race window); loads dedupe by seq keeping the
 * first writer. Consequence: step effects are at-least-once — the log and
 * replay stay consistent even if two executors briefly overlap.
 */

export const WIRE_FORMAT_VERSION = 1;

/**
 * External events append from outside a live pass (deliverEvent,
 * completeSleep, cancelRun) and can land mid-pass. A pass allocates seqs
 * densely from its own counter, so an external append computing max+1 from a
 * fresh load wrote the SAME seq as the pass's next append — and load()'s
 * dedupe (first writer wins, for lease races) silently deleted one of two
 * DIFFERENT events, bricking the next replay with NondeterminismError. The
 * fix is a partition: pass events stay dense below this base (2^40 — no run
 * log ever reaches a trillion events, and legacy dense logs resume unchanged
 * since their seqs all sort below it), external events allocate at or above
 * it. Neither writer can ever take the other's seq.
 */
export const EXTERNAL_SEQ_BASE = 2 ** 40;

export type WorkflowEventType =
  | "run-started"
  | "step-completed"
  | "step-failed"
  | "now"
  | "random"
  | "sleep-started"
  | "sleep-completed"
  | "event-waiting"
  | "event-received"
  | "run-completed"
  | "run-failed"
  | "run-cancelled";

export interface WorkflowEvent {
  v: number;
  seq: number;
  type: WorkflowEventType;
  /** ISO-8601 append time. Informational; never used by replay. */
  at: string;
  /** Step name, event name, or sleep label. */
  name?: string;
  data?: unknown;
}

export interface RunStartedData {
  workflow: string;
  input: unknown;
}

export interface StepCompletedData {
  result: unknown;
}

export interface StepFailedData {
  error: { message: string };
  attempt: number;
}

export interface SleepStartedData {
  /** ISO-8601 wake time, computed once at live execution. */
  until: string;
}

export interface EventReceivedData {
  payload: unknown;
}

export interface RunCompletedData {
  output: unknown;
}

export interface RunCancelledData {
  reason: string | null;
}

export interface RunFailedData {
  error: { message: string };
}

/** Events replayed at the deterministic cursor, in execution order. */
export function isCursorEvent(event: WorkflowEvent): boolean {
  return (
    event.type === "step-completed" ||
    event.type === "step-failed" ||
    event.type === "now" ||
    event.type === "random" ||
    event.type === "sleep-started" ||
    event.type === "event-waiting"
  );
}

/** Events appended from outside the run while it is suspended. */
export function isExternalEvent(event: WorkflowEvent): boolean {
  return (
    event.type === "sleep-completed" ||
    event.type === "event-received" ||
    event.type === "run-cancelled"
  );
}
