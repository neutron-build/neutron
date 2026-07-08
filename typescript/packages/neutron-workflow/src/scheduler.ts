import type { LeaseManager } from "./lease.js";
import { executeRunExclusive } from "./lease.js";
import type { RunOutcome } from "./run.js";
import { completeSleep } from "./run.js";
import type { EventStore } from "./store.js";
import type { WorkflowDefinition } from "./workflow.js";

/**
 * Structurally matches @neutron-build/nucleus's DocumentModel surface the
 * run index needs. Nucleus filters are equality-only, so wake times are
 * ISO-8601 strings compared client-side (ISO sorts correctly as text).
 */
export interface DocumentLike {
  insert(collection: string, doc: Record<string, unknown>): Promise<number>;
  find(collection: string, filter: Record<string, unknown>): Promise<Record<string, unknown>[]>;
  update(
    collection: string,
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
  ): Promise<number>;
}

export interface RunRecord {
  runId: string;
  workflow: string;
  status: "sleeping" | "waiting" | "retrying" | "wake" | "completed" | "failed" | "cancelled";
  wakeAt?: string;
  eventName?: string;
  updatedAt: string;
}

/** Queryable run metadata — the scheduler's view; the event log stays the truth. */
export class RunIndex {
  #docs: DocumentLike;
  #collection: string;

  constructor(docs: DocumentLike, options: { collection?: string } = {}) {
    this.#docs = docs;
    this.#collection = options.collection ?? "wf_runs";
  }

  async record(runId: string, workflowName: string, outcome: RunOutcome): Promise<void> {
    const record: Record<string, unknown> = {
      runId,
      workflow: workflowName,
      status: outcome.status,
      updatedAt: new Date().toISOString(),
    };
    if (outcome.wakeAt !== undefined) record.wakeAt = outcome.wakeAt;
    if (outcome.eventName !== undefined) record.eventName = outcome.eventName;
    const updated = await this.#docs.update(this.#collection, { runId }, record);
    if (updated === 0) await this.#docs.insert(this.#collection, record);
  }

  /** Flag a run for immediate execution (an external event arrived). */
  async markWake(runId: string): Promise<void> {
    await this.#docs.update(this.#collection, { runId }, {
      status: "wake",
      updatedAt: new Date().toISOString(),
    });
  }

  /** Runs the scheduler should execute now: due sleepers, due retries, and flagged wakes. */
  async due(now: Date): Promise<Array<{ runId: string; sleeping: boolean }>> {
    const nowIso = now.toISOString();
    const sleeping = await this.#docs.find(this.#collection, { status: "sleeping" });
    const retrying = await this.#docs.find(this.#collection, { status: "retrying" });
    const woken = await this.#docs.find(this.#collection, { status: "wake" });
    const due: Array<{ runId: string; sleeping: boolean }> = [];
    for (const doc of sleeping) {
      if (typeof doc.runId === "string" && typeof doc.wakeAt === "string" && doc.wakeAt <= nowIso) {
        due.push({ runId: doc.runId, sleeping: true });
      }
    }
    // Retrying runs re-execute directly — there is no pending sleep to complete;
    // replay consumes the recorded failed attempts and re-runs the step.
    for (const doc of retrying) {
      if (typeof doc.runId === "string" && typeof doc.wakeAt === "string" && doc.wakeAt <= nowIso) {
        due.push({ runId: doc.runId, sleeping: false });
      }
    }
    for (const doc of woken) {
      if (typeof doc.runId === "string") due.push({ runId: doc.runId, sleeping: false });
    }
    return due;
  }
}

export interface SchedulerOptions {
  workflows: Array<WorkflowDefinition<never, unknown>>;
  store: EventStore;
  leases: LeaseManager;
  index: RunIndex;
  owner: string;
  /** Poll interval (default 15s). */
  intervalMs?: number;
  onError?: (runId: string, error: unknown) => void;
  /**
   * Tick-level failures (e.g. the run index is unreachable) that abort a
   * whole poll pass. Falls back to `onError` with run id "(tick)" when
   * absent. The interval keeps running either way.
   */
  onTickError?: (error: unknown) => void;
  /** Called just before a due run is executed this tick — for pickup logging. */
  onRunStart?: (runId: string) => void;
  /**
   * Called after a due run executes and produces a terminal-or-suspend
   * outcome — for completion logging/metrics. Not called for runs skipped
   * because another executor holds the lease (outcome null).
   */
  onComplete?: (runId: string, outcome: RunOutcome) => void;
}

/**
 * The wake loop: finds due runs, completes their pending sleep, executes
 * each under a lease, and re-records the outcome. Safe to run on many
 * machines at once — leases make ticks idempotent, and a run another
 * executor holds is simply skipped.
 */
export class Scheduler {
  #options: SchedulerOptions;
  #byName = new Map<string, WorkflowDefinition<never, unknown>>();
  #timer: ReturnType<typeof setInterval> | null = null;

  constructor(options: SchedulerOptions) {
    this.#options = options;
    for (const wf of options.workflows) this.#byName.set(wf.name, wf);
  }

  /** One pass; exposed for tests and for serverless cron-style invocation. */
  async tick(now = new Date()): Promise<void> {
    const due = await this.#options.index.due(now);
    for (const { runId, sleeping } of due) {
      try {
        const events = await this.#options.store.load(runId);
        const started = events.find((event) => event.type === "run-started");
        const name = (started?.data as { workflow?: string } | undefined)?.workflow;
        const wf = name !== undefined ? this.#byName.get(name) : undefined;
        if (wf === undefined) continue; // not this worker's workflow
        if (sleeping) await completeSleep(this.#options.store, runId);
        this.#options.onRunStart?.(runId);
        const outcome = await executeRunExclusive({
          workflow: wf,
          runId,
          store: this.#options.store,
          leases: this.#options.leases,
          owner: this.#options.owner,
        });
        if (outcome !== null) {
          await this.#options.index.record(runId, wf.name, outcome);
          this.#options.onComplete?.(runId, outcome);
        }
      } catch (error) {
        this.#options.onError?.(runId, error);
      }
    }
  }

  start(): void {
    if (this.#timer !== null) return;
    this.#timer = setInterval(() => {
      void this.tick().catch((error) => this.#reportTickError(error));
    }, this.#options.intervalMs ?? 15_000);
    this.#timer.unref?.();
  }

  /** Surface a tick-level failure without killing the poll loop. */
  #reportTickError(error: unknown): void {
    if (this.#options.onTickError !== undefined) this.#options.onTickError(error);
    else this.#options.onError?.("(tick)", error);
  }

  stop(): void {
    if (this.#timer !== null) clearInterval(this.#timer);
    this.#timer = null;
  }
}
