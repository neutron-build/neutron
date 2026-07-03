import type { WorkflowEvent } from "./events.js";

/**
 * The durable substrate boundary. The Nucleus implementation maps append
 * to STREAM_XADD on `wf:{runId}` and load to XRANGE-from-zero; this
 * in-memory implementation is the M1 driver and the unit-test workhorse.
 *
 * Contract: load returns events seq-ordered and deduped by seq (first
 * writer wins) — the property that keeps replay consistent when two
 * executors briefly race a lease.
 */
export interface EventStore {
  append(runId: string, event: WorkflowEvent): Promise<void>;
  load(runId: string): Promise<WorkflowEvent[]>;
}

export class MemoryEventStore implements EventStore {
  #runs = new Map<string, WorkflowEvent[]>();

  async append(runId: string, event: WorkflowEvent): Promise<void> {
    const events = this.#runs.get(runId) ?? [];
    events.push(event);
    this.#runs.set(runId, events);
  }

  async load(runId: string): Promise<WorkflowEvent[]> {
    const events = this.#runs.get(runId) ?? [];
    const seen = new Set<number>();
    const deduped: WorkflowEvent[] = [];
    for (const event of [...events].sort((a, b) => a.seq - b.seq)) {
      if (seen.has(event.seq)) continue;
      seen.add(event.seq);
      deduped.push(event);
    }
    return deduped;
  }
}
