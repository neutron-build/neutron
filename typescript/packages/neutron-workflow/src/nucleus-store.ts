import { WorkflowError, problemFromStatus } from "./errors.js";
import type { WorkflowEvent } from "./events.js";
import type { EventStore } from "./store.js";

/**
 * Structurally matches @neutron-build/nucleus's StreamsModel — a Nucleus
 * client plugs in directly without this package depending on it.
 */
export interface StreamsLike {
  xadd(stream: string, fields: Record<string, unknown>): Promise<string>;
  xrange(
    stream: string,
    startMs: number,
    endMs: number,
    count: number,
  ): Promise<Array<{ id: string; fields: Record<string, unknown> }>>;
}

/**
 * The durable event log: one Nucleus stream per run (`wf:{runId}`), one
 * JSON event per entry. Loads dedupe by seq keeping the FIRST writer in
 * stream order — the property that keeps replay consistent if two
 * executors ever race a lease.
 */
export class NucleusEventStore implements EventStore {
  #streams: StreamsLike;
  #prefix: string;

  constructor(streams: StreamsLike, options: { prefix?: string } = {}) {
    this.#streams = streams;
    this.#prefix = options.prefix ?? "wf";
  }

  #stream(runId: string): string {
    return `${this.#prefix}:${runId}`;
  }

  async append(runId: string, event: WorkflowEvent): Promise<void> {
    await this.#streams.xadd(this.#stream(runId), { event: JSON.stringify(event) });
  }

  async load(runId: string): Promise<WorkflowEvent[]> {
    const entries = await this.#streams.xrange(this.#stream(runId), 0, Number.MAX_SAFE_INTEGER, 1_000_000);
    const seen = new Set<number>();
    const events: WorkflowEvent[] = [];
    for (const entry of entries) {
      const raw = entry.fields.event;
      if (typeof raw !== "string") continue;
      let event: WorkflowEvent;
      try {
        event = JSON.parse(raw) as WorkflowEvent;
      } catch {
        throw new WorkflowError(
          problemFromStatus(500, `Run ${runId} has a corrupt event log entry (${entry.id}).`),
        );
      }
      if (seen.has(event.seq)) continue;
      seen.add(event.seq);
      events.push(event);
    }
    return events.sort((a, b) => a.seq - b.seq);
  }
}
