import { WorkflowError, problemFromStatus } from "./errors.js";
import type { WorkflowEvent } from "./events.js";
import type { KVLike } from "./lease.js";
import type { EventStore } from "./store.js";

/**
 * Structurally matches the porsager `postgres` query surface: a client,
 * connection, or transaction handle all fit, so appends can join caller
 * transactions. The client's call signature is generic with conditional
 * return types that do not reduce to a simpler one, so parameters are
 * `any[]` and results are opaque — the store treats every result as
 * `Record<string, unknown>[]`. (Transaction handles are callable at
 * runtime but their TYPE assigns to no simpler signature — cast those;
 * `postgres` is an optional peer dependency.)
 */
export interface PostgresLike {
  (strings: TemplateStringsArray, ...values: any[]): PromiseLike<unknown>;
}

/** Lease TTLs cap at int32 seconds (~68 years) — the KVLike "no TTL" case. */
const NEVER_EXPIRES_SECONDS = 2_147_483_647;

/**
 * The durable event log and executor leases on PostgreSQL wire semantics.
 * Events are keyed (run_id, seq) with append-only inserts; the primary key
 * IS the dedupe — a racing duplicate seq is silently dropped, first writer
 * wins, exactly what the EventStore contract requires replay consistency
 * for when two executors briefly overlap a lease. Run claiming implements
 * the KVLike lease primitives (setNX/cdel/cexpire) as one row per run, so
 * `new LeaseManager(pgStore)` gives the same heartbeat/steal semantics the
 * Nucleus KV driver has. All time math runs on the database clock.
 */
export class PostgresEventStore implements EventStore, KVLike {
  #sql: PostgresLike;
  #ready: Promise<void> | null = null;

  constructor(sql: PostgresLike) {
    this.#sql = sql;
  }

  /** Tagged call through the client, result typed as rows. */
  async #query(strings: TemplateStringsArray, ...values: unknown[]): Promise<Record<string, unknown>[]> {
    const result = await this.#sql(strings, ...values);
    return result as Record<string, unknown>[];
  }

  /** Apply the DDL idempotently. Memoized; awaited by every operation. */
  connect(): Promise<void> {
    this.#ready ??= this.#applyDdl();
    return this.#ready;
  }

  async #applyDdl(): Promise<void> {
    await this.#query`
      CREATE TABLE IF NOT EXISTS neutron_workflow_events (
        run_id text NOT NULL,
        seq bigint NOT NULL,
        event jsonb NOT NULL,
        appended_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, seq)
      )
    `;
    await this.#query`
      CREATE TABLE IF NOT EXISTS neutron_workflow_leases (
        key text PRIMARY KEY,
        token text NOT NULL,
        expires_at timestamptz NOT NULL
      )
    `;
  }

  async append(runId: string, event: WorkflowEvent): Promise<void> {
    await this.connect();
    await this.#query`
      INSERT INTO neutron_workflow_events (run_id, seq, event)
      VALUES (${runId}, ${event.seq}, ${JSON.stringify(event)})
      ON CONFLICT (run_id, seq) DO NOTHING
    `;
  }

  async load(runId: string): Promise<WorkflowEvent[]> {
    await this.connect();
    const rows = await this.#query`
      SELECT event FROM neutron_workflow_events WHERE run_id = ${runId} ORDER BY seq
    `;
    const seen = new Set<number>();
    const events: WorkflowEvent[] = [];
    for (const [index, row] of rows.entries()) {
      // this driver hands jsonb columns back as text; parse defensively
      let event: unknown = row.event;
      if (typeof event === "string") {
        try {
          event = JSON.parse(event);
        } catch {
          event = undefined;
        }
      }
      if (event === null || typeof event !== "object") {
        throw new WorkflowError(
          problemFromStatus(500, `Run ${runId} has a corrupt event log entry (${index}).`),
        );
      }
      const typed = event as WorkflowEvent;
      if (seen.has(typed.seq)) continue;
      seen.add(typed.seq);
      events.push(typed);
    }
    return events;
  }

  async setNX(key: string, value: string, opts?: { ttl?: number }): Promise<boolean> {
    await this.connect();
    const seconds = opts?.ttl ?? NEVER_EXPIRES_SECONDS;
    const rows = await this.#query`
      INSERT INTO neutron_workflow_leases (key, token, expires_at)
      VALUES (${key}, ${value}, now() + make_interval(secs => ${seconds}))
      ON CONFLICT (key) DO UPDATE SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
        WHERE neutron_workflow_leases.expires_at <= now()
      RETURNING token
    `;
    return rows.length > 0;
  }

  async cdel(key: string, expected: string): Promise<boolean> {
    await this.connect();
    const rows = await this.#query`
      DELETE FROM neutron_workflow_leases
      WHERE key = ${key} AND token = ${expected} AND expires_at > now()
      RETURNING key
    `;
    return rows.length > 0;
  }

  async cexpire(key: string, expected: string, seconds: number): Promise<boolean> {
    await this.connect();
    const rows = await this.#query`
      UPDATE neutron_workflow_leases
      SET expires_at = now() + make_interval(secs => ${seconds})
      WHERE key = ${key} AND token = ${expected} AND expires_at > now()
      RETURNING key
    `;
    return rows.length > 0;
  }
}
