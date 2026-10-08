import { cleanupAfterFailure } from "../internal/resources.js";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import type { Job, JobHandler, QueueDriver, ScheduleOptions } from "./index.js";
import { lazyImport } from "../internal/lazy-import.js";
import { parseCron } from "./cron.js";

/**
 * Structural slice of the postgres.js client (and its transaction handle)
 * that the driver needs. Inject a mock conforming to this shape to test the
 * driver without a database.
 */
export interface PostgresQueueSql {
  unsafe<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<T[]>;
  begin<T>(fn: (tx: PostgresQueueSql) => Promise<T>): Promise<T>;
  end(options?: { timeout?: number }): Promise<void>;
}

interface ClaimedJobRow {
  id: string;
  name: string;
  payload: unknown;
  attempts: number;
  max_attempts: number;
  created_at: Date;
}

interface DueScheduleRow {
  id: string;
  name: string;
  cron: string;
  payload: unknown;
}

export interface PostgresQueueDriverOptions {
  /** Connection string. Resolved from POSTGRES_URL / DATABASE_URL, defaulting to local Postgres. */
  url?: string;
  /** Existing postgres.js client. When given, `url` is ignored and `close()` still ends it. */
  sql?: PostgresQueueSql;
  queueName?: string;
  workerId?: string;
  /** Delay between poll ticks. Default 2000ms. */
  pollIntervalMs?: number;
  /** Maximum sequential jobs per tick; only the running slot is leased. Default 10. */
  batchSize?: number;
  /** Lease length: an active job whose locked_at is older is reaped back to pending. Default 60s. */
  leaseMs?: number;
  /** Attempt budget per job before it is dead-lettered. Default 5. */
  maxAttempts?: number;
  /** First retry delay; grows exponentially per attempt. Default 1000ms. */
  backoffBaseMs?: number;
  /** Ceiling for the exponential retry delay. Default 300000ms. */
  backoffMaxMs?: number;
  /** Retention window for done/dead rows. Default 24h. */
  retentionMs?: number;
  /** How often the retention sweep runs. Default 60s. */
  retentionSweepIntervalMs?: number;
  /** Reports uncertain/lost ownership. Handlers also receive an aborted signal. */
  onLeaseLost?: (jobId: string, error: unknown) => void;
}

const DEFAULT_URL_FALLBACK = "postgres://127.0.0.1:5432/postgres";

const CREATE_JOBS = `CREATE TABLE IF NOT EXISTS neutron_jobs (
  id uuid PRIMARY KEY,
  queue text NOT NULL,
  name text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'pending',
  run_at timestamptz NOT NULL DEFAULT now(),
  priority integer NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 5,
  locked_at timestamptz,
  locked_by text,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  done_at timestamptz
)`;

const CREATE_JOBS_INDEX_PARTIAL =
  "CREATE INDEX IF NOT EXISTS neutron_jobs_ready ON neutron_jobs (priority, run_at) WHERE status = 'pending'";
// Fallback when the server rejects predicate (partial) indexes — e.g. Nucleus
// today. The claim query's WHERE already filters status, so the plain index
// stays correct; the partial form is only a size optimization.
const CREATE_JOBS_INDEX_PLAIN =
  "CREATE INDEX IF NOT EXISTS neutron_jobs_ready ON neutron_jobs (priority, run_at)";

const CREATE_SCHEDULES = `CREATE TABLE IF NOT EXISTS neutron_schedules (
  id uuid PRIMARY KEY,
  queue text NOT NULL,
  name text NOT NULL,
  cron text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  next_run_at timestamptz NOT NULL,
  last_run_at timestamptz,
  UNIQUE (queue, name)
)`;

function nextCronDate(pattern: string, from: Date): Date {
  return parseCron(pattern).next(from);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class PostgresQueueDriver implements QueueDriver {
  readonly capabilities = Object.freeze({ durability: "backend", unknownHandlers: "filter-before-claim", claimFencing: "attempt", handlerSignal: true, close: "drain-sql-worker", acknowledgement: "fenced-uncertain-stop", errorObservation: "workerError-and-close", registration: "process-starts-worker", retry: "persisted-budget", scheduling: "persisted-cron", routing: "registered-names", postClose: "all-refusal" } as const);
  private readonly handlers = new Map<string, JobHandler<unknown>>();
  private readonly scheduleQueues = new Map<string, string>();
  private readonly workerId: string;
  private readonly queueName: string;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;
  private readonly retentionMs: number;
  private readonly retentionSweepIntervalMs: number;
  private readonly heartbeatMs: number;
  private readonly onLeaseLost: PostgresQueueDriverOptions["onLeaseLost"];
  /** Last poll failure, retained even when the background loop has observed it. */
  workerError: unknown;
  private acknowledgementUnknown = false;
  private ready: Promise<void> | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;
  private tickCount = 0;
  private inFlight: Promise<void> = Promise.resolve();
  private closePromise: Promise<void> | null = null;

  constructor(
    private readonly sql: PostgresQueueSql,
    options: Omit<PostgresQueueDriverOptions, "url" | "sql"> = {}
  ) {
    this.onLeaseLost = options.onLeaseLost;
    this.queueName = options.queueName ?? "neutron";
    this.workerId =
      options.workerId ?? `${this.queueName}:${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.pollIntervalMs = options.pollIntervalMs ?? 2000;
    const batchSize = options.batchSize ?? 10;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 1000) {
      throw new Error("batchSize must be an integer between 1 and 1000");
    }
    this.batchSize = batchSize;
    this.leaseMs = options.leaseMs ?? 60_000;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.backoffBaseMs = options.backoffBaseMs ?? 1000;
    this.backoffMaxMs = options.backoffMaxMs ?? 300_000;
    this.retentionMs = options.retentionMs ?? 24 * 60 * 60 * 1000;
    this.retentionSweepIntervalMs = options.retentionSweepIntervalMs ?? 60_000;
    this.heartbeatMs = Math.max(50, Math.floor(this.leaseMs / 3));
  }

  async add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>> {
    this.assertOpen();
    await this.ensureSchema();
    this.assertOpen();
    const id = randomUUID();
    const createdAt = new Date();
    await this.sql.unsafe(
      `INSERT INTO neutron_jobs (id, queue, name, payload, max_attempts, created_at)
       VALUES ($1::uuid, $2, $3, COALESCE($4::text::jsonb, 'null'::jsonb), $5, $6::timestamptz)`,
      [id, this.queueName, name, JSON.stringify(payload ?? null), this.maxAttempts, createdAt]
    );
    return { id, name, payload, createdAt: createdAt.getTime() };
  }

  async process<TPayload = unknown>(
    name: string,
    handler: JobHandler<TPayload>
  ): Promise<void> {
    this.assertOpen();
    this.handlers.set(name, handler as JobHandler<unknown>);
    await this.ensureSchema();
    this.assertOpen();
    this.startLoop();
  }

  async schedule(
    id: string,
    pattern: string,
    payload: unknown,
    opts?: ScheduleOptions
  ): Promise<void> {
    this.assertOpen();
    await this.ensureSchema();
    this.assertOpen();
    const next = nextCronDate(pattern, new Date());
    const queue = opts?.queue ?? this.queueName;
    await this.sql.unsafe(
      `INSERT INTO neutron_schedules (id, queue, name, cron, payload, next_run_at)
       VALUES ($1::uuid, $2, $3, $4, COALESCE($5::text::jsonb, 'null'::jsonb), $6::timestamptz)
       ON CONFLICT (queue, name) DO UPDATE SET
         cron = EXCLUDED.cron,
         payload = EXCLUDED.payload,
         next_run_at = EXCLUDED.next_run_at`,
      [randomUUID(), queue, id, pattern, JSON.stringify(payload ?? null), next]
    );
    this.scheduleQueues.set(id, queue);
  }

  async unschedule(id: string): Promise<void> {
    this.assertOpen();
    await this.ensureSchema();
    this.assertOpen();
    const queue = this.scheduleQueues.get(id) ?? this.queueName;
    await this.sql.unsafe(
      `DELETE FROM neutron_schedules WHERE queue = $1 AND name = $2`,
      [queue, id]
    );
    this.scheduleQueues.delete(id);
  }

  private assertOpen(): void { if (this.stopped) throw new Error("PostgresQueueDriver is stopped; reconcile workerError before replacement"); }

  async close(): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.closePromise = this.inFlight.then(async () => {
      try { await this.sql.end(); }
      catch (cleanupError) {
        if (this.acknowledgementUnknown) throw new AggregateError([this.workerError, cleanupError], "Queue acknowledgement and cleanup failed", { cause: this.workerError });
        throw cleanupError;
      }
      if (this.acknowledgementUnknown) throw this.workerError;
    });
    return this.closePromise;
  }

  private ensureSchema(): Promise<void> {
    if (!this.ready) {
      this.ready = (async () => {
        await this.sql.unsafe(CREATE_JOBS);
        try {
          await this.sql.unsafe(CREATE_JOBS_INDEX_PARTIAL);
        } catch {
          await this.sql.unsafe(CREATE_JOBS_INDEX_PLAIN);
        }
        await this.sql.unsafe(CREATE_SCHEDULES);
      })();
    }
    return this.ready;
  }

  private startLoop(): void {
    if (this.stopped || this.timer) {
      return;
    }
    this.queueTick(0);
  }

  private queueTick(delayMs: number): void {
    if (this.stopped) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      this.inFlight = this.tick().then(
        () => this.queueTick(this.pollIntervalMs),
        (error: unknown) => {
          this.workerError = error;
          // An acknowledgement may already have committed. Stop this worker;
          // reconciliation belongs to its owner, never replay the UPDATE.
          if (this.acknowledgementUnknown) this.stopped = true;
          else this.queueTick(this.pollIntervalMs);
        }
      );
    }, delayMs);
  }

  private async tick(): Promise<void> {
    this.tickCount += 1;
    await this.reapExpiredLeases();
    if (this.tickCount * this.pollIntervalMs >= this.retentionSweepIntervalMs) {
      this.tickCount = 0;
      await this.sweepRetention();
    }
    await this.fireDueSchedules();
    await this.claimAndRun();
  }

  private async reapExpiredLeases(): Promise<void> {
    const cutoff = new Date(Date.now() - this.leaseMs);
    await this.sql.unsafe(
      `UPDATE neutron_jobs SET status = 'pending', locked_at = NULL, locked_by = NULL
       WHERE queue = $1 AND status = 'active' AND locked_at < $2::timestamptz
       RETURNING id`,
      [this.queueName, cutoff]
    );
  }

  private async sweepRetention(): Promise<void> {
    const cutoff = new Date(Date.now() - this.retentionMs);
    await this.sql.unsafe(
      `DELETE FROM neutron_jobs
       WHERE status IN ('done', 'dead') AND COALESCE(done_at, created_at) < $1::timestamptz`,
      [cutoff]
    );
  }

  private async fireDueSchedules(): Promise<void> {
    await this.sql.begin(async (tx) => {
      const due = await tx.unsafe<DueScheduleRow>(
        `SELECT id, name, cron, payload FROM neutron_schedules
         WHERE queue = $1 AND next_run_at <= now()
         ORDER BY next_run_at
         FOR UPDATE SKIP LOCKED`,
        [this.queueName]
      );
      for (const row of due) {
        // Compute the next occurrence from now, not from next_run_at: a
        // missed window produces one catch-up run, not N.
        const next = nextCronDate(row.cron, new Date());
        await tx.unsafe(
          `INSERT INTO neutron_jobs (id, queue, name, payload)
           VALUES ($1::uuid, $2, $3, COALESCE($4::text::jsonb, 'null'::jsonb))`,
          [randomUUID(), this.queueName, row.name, JSON.stringify(row.payload ?? null)]
        );
        await tx.unsafe(
          `UPDATE neutron_schedules SET last_run_at = now(), next_run_at = $2::timestamptz
           WHERE id = $1::uuid`,
          [row.id, next]
        );
      }
    });
  }

  private async claimAndRun(): Promise<void> {
    if (this.handlers.size === 0) {
      return;
    }
    const names = [...this.handlers.keys()];
    // Acquire only the execution slot we can run now. Waiting jobs stay
    // pending and available to other workers, rather than aging unheartbeated.
    for (let slot = 0; slot < this.batchSize && !this.stopped; slot += 1) {
      const claimed = await this.sql.unsafe<ClaimedJobRow>(
        `UPDATE neutron_jobs SET status = 'active', locked_at = now(), locked_by = $1,
           attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM neutron_jobs
           WHERE queue = $2 AND status = 'pending' AND run_at <= now()
             AND name = ANY($3::text[])
           ORDER BY priority, run_at
           LIMIT 1
           FOR UPDATE SKIP LOCKED
         )
         RETURNING id, name, payload, attempts, max_attempts, created_at`,
        [this.workerId, this.queueName, names]
      );
      if (claimed.length === 0) break;
      await this.runClaimed(claimed[0]!);
    }
  }

  private async runClaimed(row: ClaimedJobRow): Promise<void> {
    const handler = this.handlers.get(row.name);
    if (!handler) {
      return;
    }
    const controller = new AbortController();
    let lost = false;
    const lose = (error: unknown): void => {
      if (lost) return;
      lost = true;
      controller.abort(error);
      try { this.onLeaseLost?.(row.id, error); } catch { /* observer cannot restore ownership */ }
    };
    const job: Job<unknown> = {
      id: row.id, name: row.name, payload: row.payload,
      createdAt: new Date(row.created_at).getTime(), signal: controller.signal,
    };
    // attempts increases on every acquisition, including reuse of workerId.
    // It is the claim generation, not merely a retry counter.
    let heartbeatWork = Promise.resolve();
    const heartbeat = setInterval(() => {
      heartbeatWork = heartbeatWork.then(async () => {
        if (lost) return;
        try {
          const rows = await this.sql.unsafe(
            `UPDATE neutron_jobs SET locked_at = now()
             WHERE id = $1::uuid AND locked_by = $2 AND attempts = $3 AND status = 'active'
             RETURNING id`,
            [row.id, this.workerId, row.attempts],
          );
          if (rows.length !== 1) lose(new Error("queue lease lost"));
        } catch (error) { lose(error); }
      });
    }, this.heartbeatMs);
    let failed = false;
    let failure: unknown;
    try { await handler(job); } catch (error) { failed = true; failure = error; }
    clearInterval(heartbeat);
    await heartbeatWork;
    if (lost) return;
    const fence = "AND locked_by = $4 AND attempts = $5 AND status = 'active' RETURNING id";
    try {
      let updated: Record<string, unknown>[];
      if (!failed) {
        updated = await this.sql.unsafe(
          `UPDATE neutron_jobs SET status = 'done', done_at = now(), locked_at = NULL, locked_by = NULL
           WHERE id = $1::uuid AND locked_by = $2 AND attempts = $3 AND status = 'active' RETURNING id`,
          [row.id, this.workerId, row.attempts],
        );
      } else if (row.attempts >= row.max_attempts) {
        updated = await this.sql.unsafe(
          `UPDATE neutron_jobs SET status = 'dead', done_at = now(), locked_at = NULL,
             locked_by = NULL, last_error = $2
           WHERE id = $1::uuid AND locked_by = $3 AND attempts = $4 AND status = 'active' RETURNING id`,
          [row.id, errorMessage(failure), this.workerId, row.attempts],
        );
      } else {
        updated = await this.sql.unsafe(
          `UPDATE neutron_jobs SET status = 'pending', locked_at = NULL, locked_by = NULL,
             last_error = $2, run_at = $3::timestamptz
           WHERE id = $1::uuid ${fence}`,
          [row.id, errorMessage(failure), new Date(Date.now() + this.backoffMs(row.attempts)), this.workerId, row.attempts],
        );
      }
      if (updated.length !== 1) lose(new Error("queue lease lost before acknowledgement"));
    } catch (error) {
      this.acknowledgementUnknown = true;
      this.workerError = error;
      this.stopped = true;
      lose(error);
      throw error;
    }
  }

  private backoffMs(attempts: number): number {
    const exponential = Math.min(
      this.backoffBaseMs * 2 ** Math.max(0, attempts - 1),
      this.backoffMaxMs
    );
    return Math.round(exponential * (0.5 + Math.random()));
  }
}

export async function createPostgresQueueDriver(
  options: PostgresQueueDriverOptions = {}
): Promise<PostgresQueueDriver> {
  if (options.sql) {
    return new PostgresQueueDriver(options.sql, options);
  }
  const postgresModule = await lazyImport<{ default?: (...args: unknown[]) => any }>(
    "postgres",
    "Install with `pnpm add postgres` (or npm/yarn equivalent)"
  );
  if (!postgresModule.default) {
    throw new Error("Failed to initialize Postgres queue driver.");
  }
  const url =
    options.url || process.env.POSTGRES_URL || process.env.DATABASE_URL || DEFAULT_URL_FALLBACK;
  const sql = postgresModule.default(url, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
  }) as unknown as PostgresQueueSql;
  try { return new PostgresQueueDriver(sql, options); }
  catch (error) { return cleanupAfterFailure(error, [() => sql.end({ timeout: 5 })]); }
}
