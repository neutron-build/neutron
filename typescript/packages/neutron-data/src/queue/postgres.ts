import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import cronParser from "cron-parser";

const { parseExpression } = cronParser;
import type { Job, JobHandler, QueueDriver, ScheduleOptions } from "./index.js";
import { lazyImport } from "../internal/lazy-import.js";

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
  /** Jobs claimed per tick. Default 10. */
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

const CREATE_JOBS_INDEX =
  "CREATE INDEX IF NOT EXISTS neutron_jobs_ready ON neutron_jobs (priority, run_at) WHERE status = 'pending'";

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
  return parseExpression(pattern, { currentDate: from }).next().toDate();
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class PostgresQueueDriver implements QueueDriver {
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
    this.queueName = options.queueName ?? "neutron";
    this.workerId =
      options.workerId ?? `${this.queueName}:${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.pollIntervalMs = options.pollIntervalMs ?? 2000;
    this.batchSize = options.batchSize ?? 10;
    this.leaseMs = options.leaseMs ?? 60_000;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.backoffBaseMs = options.backoffBaseMs ?? 1000;
    this.backoffMaxMs = options.backoffMaxMs ?? 300_000;
    this.retentionMs = options.retentionMs ?? 24 * 60 * 60 * 1000;
    this.retentionSweepIntervalMs = options.retentionSweepIntervalMs ?? 60_000;
    this.heartbeatMs = Math.max(50, Math.floor(this.leaseMs / 3));
  }

  async add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>> {
    await this.ensureSchema();
    const id = randomUUID();
    const createdAt = new Date();
    await this.sql.unsafe(
      `INSERT INTO neutron_jobs (id, queue, name, payload, max_attempts, created_at)
       VALUES ($1::uuid, $2, $3, $4::jsonb, $5, $6::timestamptz)`,
      [id, this.queueName, name, JSON.stringify(payload ?? null), this.maxAttempts, createdAt]
    );
    return { id, name, payload, createdAt: createdAt.getTime() };
  }

  async process<TPayload = unknown>(
    name: string,
    handler: JobHandler<TPayload>
  ): Promise<void> {
    this.handlers.set(name, handler as JobHandler<unknown>);
    await this.ensureSchema();
    this.startLoop();
  }

  async schedule(
    id: string,
    pattern: string,
    payload: unknown,
    opts?: ScheduleOptions
  ): Promise<void> {
    await this.ensureSchema();
    const next = nextCronDate(pattern, new Date());
    const queue = opts?.queue ?? this.queueName;
    await this.sql.unsafe(
      `INSERT INTO neutron_schedules (id, queue, name, cron, payload, next_run_at)
       VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6::timestamptz)
       ON CONFLICT (queue, name) DO UPDATE SET
         cron = EXCLUDED.cron,
         payload = EXCLUDED.payload,
         next_run_at = EXCLUDED.next_run_at`,
      [randomUUID(), queue, id, pattern, JSON.stringify(payload ?? null), next]
    );
    this.scheduleQueues.set(id, queue);
  }

  async unschedule(id: string): Promise<void> {
    await this.ensureSchema();
    const queue = this.scheduleQueues.get(id) ?? this.queueName;
    await this.sql.unsafe(
      `DELETE FROM neutron_schedules WHERE queue = $1 AND name = $2`,
      [queue, id]
    );
    this.scheduleQueues.delete(id);
  }

  async close(): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.closePromise = this.inFlight.then(() => this.sql.end());
    return this.closePromise;
  }

  private ensureSchema(): Promise<void> {
    if (!this.ready) {
      this.ready = (async () => {
        await this.sql.unsafe(CREATE_JOBS);
        await this.sql.unsafe(CREATE_JOBS_INDEX);
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
        () => this.queueTick(this.pollIntervalMs)
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
           VALUES ($1::uuid, $2, $3, $4::jsonb)`,
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
    const claimed = await this.sql.unsafe<ClaimedJobRow>(
      `UPDATE neutron_jobs SET status = 'active', locked_at = now(), locked_by = $1,
         attempts = attempts + 1
       WHERE id IN (
         SELECT id FROM neutron_jobs
         WHERE queue = $2 AND status = 'pending' AND run_at <= now()
           AND name = ANY($3::text[])
         ORDER BY priority, run_at
         FOR UPDATE SKIP LOCKED
         LIMIT $4
       )
       RETURNING id, name, payload, attempts, max_attempts, created_at`,
      [this.workerId, this.queueName, names, this.batchSize]
    );
    for (const row of claimed) {
      await this.runClaimed(row);
    }
  }

  private async runClaimed(row: ClaimedJobRow): Promise<void> {
    const handler = this.handlers.get(row.name);
    if (!handler) {
      return;
    }
    const job: Job<unknown> = {
      id: row.id,
      name: row.name,
      payload: row.payload,
      createdAt: new Date(row.created_at).getTime(),
    };
    const heartbeat = setInterval(() => {
      void this.sql
        .unsafe(
          `UPDATE neutron_jobs SET locked_at = now()
           WHERE id = $1::uuid AND locked_by = $2 AND status = 'active'`,
          [row.id, this.workerId]
        )
        .catch(() => {});
    }, this.heartbeatMs);
    try {
      await handler(job);
      await this.sql.unsafe(
        `UPDATE neutron_jobs SET status = 'done', done_at = now(), locked_at = NULL, locked_by = NULL
         WHERE id = $1::uuid`,
        [row.id]
      );
    } catch (error) {
      const message = errorMessage(error);
      if (row.attempts >= row.max_attempts) {
        await this.sql.unsafe(
          `UPDATE neutron_jobs SET status = 'dead', done_at = now(), locked_at = NULL,
             locked_by = NULL, last_error = $2
           WHERE id = $1::uuid`,
          [row.id, message]
        );
      } else {
        await this.sql.unsafe(
          `UPDATE neutron_jobs SET status = 'pending', locked_at = NULL, locked_by = NULL,
             last_error = $2, run_at = $3::timestamptz
           WHERE id = $1::uuid`,
          [row.id, message, new Date(Date.now() + this.backoffMs(row.attempts))]
        );
      }
    } finally {
      clearInterval(heartbeat);
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
  return new PostgresQueueDriver(sql, options);
}
