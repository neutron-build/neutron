import { parseCron, type CronSchedule } from "./cron.js";

export interface Job<TPayload = unknown> {
  id: string;
  name: string;
  payload: TPayload;
  createdAt: number;
  /** Aborted when a leased worker can no longer prove ownership. */
  signal?: AbortSignal;
}

export type JobHandler<TPayload = unknown> = (job: Job<TPayload>) => Promise<void> | void;

export interface DeadLetter<TPayload = unknown> {
  job: Job<TPayload>;
  attempts: number;
  error: unknown;
}

export interface ScheduleOptions {
  /**
   * Queue the recurring job should be enqueued on. Only the Postgres driver
   * honors this today; other drivers ignore it.
   */
  queue?: string;
}

/** Operational guarantees are independent from the shared method names. */
export interface QueueCapabilities {
  durability: "process" | "backend" | "unknown";
  unknownHandlers: "retain" | "filter-before-claim" | "defer-or-fail";
  claimFencing: "none" | "attempt" | "native" | "unknown";
  handlerSignal: boolean;
  close: "schedule-timers-only" | "drain-sql-worker" | "native-worker" | "unknown";
  acknowledgement: 'process' | 'fenced-uncertain-stop' | 'native' | 'unknown';
  errorObservation: 'dead-letters' | 'workerError-and-close' | 'workerError' | 'unknown';
  registration: 'process-starts-worker' | 'register-and-drain-inline';
  retry: 'three-process-attempts' | 'persisted-budget' | 'native-default-one-attempt' | 'unknown';
  scheduling: 'process-cron' | 'persisted-cron' | 'native-repeatables' | 'unknown';
  routing: 'registered-names' | 'shared-queue';
  postClose: 'schedule-refusal' | 'all-refusal';
}

/** Select adapters using required guarantees, never class or method names. */
export function admitQueue<T extends QueueDriver>(driver: T, required: Partial<QueueCapabilities>): T {
  for (const [key, value] of Object.entries(required)) {
    if (!driver.capabilities || driver.capabilities[key as keyof QueueCapabilities] !== value)
      throw new Error(`Queue capability not admitted: ${key}`);
  }
  return driver;
}

export interface QueueDriver {
  /** Absent for legacy/custom drivers: guarantees must be supplied by their owner. */
  readonly capabilities?: Readonly<QueueCapabilities>;
  close?(): void | Promise<void>;
  /** Observable uncertainty for adapters advertising workerError-and-close. */
  readonly workerError?: unknown;
  add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>>;
  process<TPayload = unknown>(name: string, handler: JobHandler<TPayload>): Promise<void>;
  /**
   * Register or replace a recurring job identified by `id`, firing on the
   * cron `pattern` (five or six fields; six-field patterns add a leading
   * seconds field). The fired job's name is `id`.
   *
   * Durability is driver-specific: the Postgres driver persists schedules in
   * the `neutron_schedules` table, the BullMQ driver uses its native
   * repeatables, and the InMemory driver is dev-only — schedules vanish on
   * restart. A suspended process emits one catch-up job, then advances from now.
   */
  schedule(id: string, pattern: string, payload: unknown, opts?: ScheduleOptions): Promise<void>;
  /** Remove a schedule previously registered with `schedule()`. */
  unschedule(id: string): Promise<void>;
}

const MAX_ATTEMPTS = 3;
const RETRY_BACKOFF_MS = 10;

export class InMemoryQueueDriver implements QueueDriver {
  readonly capabilities = Object.freeze({ durability: "process", unknownHandlers: "retain", claimFencing: "none", handlerSignal: false, close: "schedule-timers-only", acknowledgement: "process", errorObservation: "dead-letters", registration: "register-and-drain-inline", retry: "three-process-attempts", scheduling: "process-cron", routing: "registered-names", postClose: "schedule-refusal" } as const);
  private closed = false;
  private idCounter = 0;
  private handlers = new Map<string, JobHandler<any>>();
  private jobs: Job<any>[] = [];
  private draining = false;
  private deadLettersInternal: DeadLetter<any>[] = [];
  private scheduleTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private scheduleIds = new Set<string>();

  get deadLetters(): DeadLetter<any>[] {
    return [...this.deadLettersInternal];
  }

  async add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>> {
    const job: Job<TPayload> = {
      id: String(++this.idCounter),
      name,
      payload,
      createdAt: Date.now(),
    };
    this.jobs.push(job);
    await this.drain();
    return job;
  }

  async process<TPayload = unknown>(
    name: string,
    handler: JobHandler<TPayload>
  ): Promise<void> {
    this.handlers.set(name, handler as JobHandler<any>);
    await this.drain();
  }

  private async drain(): Promise<void> {
    if (this.draining) {
      return;
    }
    this.draining = true;
    try {
      for (let i = 0; i < this.jobs.length; ) {
        const job = this.jobs[i];
        const handler = this.handlers.get(job.name);
        if (!handler) {
          i += 1;
          continue;
        }
        this.jobs.splice(i, 1);
        await this.runWithRetries(job, handler);
      }
    } finally {
      this.draining = false;
    }
  }

  private async runWithRetries(
    job: Job<any>,
    handler: JobHandler<any>
  ): Promise<void> {
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
      try {
        await handler(job);
        return;
      } catch (error) {
        if (attempt === MAX_ATTEMPTS) {
          this.deadLettersInternal.push({ job, attempts: attempt, error });
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS));
      }
    }
  }

  async schedule(
    id: string,
    pattern: string,
    payload: unknown,
    _opts?: ScheduleOptions
  ): Promise<void> {
    if (this.closed) throw new Error("Queue schedules are closed");
    const cron = parseCron(pattern);
    const first = cron.next(new Date());
    this.clearSchedule(id);
    this.scheduleIds.add(id);
    this.armSchedule(id, cron, first, payload);
  }

  async unschedule(id: string): Promise<void> {
    this.scheduleIds.delete(id);
    this.clearSchedule(id);
  }

  /**
   * Dev-only: clears all pending schedule timers. Jobs already queued or
   * mid-flight are unaffected.
   */
  close(): void {
    this.closed = true;
    this.scheduleIds.clear();
    for (const id of [...this.scheduleTimers.keys()]) {
      this.clearSchedule(id);
    }
  }

  private armSchedule(
    id: string,
    cron: CronSchedule,
    fireAt: Date,
    payload: unknown
  ): void {
    const delay = Math.min(2_147_483_647, Math.max(0, fireAt.getTime() - Date.now()));
    const timer = setTimeout(() => {
      this.scheduleTimers.delete(id);
      if (!this.scheduleIds.has(id)) {
        return;
      }
      if (Date.now() < fireAt.getTime()) {
        this.armSchedule(id, cron, fireAt, payload);
        return;
      }
      this.jobs.push({
        id: `sched-${id}-${Date.now()}`,
        name: id,
        payload,
        createdAt: Date.now(),
      });
      void this.drain();
      this.armSchedule(id, cron, cron.next(new Date()), payload);
    }, delay);
    if (typeof timer.unref === "function") {
      timer.unref();
    }
    this.scheduleTimers.set(id, timer);
  }

  private clearSchedule(id: string): void {
    const timer = this.scheduleTimers.get(id);
    if (timer) {
      clearTimeout(timer);
      this.scheduleTimers.delete(id);
    }
  }
}

