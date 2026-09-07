import cronParser from "cron-parser";

const { parseExpression } = cronParser;

export interface Job<TPayload = unknown> {
  id: string;
  name: string;
  payload: TPayload;
  createdAt: number;
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

export interface QueueDriver {
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
   * restart and missed windows are not caught up.
   */
  schedule(id: string, pattern: string, payload: unknown, opts?: ScheduleOptions): Promise<void>;
  /** Remove a schedule previously registered with `schedule()`. */
  unschedule(id: string): Promise<void>;
}

type CronInterval = ReturnType<typeof parseExpression<false>>;

const MAX_ATTEMPTS = 3;
const RETRY_BACKOFF_MS = 10;

export class InMemoryQueueDriver implements QueueDriver {
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
    const interval = parseExpression(pattern);
    this.clearSchedule(id);
    this.scheduleIds.add(id);
    this.armSchedule(id, interval, payload);
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
    for (const id of [...this.scheduleTimers.keys()]) {
      this.clearSchedule(id);
    }
  }

  private armSchedule(
    id: string,
    interval: CronInterval,
    payload: unknown
  ): void {
    const delay = Math.max(0, interval.next().toDate().getTime() - Date.now());
    const timer = setTimeout(() => {
      this.scheduleTimers.delete(id);
      if (!this.scheduleIds.has(id)) {
        return;
      }
      this.jobs.push({
        id: `sched-${id}-${Date.now()}`,
        name: id,
        payload,
        createdAt: Date.now(),
      });
      void this.drain();
      this.armSchedule(id, interval, payload);
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

