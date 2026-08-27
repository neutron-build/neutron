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

export interface QueueDriver {
  add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>>;
  process<TPayload = unknown>(name: string, handler: JobHandler<TPayload>): Promise<void>;
}

const MAX_ATTEMPTS = 3;
const RETRY_BACKOFF_MS = 10;

export class InMemoryQueueDriver implements QueueDriver {
  private idCounter = 0;
  private handlers = new Map<string, JobHandler<any>>();
  private jobs: Job<any>[] = [];
  private draining = false;
  private deadLettersInternal: DeadLetter<any>[] = [];

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
}

