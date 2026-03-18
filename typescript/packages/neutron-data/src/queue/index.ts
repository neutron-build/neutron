export interface Job<TPayload = unknown> {
  id: string;
  name: string;
  payload: TPayload;
  createdAt: number;
}

export type JobHandler<TPayload = unknown> = (job: Job<TPayload>) => Promise<void> | void;

export interface QueueDriver {
  add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>>;
  process<TPayload = unknown>(name: string, handler: JobHandler<TPayload>): Promise<void>;
}

export class InMemoryQueueDriver implements QueueDriver {
  private idCounter = 0;
  private handlers = new Map<string, JobHandler<any>>();
  private jobs: Job<any>[] = [];
  private draining = false;

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
        await handler(job);
      }
    } finally {
      this.draining = false;
    }
  }
}

