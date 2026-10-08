import type { Job, JobHandler, QueueDriver, ScheduleOptions } from "./index.js";
import { closeResources } from "../internal/resources.js";
import { lazyImport } from "../internal/lazy-import.js";

export interface BullMqQueueDriverOptions {
  url?: string;
  queueName?: string;
  prefix?: string;
  concurrency?: number;
}

interface RedisLikeConnection {
  quit(): Promise<unknown>;
}

interface BullMqRepeatOptions {
  pattern?: string;
  key?: string;
}

interface BullMqQueueLike {
  add(
    name: string,
    payload: unknown,
    opts?: { repeat?: BullMqRepeatOptions }
  ): Promise<{ id: string | number | undefined }>;
  removeRepeatable(name: string, repeatOpts: BullMqRepeatOptions): Promise<unknown>;
  close(): Promise<void>;
}

interface BullMqWorkerLike {
  close(): Promise<void>;
  on?(event: 'error' | 'failed', listener: (...args: unknown[]) => void): unknown;
}

interface BullMqJob {
  id?: string | number; name: string; data: unknown; timestamp?: number;
  moveToDelayed?(timestamp: number, token?: string): Promise<void>;
}

type BullMqWorkerCtor = new (
  queueName: string,
  processor: (job: BullMqJob, token?: string) => Promise<void>,
  options?: Record<string, unknown>
) => BullMqWorkerLike;

type BullMqQueueCtor = new (
  queueName: string,
  options?: Record<string, unknown>
) => BullMqQueueLike;

const nativeFactoryAdmission = Symbol('native BullMQ factory');

export class BullMqQueueDriver implements QueueDriver {
  readonly capabilities: Readonly<import("./index.js").QueueCapabilities>;
  workerError: unknown;
  private readonly handlers = new Map<string, JobHandler<unknown>>();
  private readonly schedules = new Map<string, { pattern: string }>();
  private worker: BullMqWorkerLike | null = null;
  private closing: Promise<void> | null = null;

  constructor(
    private readonly queue: BullMqQueueLike,
    private readonly WorkerCtor: BullMqWorkerCtor,
    private readonly workerOptions: Record<string, unknown>,
    private readonly queueName: string,
    private readonly connection: RedisLikeConnection,
    private readonly DelayedErrorCtor?: new () => Error,
    admission?: typeof nativeFactoryAdmission,
  ) {
    const nativeAdmission = admission === nativeFactoryAdmission;
    this.capabilities = Object.freeze({ durability: nativeAdmission ? 'backend' : 'unknown', unknownHandlers: 'defer-or-fail', claimFencing: nativeAdmission ? 'native' : 'unknown', handlerSignal: false, close: nativeAdmission ? 'native-worker' : 'unknown', acknowledgement: nativeAdmission ? 'native' : 'unknown', errorObservation: nativeAdmission ? 'workerError' : 'unknown', registration: 'process-starts-worker', retry: nativeAdmission ? 'native-default-one-attempt' : 'unknown', scheduling: nativeAdmission ? 'native-repeatables' : 'unknown', routing: 'shared-queue', postClose: 'all-refusal' });
  }

  async add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>> {
    if (this.closing) throw new Error("BullMqQueueDriver is closed");
    const result = await this.queue.add(name, payload);
    return {
      id: String(result.id ?? ""),
      name,
      payload,
      createdAt: Date.now(),
    };
  }

  async process<TPayload = unknown>(
    name: string,
    handler: JobHandler<TPayload>
  ): Promise<void> {
    if (this.closing) throw new Error("BullMqQueueDriver is closed");
    this.handlers.set(name, handler as JobHandler<unknown>);
    await this.ensureWorker();
  }

  async close(): Promise<void> {
    if (this.closing) return this.closing;
    const worker = this.worker;
    this.worker = null;
    this.closing = (async () => {
      // Worker drain needs its Redis connection. Preserve this dependency order,
      // while still attempting every later resource after an earlier failure.
      const failures = [
        ...await closeResources(worker ? [() => worker.close()] : []),
        ...await closeResources([() => this.queue.close()]),
        ...await closeResources([() => this.connection.quit()]),
      ];
      if (failures.length) throw new AggregateError(failures, "BullMQ cleanup failed", { cause: failures[0] });
    })();
    return this.closing;
  }


  async schedule(
    id: string,
    pattern: string,
    payload: unknown,
    _opts?: ScheduleOptions
  ): Promise<void> {
    if (this.closing) throw new Error("BullMqQueueDriver is closed");
    const repeat = { pattern, key: id };
    await this.queue.add(id, payload, { repeat });
    this.schedules.set(id, { pattern });
  }

  async unschedule(id: string): Promise<void> {
    if (this.closing) throw new Error("BullMqQueueDriver is closed");
    const known = this.schedules.get(id);
    if (!known) {
      return;
    }
    await this.queue.removeRepeatable(id, { pattern: known.pattern, key: id });
    this.schedules.delete(id);
  }

  private async ensureWorker(): Promise<void> {
    if (this.worker) {
      return;
    }

    this.worker = new this.WorkerCtor(
      this.queueName,
      async (job, token) => {
        const handler = this.handlers.get(job.name);
        if (!handler) {
          // A shared queue can have workers with disjoint registries. Leave
          // unknown jobs durable and retryable without spending attempts.
          if (job.moveToDelayed && this.DelayedErrorCtor) {
            await job.moveToDelayed(Date.now() + 1000, token);
            throw new this.DelayedErrorCtor();
          }
          throw new Error(`no handler registered for BullMQ job ${JSON.stringify(job.name)}; job retained as failed for operator retry`);
        }
        await handler({
          id: String(job.id ?? ""),
          name: job.name,
          payload: job.data,
          createdAt: Number(job.timestamp || Date.now()),
        });
      },
      this.workerOptions
    );
    this.worker.on?.('error', error => { this.workerError = error; });
    this.worker.on?.('failed', (_job, error) => { this.workerError = error; });
  }
}

export async function createBullMqQueueDriver(
  options: BullMqQueueDriverOptions = {}
): Promise<BullMqQueueDriver> {
  const redisModule = await lazyImport<{ default?: new (...args: unknown[]) => RedisLikeConnection }>(
    "ioredis",
    "Install with `pnpm add ioredis bullmq` (or npm/yarn equivalent)"
  );
  const bullMqModule = await lazyImport<{
    Queue?: BullMqQueueCtor;
    Worker?: BullMqWorkerCtor;
    DelayedError?: new () => Error;
  }>(
    "bullmq",
    "Install with `pnpm add bullmq ioredis` (or npm/yarn equivalent)"
  );

  if (!redisModule.default || !bullMqModule.Queue || !bullMqModule.Worker || !bullMqModule.DelayedError) {
    throw new Error("Failed to initialize BullMQ queue driver.");
  }

  const url = options.url || process.env.DRAGONFLY_URL || process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const queueName = options.queueName || "neutron";
  const prefix = options.prefix || "neutron";
  const concurrency = options.concurrency ?? 8;
  const connection = new redisModule.default(url, {
    lazyConnect: false,
    maxRetriesPerRequest: null,
  }) as RedisLikeConnection;

  const resources: (() => unknown | Promise<unknown>)[] = [() => connection.quit()];
  try {
  const queue = new bullMqModule.Queue(queueName, {
    connection,
    prefix,
  });

  resources.unshift(() => queue.close());
  return new BullMqQueueDriver(
    queue,
    bullMqModule.Worker,
    {
      connection,
      prefix,
      concurrency,
    },
    queueName,
    connection,
    bullMqModule.DelayedError,
    nativeFactoryAdmission,
  );
  } catch (error) {
    // The queue depends on Redis. Dispose it before its connection even on startup failure.
    const failures: unknown[] = [];
    for (const resource of resources) failures.push(...await closeResources([resource]));
    if (failures.length) throw new AggregateError([error, ...failures], 'BullMQ startup and cleanup failed', { cause: error });
    throw error;
  }
}

