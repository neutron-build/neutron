import type { Job, JobHandler, QueueDriver } from "./index.js";
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

interface BullMqQueueLike {
  add(name: string, payload: unknown): Promise<{ id: string | number | undefined }>;
  close(): Promise<void>;
}

interface BullMqWorkerLike {
  close(): Promise<void>;
}

type BullMqWorkerCtor = new (
  queueName: string,
  processor: (job: { id?: string | number; name: string; data: unknown; timestamp?: number }) => Promise<void>,
  options?: Record<string, unknown>
) => BullMqWorkerLike;

type BullMqQueueCtor = new (
  queueName: string,
  options?: Record<string, unknown>
) => BullMqQueueLike;

export class BullMqQueueDriver implements QueueDriver {
  private readonly handlers = new Map<string, JobHandler<unknown>>();
  private worker: BullMqWorkerLike | null = null;

  constructor(
    private readonly queue: BullMqQueueLike,
    private readonly WorkerCtor: BullMqWorkerCtor,
    private readonly workerOptions: Record<string, unknown>,
    private readonly queueName: string,
    private readonly connection: RedisLikeConnection
  ) {}

  async add<TPayload = unknown>(name: string, payload: TPayload): Promise<Job<TPayload>> {
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
    this.handlers.set(name, handler as JobHandler<unknown>);
    await this.ensureWorker();
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
    }
    await this.queue.close();
    await this.connection.quit();
  }

  private async ensureWorker(): Promise<void> {
    if (this.worker) {
      return;
    }

    this.worker = new this.WorkerCtor(
      this.queueName,
      async (job) => {
        const handler = this.handlers.get(job.name);
        if (!handler) {
          return;
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
  }>(
    "bullmq",
    "Install with `pnpm add bullmq ioredis` (or npm/yarn equivalent)"
  );

  if (!redisModule.default || !bullMqModule.Queue || !bullMqModule.Worker) {
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

  const queue = new bullMqModule.Queue(queueName, {
    connection,
    prefix,
  });

  return new BullMqQueueDriver(
    queue,
    bullMqModule.Worker,
    {
      connection,
      prefix,
      concurrency,
    },
    queueName,
    connection
  );
}

