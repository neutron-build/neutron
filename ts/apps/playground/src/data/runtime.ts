import {
  createBullMqQueueDriver,
  createDrizzleDatabase,
  createRedisCacheClient,
  createRedisSessionStore,
  createSessionStore,
  InMemoryQueueDriver,
  InMemoryStorageDriver,
  MemoryCacheClient,
  resolveDataConfig,
  type CacheClient,
  type DrizzleDatabase,
  type QueueDriver,
  type SessionStore,
  type StorageDriver,
} from "neutron-data";
import {
  createMemoryTodoStore,
  createSqlTodoStore,
  type TodoStore,
} from "./todos.js";
import { createS3StorageDriver } from "neutron-data";

type RuntimeProfile = "memory" | "production";

export interface DataRuntime {
  profile: RuntimeProfile;
  drivers: {
    database: string;
    cache: string;
    session: string;
    queue: string;
    storage: string;
  };
  todos: TodoStore;
  cache: CacheClient;
  sessions: SessionStore;
  queue: QueueDriver;
  storage: StorageDriver;
  database: DrizzleDatabase | null;
  close(): Promise<void>;
}

let runtimePromise: Promise<DataRuntime> | null = null;

export async function getDataRuntime(): Promise<DataRuntime> {
  if (!runtimePromise) {
    runtimePromise = createRuntime();
  }
  return runtimePromise;
}

export async function getDataRuntimeSummary(): Promise<{
  profile: RuntimeProfile;
  drivers: DataRuntime["drivers"];
  databaseProvider: string;
}> {
  const runtime = await getDataRuntime();
  return {
    profile: runtime.profile,
    drivers: runtime.drivers,
    databaseProvider: runtime.database?.profile.provider || "none",
  };
}

async function createRuntime(): Promise<DataRuntime> {
  const profile = resolveRuntimeProfile();
  if (profile === "production") {
    return createProductionRuntime();
  }
  return createMemoryRuntime();
}

function resolveRuntimeProfile(): RuntimeProfile {
  return process.env.NEUTRON_DATA_PROFILE === "production" ? "production" : "memory";
}

async function createMemoryRuntime(): Promise<DataRuntime> {
  const cache = new MemoryCacheClient();
  const sessions = createSessionStore({ cache, prefix: "playground:session:" });
  const queue = new InMemoryQueueDriver();
  const storage = new InMemoryStorageDriver();
  const todos = createMemoryTodoStore([
    {
      id: "1",
      text: "Build the framework",
      done: true,
      createdAt: Date.now() - 3000,
    },
    {
      id: "2",
      text: "Wire neutron-data profile",
      done: false,
      createdAt: Date.now() - 2000,
    },
    {
      id: "3",
      text: "Ship benchmark improvements",
      done: false,
      createdAt: Date.now() - 1000,
    },
  ]);

  return {
    profile: "memory",
    drivers: {
      database: "memory",
      cache: "memory",
      session: "memory",
      queue: "memory",
      storage: "memory",
    },
    todos,
    cache,
    sessions,
    queue,
    storage,
    database: null,
    close: async () => {
      // memory-only resources; no teardown required.
    },
  };
}

async function createProductionRuntime(): Promise<DataRuntime> {
  const resolvedDb = resolveDataConfig();
  const database = await createDrizzleDatabase();
  const todos = await createSqlTodoStore(database);

  const cache = await createRedisCacheClient({
    url: process.env.DRAGONFLY_URL || process.env.REDIS_URL,
    keyPrefix: "playground:cache:",
  });

  const redisSessions = await createRedisSessionStore({
    url: process.env.DRAGONFLY_URL || process.env.REDIS_URL,
    keyPrefix: "playground:",
    sessionPrefix: "session:",
    sessionTtlSec: 60 * 60 * 24 * 7,
  });

  const queue = await createBullMqQueueDriver({
    url: process.env.DRAGONFLY_URL || process.env.REDIS_URL,
    queueName: process.env.NEUTRON_QUEUE_NAME || "neutron-playground",
    prefix: "neutron",
    concurrency: 10,
  });

  const storage = process.env.S3_BUCKET
    ? await createS3StorageDriver({
        bucket: process.env.S3_BUCKET,
        region: process.env.S3_REGION || process.env.AWS_REGION || "auto",
        endpoint: process.env.S3_ENDPOINT,
        forcePathStyle: process.env.S3_FORCE_PATH_STYLE === "1",
        accessKeyId: process.env.S3_ACCESS_KEY || process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.S3_SECRET_KEY || process.env.AWS_SECRET_ACCESS_KEY,
      })
    : new InMemoryStorageDriver();

  return {
    profile: "production",
    drivers: {
      database: `drizzle:${resolvedDb.database}`,
      cache: "redis-compatible",
      session: "redis-compatible",
      queue: "bullmq",
      storage: process.env.S3_BUCKET ? "s3-compatible" : "memory",
    },
    todos,
    cache,
    sessions: redisSessions,
    queue,
    storage,
    database,
    close: async () => {
      await safeClose(queue);
      await safeClose(storage);
      await safeClose(redisSessions);
      await safeClose(cache);
      await safeClose(database);
    },
  };
}

async function safeClose(value: unknown): Promise<void> {
  if (!value || typeof value !== "object") {
    return;
  }
  const close = (value as { close?: () => Promise<void> | void }).close;
  if (typeof close === "function") {
    await close.call(value);
  }
}
