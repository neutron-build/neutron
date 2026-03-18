import {
  createRedisCacheClient,
  type RedisCacheClient,
  type RedisCacheClientOptions,
} from "../cache/redis.js";
import {
  createSessionStore,
  type SessionStore,
} from "./index.js";

export interface RedisSessionStoreOptions extends RedisCacheClientOptions {
  sessionPrefix?: string;
  sessionTtlSec?: number;
}

export type RedisSessionStore = SessionStore & {
  cache: RedisCacheClient;
  close: () => Promise<void>;
};

export async function createRedisSessionStore(
  options: RedisSessionStoreOptions = {}
): Promise<RedisSessionStore> {
  const cache = await createRedisCacheClient({
    url: options.url,
    keyPrefix: options.keyPrefix,
    connectTimeoutMs: options.connectTimeoutMs,
  });

  const store = createSessionStore({
    cache,
    prefix: options.sessionPrefix,
    ttlSec: options.sessionTtlSec,
  });

  return {
    ...store,
    cache,
    close: async () => {
      await cache.close();
    },
  };
}
