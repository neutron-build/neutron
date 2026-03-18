import { randomUUID } from "node:crypto";
import type { CacheClient } from "../cache/index.js";

export type SessionData = Record<string, unknown>;

export interface SessionRecord<T extends SessionData = SessionData> {
  id: string;
  data: T;
}

export interface SessionStore {
  create<T extends SessionData = SessionData>(data?: T): Promise<SessionRecord<T>>;
  get<T extends SessionData = SessionData>(id: string): Promise<SessionRecord<T> | null>;
  set<T extends SessionData = SessionData>(id: string, data: T): Promise<void>;
  destroy(id: string): Promise<void>;
}

export interface SessionStoreOptions {
  cache: CacheClient;
  prefix?: string;
  ttlSec?: number;
}

export function createSessionStore(options: SessionStoreOptions): SessionStore {
  const prefix = options.prefix || "session:";
  const ttlSec = options.ttlSec ?? 60 * 60 * 24 * 7;

  const cacheKey = (id: string) => `${prefix}${id}`;

  return {
    async create<T extends SessionData = SessionData>(data?: T): Promise<SessionRecord<T>> {
      const id = randomUUID();
      const payload = (data || ({} as T)) as T;
      await options.cache.set(cacheKey(id), JSON.stringify(payload), ttlSec);
      return { id, data: payload };
    },
    async get<T extends SessionData = SessionData>(id: string): Promise<SessionRecord<T> | null> {
      const raw = await options.cache.get(cacheKey(id));
      if (!raw) {
        return null;
      }
      return {
        id,
        data: JSON.parse(raw) as T,
      };
    },
    async set<T extends SessionData = SessionData>(id: string, data: T): Promise<void> {
      await options.cache.set(cacheKey(id), JSON.stringify(data), ttlSec);
    },
    async destroy(id: string): Promise<void> {
      await options.cache.del(cacheKey(id));
    },
  };
}

