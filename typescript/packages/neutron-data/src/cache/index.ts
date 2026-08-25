interface CacheRecord {
  value: string;
  expiresAt: number | null;
}

export interface CacheClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSec?: number): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string, ttlSec?: number): Promise<number>;
}

export class MemoryCacheClient implements CacheClient {
  private store = new Map<string, CacheRecord>();

  async get(key: string): Promise<string | null> {
    const record = this.store.get(key);
    if (!record) {
      return null;
    }

    if (record.expiresAt !== null && record.expiresAt <= Date.now()) {
      this.store.delete(key);
      return null;
    }

    return record.value;
  }

  async set(key: string, value: string, ttlSec?: number): Promise<void> {
    const expiresAt =
      typeof ttlSec === "number" && ttlSec > 0 ? Date.now() + ttlSec * 1000 : null;
    this.store.set(key, { value, expiresAt });
  }

  async del(key: string): Promise<void> {
    this.store.delete(key);
  }

  async incr(key: string, ttlSec?: number): Promise<number> {
    const existing = this.store.get(key);
    const expired =
      existing?.expiresAt != null && existing.expiresAt <= Date.now();
    if (existing && !expired) {
      // The TTL is anchored at key creation (Redis INCR + EXPIRE-once
      // semantics); a later increment must not extend the key's expiry.
      this.store.set(key, { value: String(next(existing.value)), expiresAt: existing.expiresAt });
      return next(existing.value);
    }
    const expiresAt =
      typeof ttlSec === "number" && ttlSec > 0 ? Date.now() + ttlSec * 1000 : null;
    this.store.set(key, { value: "1", expiresAt });
    return 1;
  }
}

function next(raw: string): number {
  const current = Number.parseInt(raw, 10);
  return Number.isFinite(current) ? current + 1 : 1;
}

