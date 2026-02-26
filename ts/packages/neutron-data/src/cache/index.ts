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
    const currentRaw = await this.get(key);
    const current = currentRaw ? Number.parseInt(currentRaw, 10) : 0;
    const next = Number.isFinite(current) ? current + 1 : 1;
    await this.set(key, String(next), ttlSec);
    return next;
  }
}

