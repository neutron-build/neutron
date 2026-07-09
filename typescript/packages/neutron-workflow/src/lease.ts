import type { ExecuteRunOptions, RunOutcome } from "./run.js";
import { executeRun } from "./run.js";

/**
 * Structurally matches @neutron-build/nucleus's KVModel lease surface
 * (KV_SETNX with TTL, KV_CDEL, KV_CEXPIRE — the atomic primitives).
 */
export interface KVLike {
  setNX(key: string, value: string, opts?: { ttl?: number }): Promise<boolean>;
  cdel(key: string, expected: string): Promise<boolean>;
  cexpire(key: string, expected: string, seconds: number): Promise<boolean>;
}

export interface LeaseManagerOptions {
  /** Key prefix (default "wf:lease"). */
  prefix?: string;
  /** Lease TTL in seconds (default 30). Crashed holders free up after this. */
  ttlSeconds?: number;
}

/**
 * Single-executor-per-run locks on the atomic KV primitives: acquire is
 * SETNX-with-TTL (value and expiry in one critical section), renewal and
 * release are value-conditional so a holder whose lease was taken over
 * can neither extend nor delete the new holder's lock. Leases guarantee
 * liveness, not exclusivity under all physics — a lease can expire while
 * its holder stalls mid-step, which is why the event log dedupes by seq.
 */
export class LeaseManager {
  #kv: KVLike;
  #prefix: string;
  readonly ttlSeconds: number;

  constructor(kv: KVLike, options: LeaseManagerOptions = {}) {
    this.#kv = kv;
    this.#prefix = options.prefix ?? "wf:lease";
    this.ttlSeconds = options.ttlSeconds ?? 30;
  }

  /** Try to claim the run. Returns null when another executor holds it. */
  async acquire(runId: string, owner: string): Promise<Lease | null> {
    const key = `${this.#prefix}:${runId}`;
    const token = `${owner}:${crypto.randomUUID()}`;
    const acquired = await this.#kv.setNX(key, token, { ttl: this.ttlSeconds });
    return acquired ? new Lease(this.#kv, key, token, this.ttlSeconds) : null;
  }
}

export class Lease {
  #kv: KVLike;
  readonly key: string;
  readonly token: string;
  readonly ttlSeconds: number;

  constructor(kv: KVLike, key: string, token: string, ttlSeconds: number) {
    this.#kv = kv;
    this.key = key;
    this.token = token;
    this.ttlSeconds = ttlSeconds;
  }

  /** Heartbeat: extend the TTL. False means the lease was lost. */
  renew(): Promise<boolean> {
    return this.#kv.cexpire(this.key, this.token, this.ttlSeconds);
  }

  /** Release if still held; a lost lease releases nothing (and that's correct). */
  release(): Promise<boolean> {
    return this.#kv.cdel(this.key, this.token);
  }
}

export interface ExecuteRunExclusiveOptions<In> extends ExecuteRunOptions<In> {
  leases: LeaseManager;
  /** Executor identity, embedded in the lease token. */
  owner: string;
  /**
   * Fired once, only when THIS call wins the lease and is about to execute —
   * not when the lease is held elsewhere. Pairs 1:1 with the returned outcome,
   * so callers can count in-flight work without leaking on lease contention.
   */
  onStart?: () => void;
}

/**
 * Lease-guarded execution pass: claim the run, heartbeat at ttl/3 while
 * it executes, release afterward. Returns null when another executor
 * holds the run — callers just move on. Crash-safe by construction: a
 * dead executor's lease expires and the next claimer replays the log.
 */
export async function executeRunExclusive<In>(
  options: ExecuteRunExclusiveOptions<In>,
): Promise<RunOutcome | null> {
  const lease = await options.leases.acquire(options.runId, options.owner);
  if (lease === null) return null;
  options.onStart?.();

  const intervalMs = Math.max(1000, (lease.ttlSeconds * 1000) / 3);
  const heartbeat = setInterval(() => {
    void lease
      .renew()
      .then((held) => {
        if (!held) clearInterval(heartbeat);
      })
      .catch(() => {});
  }, intervalMs);
  heartbeat.unref?.();

  try {
    return await executeRun(options);
  } finally {
    clearInterval(heartbeat);
    await lease.release().catch(() => {});
  }
}
