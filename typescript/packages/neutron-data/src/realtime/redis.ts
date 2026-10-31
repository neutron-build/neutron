import type { RealtimeBus } from "./index.js";
import { closeResources } from "../internal/resources.js";
import { lazyImport } from "../internal/lazy-import.js";

type Subscriber = (payload: unknown) => void;

/**
 * Minimal interface for the ioredis publisher connection.
 * Only the methods we actually call are declared.
 */
interface RedisPublisherLike {
  publish(channel: string, message: string): Promise<number>;
  duplicate(): RedisSubscriberLike;
  quit(): Promise<unknown>;
  status: string;
}

/**
 * Minimal interface for the ioredis subscriber connection.
 * In ioredis, once `subscribe()` is called the connection enters
 * subscriber mode and can only issue (p)subscribe/(p)unsubscribe.
 */
interface RedisSubscriberLike {
  subscribe(...channels: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  on(event: string, listener: (...args: unknown[]) => void): this;
  removeAllListeners(event?: string): this;
  quit(): Promise<unknown>;
  status: string;
}

export interface RedisRealtimeBusOptions {
  /** Redis connection URL. Falls back to DRAGONFLY_URL / REDIS_URL / localhost. */
  url?: string;
  /** Supply your own ioredis client to use as the publisher connection. */
  publisherClient?: RedisPublisherLike;
  /** Injected publishers default to borrowed; factory-created publishers are owned. */
  publisherOwnership?: "borrowed" | "owned";
  /** Optional channel prefix, e.g. "neutron:" → publish to "neutron:my-channel". */
  channelPrefix?: string;
}

export class RedisRealtimeBus implements RealtimeBus {
  private readonly publisher: RedisPublisherLike;
  private subscriber: RedisSubscriberLike | null = null;
  private readonly channels = new Map<string, Set<Subscriber>>();
  /** Channels with a successfully completed Redis SUBSCRIBE. */
  private readonly subscribed = new Set<string>();
  private readonly channelPrefix: string;
  private closed = false;
  private operations: Promise<void> = Promise.resolve();
  private closePromise: Promise<void> | null = null;
  private cleanupErrors: unknown[] = [];

  constructor(
    publisher: RedisPublisherLike,
    channelPrefix = "",
    private readonly publisherOwnership: "borrowed" | "owned" = "owned",
  ) {
    this.publisher = publisher;
    this.channelPrefix = channelPrefix;
  }

  // ── publish ──────────────────────────────────────────────────────────
  async publish(channel: string, payload: unknown): Promise<void> {
    if (this.closed) {
      throw new Error("RedisRealtimeBus is closed.");
    }
    await this.publisher.publish(
      this.prefixed(channel),
      JSON.stringify(payload)
    );
  }

  // ── subscribe ────────────────────────────────────────────────────────
  subscribe(channel: string, subscriber: Subscriber): () => void {
    const registration = this.register(channel, subscriber);
    registration.ready.catch((error: unknown) => {
      console.error(`[neutron-data] RedisRealtimeBus: failed to subscribe to "${channel}": ${String(error)}`);
    });
    return registration.unsubscribe;
  }

  async subscribeAsync(channel: string, subscriber: Subscriber): Promise<() => void> {
    const registration = this.register(channel, subscriber);
    try { await registration.ready; }
    catch (error) { registration.unsubscribe(); throw error; }
    return registration.unsubscribe;
  }

  private enqueue(operation: () => Promise<void>): Promise<void> {
    const work = this.operations.then(operation);
    this.operations = work.catch(() => {});
    return work;
  }

  private register(channel: string, subscriber: Subscriber): { ready: Promise<void>; unsubscribe: () => void } {
    if (this.closed) throw new Error("RedisRealtimeBus is closed.");
    const key = this.prefixed(channel);
    const native = this.ensureSubscriber();
    let handlers = this.channels.get(key);
    if (!handlers) { handlers = new Set<Subscriber>(); this.channels.set(key, handlers); }
    handlers.add(subscriber);
    const registered = handlers;
    const ready = this.enqueue(async () => {
      if (this.closed || this.channels.get(key) !== registered) throw new Error("Subscription closed before readiness");
      if (!this.subscribed.has(key)) {
        await native.subscribe(key);
        if (this.closed) {
          try { await native.unsubscribe(key); } catch (error) { this.cleanupErrors.push(error); throw error; }
          throw new Error("Subscription closed during acquisition");
        }
        if (this.channels.get(key) !== registered) throw new Error("Subscription removed during acquisition");
        this.subscribed.add(key);
      }
    });
    let removed = false;
    const unsubscribe = (): void => {
      if (removed) return;
      removed = true;
      if (this.channels.get(key) !== registered) return;
      registered.delete(subscriber);
      if (registered.size) return;
      this.channels.delete(key);
      this.subscribed.delete(key);
      // Serialize teardown behind acquisitions and ahead of later registrations.
      void this.enqueue(async () => {
        if (!this.closed) {
          try { await native.unsubscribe(key); } catch (error) { this.cleanupErrors.push(error); throw error; }
        }
      }).catch(() => {});
    };
    return { ready, unsubscribe };
  }

  async close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    this.channels.clear();
    this.subscribed.clear();
    this.closePromise = (async () => {
      await this.operations;
      const resources: (() => unknown | Promise<unknown>)[] = [];
      if (this.subscriber) {
        const native = this.subscriber;
        resources.push(() => native.removeAllListeners("message"), () => native.quit());
        this.subscriber = null;
      }
      if (this.publisherOwnership === "owned") resources.push(() => this.publisher.quit());
      const failures = [...this.cleanupErrors, ...await closeResources(resources)];
      if (failures.length) throw new AggregateError(failures, "Realtime cleanup failed", { cause: failures[0] });
    })();
    return this.closePromise;
  }

  // ── internals ────────────────────────────────────────────────────────

  /**
   * Lazily create the subscriber connection by duplicating the publisher.
   * ioredis's `duplicate()` creates a new connection with the same options,
   * which is exactly what we need for subscriber-mode isolation.
   */
  private ensureSubscriber(): RedisSubscriberLike {
    if (this.subscriber) {
      return this.subscriber;
    }

    const sub = this.publisher.duplicate();
    sub.on("message", (rawChannel: unknown, rawMessage: unknown) => {
      const ch = String(rawChannel);
      const handlers = this.channels.get(ch);
      if (!handlers || handlers.size === 0) {
        return;
      }

      let parsed: unknown;
      try {
        parsed = JSON.parse(String(rawMessage));
      } catch {
        parsed = rawMessage;
      }

      for (const handler of handlers) {
        try {
          handler(parsed);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.error(
            `[neutron-data] RedisRealtimeBus: handler error on "${ch}": ${msg}`
          );
        }
      }
    });

    this.subscriber = sub;
    return sub;
  }

  private prefixed(channel: string): string {
    return this.channelPrefix ? `${this.channelPrefix}${channel}` : channel;
  }
}

// ── Factory ──────────────────────────────────────────────────────────────

export async function createRedisRealtimeBus(
  options: RedisRealtimeBusOptions = {}
): Promise<RedisRealtimeBus> {
  // If the caller already has an ioredis client, use it directly.
  if (options.publisherClient) {
    return new RedisRealtimeBus(
      options.publisherClient,
      options.channelPrefix ?? "",
      options.publisherOwnership ?? "borrowed",
    );
  }

  // Otherwise, lazy-require ioredis (it's an optional peer dep).
  const redisModule = await lazyImport<{
    default?: new (...args: unknown[]) => RedisPublisherLike;
  }>(
    "ioredis",
    "Install with `pnpm add ioredis` (or npm/yarn equivalent)"
  );

  const RedisCtor = redisModule.default;
  if (!RedisCtor) {
    throw new Error("Failed to resolve ioredis default export.");
  }

  const url =
    options.url ||
    process.env.DRAGONFLY_URL ||
    process.env.REDIS_URL ||
    "redis://127.0.0.1:6379";

  const publisher = new RedisCtor(url, {
    lazyConnect: false,
    maxRetriesPerRequest: 3,
  });

  return new RedisRealtimeBus(publisher, options.channelPrefix ?? "");
}
