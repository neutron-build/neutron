import type { RealtimeBus } from "./index.js";
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
  /** Optional channel prefix, e.g. "neutron:" → publish to "neutron:my-channel". */
  channelPrefix?: string;
}

export class RedisRealtimeBus implements RealtimeBus {
  private readonly publisher: RedisPublisherLike;
  private subscriber: RedisSubscriberLike | null = null;
  private readonly channels = new Map<string, Set<Subscriber>>();
  private readonly channelPrefix: string;
  private closed = false;

  constructor(
    publisher: RedisPublisherLike,
    channelPrefix = ""
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
    if (this.closed) {
      throw new Error("RedisRealtimeBus is closed.");
    }

    const prefixedChannel = this.prefixed(channel);
    let subs = this.channels.get(prefixedChannel);
    const isNew = !subs;

    if (!subs) {
      subs = new Set<Subscriber>();
      this.channels.set(prefixedChannel, subs);
    }
    subs.add(subscriber);

    // If this is the first subscriber on this channel, tell Redis.
    if (isNew) {
      const sub = this.ensureSubscriber();
      // Fire-and-forget; the subscriber will buffer messages once acked.
      sub.subscribe(prefixedChannel).catch((err: unknown) => {
        // If subscribing fails, clean up to avoid a dangling channel.
        this.channels.delete(prefixedChannel);
        const msg = err instanceof Error ? err.message : String(err);
        console.error(
          `[neutron-data] RedisRealtimeBus: failed to subscribe to "${prefixedChannel}": ${msg}`
        );
      });
    }

    // Return an unsubscribe function (mirrors InMemoryRealtimeBus).
    return () => {
      const existing = this.channels.get(prefixedChannel);
      if (!existing) {
        return;
      }
      existing.delete(subscriber);
      if (existing.size === 0) {
        this.channels.delete(prefixedChannel);
        // Unsubscribe from Redis when no local handlers remain.
        if (this.subscriber) {
          this.subscriber.unsubscribe(prefixedChannel).catch(() => {
            // best-effort
          });
        }
      }
    };
  }

  // ── close ────────────────────────────────────────────────────────────
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.channels.clear();

    if (this.subscriber) {
      this.subscriber.removeAllListeners("message");
      await this.subscriber.quit().catch(() => {});
      this.subscriber = null;
    }
    await this.publisher.quit().catch(() => {});
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
      options.channelPrefix ?? ""
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
