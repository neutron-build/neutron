// ---------------------------------------------------------------------------
// RealtimeBus backed by Nucleus PubSub model
// ---------------------------------------------------------------------------
//
// When the application connects to Nucleus, this adapter bridges
// neutron-data's RealtimeBus interface to the PubSub model's SQL functions.
//
// Note: Nucleus PubSub.publish() is fire-and-forget over SQL. Subscription
// on the server side requires a persistent connection (LISTEN/NOTIFY or
// Nucleus's native streaming). This adapter uses polling for subscribe().
// For production use, prefer the Redis bus or Nucleus's native streaming API.
// ---------------------------------------------------------------------------

import type { RealtimeBus } from "./index.js";

/**
 * PubSub-like interface matching the subset of @neutron/nucleus PubSubModel.
 */
export interface NucleusPubSubLike {
  publish(channel: string, message: string): Promise<number>;
}

export interface NucleusRealtimeBusOptions {
  /** A PubSub model instance (from `@neutron/nucleus`). */
  pubsub: NucleusPubSubLike;
}

type Subscriber = (payload: unknown) => void;

/**
 * RealtimeBus implementation backed by Nucleus PubSub.
 *
 * Publish calls are delegated to Nucleus. Subscribe is handled in-process
 * (the PubSub SQL interface does not support server-push subscriptions).
 * Messages published from this process are delivered to local subscribers
 * immediately.
 */
export class NucleusRealtimeBus implements RealtimeBus {
  private readonly pubsub: NucleusPubSubLike;
  private readonly channels = new Map<string, Set<Subscriber>>();

  constructor(options: NucleusRealtimeBusOptions) {
    this.pubsub = options.pubsub;
  }

  async publish(channel: string, payload: unknown): Promise<void> {
    const message = JSON.stringify(payload);
    await this.pubsub.publish(channel, message);

    // Also deliver to local in-process subscribers
    const subs = this.channels.get(channel);
    if (subs) {
      for (const subscriber of subs) {
        subscriber(payload);
      }
    }
  }

  subscribe(channel: string, subscriber: Subscriber): () => void {
    let subs = this.channels.get(channel);
    if (!subs) {
      subs = new Set<Subscriber>();
      this.channels.set(channel, subs);
    }
    subs.add(subscriber);

    return () => {
      const existing = this.channels.get(channel);
      if (!existing) return;
      existing.delete(subscriber);
      if (existing.size === 0) {
        this.channels.delete(channel);
      }
    };
  }
}

/**
 * Factory function matching the pattern of `createRedisRealtimeBus`.
 */
export function createNucleusRealtimeBus(options: NucleusRealtimeBusOptions): NucleusRealtimeBus {
  return new NucleusRealtimeBus(options);
}
