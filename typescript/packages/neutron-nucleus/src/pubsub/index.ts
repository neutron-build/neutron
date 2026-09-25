// ---------------------------------------------------------------------------
// @neutron-build/nucleus/pubsub — PubSub model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// PubSubModel interface
// ---------------------------------------------------------------------------

/**
 * What this surface honestly is (Nucleus 1.0.x, verified live — see
 * conformance/live/orm x05 leg):
 *
 * The SQL `PUBSUB_*` functions address the engine-internal broadcast hub.
 * Over the PostgreSQL wire there is NO subscribe statement — nothing a SQL
 * client can do attaches a receiver to that hub. `publish` therefore returns
 * 0 and `channels`/`subscribers` report empty from a standalone SQL
 * deployment: the fan-out reaches in-process subscribers (the embedded API)
 * and cluster-forwarded nodes only. The RESP interface on port 6379 has its
 * own, SEPARATE pub/sub registry — a RESP `SUBSCRIBE` does not see a SQL
 * `PUBSUB_PUBLISH`, in either direction.
 *
 * Delivery guarantees: none that a SQL client can observe. Messages are not
 * persisted (no WAL, no replay); a publish with no receiver is discarded
 * entirely. If it were reachable, the hub's per-channel buffer is bounded
 * (1024) with oldest-first loss for lagging subscribers.
 *
 * For observable fan-out use LISTEN/NOTIFY on a dedicated listener
 * connection (`@neutron-build/sql/listen-notify`). Nucleus 1.0.x DOES
 * implement LISTEN/NOTIFY with real cross-connection delivery — verified
 * live by the x05 leg — with divergences from PostgreSQL: delivery flushes
 * around the listening connection's own statement traffic (an idle listener
 * sees nothing until it queries — pass `pollIntervalMs`), notifications
 * emitted inside rolled-back transactions are still delivered, and channel
 * keys carry the raw LISTEN statement's quotes (normalized by the client).
 */
export interface PubSubModel {
  /**
   * Publish a message on a channel. Returns the number of hub subscribers
   * reached — 0 in any standalone SQL deployment (see interface docs).
   */
  publish(channel: string, message: string): Promise<number>;

  /**
   * Return the hub's live channels as a comma-separated string (sorted by
   * the engine). No pattern filtering exists in the engine — an empty string
   * means no channel currently has a subscriber.
   */
  channels(): Promise<string>;

  /** Return the number of hub subscribers on a channel. */
  subscribers(channel: string): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class PubSubModelImpl implements PubSubModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'PubSub');
  }

  async publish(channel: string, message: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT PUBSUB_PUBLISH($1, $2)', [channel, message])) ?? 0;
  }

  async channels(): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT PUBSUB_CHANNELS()')) ?? '';
  }

  async subscribers(channel: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT PUBSUB_SUBSCRIBERS($1)', [channel])) ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.pubsub` to the client. */
export const withPubSub: NucleusPlugin<{ pubsub: PubSubModel }> = {
  name: 'pubsub',
  init(transport: Transport, features: NucleusFeatures) {
    return { pubsub: new PubSubModelImpl(transport, features) };
  },
};
