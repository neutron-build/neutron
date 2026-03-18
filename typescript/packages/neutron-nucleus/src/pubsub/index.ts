// ---------------------------------------------------------------------------
// @neutron/nucleus/pubsub — PubSub model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// PubSubModel interface
// ---------------------------------------------------------------------------

export interface PubSubModel {
  /** Publish a message on a channel. Returns the number of subscribers reached. */
  publish(channel: string, message: string): Promise<number>;

  /** Return active channels matching an optional pattern (empty = all). */
  channels(pattern?: string): Promise<string>;

  /** Return the number of subscribers on a channel. */
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

  async channels(pattern?: string): Promise<string> {
    this.require();
    if (pattern) {
      return (await this.transport.fetchval<string>('SELECT PUBSUB_CHANNELS($1)', [pattern])) ?? '';
    }
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
