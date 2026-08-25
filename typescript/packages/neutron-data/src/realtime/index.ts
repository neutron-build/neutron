type Subscriber = (payload: unknown) => void;

export interface RealtimeBus {
  publish(channel: string, payload: unknown): Promise<void>;
  subscribe(channel: string, subscriber: Subscriber): () => void;
}

export class InMemoryRealtimeBus implements RealtimeBus {
  private channels = new Map<string, Set<Subscriber>>();

  async publish(channel: string, payload: unknown): Promise<void> {
    const subs = this.channels.get(channel);
    if (!subs) {
      return;
    }
    // Same failure semantics as the Redis bus: one bad subscriber is logged
    // and skipped, and every remaining subscriber still receives the payload.
    // Letting the throw escape would abort delivery mid-list and reject a
    // publish() the publisher cannot fix.
    for (const subscriber of subs) {
      try {
        subscriber(payload);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(
          `[neutron-data] InMemoryRealtimeBus: handler error on "${channel}": ${msg}`
        );
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
      if (!existing) {
        return;
      }
      existing.delete(subscriber);
      if (existing.size === 0) {
        this.channels.delete(channel);
      }
    };
  }
}

