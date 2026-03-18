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
    for (const subscriber of subs) {
      subscriber(payload);
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

