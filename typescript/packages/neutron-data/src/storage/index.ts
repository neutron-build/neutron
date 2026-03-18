export interface StorageObject {
  key: string;
  body: Uint8Array;
  contentType?: string;
}

export interface StorageDriver {
  put(object: StorageObject): Promise<void>;
  get(key: string): Promise<StorageObject | null>;
  del(key: string): Promise<void>;
}

export class InMemoryStorageDriver implements StorageDriver {
  private store = new Map<string, StorageObject>();

  async put(object: StorageObject): Promise<void> {
    this.store.set(object.key, object);
  }

  async get(key: string): Promise<StorageObject | null> {
    return this.store.get(key) || null;
  }

  async del(key: string): Promise<void> {
    this.store.delete(key);
  }
}

