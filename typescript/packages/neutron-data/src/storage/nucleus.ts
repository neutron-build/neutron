// ---------------------------------------------------------------------------
// StorageDriver backed by Nucleus Blob model
// ---------------------------------------------------------------------------
//
// When the application connects to Nucleus, this adapter bridges
// neutron-data's StorageDriver interface to the Blob model's SQL functions.
// ---------------------------------------------------------------------------

import type { StorageDriver, StorageObject } from "./index.js";

/**
 * Blob-like interface matching the subset of @neutron/nucleus BlobModel
 * needed by NucleusStorageDriver.
 */
export interface NucleusBlobLike {
  put(bucket: string, key: string, data: Uint8Array | string, opts?: { contentType?: string }): Promise<void>;
  get(bucket: string, key: string): Promise<{ data: Uint8Array; meta: unknown } | null>;
  delete(bucket: string, key: string): Promise<boolean>;
}

export interface NucleusStorageDriverOptions {
  /** A Blob model instance (from `@neutron/nucleus`). */
  blob: NucleusBlobLike;
  /** Bucket name for all stored objects (default `"default"`). */
  bucket?: string;
}

/**
 * StorageDriver implementation backed by Nucleus Blob.
 *
 * Drop-in replacement for `InMemoryStorageDriver` or `S3StorageDriver`
 * that stores binary data directly in Nucleus.
 */
export class NucleusStorageDriver implements StorageDriver {
  private readonly blob: NucleusBlobLike;
  private readonly bucket: string;

  constructor(options: NucleusStorageDriverOptions) {
    this.blob = options.blob;
    this.bucket = options.bucket ?? "default";
  }

  async put(object: StorageObject): Promise<void> {
    await this.blob.put(this.bucket, object.key, object.body, {
      contentType: object.contentType,
    });
  }

  async get(key: string): Promise<StorageObject | null> {
    const result = await this.blob.get(this.bucket, key);
    if (!result) return null;
    return {
      key,
      body: result.data,
    };
  }

  async del(key: string): Promise<void> {
    await this.blob.delete(this.bucket, key);
  }
}

/**
 * Factory function matching the pattern of `createS3StorageDriver`.
 */
export function createNucleusStorageDriver(options: NucleusStorageDriverOptions): NucleusStorageDriver {
  return new NucleusStorageDriver(options);
}
