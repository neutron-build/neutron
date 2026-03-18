// ---------------------------------------------------------------------------
// @neutron/nucleus/blob — Blob storage model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BlobMeta {
  key: string;
  size: number;
  contentType: string;
  createdAt: Date;
  metadata?: Record<string, string>;
}

export interface BlobPutOptions {
  /** MIME content type (default `application/octet-stream`). */
  contentType?: string;
  /** Custom key/value metadata tags. */
  metadata?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// BlobModel interface
// ---------------------------------------------------------------------------

export interface BlobModel {
  /** Store a blob. `data` is a Uint8Array or a hex-encoded string. */
  put(bucket: string, key: string, data: Uint8Array | string, opts?: BlobPutOptions): Promise<void>;

  /** Retrieve a blob. Returns the decoded bytes and metadata, or `null`. */
  get(bucket: string, key: string): Promise<{ data: Uint8Array; meta: BlobMeta | null } | null>;

  /** Delete a blob. Returns `true` if it existed. */
  delete(bucket: string, key: string): Promise<boolean>;

  /** Get metadata for a blob. */
  meta(bucket: string, key: string): Promise<BlobMeta | null>;

  /** Tag a blob with a key/value pair. */
  tag(bucket: string, key: string, tagKey: string, tagValue: string): Promise<boolean>;

  /** List blobs matching a prefix. */
  list(bucket: string, prefix: string): Promise<BlobMeta[]>;

  /** Check if a blob exists. */
  exists(bucket: string, key: string): Promise<boolean>;

  /** Return the total number of stored blobs. */
  blobCount(): Promise<number>;

  /** Return the deduplication ratio. */
  dedupRatio(): Promise<number>;
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

function toHex(data: Uint8Array | string): string {
  if (typeof data === 'string') return data;
  return Array.from(data)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error('Invalid hex string: odd length');
  }
  if (!/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error('Invalid hex string: contains non-hex characters');
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

interface RawBlobMeta {
  key: string;
  size: number;
  content_type: string;
  created_at: string;
  metadata?: Record<string, string>;
}

function parseBlobMeta(raw: RawBlobMeta): BlobMeta {
  return {
    key: raw.key,
    size: raw.size,
    contentType: raw.content_type,
    createdAt: new Date(raw.created_at),
    metadata: raw.metadata,
  };
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class BlobModelImpl implements BlobModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Blob');
  }

  async put(bucket: string, key: string, data: Uint8Array | string, opts: BlobPutOptions = {}): Promise<void> {
    this.require();
    const fullKey = `${bucket}/${key}`;
    const hexData = toHex(data);
    const contentType = opts.contentType ?? 'application/octet-stream';

    await this.transport.execute('SELECT BLOB_STORE($1, $2, $3)', [fullKey, hexData, contentType]);

    if (opts.metadata) {
      for (const [k, v] of Object.entries(opts.metadata)) {
        await this.transport.execute('SELECT BLOB_TAG($1, $2, $3)', [fullKey, k, v]);
      }
    }
  }

  async get(bucket: string, key: string): Promise<{ data: Uint8Array; meta: BlobMeta | null } | null> {
    this.require();
    const fullKey = `${bucket}/${key}`;
    const hexData = await this.transport.fetchval<string>('SELECT BLOB_GET($1)', [fullKey]);
    if (hexData === null) return null;

    const meta = await this.meta(bucket, key);
    return { data: fromHex(hexData), meta };
  }

  async delete(bucket: string, key: string): Promise<boolean> {
    this.require();
    const fullKey = `${bucket}/${key}`;
    return (await this.transport.fetchval<boolean>('SELECT BLOB_DELETE($1)', [fullKey])) ?? false;
  }

  async meta(bucket: string, key: string): Promise<BlobMeta | null> {
    this.require();
    const fullKey = `${bucket}/${key}`;
    const raw = await this.transport.fetchval<string>('SELECT BLOB_META($1)', [fullKey]);
    if (!raw) return null;
    return parseBlobMeta(JSON.parse(raw) as RawBlobMeta);
  }

  async tag(bucket: string, key: string, tagKey: string, tagValue: string): Promise<boolean> {
    this.require();
    const fullKey = `${bucket}/${key}`;
    return (await this.transport.fetchval<boolean>('SELECT BLOB_TAG($1, $2, $3)', [fullKey, tagKey, tagValue])) ?? false;
  }

  async list(bucket: string, prefix: string): Promise<BlobMeta[]> {
    this.require();
    const fullPrefix = `${bucket}/${prefix}`;
    const raw = await this.transport.fetchval<string>('SELECT BLOB_LIST($1)', [fullPrefix]);
    if (!raw) return [];
    const items = JSON.parse(raw) as RawBlobMeta[];
    return items.map(parseBlobMeta);
  }

  async exists(bucket: string, key: string): Promise<boolean> {
    const meta = await this.meta(bucket, key);
    return meta !== null;
  }

  async blobCount(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT BLOB_COUNT()')) ?? 0;
  }

  async dedupRatio(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT BLOB_DEDUP_RATIO()')) ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.blob` to the client. */
export const withBlob: NucleusPlugin<{ blob: BlobModel }> = {
  name: 'blob',
  init(transport: Transport, features: NucleusFeatures) {
    return { blob: new BlobModelImpl(transport, features) };
  },
};
