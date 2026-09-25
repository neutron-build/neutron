// ---------------------------------------------------------------------------
// @neutron-build/nucleus/blob — Blob storage model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures, QuerySignalOptions, SqlTableIdentity } from '../types.js';
import { requireNucleus, assertIdentifier } from '../helpers.js';
import { NucleusNotFoundError, NucleusError } from '../errors.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BlobMeta {
  key: string;
  size: number;
  contentType: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface BlobPutOptions {
  /** MIME content type (default `application/octet-stream`). */
  contentType?: string;
  /** Custom key/value metadata tags. Write-only on this engine: BLOB_TAG
   *  stores tags but no SQL read function exists for them. */
  metadata?: Record<string, string>;
  /** Abort the operation. Aborting between the store and the tag loop (or
   *  mid-loop) triggers best-effort cleanup: the stored blob is deleted so
   *  a canceled put leaves no partial blob. Store + tags are separate engine
   *  statements and are NOT atomic. */
  signal?: AbortSignal;
}

export interface BlobGetOptions {
  /** Abort while fetching. */
  signal?: AbortSignal;
}

export interface BlobReadStreamOptions {
  /** Chunk size in bytes (default 65536). Chunks are slices of one fetched
   *  copy — the engine boundary is whole-object (no server streaming). */
  chunkBytes?: number;
  /** Abort between yielded chunks stops iteration. */
  signal?: AbortSignal;
}

/** Result of a ranged read: the requested bytes and the blob's total size. */
export interface BlobRangeResult {
  data: Uint8Array;
  total: number;
}

/** Error for a range that cannot be satisfied (offset beyond EOF). */
export class BlobRangeError extends NucleusError {
  constructor(key: string, offset: number, size: number) {
    super(
      'BLOB_RANGE',
      `range not satisfiable: offset ${offset} is beyond blob ${JSON.stringify(key)} size ${size}`,
    );
  }
}

/**
 * A bucket bound to a name (and optionally a SQL table identity). Blob keys
 * resolve to `<schema>.<table>:<name>/<key>` — a resource-reference binding:
 * no DDL, no SQL columns, no cross-model atomicity claim. Methods take only
 * the key: the bucket is already chosen.
 */
export interface BlobBucket {
  put(key: string, data: Uint8Array | string, opts?: BlobPutOptions): Promise<void>;
  putStream(key: string, source: AsyncIterable<Uint8Array>, opts?: BlobPutOptions): Promise<{ size: number }>;
  get(key: string, opts?: BlobGetOptions): Promise<{ data: Uint8Array; meta: BlobMeta | null } | null>;
  getRange(key: string, offset: number, length: number, opts?: BlobGetOptions): Promise<BlobRangeResult | null>;
  size(key: string): Promise<number | null>;
  openRead(key: string, opts?: BlobReadStreamOptions): AsyncIterable<Uint8Array>;
  delete(key: string, opts?: BlobGetOptions): Promise<boolean>;
  meta(key: string): Promise<BlobMeta | null>;
  tag(key: string, tagKey: string, tagValue: string): Promise<boolean>;
  list(prefix: string, opts?: BlobGetOptions): Promise<string[]>;
  exists(key: string): Promise<boolean>;
}

// ---------------------------------------------------------------------------
// BlobModel interface
// ---------------------------------------------------------------------------

export interface BlobModel {
  /** Store a blob. `data` is a Uint8Array or a hex-encoded string. */
  put(bucket: string, key: string, data: Uint8Array | string, opts?: BlobPutOptions): Promise<void>;

  /**
   * Store a blob from a stream. Chunks are consumed and buffered — the
   * engine's write boundary is one whole-object BLOB_STORE (hex, 100 MB
   * cap), so this is NOT incremental server upload; the cap is enforced
   * client-side BEFORE anything is stored. Aborting during accumulation
   * leaves the store untouched.
   */
  putStream(bucket: string, key: string, source: AsyncIterable<Uint8Array>, opts?: BlobPutOptions): Promise<{ size: number }>;

  /** Retrieve a blob. Returns the decoded bytes and metadata, or `null`. */
  get(bucket: string, key: string, opts?: BlobGetOptions): Promise<{ data: Uint8Array; meta: BlobMeta | null } | null>;

  /**
   * Read `length` bytes starting at `offset`. Semantics: `length` 0 → empty
   * result; `offset == size` → empty result; `offset + length > size` →
   * clamped to EOF; `offset > size` → BlobRangeError; negative offset or
   * length → error. The engine has no server-side ranges: this fetches the
   * blob once and slices byte-exactly.
   */
  getRange(bucket: string, key: string, offset: number, length: number, opts?: BlobGetOptions): Promise<BlobRangeResult | null>;

  /** Blob size in bytes, or null if missing. */
  size(bucket: string, key: string): Promise<number | null>;

  /**
   * Yield consecutive byte-exact chunks of the blob (one fetch, sliced —
   * see BlobReadStreamOptions). Returns null-equivalent by throwing
   * NucleusNotFoundError on first iteration when the blob is missing.
   */
  openRead(bucket: string, key: string, opts?: BlobReadStreamOptions): AsyncIterable<Uint8Array>;

  /** Delete a blob. Returns `true` if it existed. */
  delete(bucket: string, key: string, opts?: BlobGetOptions): Promise<boolean>;

  /** Get metadata for a blob. */
  meta(bucket: string, key: string): Promise<BlobMeta | null>;

  /**
   * Tag a blob with a key/value pair. WRITE-ONLY on this engine: the blob
   * store keeps tags but exposes no SQL read function for them.
   */
  tag(bucket: string, key: string, tagKey: string, tagValue: string): Promise<boolean>;

  /** List blob keys matching a prefix (keys are returned relative to the bucket). */
  list(bucket: string, prefix: string, opts?: BlobGetOptions): Promise<string[]>;

  /** Check if a blob exists. */
  exists(bucket: string, key: string): Promise<boolean>;

  /** Return the total number of stored blobs. */
  blobCount(): Promise<number>;

  /** Return the deduplication ratio. */
  dedupRatio(): Promise<number>;

  /**
   * Create an identity-bound bucket view. Keys written through it carry the
   * `<schema>.<table>:<name>/` scope. Emits no SQL at bind time — a
   * resource reference, never a SQL column.
   */
  bucket(name: string, boundTo?: SqlTableIdentity): BlobBucket;
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

/** Engine cap: BLOB_STORE rejects data_hex longer than 200,000,000 chars. */
export const BLOB_MAX_BYTES = 100_000_000;

function toHex(data: Uint8Array | string): string {
  if (typeof data === 'string') {
    // Validate at write time with the same rule fromHex applies on read,
    // so garbage fails here instead of on every later get().
    if (data.length % 2 !== 0) {
      throw new Error('Invalid hex string: odd length');
    }
    if (!/^[0-9a-fA-F]*$/.test(data)) {
      throw new Error('Invalid hex string: contains non-hex characters');
    }
    return data;
  }
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

// Engine BLOB_META emits {"size":N,"content_type":"...","created_at":MS,"updated_at":MS}
// (timestamps are epoch-millisecond integers; the key is not included).
interface RawBlobMeta {
  size: number;
  content_type: string;
  created_at: number;
  updated_at: number;
}

function parseBlobMeta(key: string, raw: RawBlobMeta): BlobMeta {
  return {
    key,
    size: raw.size,
    contentType: raw.content_type,
    createdAt: new Date(raw.created_at),
    updatedAt: new Date(raw.updated_at),
  };
}

function isAbortError(err: unknown): boolean {
  return err instanceof Error && err.name === 'AbortError';
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class BlobModelImpl implements BlobModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
    /** Fixed bucket scope for identity-bound views; undefined on the root model. */
    private readonly scopePrefix?: string,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Blob');
  }

  private fullKey(bucket: string, key: string): string {
    const b = this.scopePrefix ? `${this.scopePrefix}${bucket}` : bucket;
    return `${b}/${key}`;
  }

  bucket(name: string, boundTo?: SqlTableIdentity): BlobBucket {
    if (boundTo != null) {
      assertIdentifier(boundTo.schema, 'bucket schema');
      assertIdentifier(boundTo.table, 'bucket table');
    }
    if (name.includes('/') || name.includes(':')) {
      throw new Error(
        `Invalid bucket name ${JSON.stringify(name)}: '/' and ':' would make scoped keys ambiguous`,
      );
    }
    const bucketName = ((this.scopePrefix ?? '') + (boundTo ? `${boundTo.schema}.${boundTo.table}:${name}` : name));
    // Pure client-side handle — binding emits no SQL.
    return new BucketView(this, bucketName);
  }

  async put(bucket: string, key: string, data: Uint8Array | string, opts: BlobPutOptions = {}): Promise<void> {
    this.require();
    const fullKey = this.fullKey(bucket, key);
    const hexData = toHex(data);
    if (hexData.length / 2 > BLOB_MAX_BYTES) {
      throw new NucleusError(
        'BLOB_TOO_LARGE',
        `blob ${JSON.stringify(key)} is ${hexData.length / 2} bytes; the engine caps BLOB_STORE at ${BLOB_MAX_BYTES}`,
      );
    }
    const contentType = opts.contentType ?? 'application/octet-stream';

    await this.transport.execute('SELECT BLOB_STORE($1, $2, $3)', [fullKey, hexData, contentType], {
      signal: opts.signal,
    });

    if (opts.metadata) {
      try {
        for (const [k, v] of Object.entries(opts.metadata)) {
          if (opts.signal?.aborted) throw new DOMException('This operation was aborted', 'AbortError');
          await this.transport.execute('SELECT BLOB_TAG($1, $2, $3)', [fullKey, k, v], {
            signal: opts.signal,
          });
        }
      } catch (err) {
        if (isAbortError(err) || opts.signal?.aborted) {
          // Cancellation between store and tags leaves a blob without its
          // metadata — a partial put. Clean up so a canceled put leaves
          // nothing, then surface the abort. (Store + tags are separate
          // engine statements; this cleanup is best-effort.)
          await this.transport.execute('SELECT BLOB_DELETE($1)', [fullKey]).catch(() => {});
          throw err;
        }
        throw err;
      }
    }
  }

  async putStream(
    bucket: string,
    key: string,
    source: AsyncIterable<Uint8Array>,
    opts: BlobPutOptions = {},
  ): Promise<{ size: number }> {
    this.require();
    const chunks: Uint8Array[] = [];
    let total = 0;
    for await (const chunk of source) {
      if (opts.signal?.aborted) {
        throw new DOMException('This operation was aborted', 'AbortError');
      }
      total += chunk.byteLength;
      if (total > BLOB_MAX_BYTES) {
        throw new NucleusError(
          'BLOB_TOO_LARGE',
          `blob ${JSON.stringify(key)} exceeded the engine's ${BLOB_MAX_BYTES}-byte cap mid-stream; nothing was stored`,
        );
      }
      chunks.push(chunk);
    }
    if (opts.signal?.aborted) {
      throw new DOMException('This operation was aborted', 'AbortError');
    }
    const data = new Uint8Array(total);
    let at = 0;
    for (const c of chunks) {
      data.set(c, at);
      at += c.byteLength;
    }
    await this.put(bucket, key, data, opts);
    return { size: total };
  }

  private async fetchBytes(fullKey: string, opts?: BlobGetOptions): Promise<Uint8Array | null> {
    // NOTE: an empty blob's hex is '' — only a true NULL means missing.
    const hexData = await this.transport.fetchval<string>('SELECT BLOB_GET($1)', [fullKey], {
      signal: opts?.signal,
    });
    if (hexData === null || hexData === undefined) return null;
    return fromHex(hexData);
  }

  async get(bucket: string, key: string, opts?: BlobGetOptions): Promise<{ data: Uint8Array; meta: BlobMeta | null } | null> {
    this.require();
    const fk = this.fullKey(bucket, key);
    const data = await this.fetchBytes(fk, opts);
    if (data === null) return null;
    const meta = await this.meta(bucket, key);
    return { data, meta };
  }

  async getRange(
    bucket: string,
    key: string,
    offset: number,
    length: number,
    opts?: BlobGetOptions,
  ): Promise<BlobRangeResult | null> {
    this.require();
    if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length < 0) {
      throw new NucleusError(
        'BLOB_RANGE',
        `invalid range offset=${offset} length=${length}: both must be non-negative integers`,
      );
    }
    const data = await this.fetchBytes(this.fullKey(bucket, key), opts);
    if (data === null) return null;
    if (offset > data.byteLength) {
      throw new BlobRangeError(key, offset, data.byteLength);
    }
    const end = Math.min(offset + length, data.byteLength);
    return { data: data.slice(offset, end), total: data.byteLength };
  }

  /** Blob size in bytes, or null if missing. Convenience over meta(). */
  async size(bucket: string, key: string): Promise<number | null> {
    const meta = await this.meta(bucket, key);
    return meta ? meta.size : null;
  }

  async *openRead(bucket: string, key: string, opts: BlobReadStreamOptions = {}): AsyncIterable<Uint8Array> {
    this.require();
    const chunkBytes = opts.chunkBytes ?? 65536;
    if (!Number.isInteger(chunkBytes) || chunkBytes <= 0) {
      throw new NucleusError('BLOB_CHUNK', `chunkBytes must be a positive integer, got ${chunkBytes}`);
    }
    const data = await this.fetchBytes(this.fullKey(bucket, key), { signal: opts.signal });
    if (data === null) {
      throw new NucleusNotFoundError(`blob ${JSON.stringify(key)} not found`);
    }
    for (let at = 0; at < data.byteLength; at += chunkBytes) {
      if (opts.signal?.aborted) {
        throw new DOMException('This operation was aborted', 'AbortError');
      }
      yield data.slice(at, Math.min(at + chunkBytes, data.byteLength));
    }
  }

  async delete(bucket: string, key: string, opts?: BlobGetOptions): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT BLOB_DELETE($1)', [this.fullKey(bucket, key)], {
        signal: opts?.signal,
      })) ?? false
    );
  }

  async meta(bucket: string, key: string): Promise<BlobMeta | null> {
    this.require();
    const fullKey = this.fullKey(bucket, key);
    const raw = await this.transport.fetchval<string>('SELECT BLOB_META($1)', [fullKey]);
    if (!raw) return null;
    return parseBlobMeta(key, JSON.parse(raw) as RawBlobMeta);
  }

  async tag(bucket: string, key: string, tagKey: string, tagValue: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT BLOB_TAG($1, $2, $3)', [this.fullKey(bucket, key), tagKey, tagValue])) ?? false;
  }

  async list(bucket: string, prefix: string, opts?: BlobGetOptions): Promise<string[]> {
    this.require();
    const b = this.scopePrefix ? `${this.scopePrefix}${bucket}` : bucket;
    const fullPrefix = `${b}/${prefix}`;
    const raw = await this.transport.fetchval<string>('SELECT BLOB_LIST($1)', [fullPrefix], {
      signal: opts?.signal,
    });
    if (!raw) return [];
    // Engine emits a JSON array of full key strings; strip the bucket prefix.
    const keys = JSON.parse(raw) as string[];
    return keys.map((k) => (k.startsWith(`${b}/`) ? k.slice(b.length + 1) : k));
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

/** A bound bucket: same engine protocol, bucket argument pre-filled. */
class BucketView implements BlobBucket {
  constructor(
    private readonly impl: BlobModelImpl,
    private readonly bucket: string,
  ) {}

  put(key: string, data: Uint8Array | string, opts?: BlobPutOptions): Promise<void> {
    return this.impl.put(this.bucket, key, data, opts);
  }
  putStream(key: string, source: AsyncIterable<Uint8Array>, opts?: BlobPutOptions): Promise<{ size: number }> {
    return this.impl.putStream(this.bucket, key, source, opts);
  }
  get(key: string, opts?: BlobGetOptions) {
    return this.impl.get(this.bucket, key, opts);
  }
  getRange(key: string, offset: number, length: number, opts?: BlobGetOptions) {
    return this.impl.getRange(this.bucket, key, offset, length, opts);
  }
  size(key: string) {
    return this.impl.size(this.bucket, key);
  }
  openRead(key: string, opts?: BlobReadStreamOptions) {
    return this.impl.openRead(this.bucket, key, opts);
  }
  delete(key: string, opts?: BlobGetOptions) {
    return this.impl.delete(this.bucket, key, opts);
  }
  meta(key: string) {
    return this.impl.meta(this.bucket, key);
  }
  tag(key: string, tagKey: string, tagValue: string) {
    return this.impl.tag(this.bucket, key, tagKey, tagValue);
  }
  list(prefix: string, opts?: BlobGetOptions) {
    return this.impl.list(this.bucket, prefix, opts);
  }
  exists(key: string) {
    return this.impl.exists(this.bucket, key);
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
