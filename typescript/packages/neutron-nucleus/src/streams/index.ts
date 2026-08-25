// ---------------------------------------------------------------------------
// @neutron-build/nucleus/streams — Streams model plugin (Redis Streams-compatible)
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';
import { NucleusQueryError } from '../errors.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface StreamEntry {
  id: string;
  fields: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// StreamsModel interface
// ---------------------------------------------------------------------------

export interface StreamsModel {
  /** Append an entry to a stream. Returns the generated entry ID. */
  xadd(stream: string, fields: Record<string, unknown>): Promise<string>;

  /** Return the number of entries in a stream. */
  xlen(stream: string): Promise<number>;

  /** Return entries between `startMs` and `endMs` timestamps (inclusive). */
  xrange(stream: string, startMs: number, endMs: number, count: number): Promise<StreamEntry[]>;

  /** Read new entries after `lastIdMs`. */
  xread(stream: string, lastIdMs: number, count: number): Promise<StreamEntry[]>;

  /** Create a consumer group on a stream. */
  xgroupCreate(stream: string, group: string, startId: number): Promise<boolean>;

  /**
   * Read entries from a consumer group.
   *
   * A missing group (or stream) answers NOGROUP as a statement error
   * (SQLSTATE 22000) since Nucleus v0.1.8; an empty delivery is `"[]"`.
   */
  xreadGroup(stream: string, group: string, consumer: string, count: number): Promise<StreamEntry[]>;

  /**
   * Acknowledge processing of an entry in a consumer group, by the id `xadd`
   * returned. Returns the number of entries acknowledged.
   *
   * The id is the `"<ms>-<seq>"` string xadd returns. This took idMs and idSeq as separate numbers, so the two ends of the same API did not compose — every caller split xadd's return value itself, and the consumer-group conformance case was xfail in all five SDKs for that reason.
   */
  xack(stream: string, group: string, entryId: string): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class StreamsModelImpl implements StreamsModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Streams');
  }

  async xadd(stream: string, fields: Record<string, unknown>): Promise<string> {
    this.require();
    if (Object.keys(fields).length === 0) {
      throw new Error('xadd requires at least one field/value pair (STREAM_XADD(stream, field1, value1, ...))');
    }
    // Build variadic args: stream, k1, v1, k2, v2, ...
    const args: unknown[] = [stream];
    for (const [k, v] of Object.entries(fields)) {
      args.push(k, v);
    }
    const placeholders = args.map((_, i) => `$${i + 1}`).join(', ');
    const sql = `SELECT STREAM_XADD(${placeholders})`;
    return (await this.transport.fetchval<string>(sql, args)) ?? '';
  }

  async xlen(stream: string): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT STREAM_XLEN($1)', [stream])) ?? 0;
  }

  async xrange(stream: string, startMs: number, endMs: number, count: number): Promise<StreamEntry[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT STREAM_XRANGE($1, $2, $3, $4)', [
      stream, startMs, endMs, count,
    ]);
    if (!raw) return [];
    return JSON.parse(raw) as StreamEntry[];
  }

  async xread(stream: string, lastIdMs: number, count: number): Promise<StreamEntry[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT STREAM_XREAD($1, $2, $3)', [
      stream, lastIdMs, count,
    ]);
    if (!raw) return [];
    return JSON.parse(raw) as StreamEntry[];
  }

  async xgroupCreate(stream: string, group: string, startId: number): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT STREAM_XGROUP_CREATE($1, $2, $3)', [
        stream, group, startId,
      ])) ?? false
    );
  }

  async xreadGroup(stream: string, group: string, consumer: string, count: number): Promise<StreamEntry[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT STREAM_XREADGROUP($1, $2, $3, $4)', [
      stream, group, consumer, count,
    ]);
    // Since Nucleus v0.1.8: a missing group answers NOGROUP as a statement
    // error (SQLSTATE 22000) which the transport throws, and a caught-up
    // success carries "[]" — never "". An empty payload is a contract
    // violation, not "caught up": reading it as [] is how a vanished group
    // silently skips every unprocessed entry, forever.
    if (!raw) {
      throw new NucleusQueryError(
        'STREAM_XREADGROUP returned an empty payload; expected "[]" when caught up ' +
          'or a NOGROUP error for a missing group',
      );
    }
    return JSON.parse(raw) as StreamEntry[];
  }

  async xack(stream: string, group: string, entryId: string): Promise<number> {
    this.require();
    return (
      (await this.transport.fetchval<number>('SELECT STREAM_XACK($1, $2, $3)', [
        stream, group, entryId,
      ])) ?? 0
    );
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.streams` to the client. */
export const withStreams: NucleusPlugin<{ streams: StreamsModel }> = {
  name: 'streams',
  init(transport: Transport, features: NucleusFeatures) {
    return { streams: new StreamsModelImpl(transport, features) };
  },
};
