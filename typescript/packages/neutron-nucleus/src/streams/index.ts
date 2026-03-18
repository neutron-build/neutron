// ---------------------------------------------------------------------------
// @neutron/nucleus/streams — Streams model plugin (Redis Streams-compatible)
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

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

  /** Read entries from a consumer group. */
  xreadGroup(stream: string, group: string, consumer: string, count: number): Promise<StreamEntry[]>;

  /** Acknowledge processing of an entry in a consumer group. */
  xack(stream: string, group: string, idMs: number, idSeq: number): Promise<boolean>;
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
    if (!raw) return [];
    return JSON.parse(raw) as StreamEntry[];
  }

  async xack(stream: string, group: string, idMs: number, idSeq: number): Promise<boolean> {
    this.require();
    return (
      (await this.transport.fetchval<boolean>('SELECT STREAM_XACK($1, $2, $3, $4)', [
        stream, group, idMs, idSeq,
      ])) ?? false
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
