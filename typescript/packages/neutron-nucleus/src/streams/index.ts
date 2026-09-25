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

/**
 * A read cursor: either a bare millisecond or a full `"<ms>-<seq>"` entry id
 * (the string `xadd` returns). The two forms are NOT interchangeable, and the
 * bare form is a silent-loss hazard in `xread`. The engine reads a bare
 * millisecond as "strictly after that WHOLE millisecond" (internally
 * `<ms>-<max seq>`; engine `stream_cursor_arg`, found by the engine's own
 * `probe_streams_oracle`): a consumer that already read up to `<ms>-0` and
 * resumes with the bare `<ms>` is never served `<ms>-1` or any later sibling
 * appended inside that same millisecond — those entries are skipped,
 * silently, forever. Resuming with the LAST ENTRY'S FULL ID is the only
 * cursor that reaches every later entry; prefer it wherever you have one.
 */
export type StreamCursor = number | string;

// ---------------------------------------------------------------------------
// StreamsModel interface
// ---------------------------------------------------------------------------

/**
 * Ordering: entry ids are strictly increasing `<ms>-<seq>`; reads return
 * entries in id order.
 *
 * Delivery semantics (Nucleus 1.0.x, verified live — see
 * conformance/live/orm x05 leg): `xreadGroup` advances the group cursor and
 * records the delivery in the same call — delivery is AT-MOST-ONCE per read.
 * There is no XCLAIM/XAUTOCLAIM and no idle reclaim: a pending (delivered,
 * unacked) entry is never redelivered, by any consumer, for the life of the
 * group. Ack what you finished; anything you read but did not ack is
 * stranded in the pending list. Exactly-once is not attempted by the engine
 * and must not be assumed by consumers.
 *
 * Backpressure: pull-based. No server push exists over the SQL surface; each
 * call bounds its own work with `count`. A consumer that stops polling
 * accumulates entries in the stream (subject to the stream's max length
 * trimming) — there is no per-consumer queue to overflow.
 *
 * Durability: entries are fsynced to the streams WAL before the append is
 * acknowledged; consumer groups, cursors, pending lists and acks are
 * persisted in the same log. A restart replays entries and group state. A
 * delivery that was read but not acked before a restart stays pending (it is
 * NOT redelivered — see above). An append inside a transaction that rolls
 * back is removed from the stream and compensated out of the WAL.
 */
export interface StreamsModel {
  /** Append an entry to a stream. Returns the generated entry id (`"<ms>-<seq>"`). */
  xadd(stream: string, fields: Record<string, unknown>): Promise<string>;

  /** Return the number of entries in a stream (0 if the stream does not exist). */
  xlen(stream: string): Promise<number>;

  /**
   * Return up to `count` entries whose ids fall in `[start, end]` (inclusive).
   * Bounds accept a bare millisecond or a full `"<ms>-<seq>"` id; a bare
   * millisecond covers its whole millisecond (start: from its first entry,
   * end: through its last). A stream that does not exist answers an empty
   * array, same as an empty range.
   */
  xrange(stream: string, start: StreamCursor, end: StreamCursor, count: number): Promise<StreamEntry[]>;

  /**
   * Read up to `count` entries with id strictly greater than `lastId`.
   * `lastId` accepts a bare millisecond or a full `"<ms>-<seq>"` id — use the
   * full id of the last entry you processed: a bare millisecond means
   * "strictly after that whole millisecond" and silently SKIPS any
   * same-millisecond entries appended after your last read (the loss hazard
   * described on {@link StreamCursor}). A stream that does not exist answers
   * an empty array, same as "no new entries".
   */
  xread(stream: string, lastId: StreamCursor, count: number): Promise<StreamEntry[]>;

  /**
   * Create a consumer group starting at `startIdMs` (bare millisecond).
   * Creating a group that already exists fails with the engine's BUSYGROUP
   * error — a plain create never resets a live group's cursor or pending
   * list (the engine's optional destructive `recreate` flag is deliberately
   * not exposed here; use the SQL model if you really need it).
   */
  xgroupCreate(stream: string, group: string, startIdMs: number): Promise<boolean>;

  /**
   * Read entries from a consumer group.
   *
   * A missing group (or stream) answers NOGROUP as a statement error
   * (SQLSTATE 22000) since Nucleus v0.1.8; an empty delivery is `"[]"`.
   */
  xreadGroup(stream: string, group: string, consumer: string, count: number): Promise<StreamEntry[]>;

  /**
   * Acknowledge processing of an entry in a consumer group, by the id `xadd`
   * returned. Returns the number of entries acknowledged (0 when the id is
   * not pending in this group — e.g. already acked, delivered to another
   * consumer, or never delivered).
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

  async xrange(stream: string, start: StreamCursor, end: StreamCursor, count: number): Promise<StreamEntry[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT STREAM_XRANGE($1, $2, $3, $4)', [
      stream, start, end, count,
    ]);
    if (!raw) return [];
    return JSON.parse(raw) as StreamEntry[];
  }

  async xread(stream: string, lastId: StreamCursor, count: number): Promise<StreamEntry[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT STREAM_XREAD($1, $2, $3)', [
      stream, lastId, count,
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
