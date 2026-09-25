// ---------------------------------------------------------------------------
// @neutron-build/nucleus/cdc — Change Data Capture model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A single change-data-capture log entry as emitted by the engine. */
export interface CDCEvent {
  /** Monotonic sequence number of the change. */
  seq: number;
  /** Table the change applies to. */
  table: string;
  /** Kind of change. Wire format; on Nucleus 1.0.2 only 'INSERT' is ever
   * emitted — UPDATE/DELETE never reach the log (see interface docs). */
  change: 'INSERT' | 'UPDATE' | 'DELETE';
  /** Timestamp of the change (epoch milliseconds). */
  ts: number;
}

// ---------------------------------------------------------------------------
// CDCModel interface
// ---------------------------------------------------------------------------

/**
 * Semantics (Nucleus 1.0.x, verified live — see conformance/live/orm x05
 * leg):
 *
 * - Events are METADATA ONLY: `seq`, `table`, `change`, `ts`. No row images,
 *   no columns, no old/new values. Re-sync from the source table.
 * - One event per DML STATEMENT that changed at least one row (the engine
 *   does not emit per-row events).
 * - Nucleus 1.0.2 emits INSERT events ONLY: UPDATE and DELETE statements
 *   change rows but never append an event (their engine emit sites exist but
 *   the executed path never reaches them — pinned live by the x05 leg's
 *   `cdc.delivery_shape`, which fails loudly if the engine starts emitting
 *   them). Until the engine changes, `change` is effectively always
 *   'INSERT': row updates and deletes are invisible to CDC consumers and
 *   must be reconciled from the source table.
 * - The resume cursor is `seq`. Events are emitted at STATEMENT time, before
 *   the surrounding transaction commits: a consumer sees INSERT/UPDATE/DELETE
 *   events for transactions that then roll back, with no compensating
 *   record. Treat a change as provisional until you can reconcile it against
 *   the table. Sequence gaps are normal (aborted transactions burn seqs) —
 *   advance your cursor to the highest seq you read, gaps included; do not
 *   treat a gap as an error.
 * - The in-memory log is bounded at 100,000 events. A consumer more than
 *   100k events behind silently skips forward (the engine evicts the oldest
 *   events with no signal). Poll frequently enough to stay inside the
 *   window, or reconcile from the source.
 * - Restart: events replay from the CDC log on disk up to the last engine
 *   checkpoint; the log is NOT fsynced per event (page-cache durability —
 *   a hard crash can lose the tail since the last checkpoint). `seq`
 *   numbering does not reset across a restart.
 * - `limit` is capped at 100,000 server-side.
 * - Row-level security: the `CDC_` prefix is denied under RLS on every
 *   function this client calls. This client deliberately uses ONLY the
 *   guarded spellings (`CDC_READ`, ...) — never `pg_catalog`-qualified
 *   lower-case spellings, which evade the engine's RLS prefix guard (known
 *   upstream defect; a policy-restricted principal must not be handed a
 *   bypass by its own client library).
 */
export interface CDCModel {
  /** Read up to `limit` CDC events with sequence greater than `afterSequence`. */
  read(afterSequence: number, limit?: number): Promise<CDCEvent[]>;

  /** Return the total number of CDC events currently retained. */
  count(): Promise<number>;

  /** Read up to `limit` CDC events for a specific table after `afterSequence`. */
  tableRead(table: string, afterSequence: number, limit?: number): Promise<CDCEvent[]>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class CDCModelImpl implements CDCModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'CDC');
  }

  async read(afterSequence: number, limit = 100): Promise<CDCEvent[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT CDC_READ($1, $2)', [afterSequence, limit]);
    if (!raw) return [];
    return JSON.parse(raw) as CDCEvent[];
  }

  async count(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT CDC_COUNT()')) ?? 0;
  }

  async tableRead(table: string, afterSequence: number, limit = 100): Promise<CDCEvent[]> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT CDC_TABLE_READ($1, $2, $3)', [
      table, afterSequence, limit,
    ]);
    if (!raw) return [];
    return JSON.parse(raw) as CDCEvent[];
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.cdc` to the client. */
export const withCDC: NucleusPlugin<{ cdc: CDCModel }> = {
  name: 'cdc',
  init(transport: Transport, features: NucleusFeatures) {
    return { cdc: new CDCModelImpl(transport, features) };
  },
};
