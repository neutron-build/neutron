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
  /** Kind of change. */
  change: 'INSERT' | 'UPDATE' | 'DELETE';
  /** Timestamp of the change (epoch milliseconds). */
  ts: number;
}

// ---------------------------------------------------------------------------
// CDCModel interface
// ---------------------------------------------------------------------------

export interface CDCModel {
  /** Read up to `limit` CDC events with sequence greater than `afterSequence`. */
  read(afterSequence: number, limit?: number): Promise<CDCEvent[]>;

  /** Return the total number of CDC events. */
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
