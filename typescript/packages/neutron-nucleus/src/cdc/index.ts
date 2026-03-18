// ---------------------------------------------------------------------------
// @neutron/nucleus/cdc — Change Data Capture model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// CDCModel interface
// ---------------------------------------------------------------------------

export interface CDCModel {
  /** Read CDC events starting from the given offset. Returns raw JSON. */
  read(offset: number): Promise<string>;

  /** Return the total number of CDC events. */
  count(): Promise<number>;

  /** Read CDC events for a specific table starting from the given offset. */
  tableRead(table: string, offset: number): Promise<string>;
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

  async read(offset: number): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT CDC_READ($1)', [offset])) ?? '';
  }

  async count(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT CDC_COUNT()')) ?? 0;
  }

  async tableRead(table: string, offset: number): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT CDC_TABLE_READ($1, $2)', [table, offset])) ?? '';
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
