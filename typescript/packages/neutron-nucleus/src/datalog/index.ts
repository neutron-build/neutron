// ---------------------------------------------------------------------------
// @neutron/nucleus/datalog — Datalog reasoning model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// DatalogModel interface
// ---------------------------------------------------------------------------

export interface DatalogModel {
  /** Add a fact to the knowledge base. */
  assert(fact: string): Promise<boolean>;

  /** Remove a fact from the knowledge base. */
  retract(fact: string): Promise<boolean>;

  /** Define a rule with a head and body. */
  rule(head: string, body: string): Promise<boolean>;

  /** Evaluate a Datalog query pattern. Returns results as CSV text. */
  query(pattern: string): Promise<string>;

  /** Clear all facts and rules. */
  clear(): Promise<boolean>;

  /** Import the graph model into the Datalog knowledge base. Returns count of facts imported. */
  importGraph(): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class DatalogModelImpl implements DatalogModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Datalog');
  }

  async assert(fact: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT DATALOG_ASSERT($1)', [fact])) ?? false;
  }

  async retract(fact: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT DATALOG_RETRACT($1)', [fact])) ?? false;
  }

  async rule(head: string, body: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT DATALOG_RULE($1, $2)', [head, body])) ?? false;
  }

  async query(pattern: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_QUERY($1)', [pattern])) ?? '';
  }

  async clear(): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT DATALOG_CLEAR()')) ?? false;
  }

  async importGraph(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT DATALOG_IMPORT_GRAPH()')) ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.datalog` to the client. */
export const withDatalog: NucleusPlugin<{ datalog: DatalogModel }> = {
  name: 'datalog',
  init(transport: Transport, features: NucleusFeatures) {
    return { datalog: new DatalogModelImpl(transport, features) };
  },
};
