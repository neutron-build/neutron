// ---------------------------------------------------------------------------
// @neutron/nucleus/columnar — Columnar analytics model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus, assertIdentifier } from '../helpers.js';

// ---------------------------------------------------------------------------
// ColumnarModel interface
// ---------------------------------------------------------------------------

export interface ColumnarModel {
  /** Insert a row into a columnar table. Returns `true` on success. */
  insert(table: string, values: Record<string, unknown>): Promise<boolean>;

  /** Return the number of rows in a columnar table. */
  count(table: string): Promise<number>;

  /** Return the sum of a numeric column. */
  sum(table: string, column: string): Promise<number>;

  /** Return the average of a numeric column. */
  avg(table: string, column: string): Promise<number>;

  /** Return the minimum value of a column. */
  min(table: string, column: string): Promise<unknown>;

  /** Return the maximum value of a column. */
  max(table: string, column: string): Promise<unknown>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class ColumnarModelImpl implements ColumnarModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Columnar');
  }

  async insert(table: string, values: Record<string, unknown>): Promise<boolean> {
    this.require();
    assertIdentifier(table, 'table name');
    const valuesJson = JSON.stringify(values);
    return (await this.transport.fetchval<boolean>('SELECT COLUMNAR_INSERT($1, $2)', [table, valuesJson])) ?? false;
  }

  async count(table: string): Promise<number> {
    this.require();
    assertIdentifier(table, 'table name');
    return (await this.transport.fetchval<number>('SELECT COLUMNAR_COUNT($1)', [table])) ?? 0;
  }

  async sum(table: string, column: string): Promise<number> {
    this.require();
    assertIdentifier(table, 'table name');
    return (await this.transport.fetchval<number>('SELECT COLUMNAR_SUM($1, $2)', [table, column])) ?? 0;
  }

  async avg(table: string, column: string): Promise<number> {
    this.require();
    assertIdentifier(table, 'table name');
    return (await this.transport.fetchval<number>('SELECT COLUMNAR_AVG($1, $2)', [table, column])) ?? 0;
  }

  async min(table: string, column: string): Promise<unknown> {
    this.require();
    assertIdentifier(table, 'table name');
    return this.transport.fetchval('SELECT COLUMNAR_MIN($1, $2)', [table, column]);
  }

  async max(table: string, column: string): Promise<unknown> {
    this.require();
    assertIdentifier(table, 'table name');
    return this.transport.fetchval('SELECT COLUMNAR_MAX($1, $2)', [table, column]);
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.columnar` to the client. */
export const withColumnar: NucleusPlugin<{ columnar: ColumnarModel }> = {
  name: 'columnar',
  init(transport: Transport, features: NucleusFeatures) {
    return { columnar: new ColumnarModelImpl(transport, features) };
  },
};
