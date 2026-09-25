// ---------------------------------------------------------------------------
// @neutron-build/nucleus/columnar — Columnar analytics model plugin (X03)
// ---------------------------------------------------------------------------
// Durability/transaction honesty (verified live, X03 Nucleus leg): the
// COLUMNAR_* store is fsync-durable at commit (kill -9 evidence — matches
// the NU-006 durability table; the older "page cache only" prose in
// MODEL_SEMANTICS is stale) and REFUSES inserts inside an explicit
// transaction ("the columnar store is not covered by transaction rollback")
// rather than silently persisting rolled-back rows. It is append-only:
// COLUMNAR_INSERT is the only mutator.
//
// Numeric typing (X03 live finding): values bound WITHOUT type context are
// stored as text and the numeric aggregates then answer a silent 0 / NULL
// (engine silent-wrong-answer class, recorded upstream). insert() therefore
// binds JS numbers with an explicit ::double precision cast so count/sum/
// avg/min/max are real for numeric columns through this client.

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
    const entries = Object.entries(values);
    if (entries.length === 0) {
      throw new Error('COLUMNAR_INSERT requires at least one column/value pair');
    }
    // Engine signature is variadic: COLUMNAR_INSERT(table, col1, val1, col2, val2, ...)
    // JS numbers bind WITH a ::double precision cast: untyped params are
    // stored as text and the numeric aggregates then silently answer 0/NULL
    // (X03 live finding against the real engine). Text/other values bind
    // uncast — the store is schema-less and the value's JS type is the only
    // type information there is.
    const args: unknown[] = [table];
    const pairs = entries.map(([col, val], i) => {
      args.push(col, val);
      const cast = typeof val === 'number' ? '::double precision' : '';
      return `$${i * 2 + 2}, $${i * 2 + 3}${cast}`;
    });
    const result = await this.transport.fetchval<string>(`SELECT COLUMNAR_INSERT($1, ${pairs.join(', ')})`, args);
    return result === 'OK';
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
