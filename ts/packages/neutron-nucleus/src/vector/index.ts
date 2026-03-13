// ---------------------------------------------------------------------------
// @neutron/nucleus/vector — Vector similarity search plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus, assertIdentifier } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type DistanceMetric = 'cosine' | 'l2' | 'inner';

export interface VectorSearchResult<T = Record<string, unknown>> {
  item: T;
  score: number;
  distance: number;
}

export interface VectorSearchOptions {
  /** Maximum number of results (default 10). */
  limit?: number;
  /** Distance metric (default `'cosine'`). */
  metric?: DistanceMetric;
  /** Metadata key/value filters. */
  filter?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// VectorModel interface
// ---------------------------------------------------------------------------

export interface VectorModel {
  /** Create a collection (table) with a VECTOR column and index. */
  createCollection(name: string, dimension: number, metric?: DistanceMetric): Promise<void>;

  /** Insert a vector with metadata into a collection. */
  insert(collection: string, id: string, vector: number[], metadata?: Record<string, unknown>): Promise<void>;

  /** Delete a vector by ID from a collection. */
  delete(collection: string, id: string): Promise<void>;

  /** Perform a vector similarity search. */
  search(collection: string, query: number[], opts?: VectorSearchOptions): Promise<VectorSearchResult[]>;

  /** Return the dimensionality of a vector. */
  dims(vector: number[]): Promise<number>;

  /** Compute the distance between two vectors. */
  distance(a: number[], b: number[], metric?: DistanceMetric): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class VectorModelImpl implements VectorModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Vector');
  }

  async createCollection(name: string, dimension: number, metric: DistanceMetric = 'cosine'): Promise<void> {
    this.require();
    assertIdentifier(name, 'collection name');

    await this.transport.execute(
      `CREATE TABLE IF NOT EXISTS ${name} (id TEXT PRIMARY KEY, embedding VECTOR(${dimension}), metadata JSONB DEFAULT '{}')`,
    );
    await this.transport.execute(
      `CREATE INDEX IF NOT EXISTS idx_${name}_embedding ON ${name} USING VECTOR (embedding) WITH (metric = '${metric}')`,
    );
  }

  async insert(
    collection: string,
    id: string,
    vector: number[],
    metadata: Record<string, unknown> = {},
  ): Promise<void> {
    this.require();
    assertIdentifier(collection, 'collection name');

    const vecJson = JSON.stringify(vector);
    const metaJson = JSON.stringify(metadata);
    await this.transport.execute(
      `INSERT INTO ${collection} (id, embedding, metadata) VALUES ($1, VECTOR($2), $3)`,
      [id, vecJson, metaJson],
    );
  }

  async delete(collection: string, id: string): Promise<void> {
    this.require();
    assertIdentifier(collection, 'collection name');
    await this.transport.execute(`DELETE FROM ${collection} WHERE id = $1`, [id]);
  }

  async search(
    collection: string,
    query: number[],
    opts: VectorSearchOptions = {},
  ): Promise<VectorSearchResult[]> {
    this.require();
    assertIdentifier(collection, 'collection name');

    const limit = opts.limit ?? 10;
    const metric = opts.metric ?? 'cosine';
    const vecJson = JSON.stringify(query);

    // Build optional WHERE clause from filter
    let filterClause = '';
    const params: unknown[] = [vecJson, metric, limit];
    if (opts.filter) {
      const clauses: string[] = [];
      for (const [k, v] of Object.entries(opts.filter)) {
        const ki = params.length + 1;
        const vi = params.length + 2;
        clauses.push(`metadata->>$${ki} = $${vi}`);
        params.push(k, String(v));
      }
      filterClause = ' WHERE ' + clauses.join(' AND ');
    }

    const sql =
      `SELECT id, metadata, VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS distance ` +
      `FROM ${collection}${filterClause} ORDER BY distance LIMIT $3`;

    const result = await this.transport.query<{ id: string; metadata: string; distance: number }>(sql, params);

    return result.rows.map((row) => {
      const meta: Record<string, unknown> = typeof row.metadata === 'string'
        ? JSON.parse(row.metadata)
        : (row.metadata ?? {});
      meta.id = row.id;
      const dist = Number(row.distance);
      return {
        item: meta,
        score: dist > 0 ? 1 / dist : 0,
        distance: dist,
      };
    });
  }

  async dims(vector: number[]): Promise<number> {
    this.require();
    const vecJson = JSON.stringify(vector);
    return (await this.transport.fetchval<number>('SELECT VECTOR_DIMS(VECTOR($1))', [vecJson])) ?? 0;
  }

  async distance(a: number[], b: number[], metric: DistanceMetric = 'cosine'): Promise<number> {
    this.require();
    const aJson = JSON.stringify(a);
    const bJson = JSON.stringify(b);
    return (
      (await this.transport.fetchval<number>('SELECT VECTOR_DISTANCE(VECTOR($1), VECTOR($2), $3)', [
        aJson,
        bJson,
        metric,
      ])) ?? 0
    );
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.vector` to the client. */
export const withVector: NucleusPlugin<{ vector: VectorModel }> = {
  name: 'vector',
  init(transport: Transport, features: NucleusFeatures) {
    return { vector: new VectorModelImpl(transport, features) };
  },
};
