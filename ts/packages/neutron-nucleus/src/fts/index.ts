// ---------------------------------------------------------------------------
// @neutron/nucleus/fts — Full-Text Search model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FTSResult {
  docId: number;
  score: number;
  highlight?: Record<string, string>;
}

export interface FTSSearchOptions {
  /** Enable fuzzy matching with the given edit distance. */
  fuzzyDistance?: number;
  /** Maximum number of results (default 10). */
  limit?: number;
  /** Fields to highlight in results. */
  highlight?: string[];
  /** Fields to compute facet counts for. */
  facets?: string[];
}

// ---------------------------------------------------------------------------
// FTSModel interface
// ---------------------------------------------------------------------------

export interface FTSModel {
  /** Add a document's text to the full-text index. */
  index(docId: number, text: string): Promise<boolean>;

  /** Search the full-text index. */
  search(query: string, opts?: FTSSearchOptions): Promise<FTSResult[]>;

  /** Remove a document from the index. */
  remove(docId: number): Promise<boolean>;

  /** Return the number of indexed documents. */
  docCount(): Promise<number>;

  /** Return the number of indexed terms. */
  termCount(): Promise<number>;

  /** Create a named FTS index with the given configuration. */
  createIndex(name: string, config: Record<string, unknown>): Promise<void>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class FTSModelImpl implements FTSModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'FTS');
  }

  async index(docId: number, text: string): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT FTS_INDEX($1, $2)', [docId, text])) ?? false;
  }

  async search(query: string, opts: FTSSearchOptions = {}): Promise<FTSResult[]> {
    this.require();
    const limit = opts.limit ?? 10;

    let raw: string | null;
    if (opts.fuzzyDistance && opts.fuzzyDistance > 0) {
      raw = await this.transport.fetchval<string>('SELECT FTS_FUZZY_SEARCH($1, $2, $3)', [
        query,
        opts.fuzzyDistance,
        limit,
      ]);
    } else {
      raw = await this.transport.fetchval<string>('SELECT FTS_SEARCH($1, $2)', [query, limit]);
    }

    if (!raw) return [];
    const results = JSON.parse(raw) as FTSResult[];

    // Ensure highlight map is present when requested
    if (opts.highlight && opts.highlight.length > 0) {
      for (const r of results) {
        if (!r.highlight) r.highlight = {};
      }
    }

    return results;
  }

  async remove(docId: number): Promise<boolean> {
    this.require();
    return (await this.transport.fetchval<boolean>('SELECT FTS_REMOVE($1)', [docId])) ?? false;
  }

  async docCount(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT FTS_DOC_COUNT()')) ?? 0;
  }

  async termCount(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT FTS_TERM_COUNT()')) ?? 0;
  }

  async createIndex(name: string, config: Record<string, unknown>): Promise<void> {
    this.require();
    const configJson = JSON.stringify(config);
    await this.transport.execute('SELECT FTS_INDEX($1, $2)', [name, configJson]);
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.fts` to the client. */
export const withFTS: NucleusPlugin<{ fts: FTSModel }> = {
  name: 'fts',
  init(transport: Transport, features: NucleusFeatures) {
    return { fts: new FTSModelImpl(transport, features) };
  },
};
