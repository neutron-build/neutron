// ---------------------------------------------------------------------------
// @neutron-build/nucleus/fts — Full-Text Search model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FTSResult {
  docId: number;
  score: number;
}

export interface FTSSearchOptions {
  /** Enable fuzzy matching with the given edit distance. */
  fuzzyDistance?: number;
  /** Maximum number of results (default 10). */
  limit?: number;
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

  /**
   * No-op kept for API compatibility.
   *
   * The engine maintains a single global FTS index — there is no named-index
   * creation. Documents are indexed directly via `index(docId, text)`.
   */
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
    // Engine emits [{"doc_id":N,"score":S}] — map to the camelCase public type.
    const results = JSON.parse(raw) as Array<{ doc_id: number; score: number }>;
    return results.map((r) => ({ docId: r.doc_id, score: r.score }));
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

  async createIndex(_name: string, _config: Record<string, unknown>): Promise<void> {
    this.require();
    // No-op: the engine has a single global FTS index (FTS_INDEX indexes a
    // document, it does not create a named index).
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
