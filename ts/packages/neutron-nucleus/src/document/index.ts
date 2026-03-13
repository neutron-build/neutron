// ---------------------------------------------------------------------------
// @neutron/nucleus/document — Document / JSON model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DocFindOptions {
  /** Sort by this field. */
  sortField?: string;
  /** Sort ascending (`true`) or descending (`false`). */
  sortAsc?: boolean;
  /** Skip the first `n` results. */
  skip?: number;
  /** Maximum number of results. */
  limit?: number;
  /** Only return these fields from each document. */
  fields?: string[];
}

// ---------------------------------------------------------------------------
// DocumentModel interface
// ---------------------------------------------------------------------------

export interface DocumentModel {
  /** Insert a document. Returns the generated document ID. */
  insert(collection: string, doc: Record<string, unknown>): Promise<number>;

  /** Get a document by ID. Returns `null` if not found. */
  get(id: number): Promise<Record<string, unknown> | null>;

  /** Get a document and cast it to `T`. Returns `null` if not found. */
  getTyped<T>(id: number): Promise<T | null>;

  /** Query documents matching a JSON filter. Returns matching IDs. */
  queryDocs(filter: Record<string, unknown>): Promise<number[]>;

  /** Extract a nested value from a document by key path. */
  path(id: number, ...keys: string[]): Promise<string | null>;

  /** Return the total number of documents. */
  count(): Promise<number>;

  /** Find full documents matching a filter. */
  find(collection: string, filter: Record<string, unknown>, opts?: DocFindOptions): Promise<Record<string, unknown>[]>;

  /** Find and return typed results. */
  findTyped<T>(collection: string, filter: Record<string, unknown>, opts?: DocFindOptions): Promise<T[]>;

  /** Find the first document matching a filter. */
  findOne(collection: string, filter: Record<string, unknown>): Promise<Record<string, unknown> | null>;

  /** Update documents matching a filter with the given partial. Returns count of updated docs. */
  update(collection: string, filter: Record<string, unknown>, update: Record<string, unknown>): Promise<number>;

  /** Delete documents matching a filter. Returns count of deleted docs. */
  delete(collection: string, filter: Record<string, unknown>): Promise<number>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class DocumentModelImpl implements DocumentModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Document');
  }

  async insert(_collection: string, doc: Record<string, unknown>): Promise<number> {
    this.require();
    const data = JSON.stringify(doc);
    return (await this.transport.fetchval<number>('SELECT DOC_INSERT($1)', [data])) ?? 0;
  }

  async get(id: number): Promise<Record<string, unknown> | null> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT DOC_GET($1)', [id]);
    if (raw === null) return null;
    return JSON.parse(raw) as Record<string, unknown>;
  }

  async getTyped<T>(id: number): Promise<T | null> {
    this.require();
    const raw = await this.transport.fetchval<string>('SELECT DOC_GET($1)', [id]);
    if (raw === null) return null;
    return JSON.parse(raw) as T;
  }

  async queryDocs(filter: Record<string, unknown>): Promise<number[]> {
    this.require();
    const q = JSON.stringify(filter);
    const raw = await this.transport.fetchval<string>('SELECT DOC_QUERY($1)', [q]);
    if (!raw) return [];
    return raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map(Number)
      .filter((n) => !Number.isNaN(n));
  }

  async path(id: number, ...keys: string[]): Promise<string | null> {
    this.require();
    const placeholders = keys.map((_, i) => `$${i + 2}`).join(', ');
    const sql = `SELECT DOC_PATH($1, ${placeholders})`;
    return this.transport.fetchval<string>(sql, [id, ...keys]);
  }

  async count(): Promise<number> {
    this.require();
    return (await this.transport.fetchval<number>('SELECT DOC_COUNT()')) ?? 0;
  }

  async find(
    collection: string,
    filter: Record<string, unknown>,
    opts: DocFindOptions = {},
  ): Promise<Record<string, unknown>[]> {
    const ids = await this.queryDocs(filter);
    let results: Record<string, unknown>[] = [];

    for (const id of ids) {
      const doc = await this.get(id);
      if (doc) results.push(doc);
    }

    // Sort
    if (opts.sortField) {
      const field = opts.sortField;
      const asc = opts.sortAsc ?? true;
      results.sort((a, b) => {
        const va = String(a[field] ?? '');
        const vb = String(b[field] ?? '');
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
      });
    }

    // Skip
    if (opts.skip && opts.skip > 0) {
      results = results.slice(opts.skip);
    }

    // Limit
    if (opts.limit && opts.limit > 0) {
      results = results.slice(0, opts.limit);
    }

    // Projection
    if (opts.fields && opts.fields.length > 0) {
      const keep = new Set(opts.fields);
      results = results.map((doc) => {
        const projected: Record<string, unknown> = {};
        for (const f of keep) {
          if (f in doc) projected[f] = doc[f];
        }
        return projected;
      });
    }

    return results;
  }

  async findTyped<T>(
    collection: string,
    filter: Record<string, unknown>,
    opts: DocFindOptions = {},
  ): Promise<T[]> {
    const ids = await this.queryDocs(filter);
    let results: T[] = [];

    for (const id of ids) {
      const item = await this.getTyped<T>(id);
      if (item !== null) results.push(item);
    }

    if (opts.skip && opts.skip > 0) results = results.slice(opts.skip);
    if (opts.limit && opts.limit > 0) results = results.slice(0, opts.limit);

    return results;
  }

  async findOne(
    collection: string,
    filter: Record<string, unknown>,
  ): Promise<Record<string, unknown> | null> {
    const docs = await this.find(collection, filter, { limit: 1 });
    return docs.length > 0 ? docs[0] : null;
  }

  async update(
    collection: string,
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
  ): Promise<number> {
    this.require();
    const ids = await this.queryDocs(filter);
    let count = 0;

    for (const id of ids) {
      const doc = await this.get(id);
      if (!doc) continue;
      Object.assign(doc, update);
      const data = JSON.stringify(doc);
      await this.transport.execute('UPDATE documents SET data = $1::jsonb WHERE id = $2', [data, id]);
      count++;
    }

    return count;
  }

  async delete(collection: string, filter: Record<string, unknown>): Promise<number> {
    this.require();
    const ids = await this.queryDocs(filter);
    let count = 0;

    for (const id of ids) {
      await this.transport.execute('DELETE FROM documents WHERE id = $1', [id]);
      count++;
    }

    return count;
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.document` to the client. */
export const withDocument: NucleusPlugin<{ document: DocumentModel }> = {
  name: 'document',
  init(transport: Transport, features: NucleusFeatures) {
    return { document: new DocumentModelImpl(transport, features) };
  },
};
