// ---------------------------------------------------------------------------
// @neutron-build/nucleus/document — Document / JSON model plugin
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
  /** Get a document by id from a specific collection (absent if it belongs to another). */
  getIn(collection: string, id: number): Promise<Record<string, unknown> | null>;
  getTypedIn<T>(collection: string, id: number): Promise<T | null>;
  /** Query one collection; matches in others are not returned. */
  queryDocsIn(collection: string, filter: Record<string, unknown>): Promise<number[]>;
  pathIn(collection: string, id: number, ...keys: string[]): Promise<unknown>;
  countIn(collection: string): Promise<number>;

  /** Get a document by ID. Returns `null` if not found. */
  get(id: number): Promise<Record<string, unknown> | null>;

  /** Get a document and cast it to `T`. Returns `null` if not found. */
  getTyped<T>(id: number): Promise<T | null>;

  /** Query documents matching a JSON filter. Returns matching IDs. */
  queryDocs(filter: Record<string, unknown>): Promise<number[]>;

  /** Extract a nested value from a document by key path. */
  path(id: number, ...keys: string[]): Promise<unknown>;

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

  /**
   * Render a document id the way the engine expects it over pgwire.
   *
   * Nucleus reports a parameter whose type it cannot infer as TEXT, and a
   * driver then refuses to bind a number to it. The engine parses a
   * text-encoded integer id for exactly this reason, so sending the digits is
   * the supported encoding, not a workaround.
   */
  private static id(id: number): string {
    return String(id);
  }

  async insert(collection: string, doc: Record<string, unknown>): Promise<number> {
    this.require();
    const data = JSON.stringify(doc);
    // The one-argument form when no collection is named, so this still works
    // against a server that predates collections.
    const [sql, args] = collection
      ? ['SELECT DOC_INSERT($1, $2)', [collection, data]]
      : ['SELECT DOC_INSERT($1)', [data]];
    return (await this.transport.fetchval<number>(sql, args)) ?? 0;
  }

  /** Fetch a document's JSON text, scoped to a collection. */
  private async rawDoc(collection: string, id: number): Promise<string | null> {
    const [sql, args] = collection
      ? ['SELECT DOC_GET($1, $2)', [collection, DocumentModelImpl.id(id)]]
      : ['SELECT DOC_GET($1)', [DocumentModelImpl.id(id)]];
    return this.transport.fetchval<string>(sql, args);
  }

  async get(id: number): Promise<Record<string, unknown> | null> {
    return this.getIn('', id);
  }

  /**
   * Get a document by id from a specific collection. A document in another
   * collection reads as absent — holding an id is not enough to read across a
   * collection boundary.
   */
  async getIn(collection: string, id: number): Promise<Record<string, unknown> | null> {
    this.require();
    const raw = await this.rawDoc(collection, id);
    if (raw === null) return null;
    return JSON.parse(raw) as Record<string, unknown>;
  }

  async getTyped<T>(id: number): Promise<T | null> {
    return this.getTypedIn<T>('', id);
  }

  async getTypedIn<T>(collection: string, id: number): Promise<T | null> {
    this.require();
    const raw = await this.rawDoc(collection, id);
    if (raw === null) return null;
    return JSON.parse(raw) as T;
  }

  async queryDocs(filter: Record<string, unknown>): Promise<number[]> {
    return this.queryDocsIn('', filter);
  }

  /** Query one collection. Matches in other collections are not returned. */
  async queryDocsIn(collection: string, filter: Record<string, unknown>): Promise<number[]> {
    this.require();
    const q = JSON.stringify(filter);
    const [sql, args] = collection
      ? ['SELECT DOC_QUERY($1, $2)', [collection, q]]
      : ['SELECT DOC_QUERY($1)', [q]];
    const raw = await this.transport.fetchval<string>(sql, args);
    if (!raw) return [];
    return raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map(Number)
      .filter((n) => !Number.isNaN(n));
  }

  async path(id: number, ...keys: string[]): Promise<unknown> {
    return this.pathIn('', id, ...keys);
  }

  /**
   * Extract a nested value from a document in a specific collection.
   *
   * The scoped form is a distinct FUNCTION rather than an extra argument: the
   * key tail is variadic, so a leading collection could not be told apart from
   * a leading id.
   */
  async pathIn(collection: string, id: number, ...keys: string[]): Promise<unknown> {
    this.require();
    if (keys.length === 0) {
      // Sending this built `DOC_PATH($1, )` — a malformed statement whose
      // error named nothing useful.
      throw new Error('document path requires at least one key');
    }
    const base = collection ? 3 : 2;
    const placeholders = keys.map((_, i) => `$${i + base}`).join(', ');
    const head = collection ? '$1, $2' : '$1';
    const fn = collection ? 'DOC_PATH_IN' : 'DOC_PATH';
    const sql = `SELECT ${fn}(${head}, ${placeholders})`;
    const args = collection
      ? [collection, DocumentModelImpl.id(id), ...keys]
      : [DocumentModelImpl.id(id), ...keys];
    const raw = await this.transport.fetchval<string>(sql, args);
    // DOC_PATH returns raw JSON, so a stored string arrived as `'"ada"'` while
    // `get`/`getIn` on the same client return a decoded object. Two shapes for
    // one idea is drift; the live conformance spec asserts the decoded form for
    // every SDK. A value that is not valid JSON passes through rather than
    // raising — the engine is the only producer, but turning a readable value
    // into an exception is worse than handing it back.
    if (typeof raw !== 'string') return raw;
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }

  async count(): Promise<number> {
    return this.countIn('');
  }

  /** Number of documents in a specific collection. */
  async countIn(collection: string): Promise<number> {
    this.require();
    const [sql, args] = collection
      ? ['SELECT DOC_COUNT($1)', [collection]]
      : ['SELECT DOC_COUNT()', []];
    return (await this.transport.fetchval<number>(sql, args)) ?? 0;
  }

  /**
   * Compare two field values for sorting. Numbers compare numerically —
   * `String()` comparison would sort 10 before 9. Everything else compares
   * as strings, as before.
   */
  private static compareValues(va: unknown, vb: unknown): number {
    if (typeof va === 'number' && typeof vb === 'number') {
      return va - vb;
    }
    return String(va ?? '').localeCompare(String(vb ?? ''));
  }

  /** Shared post-processing for find/findTyped: sort, skip, limit, project. */
  private static applyFindOptions<T extends Record<string, unknown>>(results: T[], opts: DocFindOptions): T[] {
    if (opts.sortField) {
      const field = opts.sortField;
      const asc = opts.sortAsc ?? true;
      results.sort((a, b) => {
        const cmp = DocumentModelImpl.compareValues(a[field], b[field]);
        return asc ? cmp : -cmp;
      });
    }

    let out = results;
    if (opts.skip && opts.skip > 0) {
      out = out.slice(opts.skip);
    }
    if (opts.limit && opts.limit > 0) {
      out = out.slice(0, opts.limit);
    }
    if (opts.fields && opts.fields.length > 0) {
      const keep = new Set(opts.fields);
      out = out.map((doc) => {
        const projected: Record<string, unknown> = {};
        for (const f of keep) {
          if (f in doc) projected[f] = doc[f];
        }
        return projected as T;
      });
    }
    return out;
  }

  async find(
    collection: string,
    filter: Record<string, unknown>,
    opts: DocFindOptions = {},
  ): Promise<Record<string, unknown>[]> {
    const ids = await this.queryDocsIn(collection, filter);
    const results: Record<string, unknown>[] = [];

    for (const id of ids) {
      const doc = await this.getIn(collection, id);
      if (doc) results.push(doc);
    }

    return DocumentModelImpl.applyFindOptions(results, opts);
  }

  async findTyped<T>(
    collection: string,
    filter: Record<string, unknown>,
    opts: DocFindOptions = {},
  ): Promise<T[]> {
    const ids = await this.queryDocsIn(collection, filter);
    const results: Record<string, unknown>[] = [];

    for (const id of ids) {
      const item = await this.getTypedIn<T>(collection, id);
      if (item !== null) results.push(item as Record<string, unknown>);
    }

    return DocumentModelImpl.applyFindOptions(results, opts) as T[];
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
    const ids = await this.queryDocsIn(collection, filter);
    let count = 0;

    for (const id of ids) {
      const doc = await this.getIn(collection, id);
      if (!doc) continue;
      Object.assign(doc, update);
      const data = JSON.stringify(doc);
      // DOC_UPDATE replaces the document in place (preserving id). The old
      // `UPDATE documents ...` targeted a relation that does not exist — the
      // document store is a specialty store reached only via DOC_* functions.
      const [sql, args] = collection
        ? ['SELECT DOC_UPDATE($1, $2, $3)', [collection, DocumentModelImpl.id(id), data]]
        : ['SELECT DOC_UPDATE($1, $2)', [DocumentModelImpl.id(id), data]];
      const ok = await this.transport.fetchval<boolean>(sql, args);
      if (ok) count++;
    }

    return count;
  }

  async delete(collection: string, filter: Record<string, unknown>): Promise<number> {
    this.require();
    const ids = await this.queryDocsIn(collection, filter);
    let count = 0;

    for (const id of ids) {
      // DOC_DELETE, not `DELETE FROM documents` (no such relation).
      const [sql, args] = collection
        ? ['SELECT DOC_DELETE($1, $2)', [collection, DocumentModelImpl.id(id)]]
        : ['SELECT DOC_DELETE($1)', [DocumentModelImpl.id(id)]];
      const ok = await this.transport.fetchval<boolean>(sql, args);
      if (ok) count++;
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
