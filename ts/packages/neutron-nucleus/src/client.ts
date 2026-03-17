// ---------------------------------------------------------------------------
// Nucleus client — builder + `.use()` plugin composition
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures, NucleusPlugin } from './types.js';
import { createTransport } from './transport.js';
import { detectFeatures } from './features.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface NucleusClientConfig {
  /** Base URL of the Nucleus (or PostgreSQL HTTP proxy) server. */
  url: string;
  /** Extra HTTP headers sent with every request. */
  headers?: Record<string, string>;
  /** Request timeout in milliseconds (default 30000). */
  timeout?: number;
  /** Override the default transport (e.g. for testing or explicit platform choice). */
  transport?: Transport;

  // -- Mobile-specific options (used by MobileTransport when auto-detected) --

  /** Maximum retry attempts for transient failures (default 3). */
  maxRetries?: number;
  /** Base delay in ms between retries — uses exponential backoff (default 1000). */
  retryDelay?: number;
  /** Time-to-live for cached SELECT results in ms (default 60000). */
  cacheTTL?: number;
  /** Whether to cache SELECT queries (default true on mobile). */
  cacheEnabled?: boolean;
  /** Whether to queue writes when offline (default true on mobile). */
  offlineQueueEnabled?: boolean;
  /** Maximum number of queued offline operations (default 100). */
  maxQueueSize?: number;
}

// ---------------------------------------------------------------------------
// Connected client base shape
// ---------------------------------------------------------------------------

/** The minimal client surface available before any plugins are applied. */
export interface NucleusClientBase {
  readonly transport: Transport;
  readonly features: NucleusFeatures;
  close(): Promise<void>;
  ping(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/**
 * A lazy builder that accumulates plugins via `.use()` then resolves them all
 * at `.connect()` time. The return type of `.connect()` is the intersection of
 * `NucleusClientBase` with every plugin's contribution `T`.
 *
 * ```ts
 * const db = await createClient({ url: '...' })
 *   .use(withSQL)
 *   .use(withKV)
 *   .connect();
 *
 * db.sql.query(...);
 * db.kv.get(...);
 * ```
 */
export interface NucleusClientBuilder<Acc> {
  /** Register a plugin. Returns a new builder whose type includes `T`. */
  use<T>(plugin: NucleusPlugin<T>): NucleusClientBuilder<Acc & T>;

  /** Connect to the server, detect features, and initialise all plugins. */
  connect(): Promise<NucleusClientBase & Acc>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class ClientBuilder<Acc> implements NucleusClientBuilder<Acc> {
  private readonly config: NucleusClientConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly plugins: NucleusPlugin<any>[];

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(config: NucleusClientConfig, plugins: NucleusPlugin<any>[] = []) {
    this.config = config;
    this.plugins = plugins;
  }

  use<T>(plugin: NucleusPlugin<T>): NucleusClientBuilder<Acc & T> {
    return new ClientBuilder<Acc & T>(this.config, [...this.plugins, plugin]);
  }

  async connect(): Promise<NucleusClientBase & Acc> {
    const transport = this.config.transport ?? createTransport({
      url: this.config.url,
      headers: this.config.headers,
      timeout: this.config.timeout,
      maxRetries: this.config.maxRetries,
      retryDelay: this.config.retryDelay,
      cacheTTL: this.config.cacheTTL,
      cacheEnabled: this.config.cacheEnabled,
      offlineQueueEnabled: this.config.offlineQueueEnabled,
      maxQueueSize: this.config.maxQueueSize,
    });
    const features = await detectFeatures(transport);

    // Base client object
    const base: NucleusClientBase = {
      transport,
      features,
      close: () => transport.close(),
      ping: () => transport.ping(),
    };

    // Merge plugin contributions into the base object
    const reserved = new Set(['transport', 'features', 'close', 'ping']);
    const client = base as NucleusClientBase & Acc;
    for (const plugin of this.plugins) {
      const contribution = plugin.init(transport, features);
      for (const key of Object.keys(contribution as object)) {
        if (reserved.has(key)) {
          throw new Error(`Plugin "${plugin.name}" cannot override reserved property "${key}"`);
        }
      }
      Object.assign(client, contribution);
    }

    return client;
  }
}

// ---------------------------------------------------------------------------
// Public factory
// ---------------------------------------------------------------------------

/**
 * Create a new Nucleus client builder.
 *
 * Call `.use(plugin)` to add model support, then `.connect()` to establish the
 * connection and resolve the fully-typed client.
 */
export function createClient(config: NucleusClientConfig): NucleusClientBuilder<Record<string, never>> {
  return new ClientBuilder(config);
}
