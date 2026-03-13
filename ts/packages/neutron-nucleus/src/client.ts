// ---------------------------------------------------------------------------
// Nucleus client — builder + `.use()` plugin composition
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures, NucleusPlugin } from './types.js';
import { HttpTransport } from './transport.js';
import { detectFeatures } from './features.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface NucleusClientConfig {
  /** Base URL of the Nucleus (or PostgreSQL HTTP proxy) server. */
  url: string;
  /** Extra HTTP headers sent with every request. */
  headers?: Record<string, string>;
  /** Override the default HttpTransport (e.g. for testing). */
  transport?: Transport;
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
    const transport = this.config.transport ?? new HttpTransport(this.config.url, this.config.headers);
    const features = await detectFeatures(transport);

    // Base client object
    const base: NucleusClientBase = {
      transport,
      features,
      close: () => transport.close(),
      ping: () => transport.ping(),
    };

    // Merge plugin contributions into the base object
    const client = base as NucleusClientBase & Acc;
    for (const plugin of this.plugins) {
      const contribution = plugin.init(transport, features);
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
