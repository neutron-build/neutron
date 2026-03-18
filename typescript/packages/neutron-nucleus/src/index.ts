// ---------------------------------------------------------------------------
// @neutron/nucleus — main entry point
// ---------------------------------------------------------------------------

export { createClient } from './client.js';
export type { NucleusClientConfig, NucleusClientBase, NucleusClientBuilder } from './client.js';

export type {
  Transport,
  TransactionTransport,
  QueryResult,
  IsolationLevel,
  NucleusFeatures,
  NucleusPlugin,
} from './types.js';

export {
  NucleusError,
  NucleusConnectionError,
  NucleusQueryError,
  NucleusNotFoundError,
  NucleusConflictError,
  NucleusTransactionError,
  NucleusFeatureError,
  NucleusAuthError,
} from './errors.js';

export { HttpTransport, MobileTransport, EmbeddedTransport, createTransport } from './transport.js';
export type { TransportConfig, MobileTransportConfig } from './transport.js';
export { detectFeatures } from './features.js';
export { requireNucleus, assertIdentifier } from './helpers.js';
export { migrate, migrateDown, migrationStatus } from './migrate.js';
export type { Migration, MigrationRecord } from './migrate.js';

// ---------------------------------------------------------------------------
// Model plugins — re-exported for convenience.
// Tree-shakeable: each plugin can also be imported from its own subpath
// (e.g. `@neutron/nucleus/kv`).
// ---------------------------------------------------------------------------

export { withSQL } from './sql/index.js';
export type { SQLModel } from './sql/index.js';

export { withKV } from './kv/index.js';
export type { KVModel, KVSetOptions } from './kv/index.js';

export { withVector } from './vector/index.js';
export type { VectorModel, VectorSearchResult, VectorSearchOptions, DistanceMetric } from './vector/index.js';

export { withTimeSeries } from './timeseries/index.js';
export type { TimeSeriesModel, TimeSeriesPoint, AggFunc, BucketInterval, TimeSeriesQueryOptions } from './timeseries/index.js';

export { withDocument } from './document/index.js';
export type { DocumentModel, DocFindOptions } from './document/index.js';

export { withGraph } from './graph/index.js';
export type { GraphModel, GraphNode, GraphEdge, GraphResult, Direction } from './graph/index.js';

export { withFTS } from './fts/index.js';
export type { FTSModel, FTSResult, FTSSearchOptions } from './fts/index.js';

export { withGeo } from './geo/index.js';
export type { GeoModel, GeoPoint, GeoFeature } from './geo/index.js';

export { withBlob } from './blob/index.js';
export type { BlobModel, BlobMeta, BlobPutOptions } from './blob/index.js';

export { withPubSub } from './pubsub/index.js';
export type { PubSubModel } from './pubsub/index.js';

export { withStreams } from './streams/index.js';
export type { StreamsModel, StreamEntry } from './streams/index.js';

export { withColumnar } from './columnar/index.js';
export type { ColumnarModel } from './columnar/index.js';

export { withDatalog } from './datalog/index.js';
export type { DatalogModel } from './datalog/index.js';

export { withCDC } from './cdc/index.js';
export type { CDCModel } from './cdc/index.js';
