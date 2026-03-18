// ---------------------------------------------------------------------------
// Nucleus client — shared internal helpers
// ---------------------------------------------------------------------------

import type { NucleusFeatures } from './types.js';
import { NucleusFeatureError } from './errors.js';

/** Map from feature name to the corresponding NucleusFeatures key. */
const FEATURE_KEY_MAP: Record<string, keyof NucleusFeatures> = {
  KV: 'hasKV',
  Vector: 'hasVector',
  TimeSeries: 'hasTimeSeries',
  Document: 'hasDocument',
  Graph: 'hasGraph',
  FTS: 'hasFTS',
  Geo: 'hasGeo',
  Blob: 'hasBlob',
  Streams: 'hasStreams',
  Columnar: 'hasColumnar',
  Datalog: 'hasDatalog',
  CDC: 'hasCDC',
  PubSub: 'hasPubSub',
};

/**
 * Throw if the server is not Nucleus or the specific feature is unavailable.
 *
 * When connected to plain PostgreSQL, always throws.
 * When connected to Nucleus, checks the granular feature flag so that
 * partially configured instances report the correct error.
 */
export function requireNucleus(features: NucleusFeatures, featureName: string): void {
  if (!features.isNucleus) {
    throw new NucleusFeatureError(featureName);
  }
  const key = FEATURE_KEY_MAP[featureName];
  if (key && !features[key]) {
    throw new NucleusFeatureError(featureName);
  }
}

/**
 * Validate that `name` is a safe SQL identifier (table/column name).
 * Only allows `[a-zA-Z_][a-zA-Z0-9_]*`.
 */
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function assertIdentifier(name: string, label: string): void {
  if (!IDENTIFIER_RE.test(name)) {
    throw new Error(`Invalid ${label}: ${JSON.stringify(name)}`);
  }
}
