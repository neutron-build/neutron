// ---------------------------------------------------------------------------
// Nucleus client — feature detection
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures } from './types.js';

/**
 * Detect capabilities of the connected database.
 *
 * Detection strategy:
 * 1. `SELECT VERSION()` — if it contains "Nucleus", we know this is Nucleus.
 * 2. If Nucleus is detected, query `nucleus_features()` for granular per-model
 *    availability. This allows partial installations where some engines are
 *    disabled or not yet loaded.
 * 3. On plain PostgreSQL the result is a standard PG version string and only
 *    SQL features are enabled.
 */
export async function detectFeatures(transport: Transport): Promise<NucleusFeatures> {
  const version = await transport.fetchval<string>('SELECT VERSION()');
  const ver = version ?? '';

  const isNucleus = ver.includes('Nucleus');

  if (!isNucleus) {
    return {
      isNucleus: false,
      hasKV: false,
      hasVector: false,
      hasTimeSeries: false,
      hasDocument: false,
      hasGraph: false,
      hasFTS: false,
      hasGeo: false,
      hasBlob: false,
      hasStreams: false,
      hasColumnar: false,
      hasDatalog: false,
      hasCDC: false,
      hasPubSub: false,
      version: ver,
    };
  }

  // Attempt granular feature detection via nucleus_features() function.
  // If the function does not exist (older Nucleus builds), fall back to all=true.
  try {
    const raw = await transport.fetchval<string>('SELECT NUCLEUS_FEATURES()');
    if (raw) {
      const features = JSON.parse(raw) as Record<string, boolean>;
      return {
        isNucleus: true,
        hasKV: features.kv !== false,
        hasVector: features.vector !== false,
        hasTimeSeries: features.timeseries !== false,
        hasDocument: features.document !== false,
        hasGraph: features.graph !== false,
        hasFTS: features.fts !== false,
        hasGeo: features.geo !== false,
        hasBlob: features.blob !== false,
        hasStreams: features.streams !== false,
        hasColumnar: features.columnar !== false,
        hasDatalog: features.datalog !== false,
        hasCDC: features.cdc !== false,
        hasPubSub: features.pubsub !== false,
        version: ver,
      };
    }
  } catch {
    // NUCLEUS_FEATURES() not available — fall through to all-enabled default.
  }

  // All models enabled by default on Nucleus.
  return {
    isNucleus: true,
    hasKV: true,
    hasVector: true,
    hasTimeSeries: true,
    hasDocument: true,
    hasGraph: true,
    hasFTS: true,
    hasGeo: true,
    hasBlob: true,
    hasStreams: true,
    hasColumnar: true,
    hasDatalog: true,
    hasCDC: true,
    hasPubSub: true,
    version: ver,
  };
}
