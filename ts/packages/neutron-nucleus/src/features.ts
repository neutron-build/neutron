// ---------------------------------------------------------------------------
// Nucleus client — feature detection
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures } from './types.js';

/**
 * Detect capabilities of the connected database.
 *
 * On a real Nucleus instance `SELECT VERSION()` will contain "Nucleus" and all
 * multi-model features are available. On plain PostgreSQL the result is a
 * standard PG version string and only SQL features are enabled.
 */
export async function detectFeatures(transport: Transport): Promise<NucleusFeatures> {
  const version = await transport.fetchval<string>('SELECT VERSION()');
  const ver = version ?? '';

  const isNucleus = ver.includes('Nucleus');

  return {
    isNucleus,
    hasKV: isNucleus,
    hasVector: isNucleus,
    hasTimeSeries: isNucleus,
    hasDocument: isNucleus,
    hasGraph: isNucleus,
    hasFTS: isNucleus,
    hasGeo: isNucleus,
    hasBlob: isNucleus,
    hasStreams: isNucleus,
    hasColumnar: isNucleus,
    hasDatalog: isNucleus,
    hasCDC: isNucleus,
    hasPubSub: isNucleus,
    version: ver,
  };
}
