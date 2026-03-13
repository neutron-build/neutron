// ---------------------------------------------------------------------------
// Nucleus client — shared internal helpers
// ---------------------------------------------------------------------------

import type { NucleusFeatures } from './types.js';
import { NucleusFeatureError } from './errors.js';

/** Throw if the server is not Nucleus. */
export function requireNucleus(features: NucleusFeatures, featureName: string): void {
  if (!features.isNucleus) {
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
