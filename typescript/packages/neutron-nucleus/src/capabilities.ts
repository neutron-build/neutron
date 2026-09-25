// ---------------------------------------------------------------------------
// @neutron-build/nucleus/capabilities — probe-resolved capability gate for the
// document and graph relationship surfaces (X02)
//
// Same model as @neutron-build/sql's engine registry (I01/X01): every
// capability is `supported`, `unsupported` or `unknown`, and anything but
// `supported` FAILS CLOSED before the gated operation sends a statement.
// Evidence comes from exactly two sources:
//
// - safe, side-effect-free, value-asserting probes against the live
//   connection (a probe that errors or answers the wrong value resolves
//   `unsupported`, with the observation as evidence);
// - measured engine facts for behaviours no probe can establish and no
//   engine surface offers (recorded `unsupported`, never probed).
//
// Why the models need this rather than the feature flags: NUCLEUS_FEATURES()
// does not exist on Nucleus 1.0.2, so detectFeatures() falls back to
// all-enabled once the engine is identified. A flag that is always true is
// not evidence that the scoped DOC_* forms or the graph adjacency primitives
// exist on the connected build; these probes are.
// ---------------------------------------------------------------------------

import type { Transport, NucleusFeatures } from './types.js';
import { NucleusNotSupportedError, NucleusQueryError } from './errors.js';

export type SpecialtyCapabilityStatus = 'supported' | 'unsupported' | 'unknown';

/**
 * Capabilities the X02 surfaces depend on or explicitly do not offer.
 *
 * Probe-resolved (the surface exists and answers correctly on this build):
 * - `document-collections`: collection-scoped DOC_COUNT / DOC_GET /
 *   DOC_PATH_IN — required by `document.collection()`.
 * - `graph-adjacency`: GRAPH_NODE and GRAPH_NEIGHBORS — required by
 *   `graph.traverse()`, bounded `graph.shortestPath()` and `sqlNodes()`.
 * - `graph-property-match`: GRAPH_QUERY property-equality matching —
 *   required by `sqlNodes()` row lookups.
 *
 * Measured absent (no engine surface; never advertised):
 * - `graph-tenant-isolation`: the graph is one process-global store with no
 *   namespace, owner or policy boundary.
 * - `graph-query-parameters`: GRAPH_QUERY takes Cypher text only.
 * - `graph-multi-label`: GRAPH_ADD_NODE takes exactly one label.
 * - `specialty-session-isolation`: another session reads an open
 *   transaction's document/graph writes (dirty reads).
 * - `atomic-sql-specialty-writes`: SQL rows and document/graph entries are
 *   not written atomically as one isolated unit (see the README's
 *   "Transaction scope").
 */
export type SpecialtyCapability =
  | 'document-collections'
  | 'graph-adjacency'
  | 'graph-property-match'
  | 'graph-tenant-isolation'
  | 'graph-query-parameters'
  | 'graph-multi-label'
  | 'specialty-session-isolation'
  | 'atomic-sql-specialty-writes';

export interface SpecialtyCapabilityEvidence {
  readonly capability: SpecialtyCapability;
  readonly status: SpecialtyCapabilityStatus;
  readonly evidence: string;
}

/** Thrown when an operation needs a capability the connected engine does not prove. */
export class NucleusCapabilityError extends NucleusNotSupportedError {
  readonly capability: SpecialtyCapability;
  readonly status: SpecialtyCapabilityStatus;
  readonly evidence: string;

  constructor(capability: SpecialtyCapability, status: SpecialtyCapabilityStatus, evidence: string, detail?: string) {
    super(`${detail ? `${detail}: ` : ''}capability ${capability} is ${status} on the connected engine (${evidence})`, {
      meta: { capability, status, evidence },
    });
    this.name = 'NucleusCapabilityError';
    this.capability = capability;
    this.status = status;
    this.evidence = evidence;
  }
}

/** Collection/table name the probes read. Nothing is ever written to it. */
export const CAPABILITY_PROBE_NAME = 'neutron_capability_probe';

interface ProbeStep {
  readonly sql: string;
  readonly params: readonly unknown[];
  /** Returns null when the answer is right, otherwise what was wrong. */
  readonly check: (value: unknown) => string | null;
}

const isNull = (v: unknown): string | null => (v === null || v === undefined ? null : `expected NULL, got ${JSON.stringify(v)}`);

const PROBES: Readonly<Partial<Record<SpecialtyCapability, readonly ProbeStep[]>>> = {
  'document-collections': [
    {
      sql: 'SELECT DOC_COUNT($1)',
      params: [CAPABILITY_PROBE_NAME],
      check: (v) => (v !== null && Number(v) === 0 ? null : `expected 0 documents in the probe collection, got ${JSON.stringify(v)}`),
    },
    { sql: 'SELECT DOC_GET($1, $2)', params: [CAPABILITY_PROBE_NAME, '0'], check: isNull },
    { sql: 'SELECT DOC_PATH_IN($1, $2, $3)', params: [CAPABILITY_PROBE_NAME, '0', 'k'], check: isNull },
  ],
  'graph-adjacency': [
    // Engine node ids start at 1; node 0 never exists.
    { sql: 'SELECT GRAPH_NODE($1)', params: ['0'], check: isNull },
    {
      sql: 'SELECT GRAPH_NEIGHBORS($1, $2)',
      params: [0, 'out'],
      check: (v) => {
        if (v === null || v === undefined || v === '[]') return null;
        return `expected no neighbors for a node that does not exist, got ${JSON.stringify(v)}`;
      },
    },
  ],
  'graph-property-match': [
    {
      sql: 'SELECT GRAPH_QUERY($1)',
      params: [
        `MATCH (n) WHERE n.sqlref_table = '${CAPABILITY_PROBE_NAME}' AND n.sqlref_row = 0 RETURN n, n.sqlref_schema`,
      ],
      check: (v) => {
        if (typeof v !== 'string') return `expected a JSON result, got ${JSON.stringify(v)}`;
        try {
          const parsed = JSON.parse(v) as { columns?: unknown; rows?: unknown };
          if (
            Array.isArray(parsed.columns) &&
            parsed.columns.length === 2 &&
            Array.isArray(parsed.rows) &&
            parsed.rows.length === 0
          ) {
            return null;
          }
        } catch {
          // fall through
        }
        return `expected {columns: [n, n.sqlref_schema], rows: []}, got ${v.slice(0, 80)}`;
      },
    },
  ],
};

const MEASURED_ABSENT: Readonly<Partial<Record<SpecialtyCapability, string>>> = {
  'graph-tenant-isolation':
    'the graph is one process-global store: no namespace, owner or policy boundary exists, so one tenant can traverse into another',
  'graph-query-parameters': 'GRAPH_QUERY takes the Cypher text only; `$name` references fail in the engine parser',
  'graph-multi-label': 'GRAPH_ADD_NODE takes exactly one label; a joined "A:B" string is stored as a single label',
  'specialty-session-isolation':
    "a second session reads an open transaction's uncommitted document and graph writes (dirty reads)",
  'atomic-sql-specialty-writes':
    'document/graph writes are visible to other sessions before COMMIT, and this client runs each document/graph call as its own statement outside any SQL transaction',
};

function isServerAnswer(err: unknown): boolean {
  if (err instanceof NucleusQueryError) return true;
  const code = (err as { code?: unknown } | null)?.code;
  return typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code);
}

/**
 * Resolves and caches capability status for one transport. Probes run once
 * per capability per gate; a transport-level failure (not a server answer) is
 * rethrown and NOT cached, so a dropped connection does not become a
 * permanent `unsupported`.
 */
export class SpecialtyCapabilityGate {
  private readonly cache = new Map<SpecialtyCapability, Promise<SpecialtyCapabilityEvidence>>();

  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  status(capability: SpecialtyCapability): Promise<SpecialtyCapabilityEvidence> {
    const cached = this.cache.get(capability);
    if (cached) return cached;
    const pending = this.resolve(capability);
    this.cache.set(capability, pending);
    pending.catch(() => {
      if (this.cache.get(capability) === pending) this.cache.delete(capability);
    });
    return pending;
  }

  /** Throws `NucleusCapabilityError` unless the capability resolves `supported`. */
  async require(capability: SpecialtyCapability, detail?: string): Promise<void> {
    const ev = await this.status(capability);
    if (ev.status !== 'supported') {
      throw new NucleusCapabilityError(capability, ev.status, ev.evidence, detail);
    }
  }

  private async resolve(capability: SpecialtyCapability): Promise<SpecialtyCapabilityEvidence> {
    if (!this.features.isNucleus) {
      return { capability, status: 'unsupported', evidence: 'not a Nucleus engine' };
    }
    const absent = MEASURED_ABSENT[capability];
    if (absent !== undefined) {
      return { capability, status: 'unsupported', evidence: `measured on Nucleus: ${absent}` };
    }
    const steps = PROBES[capability];
    if (steps === undefined) {
      return { capability, status: 'unknown', evidence: 'no resolution rule' };
    }
    for (const step of steps) {
      let value: unknown;
      try {
        value = await this.transport.fetchval(step.sql, [...step.params]);
      } catch (err) {
        if (!isServerAnswer(err)) throw err;
        const message = err instanceof Error ? err.message : String(err);
        return { capability, status: 'unsupported', evidence: `probe ${step.sql} failed: ${message.slice(0, 160)}` };
      }
      const wrong = step.check(value);
      if (wrong !== null) {
        return { capability, status: 'unsupported', evidence: `probe ${step.sql} answered wrong: ${wrong}` };
      }
    }
    return { capability, status: 'supported', evidence: `probed: ${steps.map((s) => s.sql).join('; ')}` };
  }
}

/** Every capability, in a stable order (for reports). */
export const SPECIALTY_CAPABILITIES: readonly SpecialtyCapability[] = [
  'document-collections',
  'graph-adjacency',
  'graph-property-match',
  'graph-tenant-isolation',
  'graph-query-parameters',
  'graph-multi-label',
  'specialty-session-isolation',
  'atomic-sql-specialty-writes',
];
