// ---------------------------------------------------------------------------
// @neutron-build/sql — engine identity and capability contracts
// ---------------------------------------------------------------------------
// Tri-state capability model (I01): every capability is supported,
// unsupported or unknown, and UNKNOWN FAILS CLOSED — a statement carrying a
// requirement the runtime cannot resolve is rejected, never run on the
// all-enabled assumption. Evidence for a status comes from exactly two
// sources: documented version facts (PostgreSQL release history) and safe
// probes executed against the live connection. Nothing else: an engine
// capability is never inferred from the absence of an error elsewhere.
//
// Engine detection follows FRAMEWORK_CONTRACT.md §1: `SELECT VERSION()`; a
// version string containing "Nucleus" identifies a Nucleus engine and carries
// its version. Deliberate divergence from the contract's boolean
// `is_nucleus`: `product` is a tri-state ("postgres" | "nucleus" |
// "unknown") because this package can be pointed at arbitrary pg-wire
// servers, and an unrecognized server must not be treated as plain Postgres.

import type { Driver } from "./drivers.js";
import type { StatementCapability } from "./codecs.js";
import { ServerSqlError } from "./errors.js";

export type EngineProduct = "postgres" | "nucleus" | "unknown";

export interface EngineIdentity {
  /** Which engine answered `SELECT VERSION()`. "unknown" = unrecognized
   *  version string; capabilities then resolve only through probes. */
  readonly product: EngineProduct;
  /** Parsed engine version ("17.11"; the Nucleus X.Y.Z for nucleus engines).
   *  Empty string when the string carried no parseable version. */
  readonly version: string;
  /** The raw `SELECT VERSION()` text, kept for evidence trails. */
  readonly raw: string;
}

const NUCLEUS_VERSION = /\(Nucleus\s+v?(\d+(?:\.\d+)+)/;
const POSTGRES_VERSION = /^PostgreSQL\s+(\d+(?:\.\d+)*)/;

/** Parse a `SELECT VERSION()` string into an engine identity. Pure. */
export function parseVersionString(raw: string): EngineIdentity {
  if (raw.includes("Nucleus")) {
    const m = NUCLEUS_VERSION.exec(raw);
    return { product: "nucleus", version: m ? m[1] : "", raw };
  }
  const m = POSTGRES_VERSION.exec(raw);
  if (m) return { product: "postgres", version: m[1], raw };
  return { product: "unknown", version: "", raw };
}

// ---------------------------------------------------------------------------
// Capability registry
// ---------------------------------------------------------------------------

export type CapabilityStatus = "supported" | "unsupported" | "unknown";

/** The resolution outcome for one capability: status plus the evidence that
 *  produced it (version fact, probe result, or the absence of any rule). */
export interface CapabilityEvidence {
  readonly capability: StatementCapability;
  readonly status: CapabilityStatus;
  readonly evidence: string;
  readonly engine: EngineIdentity;
}

interface CapabilitySpec {
  /** What the capability means (statements carrying it need this). */
  readonly description: string;
  /** Documented PostgreSQL release that introduced the capability. Statements
   *  are proven supported/unsupported on plain Postgres by version compare
   *  against this fact — no probe needed. */
  readonly postgresSince?: readonly [number, number];
  /** Safe (side-effect-free, SELECT-only) probe proving the capability on
   *  engines without a version fact (Nucleus, unrecognized servers). */
  readonly probeSql?: string;
}

/** Registered resolution rules. Capabilities without an entry resolve to
 *  `unknown` for every engine — including future vocabulary the runtime has
 *  no rule for yet. Never invent an entry without a documented fact or a
 *  genuinely proving probe. */
const REGISTRY: Readonly<Record<string, CapabilitySpec>> = {
  "jsonb-functions": {
    description: "to_jsonb / jsonb_build_object / jsonb_agg (lossless wire reads and relational aggregation)",
    // jsonb and its builder/aggregate functions shipped in PostgreSQL 9.4
    // (PostgreSQL 9.4 release notes, "JSON changes").
    postgresSince: [9, 4],
    probeSql: "select to_jsonb(1)::text as a, jsonb_build_object('k', 1)::text as b, coalesce(jsonb_agg(v), '[]'::jsonb)::text as c from (values (1)) as t(v)",
  },
  // Q08 rules. Version facts only (PostgreSQL release notes); no probes: a
  // statement that merely parses on another engine proves nothing about
  // window semantics, lock behavior or cursor lifetime, so non-Postgres
  // engines resolve `unknown` and fail closed until engine evidence exists.
  "window-functions": {
    description: "window function calls — fn(...) over (partition by / order by / frame) (PostgreSQL 8.4 release notes)",
    postgresSince: [8, 4],
  },
  "window-frame-groups": {
    description: "the GROUPS window frame mode (PostgreSQL 11 release notes)",
    postgresSince: [11, 0],
  },
  "window-frame-exclude": {
    description: "window frame EXCLUDE clauses (PostgreSQL 11 release notes)",
    postgresSince: [11, 0],
  },
  "row-locking": {
    description: "SELECT ... FOR UPDATE / FOR SHARE with NOWAIT (FOR SHARE and NOWAIT: PostgreSQL 8.1 release notes)",
    postgresSince: [8, 1],
  },
  "row-locking-key-strength": {
    description: "FOR NO KEY UPDATE / FOR KEY SHARE lock strengths (PostgreSQL 9.3 release notes)",
    postgresSince: [9, 3],
  },
  "row-locking-skip-locked": {
    description: "the SKIP LOCKED lock-wait policy (PostgreSQL 9.5 release notes)",
    postgresSince: [9, 5],
  },
  "server-cursors": {
    description: "DECLARE ... NO SCROLL CURSOR / FETCH FORWARD n / CLOSE inside a transaction (NO SCROLL: PostgreSQL 7.4 release notes)",
    postgresSince: [7, 4],
  },
};

function compareVersion(version: string, since: readonly [number, number]): number | null {
  const m = /^(\d+)(?:\.(\d+))?/.exec(version);
  if (!m) return null;
  const major = Number(m[1]);
  const minor = m[2] === undefined ? 0 : Number(m[2]);
  if (major !== since[0]) return Math.sign(major - since[0]);
  return Math.sign(minor - since[1]);
}

/** Resolve one capability against one engine identity. Pure except for the
 *  optional probe callback (a live SELECT the caller supplies). Probes are
 *  consulted only when version facts cannot decide (non-postgres engine, or
 *  an unparseable version). A probe that fails with a server SQL error is
 *  positive evidence of `unsupported`; transport errors propagate (they say
 *  nothing about the capability). */
export async function resolveCapabilityStatus(
  engine: EngineIdentity,
  capability: StatementCapability,
  probe?: (sql: string) => Promise<void>,
): Promise<CapabilityEvidence> {
  const spec = REGISTRY[capability];
  if (!spec) {
    return {
      capability,
      status: "unknown",
      evidence: `no resolution rule is registered for "${capability}" on any engine — never assumed supported`,
      engine,
    };
  }
  if (engine.product === "postgres") {
    if (engine.version === "") {
      return {
        capability,
        status: "unknown",
        evidence: `PostgreSQL version string carried no parseable version: "${engine.raw.slice(0, 120)}"`,
        engine,
      };
    }
    const cmp = spec.postgresSince ? compareVersion(engine.version, spec.postgresSince) : null;
    if (cmp !== null) {
      return {
        capability,
        status: cmp >= 0 ? "supported" : "unsupported",
        evidence:
          cmp >= 0
            ? `PostgreSQL ${engine.version} >= ${spec.postgresSince!.join(".")} (documented introduction of ${spec.description})`
            : `PostgreSQL ${engine.version} < ${spec.postgresSince!.join(".")} (documented introduction of ${spec.description})`,
        engine,
      };
    }
  }
  if (spec.probeSql && probe) {
    try {
      await probe(spec.probeSql);
      return {
        capability,
        status: "supported",
        evidence: `probe succeeded on ${engine.product === "unknown" ? "unrecognized engine" : engine.product} ${engine.version} ("${spec.probeSql.slice(0, 60)}...")`,
        engine,
      };
    } catch (err) {
      if (err instanceof ServerSqlError) {
        return {
          capability,
          status: "unsupported",
          evidence: `probe failed with server error sqlstate ${err.sqlstate} on ${engine.product} ${engine.version}: ${err.message}`,
          engine,
        };
      }
      throw err;
    }
  }
  return {
    capability,
    status: "unknown",
    evidence: `${engine.product === "nucleus" ? `Nucleus ${engine.version}` : engine.product === "unknown" ? "unrecognized engine" : `PostgreSQL ${engine.version}`} has no version rule for "${capability}" and no probe ran — never assumed supported`,
    engine,
  };
}

// ---------------------------------------------------------------------------
// Gate: driver-bound, memoized resolution
// ---------------------------------------------------------------------------

/** Thrown when a compiled statement carries capability requirements the
 *  connected engine cannot satisfy — including UNKNOWN, which fails closed
 *  (it is never treated as all-enabled). */
export class CapabilityRequirementError extends Error {
  constructor(
    message: string,
    /** The failing resolutions, one per rejected requirement. */
    readonly results: readonly CapabilityEvidence[],
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export interface CapabilityGate {
  /** Engine identity, resolved once per gate (memoized `SELECT VERSION()`). */
  engine(): Promise<EngineIdentity>;
  /** Status of one capability (memoized per capability). */
  status(capability: StatementCapability): Promise<CapabilityEvidence>;
  /** Fail closed unless every required capability resolves `supported`.
   *  Throws CapabilityRequirementError otherwise. No-op for empty lists. */
  assert(required: readonly StatementCapability[]): Promise<void>;
}

export function capabilityGate(driver: Driver): CapabilityGate {
  let enginePromise: Promise<EngineIdentity> | null = null;
  const statusPromises = new Map<string, Promise<CapabilityEvidence>>();

  const engine = (): Promise<EngineIdentity> => {
    if (!enginePromise) {
      enginePromise = (async () => {
        const rows = await driver.query<{ version: string }>("select version() as version");
        const raw = rows[0]?.version ?? "";
        return parseVersionString(typeof raw === "string" ? raw : String(raw));
      })();
      // A failed probe (connection down) must not poison the memo: retry on
      // the next call. Successful identities are stable for the gate's life.
      enginePromise.catch(() => {
        enginePromise = null;
        statusPromises.clear();
      });
    }
    return enginePromise;
  };

  const status = (capability: StatementCapability): Promise<CapabilityEvidence> => {
    let p = statusPromises.get(capability);
    if (!p) {
      p = engine().then((identity) =>
        resolveCapabilityStatus(identity, capability, async (sql) => {
          await driver.query(sql);
        }),
      );
      statusPromises.set(capability, p);
      // Only settled evidence memoizes; transport failures retry next call.
      p.catch(() => statusPromises.delete(capability));
    }
    return p;
  };

  return {
    engine,
    status,
    assert: async (required: readonly StatementCapability[]): Promise<void> => {
      if (required.length === 0) return;
      const settled = await Promise.all(required.map((cap) => status(cap)));
      const failing = settled.filter((r) => r.status !== "supported");
      if (failing.length > 0) {
        const lines = failing.map((r) => `  - "${r.capability}": ${r.status} (${r.evidence})`);
        throw new CapabilityRequirementError(
          `statement requires capabilities the connected engine does not prove:\n${lines.join("\n")}\nfailing closed — unknown is not all-enabled`,
          failing,
        );
      }
    },
  };
}
