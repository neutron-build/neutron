// ---------------------------------------------------------------------------
// @neutron-build/sql — explicit execution profiles (NP01)
// ---------------------------------------------------------------------------
// `createDatabase({ profile })` binds an explicitly named endpoint profile.
// Without `profile` nothing here runs and behavior is unchanged.
//
//   "postgres-direct"                      reported PostgreSQL identity only;
//                                          Nucleus and other pgwire servers
//                                          are refused. No statement guard.
//   "nucleus-relational-rc-v1-candidate"   an UNCERTIFIED finite Nucleus
//                                          candidate (startup 16.0 (Nucleus),
//                                          SQL 1.2.0). Immutable capabilities,
//                                          packageEnabled false. Only generated
//                                          point CRUD over registered,
//                                          schema-qualified tables whose every
//                                          column is int4/int8/bool/text/jsonb/
//                                          timestamptz is admitted. Raw SQL,
//                                          joins, relational/nested queries,
//                                          aggregates, streams, prepared
//                                          statements, catalog access, custom
//                                          codecs, numeric and stronger
//                                          isolation are refused BEFORE dispatch.
//
// Reported identity (startup parameter + SELECT version()) is not binary,
// TLS or intermediary attestation. The statement guard is a finite operation
// contract over this client's generated SQL, not a security boundary against a
// caller that holds a pinned connection from `db.driver.pin()`. Nothing in
// this module certifies PostgreSQL parity or enables a package support matrix.

import { quoteIdent } from "./compile.js";
import type { StatementCapability } from "./codecs.js";
import type { Driver } from "./drivers.js";
import {
  CapabilityRequirementError,
  capabilityGate,
  parseVersionString,
  resolveCapabilityStatus,
  type CapabilityEvidence,
  type CapabilityGate,
  type EngineIdentity,
} from "./engine.js";
import { NeutronSqlError } from "./errors.js";
import { getTableColumns, getTableName, getTableSchema, getViewDefinition, tableRefParts, type AnyPgTable } from "./schema.js";
import { renderBeginSql, runTransaction, type PinnedExecutor, type QueryExecutionOptions, type TransactionModes } from "./transactions.js";

export const POSTGRES_DIRECT_PROFILE = "postgres-direct" as const;
export const NUCLEUS_CANDIDATE_PROFILE = "nucleus-relational-rc-v1-candidate" as const;
export const NUCLEUS_CANDIDATE_VERSION = "1.2.0";
export const NUCLEUS_CAPABILITIES: readonly string[] = Object.freeze(["point-crud", "read-committed-transaction", "savepoint"]);

export type ExecutionProfile = typeof POSTGRES_DIRECT_PROFILE | typeof NUCLEUS_CANDIDATE_PROFILE;

/** Thrown when an explicit profile refuses an endpoint, table or operation. */
export class ProfileRefusedError extends NeutronSqlError {}

function refuse(reason: string): ProfileRefusedError {
  return new ProfileRefusedError(reason);
}

/** Reported engine/version admission; not TLS or intermediary attestation. */
export interface EndpointIdentity {
  readonly engine: "postgresql" | "nucleus";
  readonly version: string;
  readonly profile: ExecutionProfile;
  readonly topology: "caller-declared-direct";
  readonly capabilities: readonly string[];
  readonly packageEnabled: boolean;
  readonly qualification: "reported-identity-only" | "uncertified-finite-candidate";
}

export function validateExecutionProfile(profile: unknown): ExecutionProfile {
  if (profile !== POSTGRES_DIRECT_PROFILE && profile !== NUCLEUS_CANDIDATE_PROFILE) {
    throw refuse("unsupported/unknown execution profile; operation refused");
  }
  return profile as ExecutionProfile;
}

const STARTUP = /^([0-9]+(?:\.[0-9]+){1,2})(?:\s+\([^\r\n]*\))?$/;
const VERSION = /^PostgreSQL ([0-9]+(?:\.[0-9]+){1,2})(?:\s|$)/;
const UNSUPPORTED = ["nucleus", "cockroach", "yugabyte", "redshift", "greenplum", "materialize", "questdb", "cratedb"];

function hasUnsupportedMarker(text: string): boolean {
  const folded = text.toLowerCase();
  return UNSUPPORTED.some((marker) => folded.includes(marker));
}

const NUCLEUS_STARTUP = "16.0 (Nucleus)";
const NUCLEUS_REPORTED = "PostgreSQL 16.0 (Nucleus " + NUCLEUS_CANDIDATE_VERSION + " — The Definitive Database)";

function freezeIdentity(identity: EndpointIdentity): EndpointIdentity {
  return Object.freeze(identity);
}

/** Pure admission of a startup `server_version` report and the SELECT
 *  version() text against one named profile. */
export function admitEndpoint(startup: unknown, reported: unknown, profile: ExecutionProfile): EndpointIdentity {
  validateExecutionProfile(profile);
  if (profile === NUCLEUS_CANDIDATE_PROFILE) {
    if (startup !== NUCLEUS_STARTUP) throw refuse("unknown Nucleus candidate startup identity");
    if (reported !== NUCLEUS_REPORTED) throw refuse("unknown or contradictory Nucleus candidate reported identity");
    return freezeIdentity({
      engine: "nucleus",
      version: NUCLEUS_CANDIDATE_VERSION,
      profile,
      topology: "caller-declared-direct",
      capabilities: NUCLEUS_CAPABILITIES,
      packageEnabled: false,
      qualification: "uncertified-finite-candidate",
    });
  }
  if (typeof startup !== "string" || hasUnsupportedMarker(startup)) throw refuse("unsupported or unknown endpoint engine identity");
  const startupMatch = STARTUP.exec(startup);
  if (startupMatch === null) throw refuse("unsupported or unknown endpoint engine identity");
  if (typeof reported !== "string" || hasUnsupportedMarker(reported)) throw refuse("unsupported or unknown endpoint engine identity");
  const reportedMatch = VERSION.exec(reported);
  if (reportedMatch === null || reportedMatch[1] !== startupMatch[1]) throw refuse("contradictory or unknown endpoint engine identity");
  return freezeIdentity({
    engine: "postgresql",
    version: startupMatch[1],
    profile,
    topology: "caller-declared-direct",
    capabilities: Object.freeze([]),
    packageEnabled: true,
    qualification: "reported-identity-only",
  });
}

const STARTUP_SQL = "select current_setting('server_version') as server_version";
const VERSION_SQL = "select version() as version";

/** Two fixed read-only identity queries; they precede every caller statement. */
export async function probeEndpoint(driver: Driver, profile: ExecutionProfile): Promise<{ identity: EndpointIdentity; engine: EngineIdentity }> {
  const startupRows = await driver.query<{ server_version?: unknown }>(STARTUP_SQL);
  const versionRows = await driver.query<{ version?: unknown }>(VERSION_SQL);
  const reported = versionRows[0]?.version;
  const identity = admitEndpoint(startupRows[0]?.server_version, reported, profile);
  return { identity, engine: parseVersionString(typeof reported === "string" ? reported : String(reported)) };
}

// ---------------------------------------------------------------------------
// Table metadata admission
// ---------------------------------------------------------------------------

const FINITE_COLUMN_TYPES: ReadonlySet<string> = new Set(["integer", "bigint", "boolean", "text", "jsonb", "timestamptz"]);

/** Fail closed unless every column of the whole table (projected or not) is
 *  one of the finite scalar families and the table is a schema-qualified
 *  physical table. `serial` is refused: its default is a sequence feature. */
export function assertFiniteTable(table: AnyPgTable): void {
  const name = getTableName(table);
  if (getTableSchema(table) === undefined) throw refuse(`finite profile requires a schema-qualified table (table "${name}")`);
  if (getViewDefinition(table) !== undefined) throw refuse(`finite profile requires an ordinary physical table (view "${name}")`);
  const columns = Object.values(getTableColumns(table));
  if (columns.length === 0) throw refuse(`finite profile table "${name}" declares no columns`);
  for (const column of columns) {
    if (
      !FINITE_COLUMN_TYPES.has(column.dataType) ||
      column.arrayDimensions !== undefined ||
      column.enumDef !== undefined ||
      column.customCodec !== undefined ||
      column.generatedExpr !== undefined ||
      column.identityKind !== undefined ||
      column.valueDecoder !== undefined ||
      column.canonicalText
    ) {
      throw refuse(`column type outside uncertified Nucleus finite profile (table "${name}", column "${column.columnName}")`);
    }
  }
}

// ---------------------------------------------------------------------------
// Statement guard (generated point CRUD grammar over registered tables)
// ---------------------------------------------------------------------------

export type StatementGuard = (sqlText: string, params: readonly unknown[] | undefined, allowControl: boolean) => void;

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const ALIAS = '"(?:[^"]|"")*"';
const PARAM = "\\$[1-9][0-9]*(?:::text::(?:timestamptz|jsonb))?";
const ATOM = "(?:" + PARAM + "|default)";

function list(item: string): string {
  return item + "(?:, " + item + ")*";
}

function tableStatementPatterns(table: AnyPgTable): RegExp[] {
  const qualified = escapeRegExp(tableRefParts(table).map(quoteIdent).join("."));
  const columns = Object.values(getTableColumns(table));
  const names = columns.map((column) => escapeRegExp(quoteIdent(column.columnName)));
  const anyName = "(?:" + names.join("|") + ")";
  const projection =
    "(?:" +
    columns
      .map((column, index) => {
        const ref = qualified + "\\." + names[index];
        return column.dataType === "timestamptz"
          ? "to_jsonb\\(" + ref + " at time zone 'UTC'\\)::text as " + ALIAS
          : ref + "(?: as " + ALIAS + ")?";
      })
      .join("|") +
    ")";
  const returning = "(?: returning " + list(projection) + ")?";
  const point = "\\(" + qualified + "\\." + anyName + " = " + PARAM + "\\)";
  const select = "^select " + list(projection) + " from " + qualified + " where " + point + "$";
  const insert =
    "^insert into " + qualified + "(?: default values| \\(" + list(anyName) + "\\) values \\(" + list(ATOM) + "\\))" + returning + "$";
  const update = "^update " + qualified + " set " + list(anyName + " = " + PARAM) + " where " + point + returning + "$";
  const remove = "^delete from " + qualified + " where " + point + returning + "$";
  return [select, insert, update, remove].map((source) => new RegExp(source));
}

const TRANSACTION_CONTROL = /^(?:begin|begin isolation level read committed|commit|rollback|(?:savepoint|rollback to savepoint|release savepoint) "[A-Za-z_][A-Za-z0-9_]{0,62}")$/;
const QUOTED = /"(?:[^"]|"")*"/g;
const PLACEHOLDER = /\$([1-9][0-9]*)(::text::(timestamptz|jsonb))?/g;
const UTC_TIMESTAMPTZ = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-]00:00)$/;

function assertFiniteArgs(sqlText: string, args: readonly unknown[]): void {
  const casts: Array<string | undefined> = [];
  for (const match of sqlText.replace(QUOTED, '""').matchAll(PLACEHOLDER)) {
    if (Number(match[1]) !== casts.length + 1) throw refuse("placeholders outside uncertified Nucleus finite point profile");
    casts.push(match[3]);
  }
  if (casts.length !== args.length) throw refuse("parameter count outside uncertified Nucleus finite point profile");
  args.forEach((value, index) => {
    const admitted =
      value === null ||
      typeof value === "boolean" ||
      typeof value === "string" ||
      typeof value === "bigint" ||
      (typeof value === "number" && Number.isSafeInteger(value));
    if (!admitted) throw refuse("parameter type outside uncertified Nucleus finite profile");
    if (casts[index] === "timestamptz" && value !== null && (typeof value !== "string" || !UTC_TIMESTAMPTZ.test(value))) {
      throw refuse("timestamptz parameter must be a UTC timestamp string");
    }
  });
}

/** Build the finite statement guard for the registered tables. `allowControl`
 *  admits only the runner's own BEGIN (default or READ COMMITTED)/COMMIT/
 *  ROLLBACK/SAVEPOINT statements, which never carry parameters. */
export function finiteStatementGuard(tables: readonly AnyPgTable[]): StatementGuard {
  const patterns = tables.flatMap(tableStatementPatterns);
  return (sqlText, params, allowControl) => {
    if (typeof sqlText !== "string") throw refuse("SQL text must be a string");
    const args = params ?? [];
    if (allowControl && TRANSACTION_CONTROL.test(sqlText)) {
      if (args.length !== 0) throw refuse("transaction control takes no parameters");
      return;
    }
    if (!patterns.some((pattern) => pattern.test(sqlText))) {
      throw refuse("SQL shape outside uncertified Nucleus finite point profile; refused before dispatch");
    }
    assertFiniteArgs(sqlText, args);
  };
}

// ---------------------------------------------------------------------------
// Guarded adapter and profile capability gate
// ---------------------------------------------------------------------------

function guardedDriver(raw: Driver, guard: StatementGuard): Driver {
  const rawPin = raw.pin;
  if (typeof rawPin !== "function") {
    throw refuse("the finite Nucleus profile requires a pinnable adapter (Driver.pin); this adapter has none");
  }
  const guardPin = (pin: PinnedExecutor): PinnedExecutor => ({
    async query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<T[]> {
      guard(sqlText, params, true);
      return pin.query<T>(sqlText, params, options);
    },
    async execute(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<number> {
      guard(sqlText, params, true);
      return pin.execute(sqlText, params, options);
    },
    release: (err?: unknown): void => pin.release(err),
  });
  const pinGuarded = async (): Promise<PinnedExecutor> => guardPin(await rawPin.call(raw));
  return {
    async query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<T[]> {
      guard(sqlText, params, false);
      return raw.query<T>(sqlText, params, options);
    },
    async execute(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<number> {
      guard(sqlText, params, false);
      return raw.execute(sqlText, params, options);
    },
    async begin<T>(fn: (tx: Driver) => Promise<T>, modes: TransactionModes = {}): Promise<T> {
      renderBeginSql(modes);
      return runTransaction(await pinGuarded(), (scope) => fn(scope), modes);
    },
    close: () => raw.close(),
    lifecycle: raw.lifecycle,
    pin: pinGuarded,
  };
}

/** Capability gate of the finite profile: only `jsonb-functions` (the
 *  timestamptz wire read) can resolve, through its registered live probe;
 *  every other requirement is unsupported without any dispatch. */
function finiteCapabilityGate(raw: Driver, engine: EngineIdentity): CapabilityGate {
  const memo = new Map<string, Promise<CapabilityEvidence>>();
  const status = (capability: StatementCapability): Promise<CapabilityEvidence> => {
    if (capability !== "jsonb-functions") {
      const outside: CapabilityEvidence = {
        capability,
        status: "unsupported",
        evidence: "outside the uncertified Nucleus finite profile; refused before dispatch",
        engine,
      };
      return Promise.resolve(outside);
    }
    let pending = memo.get(capability);
    if (pending === undefined) {
      pending = resolveCapabilityStatus(engine, capability, async (probeSql) => {
        await raw.query(probeSql);
      });
      memo.set(capability, pending);
      pending.catch(() => memo.delete(capability));
    }
    return pending;
  };
  return {
    engine: () => Promise.resolve(engine),
    status,
    assert: async (required: readonly StatementCapability[]): Promise<void> => {
      if (required.length === 0) return;
      const settled = await Promise.all(required.map((capability) => status(capability)));
      const failing = settled.filter((result) => result.status !== "supported");
      if (failing.length > 0) {
        const lines = failing.map((result) => `  - "${result.capability}": ${result.status} (${result.evidence})`);
        throw new CapabilityRequirementError(
          `statement requires capabilities the uncertified Nucleus finite profile does not admit:\n${lines.join("\n")}\nfailing closed`,
          failing,
        );
      }
    },
  };
}

export interface AdmittedProfile {
  readonly driver: Driver;
  readonly capabilities: CapabilityGate;
  readonly identity: EndpointIdentity;
}

/** Validate the registered tables, probe and admit the endpoint identity, and
 *  return the driver/gate the database must use. Caller closes `raw` on error. */
export async function admitExecutionProfile(raw: Driver, profile: ExecutionProfile, tables: readonly AnyPgTable[]): Promise<AdmittedProfile> {
  if (profile === NUCLEUS_CANDIDATE_PROFILE) {
    if (typeof raw.pin !== "function") {
      throw refuse("the finite Nucleus profile requires a pinnable adapter (Driver.pin); this adapter has none");
    }
    for (const table of tables) assertFiniteTable(table);
  }
  const admitted = await probeEndpoint(raw, profile);
  if (profile !== NUCLEUS_CANDIDATE_PROFILE) {
    return { driver: raw, capabilities: capabilityGate(raw), identity: admitted.identity };
  }
  return {
    driver: guardedDriver(raw, finiteStatementGuard(tables)),
    capabilities: finiteCapabilityGate(raw, admitted.engine),
    identity: admitted.identity,
  };
}
