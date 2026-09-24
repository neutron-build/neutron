// ---------------------------------------------------------------------------
// @neutron-build/sql — transaction control (I02)
// ---------------------------------------------------------------------------
// One shared runner drives transactions for BOTH bundled drivers over a
// PinnedExecutor (a reserved/pinned connection supplied by the driver): BEGIN
// with validated modes, user callback, COMMIT/ROLLBACK, real SAVEPOINTs for
// nesting, and honest failure classification. A transport failure while
// COMMIT is in flight is a CommitAmbiguityError: the outcome is unknown and
// is NEVER automatically replayed. Retry (40001/40P01 only) is opt-in and
// requires the caller to assert the whole transaction is idempotent, because
// a retried attempt re-executes the callback from the top.

import type { Driver, PreparedStatement } from "./drivers.js";
import {
  CommitAmbiguityError,
  ConnectionFailedError,
  NeutronSqlError,
  ServerSqlError,
  isFatalConnectionLoss,
  isRetriableTransactionError,
} from "./errors.js";
import { errorSummary, statementIdOf, type IsolationLevel, type SqlEvent } from "./logger.js";

/** SQLSTATEs whose arrival during COMMIT means the outcome is UNKNOWN (the
 * session died or the statement was interrupted — the transaction may or may
 * not be durably committed): FATAL connection exceptions (57P01 admin
 * terminate, 57P02 crash shutdown, 57P03 cannot-connect-now) and 57014
 * cancellation. Any other SQL error at COMMIT is the server's definitive
 * rollback answer (including 40001 serialization failures, which are
 * retryable upstream). */
const COMMIT_AMBIGUITY_SQLSTATES = new Set(["57P01", "57P02", "57P03", "57014"]);

function commitOutcomeUnknown(err: unknown): boolean {
  return err instanceof ConnectionFailedError || (err instanceof ServerSqlError && COMMIT_AMBIGUITY_SQLSTATES.has(err.sqlstate));
}

/** Connection-level execution options (I02). Deadlines and AbortSignals
 * CANCEL THE QUERY AT THE SERVER (pg: pg_cancel_backend side channel on a
 * second pooled connection; postgres.js: the driver's native Query.cancel()
 * on a dedicated cancel connection). After a cancellation the pooled
 * connection remains usable — the server answers 57014 and the session
 * returns to a clean state. */
export interface QueryExecutionOptions {
  /** Cancel the query at the server after this many milliseconds (positive,
   *  finite). Measured from statement submission. */
  deadlineMs?: number;
  /** Cancel the query at the server when this signal fires. A signal that is
   *  already aborted rejects the call immediately without a server round
   *  trip (QueryCanceledError with dispatched: false). */
  signal?: AbortSignal;
}

/** One reserved connection (pool checkout / postgres.js reserve()), scoped to
 *  a transaction. Implemented by the driver adapters; the runner owns its
 *  lifecycle and releases it exactly once. */
export interface PinnedExecutor {
  query<T = Record<string, unknown>>(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<T[]>;
  execute(sqlText: string, params?: unknown[], options?: QueryExecutionOptions): Promise<number>;
  /** Return the connection. `err` (or an internally-detected breakage)
   *  removes it from the pool instead of returning a suspect connection. */
  release(err?: unknown): void;
  /** Connection-scoped prepared statements, when the pinned connection
   *  supports them (both bundled drivers do). */
  prepare?(sqlText: string): PreparedStatement;
}

/** Transaction modes as PostgreSQL actually supports them: the three real
 *  isolation levels (READ UNCOMMITTED does not exist — it aliases READ
 *  COMMITTED — and is deliberately not offered), read-only, and DEFERRABLE
 *  only where it is real (see validateModes). */
export interface TransactionModes {
  isolation?: IsolationLevel;
  readOnly?: boolean;
  deferrable?: boolean;
}

const ISOLATION_SQL: Readonly<Record<IsolationLevel, string>> = {
  "read-committed": "read committed",
  "repeatable-read": "repeatable read",
  serializable: "serializable",
};

/** Validate transaction modes and render the BEGIN statement. DEFERRABLE is
 *  accepted by PostgreSQL with ANY isolation level but has no effect unless
 *  the transaction is also SERIALIZABLE and READ ONLY — we refuse the silent
 *  no-op instead of accepting it. */
export function renderBeginSql(modes: TransactionModes = {}): string {
  if (modes.isolation !== undefined && !(modes.isolation in ISOLATION_SQL)) {
    throw new NeutronSqlError(
      `transaction isolation "${String(modes.isolation)}" is not supported — use one of ${Object.keys(ISOLATION_SQL).join(", ")} (PostgreSQL has no READ UNCOMMITTED; it aliases READ COMMITTED)`,
    );
  }
  if (modes.deferrable === true && (modes.isolation !== "serializable" || modes.readOnly !== true)) {
    throw new NeutronSqlError(
      "deferrable transactions require isolation: \"serializable\" and readOnly: true — PostgreSQL accepts DEFERRABLE with other modes but it has no effect there, so this combination is rejected instead of silently ignored",
    );
  }
  let sql = "begin";
  if (modes.isolation !== undefined) sql += ` isolation level ${ISOLATION_SQL[modes.isolation]}`;
  if (modes.readOnly === true) sql += " read only";
  if (modes.deferrable === true) sql += " deferrable";
  return sql;
}

/** True when any mode is set (non-default). */
export function hasModes(modes: TransactionModes): boolean {
  return modes.isolation !== undefined || modes.readOnly !== undefined || modes.deferrable !== undefined;
}

// ---------------------------------------------------------------------------
// Savepoints
// ---------------------------------------------------------------------------

const SAVEPOINT_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

/** An explicitly controlled savepoint: `rollbackTo()` undoes work after the
 *  savepoint and KEEPS it active; `release()` destroys it (and everything
 *  nested inside it). */
export interface Savepoint {
  readonly name: string;
  /** Undo everything after the savepoint; the savepoint stays usable. */
  rollbackTo(): Promise<void>;
  /** Destroy the savepoint (keeping its work). Further use is an error. */
  release(): Promise<void>;
}

/** Savepoint handle state used for honest errors on stale use. */
type SavepointState = "active" | "released";

// ---------------------------------------------------------------------------
// Transaction scope
// ---------------------------------------------------------------------------

/** The driver-level transaction scope: a Driver pinned to the transaction's
 *  connection whose `begin` nests via real SAVEPOINTs, plus explicit
 *  savepoint control. */
export interface TransactionScope extends Driver {
  /** Nested transaction: a real SAVEPOINT. On error the savepoint is rolled
   *  back AND released, then the error rethrows; the outer transaction stays
   *  usable. Isolation/read-only/deferrable modes are properties of the
   *  outer BEGIN and cannot be set on a savepoint. */
  transaction<Tx>(fn: (tx: TransactionScope) => Promise<Tx>): Promise<Tx>;
  /** Create an explicitly controlled savepoint. */
  savepoint(name?: string): Promise<Savepoint>;
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

export interface TransactionHooks {
  onEvent?(event: SqlEvent): void;
}

let txSequence = 0;

/** Drive one transaction attempt on a pinned connection. Owns the pin: it is
 *  released exactly once, with the error attached when the connection is
 *  suspect. */
export async function runTransaction<T>(
  pin: PinnedExecutor,
  fn: (scope: TransactionScope) => Promise<T>,
  modes: TransactionModes = {},
  hooks: TransactionHooks = {},
): Promise<T> {
  const beginSql = renderBeginSql(modes);
  const txId = `tx-${++txSequence}`;
  const emit = (event: SqlEvent): void => {
    hooks.onEvent?.(event);
  };
  let savepointSeq = 0;
  let released = false;
  const releasePin = (err?: unknown): void => {
    if (released) return;
    released = true;
    pin.release(err);
  };

  const makeScope = (): TransactionScope => {
    const scope: TransactionScope = {
      async query<R>(sqlText: string, params: unknown[] = [], options?: QueryExecutionOptions): Promise<R[]> {
        return pin.query<R>(sqlText, params, options);
      },
      execute: (sqlText: string, params?: unknown[], options?: QueryExecutionOptions) => pin.execute(sqlText, params, options),
      transaction: <Tx>(nested: (tx: TransactionScope) => Promise<Tx>): Promise<Tx> => runSavepoint(scope, nested),
      savepoint: (name?: string) => createSavepoint(name),
      async begin<Tx>(nested: (tx: TransactionScope) => Promise<Tx>, options?: TransactionModes): Promise<Tx> {
        if (options !== undefined && hasModes(options)) {
          throw new NeutronSqlError("isolation/read-only/deferrable are properties of the outer BEGIN — a nested transaction is a savepoint and takes no modes");
        }
        return runSavepoint(scope, nested);
      },
      close: () => scope.lifecycle.terminate(),
      lifecycle: {
        ownership: "borrowed",
        terminated: false,
        terminate: () => Promise.resolve(),
      },
    };
    if (typeof pin.prepare === "function") scope.prepare = (sqlText: string) => pin.prepare!(sqlText);
    return scope;
  };

  const nextAutoName = (): string => {
    const name = `neutron_sp_${++savepointSeq}`;
    // defensive: the generated shape always matches; keep the guard for
    // future format changes
    if (!SAVEPOINT_NAME.test(name)) throw new NeutronSqlError(`internal: generated savepoint name ${name} failed validation`);
    return name;
  };

  const createSavepoint = async (name?: string): Promise<Savepoint> => {
    const spName = name === undefined ? nextAutoName() : name;
    if (!SAVEPOINT_NAME.test(spName)) {
      throw new NeutronSqlError(`savepoint name "${spName}" is not a plain identifier (letters, digits, underscore; must not start with a digit)`);
    }
    const createSql = `savepoint "${spName}"`;
    await pin.execute(createSql);
    emit({ kind: "savepoint", statementId: statementIdOf(createSql), txId, savepointName: spName, savepointAction: "create" });
    let state: SavepointState = "active";
    return {
      name: spName,
      async rollbackTo(): Promise<void> {
        if (state === "released") throw new NeutronSqlError(`savepoint "${spName}" was already released`);
        const sql = `rollback to savepoint "${spName}"`;
        await pin.execute(sql);
        emit({ kind: "savepoint", statementId: statementIdOf(sql), txId, savepointName: spName, savepointAction: "rollback-to" });
      },
      async release(): Promise<void> {
        if (state === "released") throw new NeutronSqlError(`savepoint "${spName}" was already released`);
        const sql = `release savepoint "${spName}"`;
        try {
          await pin.execute(sql);
        } finally {
          state = "released";
        }
        emit({ kind: "savepoint", statementId: statementIdOf(sql), txId, savepointName: spName, savepointAction: "release" });
      },
    };
  };

  const runSavepoint = async <Tx>(scope: TransactionScope, nested: (tx: TransactionScope) => Promise<Tx>): Promise<Tx> => {
    const sp = await createSavepoint();
    try {
      const result = await nested(scope);
      await sp.release();
      return result;
    } catch (err) {
      // The savepoint is rolled back and released; the outer transaction
      // stays usable. A rollback failure here means the connection is in
      // trouble — the outer COMMIT/ROLLBACK will surface it.
      try {
        await sp.rollbackTo();
        await sp.release();
      } catch {
        // swallow: the original error is the user's answer
      }
      throw err;
    }
  };

  const started = performance.now();
  emit({
    kind: "tx-begin",
    statementId: statementIdOf(beginSql),
    txId,
    sql: beginSql,
    isolation: modes.isolation,
    readOnly: modes.readOnly === true || undefined,
    deferrable: modes.deferrable === true || undefined,
  });
  try {
    await pin.execute(beginSql);
  } catch (err) {
    emit({ kind: "tx-rollback", statementId: statementIdOf("rollback"), txId, durationMs: performance.now() - started, error: errorSummary(err) });
    releasePin(isFatalConnectionLoss(err) ? err : undefined);
    throw err;
  }

  let result: T;
  try {
    result = await fn(makeScope());
  } catch (err) {
    const rollbackFailure = await rollbackAndRethrow(pin, txId, started, emit, err);
    releasePin(rollbackFailure !== false ? rollbackFailure : isFatalConnectionLoss(err) ? err : undefined);
    throw err;
  }

  try {
    await pin.execute("commit");
  } catch (err) {
    if (commitOutcomeUnknown(err)) {
      const durationMs = performance.now() - started;
      const ambiguity = new CommitAmbiguityError(
        `commit outcome unknown: the connection failed while COMMIT was in flight after ${durationMs.toFixed(1)}ms — the transaction may or may not be durably committed. Inspect the database state (or an idempotency record) before re-submitting; this error is never retried automatically.`,
        { cause: err },
      );
      emit({ kind: "tx-rollback", statementId: statementIdOf("commit"), txId, durationMs, error: errorSummary(ambiguity) });
      releasePin(err);
      throw ambiguity;
    }
    // The server answered the COMMIT with an SQL error: the transaction is
    // rolled back (this includes serialization failures 40001 surfacing at
    // commit time — retryable upstream, never ambiguous).
    emit({ kind: "tx-rollback", statementId: statementIdOf("commit"), txId, durationMs: performance.now() - started, error: errorSummary(err) });
    releasePin();
    throw err;
  }
  emit({ kind: "tx-commit", statementId: statementIdOf("commit"), txId, durationMs: performance.now() - started });
  releasePin();
  return result;
}

/** Attempt ROLLBACK for a failed transaction. Returns false on success, or
 *  the failure marker when the rollback could not run. The original error
 *  always propagates to the caller — never the rollback error.
 *
 *  A FATAL connection loss as the callback's error skips the rollback
 *  entirely: the server discarded the transaction when the session died,
 *  and writing to a dead session is both meaningless and unsafe (postgres.js
 *  crashes on writes to its nulled socket). */
async function rollbackAndRethrow(
  pin: PinnedExecutor,
  txId: string,
  started: number,
  emit: (event: SqlEvent) => void,
  original: unknown,
): Promise<false | unknown> {
  if (isFatalConnectionLoss(original)) {
    emit({ kind: "tx-rollback", statementId: statementIdOf("rollback"), txId, durationMs: performance.now() - started, error: errorSummary(original) });
    return original;
  }
  try {
    await pin.execute("rollback");
    emit({ kind: "tx-rollback", statementId: statementIdOf("rollback"), txId, durationMs: performance.now() - started, error: errorSummary(original) });
    return false;
  } catch (rollbackError) {
    // The rollback itself failed (almost always a dead connection). The
    // ORIGINAL error is the user's answer; the server discards the
    // transaction when the connection goes away.
    emit({ kind: "tx-rollback", statementId: statementIdOf("rollback"), txId, durationMs: performance.now() - started, error: errorSummary(original) });
    return rollbackError;
  }
}

// ---------------------------------------------------------------------------
// Opt-in whole-transaction retry
// ---------------------------------------------------------------------------

export interface TransactionRetryOptions {
  /** Total attempts (>= 1). `maxAttempts: 1` disables retry. */
  maxAttempts: number;
  /** Delay before retry N (1-based). A number or a function of the attempt;
   *  default: exponential 25ms * 2^(n-1), capped at 1s. Deterministic — no
   *  jitter — so tests and reproducibility hold. */
  backoffMs?: number | ((attempt: number) => number);
  /** REQUIRED assertion: the whole transaction (including side effects in
   *  the callback) is idempotent-by-construction, because a retried attempt
   *  re-executes the callback from the top. */
  idempotent: true;
}

/** Validate a retry configuration. */
export function validateRetryOptions(retry: TransactionRetryOptions): void {
  if (retry.idempotent !== true) {
    throw new NeutronSqlError(
      "transaction retry requires idempotent: true — a retried attempt re-executes the whole callback from the top, so you must assert it is idempotent-by-construction (including external side effects)",
    );
  }
  if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
    throw new NeutronSqlError(`transaction retry maxAttempts must be an integer >= 1 (got ${String(retry.maxAttempts)})`);
  }
  if (retry.backoffMs !== undefined) {
    if (typeof retry.backoffMs === "number" && (!Number.isFinite(retry.backoffMs) || retry.backoffMs < 0)) {
      throw new NeutronSqlError("transaction retry backoffMs must be a non-negative number or a function");
    }
    if (typeof retry.backoffMs === "function" && retry.backoffMs.length > 1) {
      throw new NeutronSqlError("transaction retry backoffMs function takes exactly one argument (the 1-based attempt)");
    }
  }
}

function backoffFor(retry: TransactionRetryOptions, attempt: number): number {
  if (typeof retry.backoffMs === "function") {
    const value = retry.backoffMs(attempt);
    if (!Number.isFinite(value) || value < 0) throw new NeutronSqlError(`backoffMs(attempt ${attempt}) returned ${String(value)}; must be a non-negative finite number`);
    return value;
  }
  if (typeof retry.backoffMs === "number") return retry.backoffMs;
  return Math.min(25 * 2 ** (attempt - 1), 1000);
}

/** Run a transaction with the opt-in retry. Retries ONLY definite rollbacks
 *  (SQLSTATE 40001 serialization failure / 40P01 deadlock — including ones
 *  surfacing at COMMIT, which the server answers with an error). A
 *  CommitAmbiguityError (unknown outcome) is NEVER retried, and the final
 *  error after the bound is exhausted propagates unchanged. */
export async function runRetriedTransaction<T>(
  pinFactory: () => Promise<PinnedExecutor>,
  fn: (scope: TransactionScope) => Promise<T>,
  modes: TransactionModes,
  retry: TransactionRetryOptions,
  hooks: TransactionHooks = {},
): Promise<T> {
  validateRetryOptions(retry);
  for (let attempt = 1; ; attempt++) {
    const attemptHooks: TransactionHooks =
      hooks.onEvent === undefined
        ? {}
        : {
            onEvent: (event) => {
              hooks.onEvent!({ ...event, attempt });
            },
          };
    const pin = await pinFactory();
    try {
      return await runTransaction(pin, fn, modes, attemptHooks);
    } catch (err) {
      const canRetry =
        attempt < retry.maxAttempts &&
        !(err instanceof CommitAmbiguityError) &&
        isRetriableTransactionError(err);
      if (!canRetry) throw err;
      await new Promise((resolve) => setTimeout(resolve, backoffFor(retry, attempt)));
    }
  }
}
