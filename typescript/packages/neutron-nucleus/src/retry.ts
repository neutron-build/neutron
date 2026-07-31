// ---------------------------------------------------------------------------
// Nucleus client — serialization-failure retry
// ---------------------------------------------------------------------------
//
// A SERIALIZABLE transaction can lose a conflict and must then be re-run from
// the beginning. PostgreSQL drivers surface the SQLSTATE and stop there —
// deciding to retry is the application's job, and a framework SDK is that
// layer. Without this, a serializable transaction fails at random under
// concurrency and the caller has no idea it was supposed to try again.

/**
 * The transaction lost a conflict and MUST be retried from the start.
 *
 * Nucleus raises this from two mechanisms: strict 2PL wait-die on the disk
 * engine (the younger transaction is killed to break a potential deadlock) and
 * SSI on the MVCC engine (a dangerous structure detected at commit).
 */
export const SERIALIZATION_FAILURE = '40001';

/**
 * `lock_timeout` elapsed waiting for a table lock.
 *
 * Deliberately **not** retryable: the holder is still there, so retrying spins
 * against a lock that is not moving. Raise `lock_timeout`, or find the
 * transaction holding it.
 */
export const LOCK_NOT_AVAILABLE = '55P03';

/**
 * A statement was issued after the transaction had already been aborted. Only
 * ROLLBACK is accepted, so the whole transaction has to be re-run — which is
 * what {@link withRetry} does.
 */
export const IN_FAILED_TRANSACTION = '25P02';

/**
 * Pull a SQLSTATE out of whatever the driver threw.
 *
 * Walks the `cause` chain, because every layer between the driver and the
 * application adds context — classification that only works on the bare driver
 * error works nowhere real.
 */
export function sqlState(err: unknown): string | undefined {
  let current: unknown = err;
  for (let depth = 0; current && depth < 10; depth += 1) {
    const candidate = current as { code?: unknown; sqlState?: unknown; cause?: unknown };
    // node-postgres puts the SQLSTATE on `.code`; some wrappers use `.sqlState`.
    for (const value of [candidate.code, candidate.sqlState]) {
      // Guard against Nucleus's own symbolic codes ('QUERY_ERROR'): a SQLSTATE
      // is always five characters of [0-9A-Z].
      if (typeof value === 'string' && /^[0-9A-Z]{5}$/.test(value)) {
        return value;
      }
    }
    current = candidate.cause;
  }
  return undefined;
}

/**
 * Whether `err` is a conflict the caller should retry.
 *
 * Classification is by SQLSTATE, never by message text: the code is the
 * contract, the message is free-form. Nucleus itself shipped that bug twice —
 * a 2PL kill reported as XX000, then its follow-up error reported as XX000 —
 * so the client half is checked explicitly.
 */
export function isSerializationFailure(err: unknown): boolean {
  const code = sqlState(err);
  return code === SERIALIZATION_FAILURE || code === IN_FAILED_TRANSACTION;
}

/**
 * Whether `err` is a `lock_timeout` expiry (55P03).
 *
 * Kept distinct from {@link isSerializationFailure} because the two call for
 * opposite responses: a conflict means "someone else won, try again"; a lock
 * timeout means "the lock is still held, retrying will not help".
 */
export function isLockNotAvailable(err: unknown): boolean {
  return sqlState(err) === LOCK_NOT_AVAILABLE;
}

export interface RetryOptions {
  /** Attempts including the first. Values below 1 are treated as 1. */
  maxAttempts?: number;
  /** Delay before the second attempt, in ms; doubled each time after. */
  baseDelayMs?: number;
  /** Upper bound on the backoff, in ms. */
  maxDelayMs?: number;
  /** Sleep function; injectable so tests do not actually wait. */
  sleep?: (ms: number) => Promise<void>;
  /** Randomness source for jitter; injectable for determinism in tests. */
  random?: () => number;
}

/** Thrown when a transaction kept losing conflicts. `cause` is the last one. */
export class RetryExhaustedError extends Error {
  readonly attempts: number;

  constructor(attempts: number, cause?: unknown) {
    super(`transaction did not succeed in ${attempts} attempt(s)`, { cause });
    this.name = 'RetryExhaustedError';
    this.attempts = attempts;
  }
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Run `fn`, retrying it while it fails with a serialization failure.
 *
 * `fn` **must be idempotent with respect to anything outside the database**: it
 * can run more than once. Database work is rolled back between attempts;
 * anything else it does (sending mail, charging a card, mutating module state)
 * is not.
 *
 * Backoff is randomised (full jitter). Without it, two conflicting
 * transactions retry in lockstep and collide again on the same schedule — and
 * under wait-die the younger one loses every round, so a fixed backoff can
 * starve it indefinitely.
 *
 * Pair it with `sql.transaction`, which owns the BEGIN/COMMIT boundary:
 *
 * ```ts
 * await withRetry(() =>
 *   client.sql.transaction(
 *     async (tx) => {
 *       await tx.execute('UPDATE accounts SET balance = balance - 10 WHERE id = $1', [id]);
 *     },
 *     { isolationLevel: 'serializable' },
 *   ),
 * );
 * ```
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const attempts = Math.max(1, options.maxAttempts ?? 5);
  const baseDelay = options.baseDelayMs && options.baseDelayMs > 0 ? options.baseDelayMs : 2;
  const maxDelay = Math.max(options.maxDelayMs ?? 250, baseDelay);
  const sleep = options.sleep ?? defaultSleep;
  const random = options.random ?? Math.random;

  let delay = baseDelay;
  let last: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (err) {
      if (!isSerializationFailure(err)) throw err;
      last = err;
      if (attempt === attempts) break;
      await sleep(random() * delay);
      delay = Math.min(delay * 2, maxDelay);
    }
  }
  throw new RetryExhaustedError(attempts, last);
}
