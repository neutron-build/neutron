import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  IN_FAILED_TRANSACTION,
  LOCK_NOT_AVAILABLE,
  RetryExhaustedError,
  SERIALIZATION_FAILURE,
  isLockNotAvailable,
  isSerializationFailure,
  sqlState,
  withRetry,
} from './retry.js';

/** Minimal stand-in for a node-postgres error, which puts SQLSTATE on `.code`. */
class PgErr extends Error {
  code: string;
  constructor(code: string) {
    super(code);
    this.code = code;
  }
}

const noSleep = { baseDelayMs: 1, sleep: async () => {}, random: () => 0 };

describe('sqlState', () => {
  it('reads the SQLSTATE off a driver error', () => {
    assert.equal(sqlState(new PgErr('40001')), '40001');
  });

  it('walks the cause chain', () => {
    // Every layer between driver and application adds context with `cause`;
    // classification that only sees the bare error works nowhere real.
    const deep = new Error('repo', { cause: new Error('service', { cause: new PgErr('40001') }) });
    assert.equal(sqlState(deep), '40001');
  });

  it("ignores Nucleus's own symbolic codes", () => {
    // NucleusError.code is 'QUERY_ERROR' etc. Treating that as a SQLSTATE
    // would misclassify every wrapped error.
    assert.equal(sqlState({ code: 'QUERY_ERROR' }), undefined);
    assert.equal(sqlState({ code: 'CONNECTION_ERROR' }), undefined);
  });

  it('returns undefined when there is no code at all', () => {
    assert.equal(sqlState(new Error('boom')), undefined);
    assert.equal(sqlState(null), undefined);
  });
});

describe('classification', () => {
  const cases: Array<[string, boolean]> = [
    [SERIALIZATION_FAILURE, true],
    [IN_FAILED_TRANSACTION, true],
    // The one that must never be lumped in with conflicts.
    [LOCK_NOT_AVAILABLE, false],
    ['23505', false],
    // The code Nucleus itself wrongly used for a kill, twice.
    ['XX000', false],
  ];
  for (const [code, retryable] of cases) {
    it(`${code} retryable=${retryable}`, () => {
      assert.equal(isSerializationFailure(new PgErr(code)), retryable);
    });
  }

  it('separates a lock timeout from a conflict', () => {
    assert.equal(isLockNotAvailable(new PgErr(LOCK_NOT_AVAILABLE)), true);
    assert.equal(isLockNotAvailable(new PgErr(SERIALIZATION_FAILURE)), false);
  });
});

describe('withRetry', () => {
  it('retries a conflict and then succeeds', async () => {
    let calls = 0;
    const result = await withRetry(async () => {
      calls += 1;
      if (calls <= 2) throw new PgErr(SERIALIZATION_FAILURE);
      return 'done';
    }, noSleep);
    assert.equal(result, 'done');
    assert.equal(calls, 3);
  });

  it('gives up after maxAttempts and keeps the cause', async () => {
    let calls = 0;
    await assert.rejects(
      withRetry(async () => {
        calls += 1;
        throw new PgErr(SERIALIZATION_FAILURE);
      }, { ...noSleep, maxAttempts: 3 }),
      (err: unknown) => err instanceof RetryExhaustedError,
    );
    assert.equal(calls, 3);
  });

  it('attempts a lock timeout exactly once', async () => {
    // 55P03 means the holder is still there. Retrying turns one stuck
    // transaction into a busy loop against a lock that will not move.
    let calls = 0;
    await assert.rejects(
      withRetry(async () => {
        calls += 1;
        throw new PgErr(LOCK_NOT_AVAILABLE);
      }, { ...noSleep, maxAttempts: 5 }),
      (err: unknown) => err instanceof PgErr,
    );
    assert.equal(calls, 1);
  });

  it('propagates a non-retryable error unchanged', async () => {
    await assert.rejects(
      withRetry(async () => {
        throw new PgErr('23505');
      }, noSleep),
      (err: unknown) => (err as PgErr).code === '23505',
    );
  });

  it('does not retry a success', async () => {
    let calls = 0;
    await withRetry(async () => {
      calls += 1;
      return 1;
    }, noSleep);
    assert.equal(calls, 1);
  });
});
