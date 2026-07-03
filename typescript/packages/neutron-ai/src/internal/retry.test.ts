import assert from "node:assert/strict";
import { test } from "node:test";

import { AIError, problemFromStatus } from "../errors.js";
import { isRetryableError, withRetries } from "./retry.js";

function failing(times: number, status: number): { fn: () => Promise<string>; attempts: () => number } {
  let attempts = 0;
  return {
    attempts: () => attempts,
    fn: async () => {
      attempts += 1;
      if (attempts <= times) throw new AIError(problemFromStatus(status, "transient"));
      return "ok";
    },
  };
}

test("retries rate limits and server errors, then succeeds", async () => {
  const { fn, attempts } = failing(2, 429);
  const result = await withRetries(fn, { maxRetries: 2, initialDelayMs: 1 });
  assert.equal(result, "ok");
  assert.equal(attempts(), 3);
});

test("does not retry caller errors", async () => {
  const { fn, attempts } = failing(1, 400);
  await assert.rejects(withRetries(fn, { maxRetries: 2, initialDelayMs: 1 }));
  assert.equal(attempts(), 1);
});

test("throws the last error after exhausting retries", async () => {
  const { fn, attempts } = failing(10, 500);
  await assert.rejects(
    withRetries(fn, { maxRetries: 2, initialDelayMs: 1 }),
    (error: unknown) => error instanceof AIError && error.problem.status === 500,
  );
  assert.equal(attempts(), 3);
});

test("does not retry plain errors", async () => {
  let attempts = 0;
  await assert.rejects(
    withRetries(async () => {
      attempts += 1;
      throw new Error("not an AIError");
    }, { maxRetries: 2, initialDelayMs: 1 }),
  );
  assert.equal(attempts, 1);
});

test("isRetryableError classifies by status", () => {
  assert.equal(isRetryableError(new AIError(problemFromStatus(429, "x"))), true);
  assert.equal(isRetryableError(new AIError(problemFromStatus(500, "x"))), true);
  assert.equal(isRetryableError(new AIError(problemFromStatus(401, "x"))), false);
  assert.equal(isRetryableError(new Error("x")), false);
});
