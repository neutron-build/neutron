import assert from "node:assert/strict";
import test from "node:test";
import { MemoryCacheClient } from "../cache/index.js";
import { enforceSlidingWindow } from "./index.js";

test("enforceSlidingWindow blocks once request limit is exceeded", async () => {
  const cache = new MemoryCacheClient();
  const options = { limit: 2, windowSec: 60 };

  const first = await enforceSlidingWindow(cache, "user-a", options);
  const second = await enforceSlidingWindow(cache, "user-a", options);
  const third = await enforceSlidingWindow(cache, "user-a", options);

  assert.equal(first.allowed, true);
  assert.equal(second.allowed, true);
  assert.equal(third.allowed, false);
  assert.equal(third.remaining, 0);
});

test("enforceSlidingWindow applies previous-window weight at boundary", async () => {
  const cache = new MemoryCacheClient();
  const options = { limit: 2, windowSec: 10 };
  const originalNow = Date.now;

  let now = 0;
  Date.now = () => now;
  try {
    await enforceSlidingWindow(cache, "user-b", options);

    now = 9_000;
    await enforceSlidingWindow(cache, "user-b", options);

    now = 10_001;
    const boundaryAttempt = await enforceSlidingWindow(cache, "user-b", options);

    assert.equal(boundaryAttempt.allowed, false);
    assert.equal(boundaryAttempt.remaining, 0);
    assert.ok(boundaryAttempt.retryAfterSec >= 1);
  } finally {
    Date.now = originalNow;
  }
});

test("enforceSlidingWindow ignores malformed previous-window values", async () => {
  const cache = new MemoryCacheClient();
  const options = { limit: 3, windowSec: 10 };
  const originalNow = Date.now;

  let now = 20_000;
  Date.now = () => now;
  try {
    const currentWindow = Math.floor(now / (options.windowSec * 1000));
    const previousKey = `rl:user-c:${currentWindow - 1}`;
    await cache.set(previousKey, "not-a-number", 20);

    const result = await enforceSlidingWindow(cache, "user-c", options);
    assert.equal(result.allowed, true);
    assert.equal(result.remaining, 2);
    assert.ok(result.retryAfterSec >= 1);
  } finally {
    Date.now = originalNow;
  }
});

test("enforceSlidingWindow tracks keys independently", async () => {
  const cache = new MemoryCacheClient();
  const options = { limit: 1, windowSec: 60 };

  const firstA = await enforceSlidingWindow(cache, "user-a", options);
  const firstB = await enforceSlidingWindow(cache, "user-b", options);
  const secondA = await enforceSlidingWindow(cache, "user-a", options);

  assert.equal(firstA.allowed, true);
  assert.equal(firstB.allowed, true);
  assert.equal(secondA.allowed, false);
});
