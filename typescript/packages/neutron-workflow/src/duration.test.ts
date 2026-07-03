import assert from "node:assert/strict";
import { test } from "node:test";

import { WorkflowError } from "./errors.js";
import { parseDuration } from "./duration.js";

test("parses unit durations", () => {
  assert.equal(parseDuration("500ms"), 500);
  assert.equal(parseDuration("30s"), 30_000);
  assert.equal(parseDuration("15m"), 900_000);
  assert.equal(parseDuration("2h"), 7_200_000);
  assert.equal(parseDuration("7d"), 604_800_000);
  assert.equal(parseDuration("1w"), 604_800_000);
  assert.equal(parseDuration("1.5h"), 5_400_000);
});

test("passes through millisecond numbers", () => {
  assert.equal(parseDuration(1234), 1234);
  assert.equal(parseDuration(0), 0);
});

test("rejects invalid durations", () => {
  for (const bad of ["", "7", "h", "7 days", "-5s", Number.NaN, -1, Infinity]) {
    assert.throws(() => parseDuration(bad as string | number), WorkflowError);
  }
});
