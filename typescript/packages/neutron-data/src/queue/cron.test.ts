import assert from "node:assert/strict";
import test from "node:test";
import { parseCron } from "./cron.js";

function nextTimes(pattern: string, from: Date, count: number): Date[] {
  const cron = parseCron(pattern);
  const out: Date[] = [];
  let cursor = from;
  for (let i = 0; i < count; i += 1) {
    cursor = cron.next(cursor);
    out.push(cursor);
  }
  return out;
}

function withTimeZone<T>(tz: string, fn: () => T): T {
  const previous = process.env.TZ;
  process.env.TZ = tz;
  try {
    return fn();
  } finally {
    if (previous === undefined) {
      delete process.env.TZ;
    } else {
      process.env.TZ = previous;
    }
  }
}

test("five-field patterns fire on the minute, strictly after `from`", () => {
  const from = new Date(2026, 0, 5, 10, 7, 30, 250);
  assert.deepEqual(nextTimes("*/5 * * * *", from, 3), [
    new Date(2026, 0, 5, 10, 10, 0),
    new Date(2026, 0, 5, 10, 15, 0),
    new Date(2026, 0, 5, 10, 20, 0),
  ]);
  const onTheMark = new Date(2026, 0, 5, 3, 0, 0);
  assert.deepEqual(parseCron("0 3 * * *").next(onTheMark), new Date(2026, 0, 6, 3, 0, 0));
});

test("six-field patterns add a leading seconds field", () => {
  const from = new Date(2026, 0, 5, 10, 7, 59);
  assert.deepEqual(nextTimes("*/2 * * * * *", from, 2), [
    new Date(2026, 0, 5, 10, 8, 0),
    new Date(2026, 0, 5, 10, 8, 2),
  ]);
});

test("lists, ranges, steps, names and aliases", () => {
  const from = new Date(2026, 0, 1, 0, 0, 0); // Thursday
  assert.deepEqual(parseCron("15,45 9-17 * * MON-FRI").next(from), new Date(2026, 0, 1, 9, 15));
  assert.deepEqual(parseCron("0 0 * * sun,sat").next(from), new Date(2026, 0, 3, 0, 0));
  assert.deepEqual(parseCron("0 0 * * 7").next(from), new Date(2026, 0, 4, 0, 0));
  assert.deepEqual(parseCron("0 12 1 JUN *").next(from), new Date(2026, 5, 1, 12, 0));
  assert.deepEqual(parseCron("10/20 * * * *").next(from), new Date(2026, 0, 1, 0, 10));
  assert.deepEqual(parseCron("@weekly").next(from), new Date(2026, 0, 4, 0, 0));
  assert.deepEqual(parseCron("0 0 29 2 *").next(from), new Date(2028, 1, 29, 0, 0));
});

test("restricted day-of-month and day-of-week match either (Vixie semantics)", () => {
  const from = new Date(2026, 0, 1, 0, 0, 0); // Thursday
  // The 13th, or any Friday: Friday Jan 2 comes first.
  assert.deepEqual(parseCron("0 0 13 * 5").next(from), new Date(2026, 0, 2, 0, 0));
  // Day-of-week wildcard: only the day of month constrains.
  assert.deepEqual(parseCron("0 0 13 * *").next(from), new Date(2026, 0, 13, 0, 0));
});

test("invalid and unsupported patterns are rejected", () => {
  for (const pattern of [
    "not-a-cron",
    "* * *",
    "* * * * * * *",
    "60 * * * *",
    "0 0 32 * *",
    "*/0 * * * *",
    "5-1 * * * *",
    "0 0 L * *",
    "0 0 * * 1#2",
  ]) {
    assert.throws(() => parseCron(pattern), /Invalid cron pattern/, pattern);
  }
  assert.throws(() => parseCron("0 0 30 2 *").next(new Date()), /never matches/);
});

test("DST fall-back: fixed-hour patterns fire once, every-hour patterns every hour", () => {
  withTimeZone("America/New_York", () => {
    // 2026-11-01 01:00-01:59 happens twice (EDT then EST).
    const from = new Date("2026-11-01T04:30:00Z"); // 00:30 EDT
    assert.deepEqual(
      nextTimes("30 1 * * *", from, 2).map((d) => d.toISOString()),
      ["2026-11-01T05:30:00.000Z", "2026-11-02T06:30:00.000Z"]
    );
    assert.deepEqual(
      nextTimes("0 * * * *", from, 3).map((d) => d.toISOString()),
      ["2026-11-01T05:00:00.000Z", "2026-11-01T06:00:00.000Z", "2026-11-01T07:00:00.000Z"]
    );
  });
});

test("DST spring-forward: a skipped time fires as the clock jumps", () => {
  withTimeZone("America/New_York", () => {
    // 2026-03-08 02:00 EST jumps to 03:00 EDT.
    const from = new Date("2026-03-08T06:59:30Z"); // 01:59:30 EST
    assert.deepEqual(
      nextTimes("30 2 * * *", from, 2).map((d) => d.toISOString()),
      ["2026-03-08T07:30:00.000Z", "2026-03-09T06:30:00.000Z"]
    );
  });
});
