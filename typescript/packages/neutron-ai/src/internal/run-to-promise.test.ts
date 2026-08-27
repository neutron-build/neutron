import assert from "node:assert/strict";
import test from "node:test";

import { deferred } from "./deferred.js";
import { abandonmentSettler } from "./run-to-promise.js";

test("the settler rejects pending deferreds with an abandoned problem and runs cleanup", async () => {
  const text = deferred<string>();
  const usage = deferred<number>();
  let cleaned = 0;
  const settle = abandonmentSettler([text, usage], () => {
    cleaned++;
  });

  settle();

  await assert.rejects(
    text.promise,
    (err: unknown) => {
      assert.match(err instanceof Error ? err.message : String(err), /abandoned/);
      return true;
    },
  );
  await assert.rejects(usage.promise, /abandoned/);
  assert.equal(cleaned, 1, "cleanup must release what the abandoned run holds");
});

test("settling is a no-op for deferreds already settled on the normal path", async () => {
  const text = deferred<string>();
  text.resolve("done");
  const settle = abandonmentSettler([text]);

  settle();

  assert.equal(await text.promise, "done", "a settled deferred must not be re-rejected");
});

test("the settler needs no cleanup", () => {
  const text = deferred<string>();
  const settle = abandonmentSettler([text]);
  settle(); // must not throw
});
