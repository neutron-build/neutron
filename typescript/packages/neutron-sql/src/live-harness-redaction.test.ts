import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

test("required-live failures do not expose connection credentials or URL parameters", () => {
  const url = "postgresql://private-user:private-password@127.0.0.1:1/private-db?token=private-token";
  const harness = new URL("./live-harness.js", import.meta.url).href;
  for (const legacy of [false, true]) {
  const result = spawnSync(process.execPath, ["--input-type=module", "-e",
    `const { liveGate } = await import(${JSON.stringify(harness)}); console.log(JSON.stringify(await liveGate()));`], {
    env: { ...process.env, NEUTRON_LIVE_REQUIRED: "1", NEUTRON_TEST_DATABASE_URL: legacy ? "" : url, NEUTRON_SQL_TEST_URL: legacy ? url + "%ZZ" : "" },
    encoding: "utf8",
    timeout: 10_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 1);
  assert.equal(JSON.parse(result.stdout).ok, false);
  assert.match(result.stderr, /zero live cases executed/);
  const diagnostics = result.stdout + result.stderr;
  for (const secret of [url, "private-user", "private-password", "private-db", "private-token"]) {
    assert.equal(diagnostics.includes(secret), false);
  }
  }
});
