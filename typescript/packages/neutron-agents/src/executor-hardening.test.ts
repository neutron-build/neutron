import assert from "node:assert/strict";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { AgentError, LocalExecutor } from "./executor.js";
import { SandboxExecutor } from "./sandbox.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test("LocalExecutor does not inherit operator secrets, including unlisted names", async () => {
  const root = await mkdtemp(join(tmpdir(), "neutron-exec-default-"));
  const names = ["ANTHROPIC_API_KEY", "MY_APP_DB_PASSWORD"];
  const saved = names.map((name) => process.env[name]);
  for (const name of names) process.env[name] = "operator-secret-value";
  try {
    const executor = new LocalExecutor({ root });
    const result = await executor.exec("env");
    assert.equal(result.stdout.includes("operator-secret-value"), false);
    const path = await executor.exec('printf %s "$PATH"');
    assert.equal(path.stdout, process.env.PATH ?? "", "PATH stays inherited");
  } finally {
    names.forEach((name, i) => {
      const value = saved[i];
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    });
    await rm(root, { recursive: true, force: true });
  }
});

test("LocalExecutor inheritEnv adds caller-listed names to the allowlist", async () => {
  const root = await mkdtemp(join(tmpdir(), "neutron-exec-inherit-"));
  const saved = process.env.TEST_EXEC_EXTRA;
  process.env.TEST_EXEC_EXTRA = "passed-through";
  try {
    const executor = new LocalExecutor({ root, inheritEnv: ["TEST_EXEC_EXTRA"] });
    const result = await executor.exec("printenv TEST_EXEC_EXTRA");
    assert.equal(result.stdout.trim(), "passed-through");
  } finally {
    if (saved === undefined) delete process.env.TEST_EXEC_EXTRA;
    else process.env.TEST_EXEC_EXTRA = saved;
    await rm(root, { recursive: true, force: true });
  }
});

test("LocalExecutor timeout kills the process group, not just the shell", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "neutron-exec-group-"));
  const marker = join(root, "late");
  try {
    const executor = new LocalExecutor({ root });
    // A background grandchild that outlives the shell; the shell blocks so
    // the timeout is what kills it. Killing only the shell pid orphans the
    // grandchild, which then writes the marker.
    const grandchild = `setTimeout(()=>require("fs").writeFileSync(${JSON.stringify(marker)},"x"),700)`;
    const result = await executor.exec(
      `'${process.execPath}' -e '${grandchild}' & sleep 30`,
      { timeoutMs: 200 },
    );
    assert.equal(result.timedOut, true);

    await sleep(1500);
    await assert.rejects(stat(marker), /ENOENT/, "grandchild must be killed with the group");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("SandboxExecutor.exec rejects when the daemon hangs (request timeout)", { timeout: 10_000 }, async () => {
  const never: typeof globalThis.fetch = (() =>
    new Promise<Response>(() => {})) as typeof globalThis.fetch;
  const sandbox = SandboxExecutor.attach("run-1", {
    baseURL: "http://127.0.0.1:9",
    token: "t",
    fetch: never,
    requestTimeoutMs: 50,
  });

  const rejection = assert.rejects(
    sandbox.exec("true"),
    (error: unknown) =>
      error instanceof AgentError && /timed out|abort/i.test(error.message),
  );
  const guard = sleep(3000).then(() => {
    throw new Error("sandbox exec hung: no request timeout");
  });
  await Promise.race([rejection, guard]);
});
