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

test("LocalExecutor strips well-known operator secrets from the inherited env by default", async () => {
  const root = await mkdtemp(join(tmpdir(), "neutron-exec-default-"));
  try {
    const saved = process.env.ANTHROPIC_API_KEY;
    process.env.ANTHROPIC_API_KEY = "operator-secret-value";
    try {
      const executor = new LocalExecutor({ root });
      const result = await executor.exec("printenv ANTHROPIC_API_KEY");
      assert.equal(result.stdout.trim(), "");
      assert.notEqual(result.exitCode, 0);
    } finally {
      if (saved === undefined) delete process.env.ANTHROPIC_API_KEY;
      else process.env.ANTHROPIC_API_KEY = saved;
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("LocalExecutor envDenylist: [] restores full inheritance (explicit opt-out)", async () => {
  const root = await mkdtemp(join(tmpdir(), "neutron-exec-optout-"));
  try {
    const saved = process.env.ANTHROPIC_API_KEY;
    process.env.ANTHROPIC_API_KEY = "operator-secret-value";
    try {
      const executor = new LocalExecutor({ root, envDenylist: [] });
      const result = await executor.exec("printenv ANTHROPIC_API_KEY");
      assert.equal(result.stdout.trim(), "operator-secret-value");
    } finally {
      if (saved === undefined) delete process.env.ANTHROPIC_API_KEY;
      else process.env.ANTHROPIC_API_KEY = saved;
    }
  } finally {
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
