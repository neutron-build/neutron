import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { AdapterCallOptions, AdapterGenerateResult, ModelAdapter } from "@neutron-build/ai";
import { jsonSchema } from "@neutron-build/ai";

import type { LoadedAgent } from "./agent.js";
import { AgentError, LocalExecutor } from "./executor.js";
import { loadAgent } from "./loader.js";
import { runTurn } from "./runtime.js";

// ---------------------------------------------------------------------------
// LocalExecutor
// ---------------------------------------------------------------------------

async function makeExecutor(): Promise<LocalExecutor> {
  return new LocalExecutor({ root: await mkdtemp(join(tmpdir(), "agent-exec-")) });
}

test("exec runs commands and reports exit codes", async () => {
  const executor = await makeExecutor();
  const ok = await executor.exec("echo hello");
  assert.equal(ok.exitCode, 0);
  assert.equal(ok.stdout.trim(), "hello");
  assert.equal(ok.timedOut, false);

  const fail = await executor.exec("echo oops >&2; exit 3");
  assert.equal(fail.exitCode, 3);
  assert.equal(fail.stderr.trim(), "oops");
});

test("exec respects cwd inside the root and kills on timeout", async () => {
  const executor = await makeExecutor();
  await executor.putFile("sub/marker.txt", "here");
  const listed = await executor.exec("ls", { cwd: "sub" });
  assert.match(listed.stdout, /marker\.txt/);

  const slow = await executor.exec("sleep 5", { timeoutMs: 100 });
  assert.equal(slow.timedOut, true);
  assert.notEqual(slow.exitCode, 0);
});

test("exec truncates runaway output", async () => {
  const executor = await makeExecutor();
  const result = await executor.exec("yes x | head -c 100000", { maxOutputBytes: 1000 });
  assert.equal(result.truncated, true);
  assert.equal(result.stdout.length, 1000);
});

test("files round-trip and paths cannot escape the root", async () => {
  const executor = await makeExecutor();
  await executor.putFile("notes/a.txt", "alpha");
  assert.equal(new TextDecoder().decode(await executor.getFile("notes/a.txt")), "alpha");

  await assert.rejects(executor.getFile("missing.txt"), (e: unknown) => e instanceof AgentError && (e as AgentError).problem.status === 404);
  await assert.rejects(executor.putFile("../outside.txt", "x"), (e: unknown) => e instanceof AgentError && (e as AgentError).problem.status === 400);
  await assert.rejects(executor.exec("true", { cwd: "../.." }), AgentError);

  await executor.destroy();
  await assert.rejects(executor.exec("true"), /destroyed/);
});

// ---------------------------------------------------------------------------
// Convention loader
// ---------------------------------------------------------------------------

const FAKE_MODEL_SRC = `{ provider: "fixture", modelId: "fixture-1", async doGenerate() { throw new Error("unused"); }, async *doStream() { throw new Error("unused"); } }`;

test("loadAgent assembles agent.js, instructions.md, and tools/", async () => {
  const dir = await mkdtemp(join(tmpdir(), "agent-fixture-"));
  await writeFile(
    join(dir, "agent.mjs"),
    `export default { name: "fixture", model: ${FAKE_MODEL_SRC}, maxSteps: 3, tools: [
       { name: "inline", inputSchema: { jsonSchema: { type: "object" }, validate: (v) => ({ success: true, value: v }) } },
       { name: "shadowed", inputSchema: { jsonSchema: { type: "object" }, validate: (v) => ({ success: true, value: v }) } },
     ] };`,
  );
  await writeFile(join(dir, "instructions.md"), "Be terse.\n");
  await mkdir(join(dir, "tools"));
  await writeFile(
    join(dir, "tools", "shadowed.mjs"),
    `export default { name: "shadowed", description: "file wins", inputSchema: { jsonSchema: { type: "object" }, validate: (v) => ({ success: true, value: v }) } };`,
  );

  const agent = await loadAgent(dir);
  assert.equal(agent.definition.name, "fixture");
  assert.equal(agent.instructions, "Be terse.");
  assert.deepEqual(agent.tools.map((t) => t.name).sort(), ["inline", "shadowed"]);
  assert.equal(agent.tools.find((t) => t.name === "shadowed")?.description, "file wins");
});

test("loadAgent fails loudly without agent.js", async () => {
  const dir = await mkdtemp(join(tmpdir(), "agent-empty-"));
  await assert.rejects(loadAgent(dir), (e: unknown) => e instanceof AgentError && (e as AgentError).problem.status === 404);
});

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

function scriptedModel(script: AdapterGenerateResult[]): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
  let index = 0;
  return {
    calls,
    model: {
      provider: "scripted",
      modelId: "scripted-1",
      async doGenerate(options) {
        calls.push(structuredClone(options));
        if (index >= script.length) throw new Error("script exhausted");
        return script[index++]!;
      },
      async *doStream() {
        throw new Error("not used");
      },
    },
  };
}

const usage = { inputTokens: 5, outputTokens: 3, totalTokens: 8 };

function inlineAgent(model: ModelAdapter, instructions = "You are the fixture agent."): LoadedAgent {
  return {
    definition: { name: "fixture", model, maxSteps: 4 },
    instructions,
    tools: [],
  };
}

test("runTurn wires instructions, the exec tool, and a real command end-to-end", async () => {
  const { model, calls } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "exec", input: { command: "echo from-agent" } }],
      finishReason: "tool-calls",
      usage,
      raw: null,
    },
    { content: [{ type: "text", text: "The command printed from-agent." }], finishReason: "stop", usage, raw: null },
  ]);

  const result = await runTurn(inlineAgent(model), {
    input: "run echo",
    executor: await makeExecutor(),
  });

  assert.equal(result.text, "The command printed from-agent.");
  // the model saw the system prompt and the exec tool
  assert.equal(calls[0]?.messages[0]?.role, "system");
  assert.equal(calls[0]?.tools?.[0]?.name, "exec");
  // and the second call carried the real execution result back
  const toolMessage = calls[1]?.messages.find((m) => m.role === "tool") as
    | { content: Array<{ output: { exitCode: number; stdout: string } }> }
    | undefined;
  assert.equal(toolMessage?.content[0]?.output.exitCode, 0);
  assert.match(toolMessage?.content[0]?.output.stdout ?? "", /from-agent/);
});

test("approval suspension passes through with resume-ready messages", async () => {
  const { model } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} }],
      finishReason: "tool-calls",
      usage,
      raw: null,
    },
    { content: [{ type: "text", text: "done" }], finishReason: "stop", usage, raw: null },
  ]);
  const agent: LoadedAgent = {
    definition: { name: "fixture", model, maxSteps: 4 },
    instructions: "",
    tools: [
      {
        name: "danger",
        inputSchema: jsonSchema({ type: "object" }),
        execute: async () => "did it",
        needsApproval: true,
      },
    ],
  };

  const suspended = await runTurn(agent, { input: "do the thing" });
  assert.equal(suspended.finishReason, "tool-approval");
  assert.equal(suspended.approvalRequests[0]?.toolName, "danger");

  const resumed = await runTurn(agent, {
    messages: suspended.messages,
    toolApprovals: [{ toolCallId: "c1", approved: true }],
  });
  assert.equal(resumed.finishReason, "stop");
  assert.equal(resumed.text, "done");
});

test("LocalExecutor envDenylist strips secrets from exec env; explicit env still wins", async () => {
  process.env.TEST_SBX_SECRET = "leaky";
  try {
    const scrubbed = new LocalExecutor({
      root: await mkdtemp(join(tmpdir(), "exec-deny-")),
      envDenylist: ["TEST_SBX_SECRET"],
    });
    const result = await scrubbed.exec('echo "got:${TEST_SBX_SECRET:-none}"');
    assert.equal(result.stdout.trim(), "got:none", "denylisted var must not reach the child");

    const withOverride = new LocalExecutor({
      root: await mkdtemp(join(tmpdir(), "exec-deny2-")),
      envDenylist: ["TEST_SBX_SECRET"],
      env: { TEST_SBX_SECRET: "scoped-replacement" },
    });
    const overridden = await withOverride.exec('echo "got:${TEST_SBX_SECRET:-none}"');
    assert.equal(overridden.stdout.trim(), "got:scoped-replacement", "explicit env outranks the strip");
  } finally {
    delete process.env.TEST_SBX_SECRET;
  }
});
