import assert from "node:assert/strict";
import { test } from "node:test";

import type { AgentEvent } from "./index.js";
import { claudeCode, type SpawnedProcess, type SpawnFn } from "./claude-code.js";

function fakeSpawn(
  lines: unknown[],
  options: { exitCode?: number; stderr?: string } = {},
): { spawn: SpawnFn; calls: Array<{ command: string; args: string[]; cwd?: string }>; killed: () => boolean } {
  const calls: Array<{ command: string; args: string[]; cwd?: string }> = [];
  let killed = false;
  const spawn: SpawnFn = (command, args, spawnOptions) => {
    const call: { command: string; args: string[]; cwd?: string } = { command, args };
    if (spawnOptions.cwd !== undefined) call.cwd = spawnOptions.cwd;
    calls.push(call);
    const process: SpawnedProcess = {
      stdout: (async function* () {
        for (const line of lines) yield `${JSON.stringify(line)}\n`;
      })(),
      stderr: (async function* () {
        if (options.stderr !== undefined) yield options.stderr;
      })(),
      kill() {
        killed = true;
      },
      exited: Promise.resolve(options.exitCode ?? 0),
    };
    return process;
  };
  return { spawn, calls, killed: () => killed };
}

const STREAM_JSON_SESSION = [
  { type: "system", subtype: "init", session_id: "sess-1", model: "claude-sonnet-5" },
  {
    type: "assistant",
    session_id: "sess-1",
    message: {
      content: [
        { type: "text", text: "Reading the file. " },
        { type: "tool_use", id: "toolu_1", name: "Read", input: { file_path: "/tmp/a" } },
      ],
    },
  },
  {
    type: "user",
    session_id: "sess-1",
    message: { content: [{ type: "tool_result", tool_use_id: "toolu_1", content: "file contents" }] },
  },
  {
    type: "assistant",
    session_id: "sess-1",
    message: { content: [{ type: "text", text: "Done." }] },
  },
  {
    type: "result",
    subtype: "success",
    session_id: "sess-1",
    result: "Reading the file. Done.",
    total_cost_usd: 0.042,
    usage: { input_tokens: 100, output_tokens: 40 },
    num_turns: 2,
  },
];

test("translates stream-json events and builds the result", async () => {
  const { spawn, calls } = fakeSpawn(STREAM_JSON_SESSION);
  const harness = claudeCode({ spawn, permissionMode: "acceptEdits", allowedTools: ["Read", "Bash"] });
  const run = harness.run({ prompt: "read the file", model: "claude-sonnet-5", cwd: "/tmp/project" });

  const events: AgentEvent[] = [];
  for await (const event of run.events) events.push(event);

  assert.deepEqual(events, [
    { type: "session", sessionId: "sess-1" },
    { type: "text-delta", text: "Reading the file. " },
    { type: "tool-start", toolCallId: "toolu_1", toolName: "Read", input: { file_path: "/tmp/a" } },
    { type: "tool-end", toolCallId: "toolu_1", toolName: "Read", output: "file contents" },
    { type: "text-delta", text: "Done." },
    { type: "finish", status: "completed" },
  ]);

  const result = await run.result;
  assert.equal(result.status, "completed");
  assert.equal(result.output, "Reading the file. Done.");
  assert.equal(result.sessionId, "sess-1");
  assert.equal(result.costUSD, 0.042);
  assert.deepEqual(result.usage, { inputTokens: 100, outputTokens: 40, totalTokens: 140 });

  const call = calls[0]!;
  assert.equal(call.command, "claude");
  assert.equal(call.cwd, "/tmp/project");
  assert.deepEqual(call.args, [
    "-p",
    "read the file",
    "--output-format",
    "stream-json",
    "--verbose",
    "--model",
    "claude-sonnet-5",
    "--permission-mode",
    "acceptEdits",
    "--allowed-tools",
    "Read,Bash",
  ]);
});

test("sessionId maps to --resume", async () => {
  const { spawn, calls } = fakeSpawn(STREAM_JSON_SESSION);
  const harness = claudeCode({ spawn });
  await harness.run({ prompt: "continue", sessionId: "sess-1" }).result;
  assert.ok(calls[0]?.args.includes("--resume"));
  assert.ok(calls[0]?.args.includes("sess-1"));
});

test("a nonzero exit without a result event becomes an error with stderr detail", async () => {
  const { spawn } = fakeSpawn([{ type: "system", subtype: "init", session_id: "sess-2" }], {
    exitCode: 1,
    stderr: "invalid API key",
  });
  const harness = claudeCode({ spawn });
  const result = await harness.run({ prompt: "go" }).result;
  assert.equal(result.status, "error");
  assert.match(result.error?.detail ?? "", /exited with code 1/);
  assert.match(result.error?.detail ?? "", /invalid API key/);
});

test("an error-subtype result event becomes an error status", async () => {
  const { spawn } = fakeSpawn([
    { type: "system", subtype: "init", session_id: "sess-3" },
    { type: "result", subtype: "error_max_turns", session_id: "sess-3", result: "max turns reached" },
  ]);
  const harness = claudeCode({ spawn });
  const result = await harness.run({ prompt: "go" }).result;
  assert.equal(result.status, "error");
  assert.equal(result.error?.detail, "max turns reached");
});

test("stop() kills the process and reports cancelled", async () => {
  let releaseStdout: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    releaseStdout = resolve;
  });
  const calls: string[][] = [];
  let killed = false;
  const spawn: SpawnFn = (_command, args) => {
    calls.push(args);
    return {
      stdout: (async function* () {
        yield `${JSON.stringify({ type: "system", subtype: "init", session_id: "sess-4" })}\n`;
        await gate;
      })(),
      kill() {
        killed = true;
        releaseStdout?.();
      },
      exited: gate.then(() => 143),
    };
  };
  const harness = claudeCode({ spawn });
  const run = harness.run({ prompt: "long task" });
  const resultPromise = run.result;
  setTimeout(() => run.stop(), 10);
  const result = await resultPromise;
  assert.equal(killed, true);
  assert.equal(result.status, "cancelled");
});

test("a missing prompt throws", () => {
  const { spawn } = fakeSpawn([]);
  const harness = claudeCode({ spawn });
  assert.throws(() => harness.run({}));
});
