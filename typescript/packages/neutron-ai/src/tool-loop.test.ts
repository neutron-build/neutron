import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";

import type { AdapterCallOptions, AdapterGenerateResult, ModelAdapter } from "./adapter.js";
import { AIError } from "./errors.js";
import { generateText } from "./generate-text.js";
import { jsonSchema } from "./schema.js";
import { tool } from "./tool.js";
import type { ToolApprovalRequest, ToolMessage } from "./types.js";

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
        throw new Error("doStream not scripted");
      },
    },
  };
}

const usage = (i: number, o: number) => ({ inputTokens: i, outputTokens: o, totalTokens: i + o });

function toolCallStep(calls: Array<{ id: string; name: string; input: unknown }>): AdapterGenerateResult {
  return {
    content: calls.map((call) => ({
      type: "tool-call" as const,
      toolCallId: call.id,
      toolName: call.name,
      input: call.input,
    })),
    finishReason: "tool-calls",
    usage: usage(10, 5),
    raw: null,
  };
}

function textStep(text: string): AdapterGenerateResult {
  return { content: [{ type: "text", text }], finishReason: "stop", usage: usage(20, 8), raw: null };
}

const echo = tool({
  name: "echo",
  description: "Echo the input",
  inputSchema: z.object({ value: z.string() }),
  execute: async ({ value }) => ({ echoed: value }),
});

const danger = tool({
  name: "danger",
  inputSchema: z.object({ target: z.string() }),
  execute: async ({ target }) => `destroyed ${target}`,
  needsApproval: true,
});

test("executes tools and feeds results back across steps", async () => {
  const { model, calls } = scriptedModel([
    toolCallStep([{ id: "c1", name: "echo", input: { value: "hi" } }]),
    textStep("done"),
  ]);
  const stepFinishes: string[] = [];
  const result = await generateText({
    model,
    prompt: "go",
    tools: [echo],
    maxSteps: 3,
    onStepFinish: (step) => {
      stepFinishes.push(step.finishReason);
    },
  });

  assert.equal(result.text, "done");
  assert.equal(result.finishReason, "stop");
  assert.equal(result.steps.length, 2);
  assert.deepEqual(result.steps[0]?.toolResults, [
    { type: "tool-result", toolCallId: "c1", toolName: "echo", output: { echoed: "hi" } },
  ]);
  assert.deepEqual(result.usage, usage(30, 13));
  assert.deepEqual(stepFinishes, ["tool-calls", "stop"]);

  assert.deepEqual(calls[1]?.messages, [
    { role: "user", content: "go" },
    {
      role: "assistant",
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "echo", input: { value: "hi" } }],
    },
    {
      role: "tool",
      content: [{ type: "tool-result", toolCallId: "c1", toolName: "echo", output: { echoed: "hi" } }],
    },
  ]);
  assert.deepEqual(calls[1]?.tools?.[0], {
    name: "echo",
    description: "Echo the input",
    inputSchema: {
      type: "object",
      properties: { value: { type: "string" } },
      additionalProperties: false,
      required: ["value"],
    },
  });
  assert.equal(result.messages.length, 4);
});

test("maxSteps 1 (default) executes the step's tools but does not continue", async () => {
  const { model, calls } = scriptedModel([toolCallStep([{ id: "c1", name: "echo", input: { value: "hi" } }])]);
  const result = await generateText({ model, prompt: "go", tools: [echo] });
  assert.equal(calls.length, 1);
  assert.equal(result.finishReason, "tool-calls");
  assert.deepEqual(result.toolResults[0]?.output, { echoed: "hi" });
  assert.equal(result.messages.length, 3);
});

test("a called tool without execute stops the loop with unexecuted calls", async () => {
  const clientTool = { name: "ui", inputSchema: jsonSchema({ type: "object" }) };
  const { model, calls } = scriptedModel([toolCallStep([{ id: "c1", name: "ui", input: {} }])]);
  const result = await generateText({ model, prompt: "go", tools: [clientTool], maxSteps: 3 });
  assert.equal(calls.length, 1);
  assert.equal(result.finishReason, "tool-calls");
  assert.deepEqual(result.toolResults, []);
  assert.equal(result.toolCalls.length, 1);
});

test("invalid tool input becomes an error result the model can correct", async () => {
  const { model, calls } = scriptedModel([
    toolCallStep([{ id: "c1", name: "echo", input: { value: 42 } }]),
    textStep("recovered"),
  ]);
  const result = await generateText({ model, prompt: "go", tools: [echo], maxSteps: 2 });
  const toolMessage = calls[1]?.messages[2] as ToolMessage;
  assert.equal(toolMessage.role, "tool");
  assert.equal(toolMessage.content[0]?.isError, true);
  assert.match(String(toolMessage.content[0]?.output), /^Invalid tool input: /);
  assert.equal(result.text, "recovered");
});

test("a throwing tool becomes an error result, not a thrown loop", async () => {
  const bomb = tool({
    name: "bomb",
    inputSchema: z.object({}),
    execute: async () => {
      throw new Error("kaboom");
    },
  });
  const { model } = scriptedModel([toolCallStep([{ id: "c1", name: "bomb", input: {} }]), textStep("ok")]);
  const result = await generateText({ model, prompt: "go", tools: [bomb], maxSteps: 2 });
  assert.equal(result.steps[0]?.toolResults[0]?.isError, true);
  assert.match(String(result.steps[0]?.toolResults[0]?.output), /kaboom/);
});

test("unknown tool calls get an error result fed back", async () => {
  const { model } = scriptedModel([toolCallStep([{ id: "c1", name: "nope", input: {} }]), textStep("ok")]);
  const result = await generateText({ model, prompt: "go", tools: [echo], maxSteps: 2 });
  assert.equal(result.steps[0]?.toolResults[0]?.isError, true);
  assert.match(String(result.steps[0]?.toolResults[0]?.output), /Unknown tool: nope/);
});

test("approval-requiring calls suspend the run with serializable state", async () => {
  const { model, calls } = scriptedModel([toolCallStep([{ id: "c1", name: "danger", input: { target: "prod" } }])]);
  const result = await generateText({ model, prompt: "go", tools: [danger], maxSteps: 3 });
  assert.equal(result.finishReason, "tool-approval");
  assert.deepEqual(result.approvalRequests, [{ toolCallId: "c1", toolName: "danger", input: { target: "prod" } }]);
  assert.equal(calls.length, 1);
  assert.equal(result.messages.at(-1)?.role, "assistant");
});

test("resuming with an approval executes the held call and continues", async () => {
  const first = scriptedModel([toolCallStep([{ id: "c1", name: "danger", input: { target: "prod" } }])]);
  const suspended = await generateText({ model: first.model, prompt: "go", tools: [danger], maxSteps: 3 });

  const second = scriptedModel([textStep("done")]);
  const resumed = await generateText({
    model: second.model,
    messages: suspended.messages,
    tools: [danger],
    maxSteps: 3,
    toolApprovals: [{ toolCallId: "c1", approved: true }],
  });
  assert.equal(resumed.text, "done");
  const toolMessage = second.calls[0]?.messages.at(-1) as ToolMessage;
  assert.equal(toolMessage.role, "tool");
  assert.equal(toolMessage.content[0]?.output, "destroyed prod");
});

test("resuming with a denial feeds the refusal to the model", async () => {
  const first = scriptedModel([toolCallStep([{ id: "c1", name: "danger", input: { target: "prod" } }])]);
  const suspended = await generateText({ model: first.model, prompt: "go", tools: [danger], maxSteps: 3 });

  const second = scriptedModel([textStep("understood")]);
  const resumed = await generateText({
    model: second.model,
    messages: suspended.messages,
    tools: [danger],
    maxSteps: 3,
    toolApprovals: [{ toolCallId: "c1", approved: false, reason: "not in prod" }],
  });
  const toolMessage = second.calls[0]?.messages.at(-1) as ToolMessage;
  assert.equal(toolMessage.content[0]?.isError, true);
  assert.match(String(toolMessage.content[0]?.output), /denied by the user: not in prod/);
  assert.equal(resumed.text, "understood");
});

test("onApprovalRequest resolves approvals inline without suspending", async () => {
  const seen: ToolApprovalRequest[] = [];
  const { model } = scriptedModel([
    toolCallStep([{ id: "c1", name: "danger", input: { target: "stage" } }]),
    textStep("done"),
  ]);
  const result = await generateText({
    model,
    prompt: "go",
    tools: [danger],
    maxSteps: 2,
    onApprovalRequest: (request) => {
      seen.push(request);
      return true;
    },
  });
  assert.equal(result.finishReason, "stop");
  assert.equal(result.approvalRequests.length, 0);
  assert.equal(seen[0]?.toolName, "danger");
  assert.equal(result.steps[0]?.toolResults[0]?.output, "destroyed stage");
});

test("mixed auto and approval-required calls execute what they can before suspending", async () => {
  const { model } = scriptedModel([
    toolCallStep([
      { id: "c1", name: "echo", input: { value: "safe" } },
      { id: "c2", name: "danger", input: { target: "prod" } },
    ]),
  ]);
  const suspended = await generateText({ model, prompt: "go", tools: [echo, danger], maxSteps: 3 });
  assert.equal(suspended.finishReason, "tool-approval");
  assert.deepEqual(
    suspended.approvalRequests.map((request) => request.toolCallId),
    ["c2"],
  );
  const toolMessage = suspended.messages.at(-1) as ToolMessage;
  assert.equal(toolMessage.role, "tool");
  assert.deepEqual(
    toolMessage.content.map((result) => result.toolCallId),
    ["c1"],
  );

  const second = scriptedModel([textStep("done")]);
  const resumed = await generateText({
    model: second.model,
    messages: suspended.messages,
    tools: [echo, danger],
    maxSteps: 3,
    toolApprovals: [{ toolCallId: "c2", approved: true }],
  });
  assert.equal(resumed.text, "done");
  const toolMessages = second.calls[0]!.messages.filter((message) => message.role === "tool");
  assert.equal(toolMessages.length, 2);
});

test("toolApprovals for a call not awaiting approval throws", async () => {
  const { model } = scriptedModel([textStep("hi")]);
  await assert.rejects(
    generateText({ model, prompt: "go", tools: [echo], toolApprovals: [{ toolCallId: "ghost", approved: true }] }),
    (error: unknown) => error instanceof AIError && error.problem.status === 400,
  );
});

test("generateText retries transient provider failures", async () => {
  let attempts = 0;
  const model: ModelAdapter = {
    provider: "flaky",
    modelId: "flaky-1",
    async doGenerate() {
      attempts += 1;
      if (attempts === 1) throw new AIError({ type: "https://neutron.dev/errors/rate-limited", title: "Rate Limited", status: 429, detail: "slow down" });
      return textStep("recovered");
    },
    async *doStream() {
      throw new Error("not used");
    },
  };
  const result = await generateText({ model, prompt: "go", retryDelayMs: 1 });
  assert.equal(result.text, "recovered");
  assert.equal(attempts, 2);
});

test("duplicate tool names throw", async () => {
  const { model } = scriptedModel([]);
  await assert.rejects(
    generateText({ model, prompt: "go", tools: [echo, { ...echo }] }),
    (error: unknown) => error instanceof AIError && /Duplicate tool name/.test(error.message),
  );
});
