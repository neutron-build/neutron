import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { AdapterCallOptions, AdapterGenerateResult, ModelAdapter } from "@neutron-build/ai";
import { MemoryEventStore, executeRun, deliverEvent } from "@neutron-build/workflow";
import { approvalEventName } from "@neutron-build/workflow/ai";

import type { LoadedAgent } from "./agent.js";
import { agentWorkflow, isScheduleDue, loadSchedules } from "./durable/index.js";
import { jsonSchema } from "@neutron-build/ai";
import { SandboxExecutor } from "./sandbox.js";
import { loadSkills } from "./skills.js";
import { runTurn } from "./runtime.js";
import { defineTeam, pipeline, roundtrip, runTeamTurn } from "./team.js";

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
const say = (text: string): AdapterGenerateResult => ({
  content: [{ type: "text", text }],
  finishReason: "stop",
  usage,
  raw: null,
});

function inlineAgent(model: ModelAdapter, name = "member"): LoadedAgent {
  return { definition: { name, model, maxSteps: 4 }, instructions: "", tools: [] };
}

// ---------------------------------------------------------------------------
// M3: teams
// ---------------------------------------------------------------------------

test("pipeline passes each member's text to the next", async () => {
  const a = scriptedModel([say("draft from A")]);
  const b = scriptedModel([say("B polished it")]);
  const team = defineTeam({
    name: "duo",
    members: { writer: inlineAgent(a.model, "writer"), editor: inlineAgent(b.model, "editor") },
    policy: pipeline(["writer", "editor"]),
  });

  const result = await runTeamTurn(team, { input: "write the intro" });
  assert.equal(result.text, "B polished it");
  // the editor received the writer's output
  const editorSaw = b.calls[0]?.messages.find((m) => m.role === "user");
  assert.equal(editorSaw?.content, "draft from A");
});

test("roundtrip revises until the reviewer approves", async () => {
  const proposer = scriptedModel([say("v1"), say("v2 with fixes")]);
  const reviewer = scriptedModel([say("Not yet: tighten the ending."), say("APPROVE — ship it")]);
  const team = defineTeam({
    name: "qa",
    members: { author: inlineAgent(proposer.model, "author"), critic: inlineAgent(reviewer.model, "critic") },
    policy: roundtrip({ from: "author", review: "critic", maxRounds: 3 }),
  });

  const result = await runTeamTurn(team, { input: "draft the announcement" });
  assert.equal(result.text, "v2 with fixes");
  assert.equal(proposer.calls.length, 2);
  assert.equal(reviewer.calls.length, 2);
  // the revision prompt carried the feedback
  const revisionPrompt = proposer.calls[1]?.messages.find((m) => m.role === "user");
  assert.match(String(revisionPrompt?.content), /tighten the ending/);
});

test("roundtrip returns the last proposal when rounds run out", async () => {
  const proposer = scriptedModel([say("v1"), say("v2")]);
  const reviewer = scriptedModel([say("no"), say("still no")]);
  const team = defineTeam({
    name: "qa",
    members: { author: inlineAgent(proposer.model, "author"), critic: inlineAgent(reviewer.model, "critic") },
    policy: roundtrip({ from: "author", review: "critic", maxRounds: 2 }),
  });
  const result = await runTeamTurn(team, { input: "go" });
  assert.equal(result.text, "v2");
});

test("a solo agent is the one-member degenerate team", async () => {
  const solo = scriptedModel([say("done alone")]);
  const team = defineTeam({
    name: "solo",
    members: { only: inlineAgent(solo.model, "only") },
    policy: pipeline(["only"]),
  });
  assert.equal((await runTeamTurn(team, { input: "go" })).text, "done alone");
});

// ---------------------------------------------------------------------------
// M4: skills
// ---------------------------------------------------------------------------

test("skills load from disk and their instructions arrive on demand", async () => {
  const dir = await mkdtemp(join(tmpdir(), "skills-"));
  await mkdir(join(dir, "deploy-checklist"));
  await writeFile(
    join(dir, "deploy-checklist", "SKILL.md"),
    "---\ndescription: Use before any production deploy.\n---\n1. Run the tests.\n2. Check the dashboards.\n",
  );

  const skills = await loadSkills(dir);
  assert.equal(skills.length, 1);
  assert.equal(skills[0]?.description, "Use before any production deploy.");

  const { model, calls } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "skill", input: { name: "deploy-checklist" } }],
      finishReason: "tool-calls",
      usage,
      raw: null,
    },
    say("Followed the checklist."),
  ]);
  const agent: LoadedAgent = { ...inlineAgent(model), skills };
  const result = await runTurn(agent, { input: "deploy it" });
  assert.equal(result.text, "Followed the checklist.");
  // the model saw the listing, and the loaded instructions came back as the tool result
  assert.match(calls[0]?.tools?.find((t) => t.name === "skill")?.description ?? "", /deploy-checklist/);
  const toolMessage = calls[1]?.messages.find((m) => m.role === "tool") as
    | { content: Array<{ output: string }> }
    | undefined;
  assert.match(toolMessage?.content[0]?.output ?? "", /Run the tests/);
});

// ---------------------------------------------------------------------------
// M2: durable agent runs + schedules
// ---------------------------------------------------------------------------

test("agentWorkflow parks on approval and resumes durably", async () => {
  let deployments = 0;
  const { model, calls } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "deploy", input: {} }],
      finishReason: "tool-calls",
      usage,
      raw: null,
    },
    say("Deployed."),
  ]);
  const agent: LoadedAgent = {
    definition: { name: "releaser", model, maxSteps: 4 },
    instructions: "Ship safely.",
    tools: [
      {
        name: "deploy",
        inputSchema: jsonSchema({ type: "object" }),
        execute: async () => {
          deployments += 1;
          return "ok";
        },
        needsApproval: true,
      },
    ],
  };

  const wf = agentWorkflow(agent);
  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: { input: "ship the release" } });
  assert.equal(first.status, "waiting");
  assert.equal(first.eventName, approvalEventName("turn", 0));
  assert.equal(deployments, 0);

  await deliverEvent(store, "run-1", approvalEventName("turn", 0), [{ toolCallId: "c1", approved: true }]);
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.deepEqual(second.output, { text: "Deployed.", rounds: 2 });
  assert.equal(deployments, 1);
  assert.equal(calls.length, 2); // round 0 replayed, never re-called
});

test("schedules load from convention files and report due correctly", async () => {
  const dir = await mkdtemp(join(tmpdir(), "schedules-"));
  await writeFile(
    join(dir, "nightly.mjs"),
    `export default { name: "nightly", every: "1d", input: "run the nightly review" };`,
  );
  const schedules = await loadSchedules(dir);
  assert.equal(schedules[0]?.name, "nightly");

  const nightly = schedules[0]!;
  const now = new Date("2026-07-03T12:00:00Z");
  assert.equal(isScheduleDue(nightly, null, now), true);
  assert.equal(isScheduleDue(nightly, "2026-07-03T02:00:00Z", now), false);
  assert.equal(isScheduleDue(nightly, "2026-07-02T02:00:00Z", now), true);
});

// ---------------------------------------------------------------------------
// The http channel
// ---------------------------------------------------------------------------

test("the http channel runs a turn and round-trips the suspension shape", async () => {
  const { model } = scriptedModel([say("hello from the agent")]);
  const { createAgentHandler } = await import("./channels.js");
  const handler = createAgentHandler({ agent: inlineAgent(model) });

  const ok = await handler(
    new Request("http://x/agent", { method: "POST", body: JSON.stringify({ input: "hi" }) }),
  );
  assert.equal(ok.status, 200);
  const body = (await ok.json()) as { text: string; messages: unknown[] };
  assert.equal(body.text, "hello from the agent");
  assert.ok(Array.isArray(body.messages));

  const bad = await handler(new Request("http://x/agent", { method: "POST", body: "not json" }));
  assert.equal(bad.status, 400);
  const wrongMethod = await handler(new Request("http://x/agent", { method: "GET" }));
  assert.equal(wrongMethod.status, 400);
});

// ---------------------------------------------------------------------------
// M5: SandboxExecutor against the daemon contract
// ---------------------------------------------------------------------------

function fakeDaemon(): { impl: typeof globalThis.fetch; requests: Array<{ method: string; url: string; auth: string | null }> } {
  const requests: Array<{ method: string; url: string; auth: string | null }> = [];
  const files = new Map<string, Uint8Array>();
  const impl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    const auth = (init?.headers as Record<string, string>)?.authorization ?? null;
    requests.push({ method, url, auth });

    if (method === "POST" && url.endsWith("/v1/runs")) {
      return Response.json({ id: "01RUN", server: "test-box" });
    }
    if (method === "POST" && url.includes("/exec")) {
      const sse =
        'event: stdout\ndata: hello \n\nevent: stdout\ndata: sandbox\n\nevent: stderr\ndata: warn: slow\n\nevent: exit\ndata: {"exitCode":0,"timedOut":false}\n\n';
      return new Response(sse, { status: 200, headers: { "content-type": "text/event-stream" } });
    }
    if (method === "POST" && url.includes("/snapshot")) {
      return Response.json({ image: "teploy-sbx-snap:01test", server: "test-box" }, { status: 201 });
    }
    if (method === "DELETE" && url.includes("/v1/snapshots")) {
      return new Response(null, { status: 204 });
    }
    if (method === "PUT" && url.includes("/files/")) {
      files.set(url, new Uint8Array(await new Response(init?.body as BodyInit).arrayBuffer()));
      return new Response(null, { status: 204 });
    }
    if (method === "GET" && url.includes("/files/")) {
      const data = files.get(url.replace("GET", ""));
      const stored = files.get(url) ?? data;
      if (stored === undefined) return Response.json({ detail: "no such file" }, { status: 404 });
      return new Response(stored, { status: 200 });
    }
    if (method === "DELETE") return new Response(null, { status: 204 });
    return Response.json({ detail: "unexpected request" }, { status: 500 });
  }) as typeof globalThis.fetch;
  return { impl, requests };
}

test("SandboxExecutor speaks the daemon contract end-to-end", async () => {
  const daemon = fakeDaemon();
  const sandbox = await SandboxExecutor.start({
    baseURL: "http://127.0.0.1:7070",
    token: "secret",
    fetch: daemon.impl,
    create: { image: "python:3.12-slim", ttlSec: 600 },
  });
  assert.equal(sandbox.runId, "01RUN");

  const result = await sandbox.exec("echo hello sandbox");
  assert.equal(result.exitCode, 0);
  assert.equal(result.stdout, "hello sandbox");
  assert.equal(result.stderr, "warn: slow");
  assert.equal(result.timedOut, false);

  await sandbox.putFile("work/notes.txt", "alpha");
  assert.equal(new TextDecoder().decode(await sandbox.getFile("work/notes.txt")), "alpha");
  await assert.rejects(sandbox.getFile("missing.txt"), /no such file|404|not-found|Sandbox request failed/);
  await assert.rejects(sandbox.putFile("../escape.txt", "x"), /escapes the sandbox/);

  await sandbox.destroy();
  assert.ok(daemon.requests.every((r) => r.auth === "Bearer secret"));
  assert.equal(daemon.requests.at(-1)?.method, "DELETE");
});

test("SandboxExecutor snapshot lifecycle speaks the daemon contract", async () => {
  const daemon = fakeDaemon();
  const sandbox = await SandboxExecutor.start({
    baseURL: "http://127.0.0.1:7070",
    token: "secret",
    fetch: daemon.impl,
    create: { image: "python:3.12-slim" },
  });
  const image = await sandbox.snapshot();
  assert.equal(image, "teploy-sbx-snap:01test");
  assert.ok(daemon.requests.some((r) => r.method === "POST" && r.url.endsWith(`/v1/runs/${sandbox.runId}/snapshot`)));

  await SandboxExecutor.deleteSnapshot({ baseURL: "http://127.0.0.1:7070", token: "secret", fetch: daemon.impl, image });
  const del = daemon.requests.at(-1);
  assert.equal(del?.method, "DELETE");
  assert.ok(del?.url.includes("image=teploy-sbx-snap%3A01test"));
});
