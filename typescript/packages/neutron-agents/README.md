# @neutron-build/agents

Neutron Agents. File-based agent authoring that composes the AI SDK
(model calls) and the Workflow SDK (durability) into agents you can run
anywhere — locally, behind an HTTP route, or inside a Teploy sandbox.

## Conventions

```
my-agent/
└── agent/
    ├── agent.js          # export default defineAgent({ name, model, maxSteps })
    ├── instructions.md   # always-on system prompt
    ├── tools/            # one file per AI SDK tool() export
    ├── skills/           # procedures loaded on demand (SKILL.md + tools)
    └── schedules/        # { name, every: "1d", input } per file
```

```ts
import { loadAgent, runTurn, LocalExecutor } from "@neutron-build/agents";

const agent = await loadAgent("./agent");
const result = await runTurn(agent, {
  input: "fix the failing test",
  executor: new LocalExecutor({ root: "/workspace" }), // adds the exec tool
});
```

Approval-requiring tools suspend with the same resume shape as the rest
of the stack (`finishReason: "tool-approval"` → pass `messages` +
`toolApprovals` back).

## Teams

The execution unit is a team of 1..N agents; solo is the one-member case.
Policies are pure routing — C3 verification strategies plug in here:

```ts
import { defineTeam, pipeline, roundtrip, runTeamTurn } from "@neutron-build/agents";

const qa = defineTeam({
  name: "qa",
  members: { author, critic },
  policy: roundtrip({ from: "author", review: "critic", maxRounds: 3 }),
});
const result = await runTeamTurn(qa, { input: "draft the announcement" });
```

## Durable runs

With `@neutron-build/workflow` installed (optional peer):

```ts
import { agentWorkflow, loadSchedules, isScheduleDue } from "@neutron-build/agents/durable";

const wf = agentWorkflow(agent, { executor });
// executeRun / Scheduler / deliverEvent from @neutron-build/workflow —
// approvals park the run for free, for as long as it takes.
```

## Execution

Agents act on compute through one interface — `AgentExecutor`
(`exec`/`putFile`/`getFile`/`destroy`):

- `LocalExecutor` — child processes under a root dir (dev/test/trusted).
- `SandboxExecutor` — client for the `teploy-sandbox` daemon (ephemeral
  containers; the client defines the wire contract both sides build to).

## HTTP channel

```ts
import { createAgentHandler } from "@neutron-build/agents";
export const POST = createAgentHandler({ agent, executor }); // any mode:"api" route
```

## Status

M1–M5 core complete: conventions loader, executor contract +
LocalExecutor, solo runtime, teams (pipeline/roundtrip), skills
(local, on-demand), durable runs + schedules, http channel,
SandboxExecutor. Deferred: skills from the Teploy catalog and MCP
servers (C1), non-http channels (Chat SDK territory), the reference
starter example.
