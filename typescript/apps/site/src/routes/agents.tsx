import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Agents - Neutron",
    description: "File-based agent authoring for TypeScript. agent.ts, instructions.md, and typed tools/ compose the AI and Workflow SDKs into durable agents that pause, survive restarts, and resume.",
  };
}

export default function AgentsPage() {
  return (
    <ProductPage
      title="Neutron Agents"
      description="File-based agents: a definition, a system prompt, and typed tools in a directory. Composes the AI SDK with the Workflow SDK so an agent IS a durable workflow — it can pause for human approval, survive a restart, and resume exactly where it stopped."
      category="tool"
      status="available"
      accent="var(--accent)"
      heroAccentRgb="0, 229, 160"
      heroTagline="File-based agents that plan, act, and survive restarts."
      stats={[
        { value: 'File-Based', label: 'agent.ts + tools/' },
        { value: 'Durable', label: 'Agent IS a Workflow' },
        { value: 'Approvals', label: 'Pause for a Human' },
        { value: 'Resumable', label: 'Survives Restarts' },
      ]}
    >
      <section>
        <h2>An agent is a folder, not a framework config.</h2>
        <p>Most agent frameworks make you assemble the agent in code &mdash; register tools, thread a system prompt, wire a loop. Neutron Agents uses the same file conventions as the rest of Neutron. An agent is a directory: <code>agent.ts</code> defines it, <code>instructions.md</code> is the always-on system prompt, and every file under <code>tools/</code> is one typed tool. The loader reads the directory, merges the tools files over the inline ones, and hands you a runnable agent. No registry, no wiring.</p>
        <p>Teploy Ship &mdash; the autonomous coding agent that turns a GitHub issue into a pull request &mdash; is built on Neutron Agents. This is the authoring model behind a shipping product, not a demo.</p>
      </section>

      <CodeBlock filename="review-agent/ (the whole agent)" annotation="Convention over configuration. The directory IS the agent.">
        <pre><code>{`review-agent/
├── agent.ts          # defineAgent({...}) — model, budget, inline tools
├── instructions.md   # always-on system prompt
└── tools/
    ├── read-file.ts    # default-exports a typed Tool
    └── open-pr.ts      # one file, one tool`}</code></pre>
      </CodeBlock>

      <CodeBlock filename="review-agent/agent.ts" annotation="The definition. Tools under tools/ merge on top of these.">
        <pre><code>{`import { defineAgent } from "@neutron-build/agents";
import { anthropic } from "@neutron-build/ai/anthropic";

export default defineAgent({
  name: "review-agent",
  model: anthropic("claude-sonnet-4-5"),
  maxSteps: 8, // model-call budget per turn
});`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="0, 229, 160">
        <div class="feature-card">
          <div class="feature-card__title">File-based authoring</div>
          <div class="feature-card__desc"><code>agent.ts</code>, <code>instructions.md</code>, and one file per tool under <code>tools/</code>. <code>loadAgent(dir)</code> reads the directory and hands back a runnable, name-deduped agent.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Typed tools</div>
          <div class="feature-card__desc">Each tool is a schema plus an <code>execute</code>. Types flow from the schema into the handler, so a tool call is checked, not stringly-typed.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Durable by composition</div>
          <div class="feature-card__desc">Wrap an agent as a workflow with <code>agentWorkflow</code> and every model round becomes a recorded step. The run survives crashes, deploys, and weeks of waiting.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Human-in-the-loop</div>
          <div class="feature-card__desc">Mark a tool <code>needsApproval</code> and the run parks on a decision instead of executing. Deliver the approval later and it resumes, never re-calling the model.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Teams and pipelines</div>
          <div class="feature-card__desc"><code>defineTeam</code>, <code>pipeline</code>, and <code>roundtrip</code> compose multiple agents into one turn &mdash; hand-offs and multi-member runs without a bespoke orchestrator.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Real execution</div>
          <div class="feature-card__desc">A built-in exec tool backed by a <code>LocalExecutor</code> or a <code>SandboxExecutor</code> lets an agent run commands in a controlled environment &mdash; the mechanism behind coding agents.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Run a turn. Or make it durable.</h2>
        <p>The simple path is <code>runTurn</code> &mdash; instructions plus tools through the AI SDK's multi-step loop, back in one call. The durable path wraps the same agent as a workflow, so a turn that pauses for approval or crashes mid-flight picks up exactly where it left off.</p>

        <CodeBlock filename="run.ts" annotation="Same agent, two modes: ephemeral turn or durable run.">
          <pre><code>{`import { loadAgent, runTurn } from "@neutron-build/agents";
import { agentWorkflow } from "@neutron-build/agents/durable";
import { executeRun, MemoryEventStore } from "@neutron-build/workflow";

const agent = await loadAgent("./review-agent");

// Ephemeral: one turn, back in one call.
const result = await runTurn(agent, { input: "Review PR #482" });
console.log(result.text);

// Durable: the same agent as an event-sourced workflow.
const wf = agentWorkflow(agent);
await executeRun({
  workflow: wf,
  runId: "review-482",
  store: new MemoryEventStore(), // NucleusEventStore in production
  input: { input: "Review PR #482" },
});`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h2>Approval that parks the run, not a thread.</h2>
        <p>A tool with side effects &mdash; open a pull request, delete a resource, spend money &mdash; can require approval. When the model calls it, the durable run suspends and waits. No process stays alive. Deliver the decision minutes or days later and the run resumes, feeding the outcome back to the model without re-running a single earlier step.</p>

        <CodeBlock filename="review-agent/tools/open-pr.ts" annotation="needsApproval parks the run until a human decides.">
          <pre><code>{`import { tool, jsonSchema } from "@neutron-build/ai";

export default tool({
  name: "open_pr",
  description: "Open a pull request from the working branch.",
  inputSchema: jsonSchema<{ title: string; body: string }>({
    type: "object",
    properties: { title: { type: "string" }, body: { type: "string" } },
    required: ["title", "body"],
  }),
  needsApproval: true, // durable run suspends here, waits for a decision
  execute: async ({ title, body }) => openPullRequest(title, body),
});`}</code></pre>
        </CodeBlock>
      </section>

      <BenchmarkBars
        title="What's in the box"
        bars={[
          { label: 'Authoring', value: 'agent.ts + instructions.md + tools/', width: 100, color: '#00E5A0' },
          { label: 'Runtime', value: 'runTurn, multi-step loop, exec tool', width: 92, color: '#33EBB3' },
          { label: 'Durable', value: 'agentWorkflow over the Workflow SDK', width: 86, color: '#5CF0C6' },
          { label: 'Teams', value: 'defineTeam, pipeline, roundtrip', width: 76, color: '#8AF5D8' },
          { label: 'Sandbox', value: 'Local + sandboxed command execution', width: 68, color: '#B5FAE8' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Autonomous coding agents that read a repo, run commands, and open pull requests &mdash; exactly what Teploy Ship does. Long-running assistants that pause for a human on anything irreversible. Scheduled agents that wake on an interval and do a bounded piece of work. Multi-agent pipelines where one agent's output is another's input. Anything where "call the model in a loop" needs to also mean "and survive the process dying."</p>

        <h3>Why file-based?</h3>
        <p>Because conventions beat configuration once you have more than one agent. A directory you can read top-to-bottom is easier to review, diff, and reason about than an agent assembled across a codebase. It mirrors how the rest of Neutron works &mdash; routes, tools, schedules are all files &mdash; so there's one mental model, not five.</p>

        <h3>Part of a bigger system</h3>
        <p>Neutron Agents sits between two SDKs: it uses Neutron AI for the model calls, tools, and streaming, and Neutron Workflow for durability. An agent turn is a workflow step; approvals are workflow suspensions; scheduled agents are parked runs the scheduler wakes. Same contract, same errors, same Nucleus database as everything else in the stack.</p>
      </section>
    </ProductPage>
  );
}
