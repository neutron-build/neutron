import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "AI - Neutron",
    description: "TypeScript AI SDK for model calls, streaming, schema-validated structured output, and tool-calling over pluggable OpenAI and Anthropic adapters. No provider lock-in.",
  };
}

export default function AIPage() {
  return (
    <ProductPage
      title="Neutron AI"
      description="One SDK for model calls, streaming, structured output, and tools — over pluggable provider adapters. Swap OpenAI for Anthropic for any OpenAI-compatible endpoint by config, not by rewrite. Token accounting included."
      category="tool"
      status="available"
      accent="var(--accent)"
      heroAccentRgb="0, 229, 160"
      heroTagline="Model calls, streaming, structured output, and tools — one SDK, any provider."
      stats={[
        { value: 'OpenAI + Anthropic', label: 'Wire Protocols' },
        { value: 'Streaming', label: 'Token Deltas' },
        { value: 'Structured', label: 'Schema-Validated Output' },
        { value: 'Tool-Calling', label: 'Multi-Step Loop' },
      ]}
    >
      <section>
        <h2>The model call, without the provider tax.</h2>
        <p>Every AI app pays the same tax: you wire directly to one vendor's SDK, hardcode its message shape, and then rewrite everything the day you want to switch. Neutron AI puts one interface in front of both wire protocols &mdash; the OpenAI Chat Completions format and the Anthropic Messages format. You call <code>generateText</code>, <code>streamText</code>, <code>generateObject</code>, and <code>streamObject</code>; the adapter underneath is a config detail. Point <code>baseURL</code> at any OpenAI-compatible server &mdash; Groq, DeepSeek, vLLM, Ollama, an AI gateway &mdash; and the same code runs unchanged.</p>
        <p>It's not a toy. Neutron AI is the model gateway behind Teploy Ship in production &mdash; the layer that turns an issue into a pull request runs every model call through this SDK.</p>
      </section>

      <CodeBlock filename="summarize.ts" annotation="Swap the model line and nothing else changes.">
        <pre><code>{`import { generateText } from "@neutron-build/ai";
import { anthropic } from "@neutron-build/ai/anthropic";
// import { openai } from "@neutron-build/ai/openai"; // same call, other provider

const { text, usage } = await generateText({
  model: anthropic("claude-sonnet-4-5"),
  system: "You write tight, factual release notes.",
  prompt: "Summarize this diff in three bullets:\\n" + diff,
});

console.log(text);
console.log(usage.inputTokens, usage.outputTokens); // accounted per call`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="0, 229, 160">
        <div class="feature-card">
          <div class="feature-card__title">Pluggable adapters</div>
          <div class="feature-card__desc">OpenAI and Anthropic wire protocols behind one <code>ModelAdapter</code> interface. Swap providers by changing the <code>model</code> argument &mdash; the call site never moves.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">No lock-in</div>
          <div class="feature-card__desc">The OpenAI adapter takes a <code>baseURL</code>, so Groq, DeepSeek, vLLM, Ollama, and gateways all work through the same code. Vendor is a config line.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Streaming</div>
          <div class="feature-card__desc"><code>streamText</code> gives you a <code>textStream</code> of deltas and a <code>fullStream</code> of typed parts. Awaitable <code>text</code>, <code>usage</code>, and <code>messages</code> settle when it's done.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Structured output</div>
          <div class="feature-card__desc"><code>generateObject</code> returns a schema-validated object via a forced tool call &mdash; the one mechanism every provider supports identically, so results are portable across adapters.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Tool-calling loop</div>
          <div class="feature-card__desc">Define a <code>tool</code> with a schema and an <code>execute</code>, set <code>maxSteps</code>, and the SDK runs the model-call-then-tool loop for you. Client-side and approval-gated tools too.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Usage accounting</div>
          <div class="feature-card__desc">Every call returns a <code>Usage</code> with input and output tokens, summed across steps. Meter, bill, and budget without bolting on a second SDK.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Structured output that actually validates.</h2>
        <p>Ask for JSON and most SDKs hand you a string and a prayer. <code>generateObject</code> forces the model through a tool call shaped by your schema, then validates the result before it reaches you &mdash; typed all the way out.</p>

        <CodeBlock filename="classify.ts" annotation="Schema-validated object, portable across every adapter.">
          <pre><code>{`import { generateObject, jsonSchema } from "@neutron-build/ai";
import { openai } from "@neutron-build/ai/openai";

const Ticket = jsonSchema<{ severity: "low" | "high"; area: string }>({
  type: "object",
  properties: {
    severity: { type: "string", enum: ["low", "high"] },
    area: { type: "string" },
  },
  required: ["severity", "area"],
});

const { object } = await generateObject({
  model: openai("gpt-4o-mini"),
  schema: Ticket,
  prompt: "Triage: 'the deploy step hangs on every push to main'",
});

route(object.severity, object.area); // object is typed and validated`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h2>Tools the model can actually call.</h2>
        <p>A tool is a schema plus a function. Hand the SDK a set of them and a step budget, and it runs the loop &mdash; call the model, execute the tools it asked for, feed the results back, repeat &mdash; until the model is done or the budget runs out.</p>

        <CodeBlock filename="agent-loop.ts" annotation="One tool, a multi-step loop, tokens accounted the whole way.">
          <pre><code>{`import { generateText, tool, jsonSchema } from "@neutron-build/ai";
import { anthropic } from "@neutron-build/ai/anthropic";

const search = tool({
  name: "search_docs",
  description: "Find relevant docs for a query.",
  inputSchema: jsonSchema<{ query: string }>({
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
  }),
  execute: async ({ query }) => db.vector("docs").search(query).k(5),
});

const { text, steps } = await generateText({
  model: anthropic("claude-sonnet-4-5"),
  tools: [search],
  maxSteps: 6,
  prompt: "How do I configure the scheduler?",
});`}</code></pre>
        </CodeBlock>
      </section>

      <BenchmarkBars
        title="What's in the box"
        bars={[
          { label: 'Generate', value: 'generateText + generateObject', width: 100, color: '#00E5A0' },
          { label: 'Stream', value: 'streamText + streamObject, typed parts', width: 92, color: '#33EBB3' },
          { label: 'Adapters', value: 'OpenAI + Anthropic + any compatible', width: 86, color: '#5CF0C6' },
          { label: 'Tools', value: 'Schema tools, multi-step, approvals', width: 78, color: '#8AF5D8' },
          { label: 'Embed', value: 'embed / embedMany / embedAndStore', width: 70, color: '#B5FAE8' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Chat and assistant features that stream tokens to the UI. Extraction and classification pipelines that need typed, validated output. Tool-using agents that call your database and your APIs. Embedding and semantic-search flows through <code>embed</code> and <code>embedAndStore</code>. Anywhere you'd reach for a vendor SDK but don't want to marry the vendor.</p>

        <h3>Why one SDK over every provider?</h3>
        <p>Because model choice is a moving target and vendor lock-in is a liability, not a feature. The wire protocols are stable; the models behind them change monthly. Program against the interface, and switching a model &mdash; or A/B testing two &mdash; is a one-line diff. Structured output goes through forced tool calls precisely so it behaves the same on a frontier model and a self-hosted one.</p>

        <h3>Part of a bigger system</h3>
        <p>Neutron AI is the foundation the rest of the agent stack builds on. Neutron Agents composes this SDK with the Workflow SDK to make durable, file-based agents. Neutron Workflow makes any tool loop survivable across restarts. Same contract, same errors (RFC 7807), same Nucleus database underneath.</p>
      </section>
    </ProductPage>
  );
}
