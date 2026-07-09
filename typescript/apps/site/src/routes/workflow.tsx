import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Workflow - Neutron",
    description: "Durable, event-sourced workflow engine for TypeScript. Every step is an event; replay reconstructs state on restart. Workflows suspend for days and resume exactly where they left off.",
  };
}

export default function WorkflowPage() {
  return (
    <ProductPage
      title="Neutron Workflow"
      description="A durable execution engine. Every step is an event on Nucleus; on restart the engine replays the log to reconstruct state and continue. Workflows suspend for an event, an approval, or a timer — and resume days later, exactly where they stopped."
      category="tool"
      status="available"
      accent="var(--accent)"
      heroAccentRgb="0, 229, 160"
      heroTagline="Durable, event-sourced workflows — suspend for days, survive restarts, resume exactly where you left off."
      stats={[
        { value: 'Event-Sourced', label: 'Replay on Nucleus' },
        { value: 'Suspend', label: 'Sleep, Wait, Approve' },
        { value: 'Deterministic', label: 'Replay Reconstructs State' },
        { value: 'Scheduler', label: 'Picks Up Parked Runs' },
      ]}
    >
      <section>
        <h2>The primitive Next.js and Astro don't have.</h2>
        <p>Web frameworks give you a request and a response. They have no answer for "run this, then wait three days for the customer to reply, then continue" &mdash; that's where you reach for a queue, a cron job, a state table, and a lot of glue. Neutron Workflow is a durable execution primitive built in. You write a plain async function; the engine makes it survivable.</p>
        <p>Every step is recorded as an event. When a process restarts &mdash; a crash, a deploy, a scale-down &mdash; the engine replays the event log from the top, feeds completed steps their recorded results instead of re-running them, and continues from the first step that never finished. State is reconstructed, not persisted by hand.</p>
      </section>

      <CodeBlock filename="onboarding.ts" annotation="A function that runs for a week without a process staying alive.">
        <pre><code>{`import { workflow } from "@neutron-build/workflow";

export const onboarding = workflow("onboarding", async (ctx, userId: string) => {
  // step() runs once, records the result, and replays it forever after.
  const user = await ctx.step("load-user", () => db.users.get(userId));
  await ctx.step("welcome-email", () => sendEmail(user.email, "welcome"));

  // Suspend for three days. No process stays alive; the scheduler wakes it.
  await ctx.sleep("3d");

  if (!(await ctx.step("did-activate", () => hasActivated(userId)))) {
    await ctx.step("nudge-email", () => sendEmail(user.email, "nudge"));
  }
  return { onboarded: true };
});`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="0, 229, 160">
        <div class="feature-card">
          <div class="feature-card__title">Event-sourced steps</div>
          <div class="feature-card__desc">Every <code>ctx.step()</code> runs once, records its result as an event, and replays that result on every later pass. The event log is the source of truth.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Replay on restart</div>
          <div class="feature-card__desc">The run function re-executes from the top on every resume. Completed steps replay from the log instead of re-running, so a crash costs nothing already done.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Suspend and resume</div>
          <div class="feature-card__desc"><code>sleep</code> parks on a timer, <code>waitForEvent</code> parks until a payload arrives. No thread, no process &mdash; the run is durable state, woken later.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Deterministic by design</div>
          <div class="feature-card__desc"><code>ctx.now()</code> and <code>ctx.random()</code> are recorded like steps, and a <code>NondeterminismError</code> catches replay drift. All I/O goes inside <code>step()</code>.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Nucleus event store</div>
          <div class="feature-card__desc"><code>NucleusEventStore</code> persists the log to Nucleus streams; <code>MemoryEventStore</code> runs the same semantics in tests. Same engine, swappable backing.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Scheduler</div>
          <div class="feature-card__desc">A <code>Scheduler</code> over a <code>RunIndex</code> picks up runs that are due or parked &mdash; a slept timer fired, a retry delay elapsed &mdash; and drives them forward.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Wait for the outside world, then continue.</h2>
        <p>A workflow can suspend until an external event arrives &mdash; a webhook, a human approval, a payment confirmation. The run parks as durable state; when you deliver the event, it wakes and continues with the payload in hand. The waiting costs nothing, and it can last days.</p>

        <CodeBlock filename="expense.ts" annotation="Parks on a human decision. Resumes when the event is delivered.">
          <pre><code>{`import { workflow } from "@neutron-build/workflow";

export const expense = workflow("expense", async (ctx, claim: Claim) => {
  await ctx.step("record", () => db.claims.insert(claim));

  // Suspend until deliverEvent(runId, "decision") supplies a payload.
  const decision = await ctx.waitForEvent<{ approved: boolean }>("decision");

  if (decision.approved) {
    await ctx.step("reimburse", () => pay(claim.userId, claim.amount));
  }
  return { status: decision.approved ? "paid" : "denied" };
});`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h2>Run it, park it, wake it.</h2>
        <p><code>executeRun</code> drives a run against an event store until it completes or suspends. A suspended run returns to the store as durable state. Later &mdash; when a timer fires or an event lands &mdash; the scheduler or an explicit <code>deliverEvent</code> resumes it. Same engine, whether it runs for ten milliseconds or ten days.</p>

        <CodeBlock filename="driver.ts" annotation="Start a run; deliver an event days later to resume it.">
          <pre><code>{`import { executeRun, deliverEvent, NucleusEventStore } from "@neutron-build/workflow";

const store = new NucleusEventStore(nucleus.streams);

// First pass: runs until the workflow suspends on waitForEvent.
await executeRun({ workflow: expense, runId: "claim-91", store, input: claim });

// ...minutes or days later, from a webhook handler:
await deliverEvent(store, "claim-91", "decision", { approved: true });
// The scheduler picks the run back up and continues from the wait.`}</code></pre>
        </CodeBlock>
      </section>

      <BenchmarkBars
        title="What's in the box"
        bars={[
          { label: 'Engine', value: 'workflow() + ctx.step, event-sourced', width: 100, color: '#00E5A0' },
          { label: 'Suspend', value: 'sleep, waitForEvent, retry backoff', width: 92, color: '#33EBB3' },
          { label: 'Store', value: 'NucleusEventStore + MemoryEventStore', width: 84, color: '#5CF0C6' },
          { label: 'Scheduler', value: 'RunIndex + due/parked run pickup', width: 76, color: '#8AF5D8' },
          { label: 'Leasing', value: 'LeaseManager for exclusive execution', width: 66, color: '#B5FAE8' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Multi-day business processes &mdash; onboarding drips, approval chains, subscription lifecycles. Sagas that coordinate several services and need to unwind cleanly on failure. Retry-heavy jobs where a delayed retry should park for free rather than hold a worker. Anything that has to wait on a human or an external system and then reliably pick up where it stopped. And it's the substrate under Neutron Agents and Teploy Ship &mdash; an agent turn is a durable workflow.</p>

        <h3>Why durable execution?</h3>
        <p>Because the alternative is a queue plus a state table plus a cron job plus reconciliation logic, reinvented per feature and wrong in a different way each time. Event sourcing gives you the audit log for free: every run's history is the exact sequence of steps that happened. Deterministic replay means the same inputs always reconstruct the same state, so a resumed run is indistinguishable from one that never stopped.</p>

        <h3>Part of a bigger system</h3>
        <p>Neutron Workflow is the durability layer the rest of the stack stands on. Neutron Agents turns an agent turn into a workflow so it survives restarts and pauses for approval. The AI SDK's tool loop becomes durable through <code>agentStep</code>. The event log lives in Nucleus, alongside everything else &mdash; one database, one contract, one system.</p>
      </section>
    </ProductPage>
  );
}
