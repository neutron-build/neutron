import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Quint Protocol Verification - Neutron",
    description: "15 Quint specs for Neutron protocols: Multi-Raft, resharding, distributed transactions, real-time presence. TLA+ under the hood with modern syntax.",
  };
}

export default function QuintPage() {
  return (
    <ProductPage
      title="Quint Verification"
      description="15 Quint specs covering Nucleus, framework, and realtime protocols. Multi-Raft consensus, resharding, distributed transactions, presence CRDTs &mdash; modeled and explored before the implementation."
      category="tool"
      status="available"
      accent="var(--accent-quint)"
      heroAccentRgb="245, 158, 11"
      heroTagline="Break the design before the design breaks production."
      stats={[
        { value: '15', label: 'Spec Files' },
        { value: '14', label: 'Test Files' },
        { value: 'TLA+', label: 'Model Checker' },
        { value: 'TS-like', label: 'Syntax' },
      ]}
    >
      <section>
        <h2>Design bugs are the expensive ones.</h2>
        <p>Distributed systems don't fail where your tests look. They fail when a message arrives in the wrong order, a node crashes mid-commit, a network partition flips a leader. Quint lets you model the protocol first, explore every reachable state with TLC under the hood, and find the 40-step trace that violates your invariant &mdash; before you write a line of Go or Rust.</p>
      </section>

      <CodeBlock filename="specs/raft/log_safety.qnt" annotation="Simplified fragment. The real spec models elections, replication, and crash recovery.">
        <pre><code>{`module raftLogSafety {
  var log: Node -> List[Entry]
  var commitIndex: Node -> Int
  var currentTerm: Node -> Int

  action appendEntry(leader: Node, follower: Node, entry: Entry): bool = {
    // ... transition rules ...
  }

  val logMatchingInvariant =
    forall n1, n2 in nodes:
      forall i in 0.to(min(length(log.get(n1)), length(log.get(n2))) - 1):
        log.get(n1)[i].term == log.get(n2)[i].term
          implies log.get(n1)[i] == log.get(n2)[i]
}`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="245, 158, 11">
        <div class="feature-card">
          <div class="feature-card__title">Nucleus specs</div>
          <div class="feature-card__desc">Multi-Raft replication, resharding, distributed transactions, vector-clock merge rules. Each spec has invariant properties checked exhaustively.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Framework specs</div>
          <div class="feature-card__desc">Middleware ordering, request-context isolation, graceful-shutdown drain semantics &mdash; the protocol-level guarantees the SDKs must uphold.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Realtime specs</div>
          <div class="feature-card__desc">Presence CRDTs, channel membership under node failure, pubsub fan-out ordering. Correctness you can't get from testing alone.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Modern syntax</div>
          <div class="feature-card__desc">TypeScript-ish keywords, structural types, pattern matching. Compiles to TLA+ where the actual checking happens.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Simulation mode</div>
          <div class="feature-card__desc">Run randomized simulations for fast feedback (seconds). Switch to exhaustive checking for the final pass (minutes).</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Counterexample traces</div>
          <div class="feature-card__desc">When an invariant fails, Quint prints the exact sequence of events that violated it. Fix the design, re-run.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['', 'Unit tests', 'Integration tests', 'Quint + TLC']}
        rows={[
          ['Concurrency coverage', 'One interleaving', 'A few', 'Every reachable state'],
          ['Failure modes', 'Mocked manually', 'Chaos tools sample', 'All crash / delay combos'],
          ['Runs in', 'Milliseconds', 'Seconds', 'Seconds to minutes'],
          ['Caught before implementation', 'No', 'No', 'Yes'],
          ['Output when broken', 'Assertion fail', 'Flaky test', 'Exact counterexample trace'],
        ]}
        highlightColumn={3}
        accentRgb="245, 158, 11"
      />

      <section>
        <h3>Where this shows up in Neutron</h3>
        <p>Every distributed protocol in Nucleus has a Quint spec. When we design a new one &mdash; say, a change to resharding &mdash; the spec comes first. We run exhaustive model checking, fix the invariant violations the checker finds, and only then implement. The spec and the implementation live in the same repo; they drift together, they're reviewed together.</p>

        <h3>What about my application code?</h3>
        <p>You don't need to write Quint specs for a regular web app &mdash; the hard protocols are ours to verify. If you're designing a custom distributed algorithm, Quint is the tool. If you want to verify Rust implementation code, use <a href="/docs/verification/shuttle">Shuttle</a> or <a href="/docs/verification/kani">Kani</a> instead.</p>

        <h3>Part of a bigger system</h3>
        <p>Quint at the design layer. Lean 4 for algorithm-level correctness. Verus for Rust code proofs. Shuttle for concurrency bugs in running Rust. Four complementary tools; each finds what the others can't.</p>
      </section>
    </ProductPage>
  );
}
