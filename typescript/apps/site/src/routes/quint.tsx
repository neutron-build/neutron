import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";

export function head() {
  return {
    title: "Neutron Quint — Protocol Modeling",
    description: "Model state transitions, concurrency, and failure scenarios across Neutron. Existing protocol specifications and simulation tools, with live implementation integration still in progress.",
  };
}

export default function QuintPage() {
  return (
    <ProductPage
      title="Neutron Quint"
      description="Model how your system behaves when events race, messages arrive late, or a process fails. Protocol modeling for Neutron’s multi-language ecosystem."
      category="tool"
      status="in-progress"
      actions={[
        { label: "Explore the specifications", href: "/docs/verification/quint" },
        { label: "How the components fit", href: "/docs/modeling/architecture" },
      ]}
    >
      <section>
        <h2>Make the protocol explicit.</h2>
        <p>A service’s behavior spans more than one request. Sessions expire, workers retry, replicas exchange messages, and clients reconnect. Quint expresses those behaviors as states, actions, and invariants that can be explored independently of the implementation language.</p>
        <p>Neutron already contains Quint models for database protocols, framework state machines, and real-time communication. The application-facing workflow and direct checks against live implementations are still in development.</p>
      </section>
      <FeatureGrid columns={3} accentRgb="245, 158, 11">
        <div class="feature-card">
          <div class="feature-card__title">Database protocols</div>
          <div class="feature-card__desc">Models for Multi-Raft, resharding, distributed transactions, replication, membership, and snapshot transfer. A model describes a design; it does not establish that the corresponding production feature is complete.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Service state machines</div>
          <div class="feature-card__desc">Circuit breakers, rate limiting, CSRF tokens, and sessions. Explore the transitions and invariants that services depend on.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Real-time behavior</div>
          <div class="feature-card__desc">WebSocket hub and hot-reload models cover scoped delivery and version ordering, with scenarios for exercising their behavior.</div>
        </div>
      </FeatureGrid>
      <section>
        <h2>Different checks answer different questions.</h2>
        <p>Type checking checks the specification’s structure. Scenario tests exercise selected traces. Random simulation samples possible behaviors. Model checking evaluates properties within the selected model and checker configuration.</p>
        <p>The repository’s CI gate runs type checking, random simulation, Quint conformance scenarios, and a Rust model harness. A passing gate is not evidence that every reachable state or production execution has been checked.</p>
        <p>Quint is the specification language. TLA+ is related specification technology; Apalache and TLC are model checkers. Current Quint supports both backends. See the <a href="https://quint.sh/docs/model-checkers">Quint model-checker documentation</a> for their different bounds and behavior.</p>
      </section>
      <section>
        <h2>Connect the model to the system.</h2>
        <p>The intended next step is to replay model traces against real Neutron services, compare observable results, and preserve failing traces as regression cases. That connection makes specifications useful across Rust, Go, Python, TypeScript, and other implementations.</p>
        <p>Today the Rust conformance harness reimplements modeled state machines. It does not drive the live Nucleus engine. Direct model-to-implementation integration remains unfinished.</p>
      </section>
      <section>
        <h2>Part of the same Neutron system.</h2>
        <p><a href="/lean">Neutron Lean</a> is the planned home for reusable models and deductive proofs. Quint explores stateful protocols. <a href="/modelica">Neutron Modelica</a> simulates physical systems. Each contributes a different kind of evidence, with its assumptions and scope kept explicit.</p>
      </section>
    </ProductPage>
  );
}
