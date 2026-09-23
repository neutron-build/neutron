import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";

export function head() {
  return {
    title: "Neutron Lean — Verification Framework · Coming Soon",
    description: "The verification framework for Neutron’s multi-language ecosystem. Reusable models, application proofs, and a shared verification workflow — coming soon. Explore the existing Nucleus proof suite today.",
  };
}

export default function LeanPage() {
  return (
    <ProductPage
      title="Neutron Lean"
      description="Prove the rules your application depends on. The verification framework for Neutron’s multi-language ecosystem — bringing Lean models, reusable proofs, and application contracts into one development workflow."
      category="language"
      status="coming-soon"
      actions={[
        { label: "Explore the direction", href: "#framework" },
        { label: "Existing proof suite", href: "/docs/verification/lean4" },
      ]}
    >
      <section id="framework">
        <h2>One ecosystem. A language for correctness.</h2>
        <p>Neutron gives each language a role: TypeScript for interfaces, Go and Rust for services, Python for AI applications, and specialist libraries for scientific computing and ML. The application frameworks share a behavioral contract and connect to Nucleus through its PostgreSQL wire protocol.</p>
        <p>Lean adds a place to express the rules those applications depend on: valid state transitions, authorization decisions, resource limits, and the properties of core algorithms. The planned framework brings that work into Neutron projects as a reusable component alongside the languages that run the application.</p>
        <p><strong>Coming soon:</strong> application-facing proof libraries, connections to the language SDKs, and integrated build tooling are planned. The Nucleus algorithm-model proof suite is available today.</p>
      </section>

      <section>
        <h2>Define. Prove. Connect.</h2>
        <p>The intended workflow starts with a small, explicit model of an important rule. Prove the properties that rule must preserve, then connect the model to the application through implementation checks. Keep the model, assumptions, and implementation checks together as the application changes.</p>
      </section>

      <FeatureGrid columns={3} accentRgb="59, 130, 246">
        <div class="feature-card">
          <div class="feature-card__title">Model your application rules</div>
          <div class="feature-card__desc">Planned libraries for describing state, operations, and invariants in Lean. Start with a critical part of an application, such as an inventory reservation or an access policy.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Build on reusable proofs</div>
          <div class="feature-card__desc">A planned foundation of models and lemmas that projects can compose and extend, building on the existing work on Nucleus algorithm models.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Connect across languages</div>
          <div class="feature-card__desc">Planned conformance checks to compare application behavior with its model. Each language integration needs an explicit boundary between what is proven and what is tested.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>From a business rule to a checked model.</h2>
        <p>Consider an inventory service written in Go with a TypeScript storefront. The intended Lean workflow would model reservations and cancellations, then prove that accepted operations preserve the model’s stock limits. Integration checks would exercise the Go service against that model.</p>
        <p>The proof establishes a property of the Lean model under its assumptions. Tests, implementation review, and any future verified translation establish how the running service relates to that model. Sharing Nucleus or an API contract alone does not transfer a proof to another language.</p>
      </section>

      <section>
        <h2>A foundation you can inspect today.</h2>
        <p>The repository already contains hand-written Lean models and proofs for Nucleus algorithms, including MVCC visibility, B-tree structure, write-ahead logging, caching, and cryptographic constructions. The suite uses Lean’s kernel to check proofs and an axiom audit to track their assumptions.</p>
        <p>These are proofs about algorithm models. They do not certify the shipping Nucleus binary or application code. The broader Neutron Lean framework will build on this foundation; its application integrations are still to be built.</p>
        <p><a href="/docs/verification/lean4">Read the current suite documentation</a> or <a href="https://github.com/neutron-build/neutron/tree/main/lean4">inspect the models and proofs</a>.</p>
      </section>

      <section>
        <h2>Verification that fits the whole system.</h2>
        <p><a href="/docs/modeling/architecture">Explore the proposed architecture</a> for proof libraries, language adapters, and reproducible evidence across Neutron.</p>
        <p>Lean’s proposed role is reusable models and deductive proofs. <a href="/quint">Quint</a> models stateful protocols, while <a href="/docs/verification/verus">Verus</a> supports verification of annotated Rust. Neutron Lean is intended to connect proof work to the wider development experience, with each tool’s scope and assumptions kept explicit.</p>
        <p>The planned release includes project setup, reusable application models, language integration examples, and build checks. APIs and language coverage will be documented as those integrations are implemented.</p>
      </section>
    </ProductPage>
  );
}
