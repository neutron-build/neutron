import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Lean 4 Proofs - Neutron",
    description: "Machine-checked Lean models of Nucleus's core algorithms. 26 files, 70 theorems, zero sorry &mdash; proven modulo explicit foundational axioms. MVCC, B-tree, WAL, Raft, HMAC, Bloom, LRU.",
  };
}

export default function LeanPage() {
  return (
    <ProductPage
      title="Lean 4 Proofs"
      description="Machine-checked Lean models of the algorithms Nucleus depends on. 26 files, 70 theorems, zero uses of sorry &mdash; proofs hold for all inputs, modulo explicit and auditable foundational axioms. These are hand-written models, not the running binary."
      category="tool"
      status="available"
      accent="var(--accent-lean)"
      heroAccentRgb="59, 130, 246"
      heroTagline="Don't test. Prove."
      stats={[
        { value: '26', label: 'Proof Files' },
        { value: '70', label: 'Theorems' },
        { value: '0', label: 'Uses of sorry' },
        { value: '28', label: 'Axioms' },
      ]}
    >
      <section>
        <h2>The algorithms that can't get this wrong.</h2>
        <p>Nucleus handles your transactions, replicates your data, and signs your tokens &mdash; tests alone aren't enough for that kind of code. Neutron's Lean 4 suite contains machine-checked models of the core algorithms: MVCC snapshot isolation, B-tree invariants, write-ahead log durability, Raft safety, HMAC verification, Bloom filter false-positive bounds, LRU eviction correctness, and sliding-window rate limiting. Across 26 files and 70 theorems, every proof compiles with zero uses of <code>sorry</code> &mdash; resting on 28 explicit, auditable axioms. These are hand-written Lean models of the algorithms, not machine-extracted from the production Rust, so they establish that the <em>design</em> is correct, not that the shipped binary is.</p>
      </section>

      <CodeBlock filename="proofs/MVCC.lean" annotation="A sample of what's in the suite. The full file proves snapshot isolation.">
        <pre><code>{`namespace Nucleus.MVCC

/-- Two transactions that read the same key must see the same value
    under snapshot isolation, regardless of concurrent writes. -/
theorem snapshot_read_consistency
    (t₁ t₂ : Txn) (k : Key) (db : Database)
    (h_same_snapshot : t₁.snapshot = t₂.snapshot) :
    t₁.read db k = t₂.read db k := by
  unfold Txn.read
  rw [h_same_snapshot]
  rfl

end Nucleus.MVCC`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="59, 130, 246">
        <div class="feature-card">
          <div class="feature-card__title">MVCC &amp; B-tree</div>
          <div class="feature-card__desc">Snapshot isolation rules proven. B-tree structural invariants (ordering, balance, split correctness) proven.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">WAL &amp; Raft</div>
          <div class="feature-card__desc">Write-ahead log durability across crashes proven. Raft leader election and log safety proven from the original TLA+ spec.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Crypto primitives</div>
          <div class="feature-card__desc">HMAC message authentication proven correct against the RFC 2104 spec. Constant-time comparison proven timing-safe.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Data structures</div>
          <div class="feature-card__desc">Bloom filter false-positive bound proven. LRU eviction ordering proven. Sliding-window rate limiter proven fair.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Zero sorry</div>
          <div class="feature-card__desc">Every theorem in the suite has a complete proof. <code>sorry</code> (the "trust me" keyword) appears nowhere. Proofs hold modulo 28 explicit, auditable foundational axioms.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Models, not the binary</div>
          <div class="feature-card__desc">These are hand-written Lean models of the algorithms &mdash; not machine-extracted from Nucleus's Rust. They prove the design is sound; they don't certify the running binary. The Rust is validated separately by its test and fuzzing suites.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['', 'Unit tests', 'Property tests', 'Lean 4 proof']}
        rows={[
          ['Input coverage', 'Hand-picked cases', 'Random cases', 'All possible inputs'],
          ['Correctness', 'Likely correct', 'Probably correct', 'Mathematically correct'],
          ['Stays correct', 'Until refactor', 'Until refactor', 'Permanent (for the model)'],
          ['Finds edge cases', 'If you thought of them', 'Eventually', 'Cannot exist by construction'],
          ['Runtime cost', 'Zero', 'Zero', 'Zero'],
        ]}
        highlightColumn={3}
        accentRgb="59, 130, 246"
      />

      <section>
        <h3>Where this shows up in Neutron</h3>
        <p>Each algorithm modeled here corresponds to one Nucleus depends on &mdash; the B-tree behind the SQL index, the WAL behind every durable write, Raft for replication, HMAC for JWT signing. The Lean files model those algorithms and prove their key properties; they sit next to the source as an executable specification, not as the compiled code itself.</p>

        <h3>What about my application code?</h3>
        <p>You don't need to write Lean to use Nucleus &mdash; the proofs are ours to maintain. For your own code, <a href="/docs/verification/overview">Neutron's verification overview</a> covers Kani (bounded model checking for Rust), Shuttle (concurrency testing), Verus (SMT verification), and Quint (protocol modeling). Different tools for different problems.</p>

        <h3>Part of a bigger system</h3>
        <p>Lean sits alongside Nucleus as a specification layer. Your app talks to Nucleus; Nucleus implements algorithms whose designs are proven correct in Lean, modulo explicit axioms. The proofs raise confidence in the design &mdash; they are not a guarantee about the running binary, which is covered by tests, property checks, and differential fuzzing.</p>
      </section>
    </ProductPage>
  );
}
