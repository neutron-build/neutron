import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Lean 4 Proofs - Neutron",
    description: "Machine-checked proofs for Nucleus's core algorithms. 26 proof files, zero sorry, 100% proven. MVCC, B-tree, WAL, Raft, HMAC, Bloom, LRU &mdash; verified, not just tested.",
  };
}

export default function LeanPage() {
  return (
    <ProductPage
      title="Lean 4 Proofs"
      description="Machine-checked correctness proofs for the algorithms Nucleus depends on. 26 proof files, zero uses of sorry, 100% proven. When the compiler accepts the file, the algorithm is right &mdash; for all inputs, forever."
      category="tool"
      status="available"
      accent="var(--accent-lean)"
      heroAccentRgb="59, 130, 246"
      heroTagline="Don't test. Prove."
      stats={[
        { value: '26', label: 'Proof Files' },
        { value: '0', label: 'Uses of sorry' },
        { value: '100%', label: 'Proven' },
        { value: 'Lean 4', label: 'Prover' },
      ]}
    >
      <section>
        <h2>The algorithms that can't get this wrong.</h2>
        <p>Nucleus handles your transactions, replicates your data, and signs your tokens &mdash; tests alone aren't enough for that kind of code. Neutron's Lean 4 suite contains machine-checked proofs of the core algorithms: MVCC snapshot isolation, B-tree invariants, write-ahead log durability, Raft safety, HMAC verification, Bloom filter false-positive bounds, LRU eviction correctness, and sliding-window rate limiting. Every proof compiles with zero use of <code>sorry</code>.</p>
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
          <div class="feature-card__desc">Every theorem in the suite has a complete proof. <code>sorry</code> (the "trust me" keyword) appears nowhere. When the file compiles, the theorem is proven.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Compiled to native</div>
          <div class="feature-card__desc">Lean 4 is a programming language, not just a proof assistant. The same definitions that we prove correct compile to C and run in production.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['', 'Unit tests', 'Property tests', 'Lean 4 proof']}
        rows={[
          ['Input coverage', 'Hand-picked cases', 'Random cases', 'All possible inputs'],
          ['Correctness', 'Likely correct', 'Probably correct', 'Mathematically correct'],
          ['Stays correct', 'Until refactor', 'Until refactor', 'Forever (proof is permanent)'],
          ['Finds edge cases', 'If you thought of them', 'Eventually', 'Cannot exist by construction'],
          ['Runtime cost', 'Zero', 'Zero', 'Zero'],
        ]}
        highlightColumn={3}
        accentRgb="59, 130, 246"
      />

      <section>
        <h3>Where this shows up in Neutron</h3>
        <p>Every algorithm proven here is live in Nucleus. The B-tree is the SQL index. The WAL is every durable write. Raft runs replication. HMAC signs every JWT. When your app writes to Nucleus, it's running algorithms that have machine-checked proofs of correctness sitting next to the source.</p>

        <h3>What about my application code?</h3>
        <p>You don't need to write Lean to use Nucleus &mdash; the proofs are ours to maintain. For your own code, <a href="/docs/verification/overview">Neutron's verification overview</a> covers Kani (bounded model checking for Rust), Shuttle (concurrency testing), Verus (SMT verification), and Quint (protocol modeling). Different tools for different problems.</p>

        <h3>Part of a bigger system</h3>
        <p>Lean sits underneath Nucleus. Your app talks to Nucleus. Nucleus runs algorithms whose correctness is proven in Lean. Three layers, one property: when a query returns a row, it's the right row.</p>
      </section>
    </ProductPage>
  );
}
