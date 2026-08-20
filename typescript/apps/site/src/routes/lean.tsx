import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Lean 4 Proofs - Neutron",
    description: "Machine-checked Lean 4 proofs of the algorithms behind Nucleus. 26 files, 92 theorems, zero sorry. MVCC, B-tree, WAL, Raft, HMAC, Bloom, LRU &mdash; the designs proven correct, not just tested.",
  };
}

export default function LeanPage() {
  return (
    <ProductPage
      title="Lean 4 Proofs"
      description="Machine-checked correctness proofs for the algorithms Nucleus is built on. 26 files, 92 theorems, zero uses of sorry. Each proof covers a Lean model of the algorithm &mdash; MVCC, B-tree, WAL, Raft and more &mdash; correct for every input, not just the cases a test happened to try."
      category="tool"
      status="available"
      accent="var(--accent-lean)"
      heroAccentRgb="59, 130, 246"
      heroTagline="Don't just test. Prove."
      stats={[
        { value: '26', label: 'Model Files' },
        { value: '92', label: 'Theorems' },
        { value: '0', label: 'Uses of sorry' },
        { value: '3', label: 'Axioms' },
      ]}
    >
      <section>
        <h2>The algorithms that can't get this wrong.</h2>
        <p>Nucleus handles your transactions, replicates your data, and signs your tokens &mdash; tests alone aren't enough for that kind of code. Neutron's Lean 4 suite contains machine-checked proofs of the core algorithms: MVCC snapshot isolation, B-tree invariants, write-ahead log durability, Raft safety, HMAC verification, Bloom filter false-positive bounds, LRU eviction correctness, and sliding-window rate limiting. Every proof compiles with zero use of <code>sorry</code> &mdash; against precise Lean models of each algorithm, resting on nothing but Lean&rsquo;s own axioms and three stated assumptions about SHA-256, which no proof can discharge.</p>
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
          <div class="feature-card__title">Zero sorry, axioms in the open</div>
          <div class="feature-card__desc">No <code>sorry</code> anywhere in the suite. The facts we don't derive from scratch &mdash; bitwise identities, standard crypto assumptions &mdash; are declared as explicit axioms you can read and audit, not hidden.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Executable models</div>
          <div class="feature-card__desc">Lean 4 is a real programming language, so each proof runs against an executable model of the algorithm. Nucleus implements the same design in Rust &mdash; the proof pins down what "correct" means for the hard parts.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['', 'Unit tests', 'Property tests', 'Lean 4 proof']}
        rows={[
          ['What it covers', 'Hand-picked cases', 'Random cases', 'Every input to the model'],
          ['Subject', 'The Rust', 'The Rust', 'A Lean model of the algorithm'],
          ['Strength of result', 'Likely correct', 'Probably correct', 'Proven, modulo stated axioms'],
          ['When the code changes', 'Rerun the tests', 'Rerun the tests', 'Proof still holds; the model may no longer match'],
          ['Runtime cost', 'Zero', 'Zero', 'Zero'],
        ]}
        highlightColumn={3}
        accentRgb="59, 130, 246"
      />

      <section>
        <h3>What the proofs do not cover</h3>
        <p>Two limits worth stating plainly, because a proof that is oversold is worse than no proof. First, the models are hand-written Lean, not extracted from the Rust &mdash; so a proof guarantees the <em>design</em> is sound, and keeping the implementation faithful to the design is still ordinary engineering work done by tests and review. Second, 3 declarations in the suite are <code>axiom</code>s rather than derived results, and all three are assumptions about an opaque SHA-256 &mdash; its output length, collision resistance, and HMAC's PRF security &mdash; which no proof can discharge. The structural obligations in the Bloom filter and LRU models were open until August 2026 and are now proven; a script walks every theorem and fails the build if one rests on an axiom outside that list of three.</p>
      </section>

      <section>
        <h3>Where this shows up in Neutron</h3>
        <p>Most algorithms modeled here are ones Nucleus runs. The B-tree is the SQL index. The WAL is every durable write. HMAC signs every JWT. Raft is modeled because it is the replication design Nucleus is building toward &mdash; the distributed/Raft mode in the shipping engine is incomplete and unsupported today. The proofs don't certify the compiled binary &mdash; they pin down the designs it's built on, so the parts that are hardest to get right are specified and machine-checked instead of improvised.</p>

        <h3>What about my application code?</h3>
        <p>You don't need to write Lean to use Nucleus &mdash; the proofs are ours to maintain. For your own code, <a href="/docs/verification/overview">Neutron's verification overview</a> covers Kani (bounded model checking for Rust), Shuttle (concurrency testing), Verus (SMT verification), and Quint (protocol modeling). Different tools for different problems.</p>

        <h3>Part of a bigger system</h3>
        <p>Lean sits underneath Nucleus. Your app talks to Nucleus. Nucleus runs the algorithms whose designs are proven in Lean. The proof doesn't replace the tests on the Rust &mdash; it means the algorithm those tests exercise is known, mathematically, to be sound.</p>
      </section>
    </ProductPage>
  );
}
