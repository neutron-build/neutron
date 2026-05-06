export function head() {
  return {
    title: "About Codex",
    description:
      "What Codex is, who it's for, and how the three-tier system works.",
  };
}

export default function About() {
  return (
    <article class="prose">
      <h1>About Codex</h1>

      <p>
        Codex is a curriculum that takes a self-taught learner from high-school algebra to
        graduate-level mastery across mathematics and physics. Every concept exists at three
        depths in a single source document; the site filters what you see.
      </p>

      <h2>The three tiers</h2>

      <ul>
        <li>
          <strong>Beginner</strong> — intuition, visuals, worked examples with everyday numbers.
          Anchor: 3Blue1Brown, Steven Strogatz <em>Infinite Powers</em>. Endpoint: scientific
          literacy — you can read popular-science writing on the topic with real comprehension.
        </li>
        <li>
          <strong>Intermediate</strong> — definitions, key proofs, exercises, scaffolded Lean
          puzzles. Anchor: Axler <em>Linear Algebra Done Right</em>, Apostol <em>Calculus</em>,
          Griffiths <em>Introduction to Electrodynamics</em>. Endpoint: undergrad-textbook
          mastery.
        </li>
        <li>
          <strong>Master</strong> — full proofs, advanced results, primary literature, hard
          problems, full Lean formalisation where Mathlib covers. Anchor: Lang <em>Algebra</em>,
          Hartshorne <em>Algebraic Geometry</em>, Lawson-Michelsohn <em>Spin Geometry</em>,
          Weinberg <em>QFT</em>, Bott-Tu <em>Differential Forms</em>. Endpoint: graduate research
          readiness.
        </li>
      </ul>

      <h2>How units work</h2>

      <p>
        Each unit is one concept (one theorem cluster, one technique). All three tiers live in the
        same source markdown file; section markers (<code>[Beginner]</code>,{" "}
        <code>[Intermediate+]</code>, <code>[Master]</code>) determine what you see at your
        current tier.
      </p>

      <p>
        Units cite real references — every <code>[ref: source locator]</code> link lands in our
        archive of source material. Units cross-reference each other via the curriculum
        prerequisite graph; clicking a related concept opens it at your current tier.
      </p>

      <h2>v0.x pilot status</h2>

      <p>
        v0.1 is apex-first: we produce Master-tier content at the top of the Fast Track curriculum
        first (spin geometry, QFT, algebraic topology), then fill in prerequisites in waves
        downward. Beginner and Intermediate tiers exist as place-holders for apex units; they fill
        in as the prerequisite chain reaches them.
      </p>

      <p class="muted">
        If you arrived hoping for accessible introductions to graduate math, you're early.{" "}
        v1 is the right time to come back. v0.x is for graduate readers who want a single
        cross-linked reference.
      </p>
    </article>
  );
}
