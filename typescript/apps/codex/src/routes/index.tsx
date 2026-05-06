import { getCollection } from "@neutron-build/core";

export function head() {
  return {
    title: "Codex — math & physics curriculum",
    description:
      "A single curriculum from high-school algebra to graduate-level mastery. Three tiers: Beginner, Intermediate, Master. Lean-verified where Mathlib covers.",
  };
}

export async function loader() {
  const units = await getCollection("units");
  const shipped = units.filter((u: any) => u.data.status === "shipped");
  const draft = units.filter((u: any) => u.data.status === "draft" || u.data.status === "review");
  return {
    shippedCount: shipped.length,
    draftCount: draft.length,
    totalCount: units.length,
    featured: units.slice(0, 5).map((u: any) => ({
      id: u.data.id,
      title: u.data.title,
      tiers_present: u.data.tiers_present,
      status: u.data.status,
    })),
  };
}

export default function HomePage({ data }: { data: any }) {
  return (
    <article class="hero">
      <h1>Codex</h1>
      <p class="lede">
        From high-school algebra to graduate-level mastery in mathematics and physics. Three tiers
        per unit. Lean-verified where Mathlib covers. Apex-first curriculum, building outward from
        the hardest material.
      </p>

      <div class="stats">
        <div>
          <strong>{data.shippedCount}</strong>
          <span>shipped</span>
        </div>
        <div>
          <strong>{data.draftCount}</strong>
          <span>in review</span>
        </div>
        <div>
          <strong>{data.totalCount}</strong>
          <span>total units</span>
        </div>
      </div>

      <h2>How it works</h2>
      <p>
        Pick a tier in the header. Beginner is intuition + visuals; Intermediate is undergrad-textbook
        rigor; Master is graduate-textbook rigor. Each unit has all three depths in one document; the
        site filters what you see.
      </p>
      <p class="muted">
        v0.1 pilot is apex-first: Master-tier graduate content seeded at the top of Fast Track, with
        prerequisite chains filling in as we produce more units.
      </p>

      <h2>Featured units</h2>
      <ul class="unit-list">
        {data.featured.map((u: any) => (
          <li>
            <a href={`/u/${u.id}`}>
              <code class="unit-id">{u.id}</code> {u.title}
            </a>
            <span class={`badge badge--${u.status}`}>{u.status}</span>
            {u.tiers_present.map((t: string) => (
              <span class={`tier-pill tier-pill--${t}`}>{t}</span>
            ))}
          </li>
        ))}
      </ul>

      <p>
        <a href="/units" class="btn btn--primary">Browse all units →</a>
      </p>
    </article>
  );
}
