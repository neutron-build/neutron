import { getCollection, getEntry } from "@neutron-build/core";
import UnitMeta from "../../components/UnitMeta";

export async function getStaticPaths() {
  const units = await getCollection("units");
  return units.map((u: any) => ({ params: { id: u.data.id } }));
}

export async function loader({ params }: { params: { id: string } }) {
  const all = await getCollection("units");
  const match = all.find((u: any) => u.data.id === params.id);
  if (!match) {
    throw new Response("Unit not found", { status: 404 });
  }
  const { Content } = await match.render();
  return { unit: match.data, Content };
}

export function head({ data }: { data: any }) {
  return {
    title: `${data.unit.id} — ${data.unit.title} | Codex`,
    description: `Codex unit ${data.unit.id}: ${data.unit.title}. Tiers present: ${data.unit.tiers_present.join(", ")}.`,
  };
}

export default function UnitPage({ data }: { data: any }) {
  const u = data.unit;
  const tierLabel = u.tiers_present.length === 1
    ? `${u.tiers_present[0][0].toUpperCase()}${u.tiers_present[0].slice(1)}-only`
    : `${u.tiers_present.length} tiers`;

  return (
    <article class="unit-page">
      <header class="unit-header">
        <p class="unit-breadcrumb">
          <code>{u.id}</code> · {u.section} / {u.chapter}
        </p>
        <h1>{u.title}</h1>
        <div class="unit-pills">
          <span class={`badge badge--${u.status}`}>{u.status}</span>
          <span class="badge">{tierLabel}</span>
          <span class={`badge badge--lean-${u.lean_status}`}>Lean: {u.lean_status}</span>
          {u.pending_prereqs && (
            <span class="badge badge--warn">pending prereqs</span>
          )}
        </div>
        <p class="unit-anchors">
          <strong>Anchor (Master):</strong>{" "}
          <em>{u.tier_anchors.master === "deferred" ? "deferred" : u.tier_anchors.master}</em>
        </p>
      </header>

      <div class="unit-body-wrap">
        <div class="unit-body">
          <data.Content />
        </div>
        <UnitMeta unit={u} />
      </div>
    </article>
  );
}
