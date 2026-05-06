import { readFileSync } from "node:fs";
import { resolve } from "node:path";

export function head() {
  return {
    title: "Dependency map | Codex",
    description: "Curriculum prerequisite graph. Apex-first; pending units fill in over time.",
  };
}

export async function loader() {
  const depsPath = resolve(import.meta.dirname, "../../../../../codex/manifests/deps.json");
  let deps: any = { edges: [], shipped: [], pending: [] };
  try {
    deps = JSON.parse(readFileSync(depsPath, "utf-8"));
  } catch (e) {
    // empty fallback
  }
  return {
    edgeCount: (deps.edges || []).length,
    shipped: deps.shipped || [],
    pending: deps.pending || [],
    edges: (deps.edges || []) as Array<{ from: string; to: string; state: string }>,
    notes: (deps._notes || {}) as Record<string, string>,
  };
}

export default function DagPage({ data }: { data: any }) {
  // For v0.1 we render the DAG as a structured list grouped by edge state.
  // Visualisation will arrive when the unit count justifies the layout cost.
  const byTo: Record<string, Array<{ from: string; state: string }>> = {};
  for (const e of data.edges) {
    if (!byTo[e.to]) byTo[e.to] = [];
    byTo[e.to].push({ from: e.from, state: e.state });
  }
  const targets = Object.keys(byTo).sort();

  return (
    <article class="prose">
      <h1>Dependency map</h1>
      <p class="muted">
        Curriculum prerequisite graph from <code>manifests/deps.json</code>. {data.edgeCount} edges across{" "}
        {data.shipped.length} shipped and {data.pending.length} pending units.
      </p>

      <h2>Shipped units</h2>
      {data.shipped.length === 0 ? (
        <p class="muted">None yet.</p>
      ) : (
        <ul>
          {data.shipped.map((id: string) => (
            <li>
              <a href={`/u/${id}`}><code>{id}</code></a>
              {data.notes[id] && <span class="muted"> — {data.notes[id]}</span>}
            </li>
          ))}
        </ul>
      )}

      <h2>Pending units (declared but not yet produced)</h2>
      <p class="muted">
        These ids are referenced by shipped units as prerequisites or successors. They will be filled in by future production waves.
      </p>
      {data.pending.length === 0 ? (
        <p class="muted">None.</p>
      ) : (
        <ul>
          {data.pending.map((id: string) => (
            <li>
              <code>{id}</code>
              {data.notes[id] && <span class="muted"> — {data.notes[id]}</span>}
            </li>
          ))}
        </ul>
      )}

      <h2>Edges (each unit's incoming dependencies)</h2>
      {targets.length === 0 ? (
        <p class="muted">No edges yet.</p>
      ) : (
        <ul>
          {targets.map((t: string) => (
            <li>
              <code>{t}</code>{" requires: "}
              {byTo[t].map((e, i) => (
                <>
                  {i > 0 && ", "}
                  <code>{e.from}</code>{" "}
                  <span class={`badge badge--${e.state === "shipped" ? "shipped" : "pending"}`}>
                    {e.state}
                  </span>
                </>
              ))}
            </li>
          ))}
        </ul>
      )}

      <p class="muted">
        Visual graph rendering (force-directed or layered DAG) will arrive when the unit count justifies the layout work — likely once Wave C (units #3–10) is done.
      </p>
    </article>
  );
}
