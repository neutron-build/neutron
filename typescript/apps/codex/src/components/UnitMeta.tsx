// Sidebar metadata for a unit: prereqs, references, lean module, etc.

export default function UnitMeta({ unit }: { unit: any }) {
  return (
    <aside class="unit-meta">
      <section>
        <h3>Prerequisites</h3>
        {unit.prerequisites.length === 0 ? (
          <p class="muted">none — this is a leaf unit</p>
        ) : (
          <ul>
            {unit.prerequisites.map((pid: string) => (
              <li key={pid}>
                <code>{pid}</code>{" "}
                {unit.pending_prereqs && (
                  <span class="badge badge--pending">pending</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      {unit.successors.length > 0 && (
        <section>
          <h3>Used in</h3>
          <ul>
            {unit.successors.map((sid: string) => (
              <li key={sid}>
                <code>{sid}</code>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section>
        <h3>Tier anchors</h3>
        <dl class="anchor-list">
          {(["beginner", "intermediate", "master"] as const).map((t) => (
            <>
              <dt>{t}</dt>
              <dd>
                {unit.tier_anchors[t] === "deferred" ? (
                  <span class="muted">deferred</span>
                ) : (
                  <em>{unit.tier_anchors[t]}</em>
                )}
              </dd>
            </>
          ))}
        </dl>
      </section>

      <section>
        <h3>References</h3>
        <ul class="ref-list">
          {unit.references.map((r: any, i: number) => (
            <li key={i}>
              <strong>{r.source}</strong>
              {r.pending && <span class="badge badge--pending"> pending</span>}
              <br />
              <code>{r.path}</code>
              {r.locator && <span class="muted"> · {r.locator}</span>}
              {r.pointer && (
                <span class="muted">
                  {" "}· see {r.pointer}
                </span>
              )}
            </li>
          ))}
        </ul>
      </section>

      {unit.lean_module && (
        <section>
          <h3>Lean module</h3>
          <code>{unit.lean_module}</code>
          {unit.lean_status === "partial" && unit.lean_mathlib_gap && (
            <details>
              <summary>Mathlib gap</summary>
              <pre class="lean-gap">{unit.lean_mathlib_gap}</pre>
            </details>
          )}
        </section>
      )}

      {unit.human_reviewer && (
        <section>
          <h3>Reviewer</h3>
          <p>{unit.human_reviewer}</p>
        </section>
      )}

      {unit.estimated_time && (
        <section>
          <h3>Estimated time</h3>
          <ul>
            {Object.entries(unit.estimated_time).map(([t, v]) => (
              <li key={t}>
                <strong>{t}:</strong> {v as string}
              </li>
            ))}
          </ul>
        </section>
      )}
    </aside>
  );
}
