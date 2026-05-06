import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { getCollection } from "@neutron-build/core";

export function head() {
  return {
    title: "Concepts | Codex",
    description: "Browse the canonical concept catalog. Each concept maps to at most one unit; prerequisites form the dependency graph.",
  };
}

interface CatalogEntry {
  id: string;
  title?: string;
  prerequisites: string[];
  master_anchor?: string;
  notes?: string;
}

// Parse CONCEPT_CATALOG.md into structured entries. The catalog is human-
// authored Markdown; we look for `### <id>` headings and their bullet
// metadata. Order is preserved from the file.
function parseCatalog(text: string): CatalogEntry[] {
  const out: CatalogEntry[] = [];
  const lines = text.split("\n");
  let cur: CatalogEntry | null = null;
  let inCodeBlock = false;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line.trim().startsWith("```")) {
      inCodeBlock = !inCodeBlock;
      continue;
    }
    if (inCodeBlock) continue;
    const headingMatch = line.match(/^###\s+([\w.\-]+)\s*$/);
    if (headingMatch) {
      if (cur) out.push(cur);
      cur = { id: headingMatch[1], prerequisites: [] };
      continue;
    }
    if (!cur) continue;
    const titleMatch = line.match(/^- \*\*title\*\*:\s*(.+)$/);
    if (titleMatch) cur.title = titleMatch[1].trim();
    const prereqMatch = line.match(/^- \*\*prerequisites\*\*:\s*(.+)$/);
    if (prereqMatch) {
      cur.prerequisites = prereqMatch[1]
        .split(",")
        .map((s: string) => s.trim().replace(/^`|`$/g, ""))
        .filter((s) => s.length);
    }
    const masterAnchorMatch = line.match(/^\s+master:\s*(.+)$/);
    if (masterAnchorMatch && !cur.master_anchor) cur.master_anchor = masterAnchorMatch[1].trim();
    const notesMatch = line.match(/^- \*\*notes\*\*:\s*(.+)$/);
    if (notesMatch) cur.notes = notesMatch[1].trim();
  }
  if (cur) out.push(cur);
  return out;
}

export async function loader() {
  const catalogPath = resolve(import.meta.dirname, "../../../../../../codex/CONCEPT_CATALOG.md");
  let entries: CatalogEntry[] = [];
  try {
    const text = readFileSync(catalogPath, "utf-8");
    entries = parseCatalog(text);
  } catch (e) {
    // If the file isn't reachable from the build context, render empty
    // page rather than crashing the build.
    entries = [];
  }

  // Cross-reference against shipped units so we can link those concepts.
  const units = await getCollection("units");
  const idByConcept: Record<string, string> = {};
  for (const u of units as any[]) {
    if (u.data.concept_catalog_id && u.data.status === "shipped") {
      idByConcept[u.data.concept_catalog_id] = u.data.id;
    }
  }

  return {
    entries: entries.map((e) => ({
      ...e,
      shipped_unit: idByConcept[e.id] ?? null,
    })),
  };
}

export default function ConceptsIndex({ data }: { data: any }) {
  return (
    <article class="concepts-index">
      <h1>Concepts</h1>
      <p class="muted">
        The canonical list of every concept the curriculum names. Each maps to at most one unit.
        Browse {data.entries.length} concepts; {data.entries.filter((e: any) => e.shipped_unit).length} have shipped units.
      </p>
      {data.entries.length === 0 ? (
        <p>No concepts loaded. Check that <code>CONCEPT_CATALOG.md</code> is reachable from the build.</p>
      ) : (
        <ul class="concept-list">
          {data.entries.map((e: any) => (
            <li class="concept-item">
              <header class="concept-item__head">
                <code class="concept-item__id">{e.id}</code>
                {e.shipped_unit && (
                  <a href={`/u/${e.shipped_unit}`} class="concept-item__link">
                    open unit {e.shipped_unit} →
                  </a>
                )}
                {!e.shipped_unit && (
                  <span class="badge badge--pending">no unit yet</span>
                )}
              </header>
              {e.title && <p class="concept-item__title"><strong>{e.title}</strong></p>}
              {e.notes && <p class="concept-item__notes muted">{e.notes}</p>}
              {e.prerequisites.length > 0 && (
                <p class="concept-item__prereqs">
                  <span class="muted">requires:</span>{" "}
                  {e.prerequisites.map((p: string, i: number) => (
                    <>
                      {i > 0 && ", "}
                      <code class="concept-item__prereq">{p}</code>
                    </>
                  ))}
                </p>
              )}
              {e.master_anchor && (
                <p class="concept-item__anchor muted">
                  Master anchor: <em>{e.master_anchor}</em>
                </p>
              )}
            </li>
          ))}
        </ul>
      )}
    </article>
  );
}
