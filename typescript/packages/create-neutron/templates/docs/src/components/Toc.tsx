import { Island } from "neutron/client";
import { TocTracker } from "./TocTracker";

export interface TocEntry {
  id: string;
  text: string;
  level: number;
}

export function Toc({ entries }: { entries: TocEntry[] }) {
  if (!entries || entries.length === 0) return null;

  const filtered = entries.filter(
    (entry) => entry.level === 2 || entry.level === 3
  );

  if (filtered.length === 0) return null;

  return (
    <div class="docs-toc">
      <p class="docs-toc-title">On this page</p>
      <nav aria-label="Table of contents">
        {filtered.map((entry) => (
          <a
            key={entry.id}
            href={`#${entry.id}`}
            class={`docs-toc-link toc-link${entry.level === 3 ? " docs-toc-link--h3" : ""}`}
          >
            {entry.text}
          </a>
        ))}
      </nav>
      <Island component={TocTracker} client="idle" id="toc-tracker" />
    </div>
  );
}
