// Citation footnote panel — vanilla JS, zero deps, deferred-loaded.
// On click of any `<span class="ref" data-ref-source data-ref-locator>` or
// `<span class="unit-ref" ...>` element, slides a side panel in from the
// right with full source detail. Esc / click-outside / close-button to close.
//
// We deliberately avoid Preact islands here — see Neutron SSR-island issues
// in pilot. This script works without hydration overhead and degrades to
// the existing title-tooltip if JS is disabled.

interface SourceMeta {
  name: string;
  archive_path?: string;
  license?: string;
  attribution?: string;
}

// Static metadata baked at build-time — populated below from the codex
// archive's SOURCES.md. Updated as the archive grows.
const SOURCE_META: Record<string, SourceMeta> = {
  tong: {
    name: "David Tong — DAMTP Lecture Notes (Cambridge)",
    archive_path: "reference/tong/",
    license: "Author-hosted, educational use; attribution required.",
    attribution:
      'Tong, D. — DAMTP Cambridge lecture notes (damtp.cam.ac.uk/user/tong/)',
  },
  "quantum-well": {
    name: "The Quantum Well (Boris)",
    archive_path: "reference/quantum-well/md/",
    license: "Creative Commons BY 4.0",
    attribution: 'Boris — "The Quantum Well", CC BY 4.0',
  },
  milekic: {
    name: "Nikola Milekic — Digital Garden",
    archive_path: "reference/milekic/md/",
    license: "Personal site; reference use only.",
  },
  "fast-track": {
    name: "The Fast Track (sheafification.com)",
    archive_path: "reference/fast-track/",
    license: "Personal site; reference use only.",
  },
  goodtheorist: {
    name: "Gerard 't Hooft — How to Become a Good Theoretical Physicist",
    archive_path: "reference/goodtheorist/",
  },
  rigetti: {
    name: "Susan Rigetti — Physics Self-Study Guide",
    archive_path: "reference/rigetti/",
  },
  "quantum-country": {
    name: "Quantum Country (Matuschak & Nielsen)",
    archive_path: "reference/quantum-country/",
  },
  diegovera: {
    name: "Diego Vera — Self-Study Essays",
    archive_path: "reference/diegovera/",
  },
  jimmyqin: {
    name: "Jimmy Qin — Study Notes",
    archive_path: "reference/jimmyqin/",
  },
  TODO_REF: {
    name: "Pending source",
    license:
      "This citation will resolve once the source is acquired. See NEED_TO_SOURCE.md.",
  },
};

const PANEL_HTML = `
<div class="cite-panel" id="cite-panel" role="dialog" aria-modal="false" aria-hidden="true">
  <div class="cite-panel__inner">
    <header class="cite-panel__header">
      <h2 class="cite-panel__title">Citation</h2>
      <button class="cite-panel__close" type="button" aria-label="Close citation">×</button>
    </header>
    <div class="cite-panel__body" id="cite-panel-body"></div>
  </div>
</div>
`;

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderRefBody(source: string, locator: string): string {
  const meta = SOURCE_META[source];
  if (!meta) {
    return `
      <p class="cite-panel__source-name">${escapeHtml(source)}</p>
      ${locator ? `<p class="cite-panel__locator">${escapeHtml(locator)}</p>` : ""}
      <p class="cite-panel__muted">No metadata registered for this source. Add it to <code>src/lib/citation-panel.ts</code>.</p>
    `;
  }
  const lines: string[] = [];
  lines.push(`<p class="cite-panel__source-name">${escapeHtml(meta.name)}</p>`);
  if (locator) {
    lines.push(`<p class="cite-panel__locator"><strong>Locator:</strong> ${escapeHtml(locator)}</p>`);
  }
  if (meta.license) {
    lines.push(`<p class="cite-panel__license"><strong>License:</strong> ${escapeHtml(meta.license)}</p>`);
  }
  if (meta.attribution) {
    lines.push(`<p class="cite-panel__attribution"><strong>Cite as:</strong> ${escapeHtml(meta.attribution)}</p>`);
  }
  if (meta.archive_path) {
    lines.push(
      `<p class="cite-panel__archive"><strong>In archive:</strong> <code>${escapeHtml(meta.archive_path)}</code></p>`,
    );
  }
  if (source === "TODO_REF") {
    lines.push(
      `<p class="cite-panel__pending">⏳ This citation is pending source acquisition. See <code>NEED_TO_SOURCE.md</code>.</p>`,
    );
  }
  return lines.join("\n");
}

function renderUnitBody(unitId: string, isPending: boolean): string {
  if (isPending) {
    return `
      <p class="cite-panel__source-name">Cross-reference: <code>${escapeHtml(unitId)}</code></p>
      <p class="cite-panel__pending">⏳ Unit not yet shipped. Reserved id; will become a working link once produced.</p>
    `;
  }
  return `
    <p class="cite-panel__source-name">Cross-reference: <code>${escapeHtml(unitId)}</code></p>
    <p><a class="btn btn--primary" href="/u/${escapeHtml(unitId)}">Open unit →</a></p>
  `;
}

export function initCitationPanel() {
  if (typeof document === "undefined") return;

  // Inject panel HTML once.
  const wrapper = document.createElement("div");
  wrapper.innerHTML = PANEL_HTML;
  document.body.appendChild(wrapper.firstElementChild!);

  const panel = document.getElementById("cite-panel")!;
  const body = document.getElementById("cite-panel-body")!;
  const titleEl = panel.querySelector(".cite-panel__title")!;
  const closeBtn = panel.querySelector(".cite-panel__close")!;

  const open = (titleText: string, html: string) => {
    titleEl.textContent = titleText;
    body.innerHTML = html;
    panel.setAttribute("aria-hidden", "false");
    document.body.classList.add("cite-panel--open");
  };
  const close = () => {
    panel.setAttribute("aria-hidden", "true");
    document.body.classList.remove("cite-panel--open");
  };

  closeBtn.addEventListener("click", close);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") close();
  });
  panel.addEventListener("click", (e) => {
    if (e.target === panel) close(); // backdrop click
  });

  document.addEventListener("click", (e) => {
    const target = e.target as Element | null;
    if (!target) return;
    const refEl = target.closest(".ref") as HTMLElement | null;
    if (refEl) {
      e.preventDefault();
      const source = refEl.dataset.refSource || "unknown";
      const locator = refEl.dataset.refLocator || "";
      open("Citation", renderRefBody(source, locator));
      return;
    }
    const unitEl = target.closest(".unit-ref") as HTMLElement | null;
    if (unitEl) {
      const isPending = unitEl.classList.contains("unit-ref--pending");
      if (!isPending) return; // let the link work normally
      e.preventDefault();
      const code = unitEl.querySelector("code");
      const unitId = code?.textContent?.trim() || "?";
      open("Cross-reference", renderUnitBody(unitId, true));
    }
  });
}
