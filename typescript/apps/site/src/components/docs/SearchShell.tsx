interface DocEntry {
  slug: string;
  data: { title: string; description?: string };
}

// Server-rendered search UI. The site ships zero framework runtime, so the
// interactivity lives in a plain /js/search.js script (matching nav.js etc.)
// rather than an Island. The docs collection is embedded as JSON for the script
// to filter client-side — no search-index build step.
export function SearchShell({ entries }: { entries?: DocEntry[] }) {
  const data = (entries ?? []).map((e) => ({
    slug: e.slug,
    title: e.data.title,
    description: e.data.description ?? "",
  }));
  // Escape `<` so a "</script>" inside any title/description can't break out of
  // the embedded JSON script tag.
  const json = JSON.stringify(data).replace(/</g, "\\u003c");

  return (
    <div class="docs-search">
      <button type="button" class="search-trigger" data-search-open aria-label="Search docs">
        <svg class="search-trigger-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <span class="search-trigger-text">Search docs</span>
        <span class="search-trigger-kbd">&#8984;K</span>
      </button>

      <div class="search-overlay" data-search-overlay hidden>
        <div class="search-modal" role="dialog" aria-modal="true" aria-label="Search documentation">
          <div class="search-input-wrap">
            <svg class="search-input-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
            <input type="text" class="search-input" data-search-input placeholder="Search documentation..." aria-label="Search documentation" />
            <kbd class="search-input-kbd">Esc</kbd>
          </div>
          <div class="search-results" data-search-results role="listbox" hidden></div>
          <div class="search-footer">
            <div class="search-footer-keys">
              <span class="search-footer-key"><kbd>&#8593;&#8595;</kbd> Navigate</span>
              <span class="search-footer-key"><kbd>&#9166;</kbd> Open</span>
              <span class="search-footer-key"><kbd>Esc</kbd> Close</span>
            </div>
          </div>
        </div>
      </div>

      <script
        type="application/json"
        data-search-data
        dangerouslySetInnerHTML={{ __html: json }}
      />
      <script src="/js/search.js" defer></script>
    </div>
  );
}
