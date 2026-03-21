import { useState, useEffect, useRef } from "preact/hooks";

interface SearchResult {
  slug: string;
  title: string;
  path: string;
}

export function Search({ entries }: { entries?: { slug: string; data: { title: string; description?: string } }[] }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [active, setActive] = useState(0);
  const modalRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen(true);
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  useEffect(() => {
    if (open) {
      requestAnimationFrame(() => inputRef.current?.focus());
    } else {
      setQuery("");
      setResults([]);
      setActive(0);
    }
  }, [open]);

  function search(q: string) {
    setQuery(q);
    setActive(0);
    if (!q.trim() || !entries) {
      setResults([]);
      return;
    }
    const lower = q.toLowerCase();
    const matched = entries
      .filter((e) => {
        const title = e.data.title.toLowerCase();
        const desc = (e.data.description || "").toLowerCase();
        return title.includes(lower) || desc.includes(lower) || e.slug.includes(lower);
      })
      .slice(0, 10)
      .map((e) => ({
        slug: e.slug,
        title: e.data.title,
        path: e.slug.replace(/\//g, " > "),
      }));
    setResults(matched);
  }

  function navigate(slug: string) {
    setOpen(false);
    window.location.href = `/docs/${slug}`;
  }

  function onKeyDown(e: KeyboardEvent) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => Math.min(i + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && results[active]) {
      navigate(results[active].slug);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  }

  return (
    <>
      <button class="search-trigger" onClick={() => setOpen(true)} aria-label="Search docs">
        <svg class="search-trigger-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <span class="search-trigger-text">Search</span>
        <span class="search-trigger-kbd">&#8984;K</span>
      </button>
      <div class={`search-overlay${open ? " is-open" : ""}`} onClick={(e) => { if (e.target === e.currentTarget) setOpen(false); }}>
        <div class="search-modal" ref={modalRef}>
          <div class="search-input-wrap">
            <svg class="search-input-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
            <input
              ref={inputRef}
              type="text"
              class="search-input"
              placeholder="Search documentation..."
              value={query}
              onInput={(e) => search((e.target as HTMLInputElement).value)}
              onKeyDown={onKeyDown}
            />
            <kbd class="search-input-kbd">Esc</kbd>
          </div>
          {results.length > 0 && (
            <div class="search-results" role="listbox">
              {results.map((r, i) => (
                <div
                  key={r.slug}
                  role="option"
                  aria-selected={i === active}
                  class="search-result"
                  onClick={() => navigate(r.slug)}
                  onMouseEnter={() => setActive(i)}
                >
                  <span class="search-result-title">{r.title}</span>
                  <span class="search-result-path">{r.path}</span>
                </div>
              ))}
            </div>
          )}
          {query && results.length === 0 && (
            <div class="search-no-results">No results for "{query}"</div>
          )}
          <div class="search-footer">
            <div class="search-footer-keys">
              <span class="search-footer-key"><kbd>&#8593;&#8595;</kbd> Navigate</span>
              <span class="search-footer-key"><kbd>&#9166;</kbd> Open</span>
              <span class="search-footer-key"><kbd>Esc</kbd> Close</span>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
