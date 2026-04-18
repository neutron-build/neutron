import { useEffect, useRef } from "preact/hooks";

const CACHE_KEY = "tebian_dl_count";
const CACHE_TTL = 60 * 60 * 1000; // 1 hour
const RELEASES_URL = "https://api.github.com/repos/tebian-os/tebian/releases";
const FALLBACK_TEXT = "Latest release";

export function DownloadCounter() {
  const ref = useRef<HTMLParagraphElement>(null);

  useEffect(() => {
    async function load() {
      const el = ref.current;
      if (!el) return;

      const cached = localStorage.getItem(CACHE_KEY);
      if (cached) {
        try {
          const { count, ts } = JSON.parse(cached);
          if (typeof count === "number" && Date.now() - ts < CACHE_TTL) {
            el.textContent = `${count.toLocaleString()} downloads`;
            return;
          }
        } catch {
          // Invalid cache entry — ignore and re-fetch.
        }
      }

      try {
        const res = await fetch(RELEASES_URL);
        if (!res.ok) {
          // Rate-limited or missing repo; keep the fallback text.
          console.warn(`[DownloadCounter] GitHub releases request: HTTP ${res.status}`);
          return;
        }
        const releases = await res.json();
        const count = releases.reduce(
          (total: number, release: any) =>
            total + release.assets.reduce((sum: number, asset: any) => sum + (asset.download_count ?? 0), 0),
          0,
        );
        el.textContent = `${count.toLocaleString()} downloads`;
        localStorage.setItem(CACHE_KEY, JSON.stringify({ count, ts: Date.now() }));
      } catch (err) {
        console.warn("[DownloadCounter] fetch failed", err);
      }
    }

    load();
  }, []);

  // SSR with a fallback so the element is never empty even if hydration or
  // the API request fails. Hydration overwrites with the live count.
  return <p class="dl-count" ref={ref}>{FALLBACK_TEXT}</p>;
}
