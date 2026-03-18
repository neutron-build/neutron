import { useEffect, useRef } from "preact/hooks";

const CACHE_KEY = "tebian_dl_count";
const CACHE_TTL = 60 * 60 * 1000; // 1 hour

export function DownloadCounter() {
  const ref = useRef<HTMLParagraphElement>(null);

  useEffect(() => {
    async function load() {
      const el = ref.current;
      if (!el) return;

      const cached = localStorage.getItem(CACHE_KEY);
      if (cached) {
        const { count, ts } = JSON.parse(cached);
        if (Date.now() - ts < CACHE_TTL) {
          el.textContent = `${count.toLocaleString()} downloads`;
          return;
        }
      }

      try {
        const res = await fetch("https://api.github.com/repos/tebian-os/tebian/releases");
        if (!res.ok) return;
        const releases = await res.json();
        const count = releases.reduce(
          (total: number, release: any) =>
            total + release.assets.reduce((sum: number, asset: any) => sum + asset.download_count, 0),
          0,
        );
        if (count > 0) {
          el.textContent = `${count.toLocaleString()} downloads`;
          localStorage.setItem(CACHE_KEY, JSON.stringify({ count, ts: Date.now() }));
        }
      } catch {}
    }

    load();
  }, []);

  return <p class="dl-count" ref={ref}></p>;
}
