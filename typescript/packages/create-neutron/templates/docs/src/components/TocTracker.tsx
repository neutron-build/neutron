import { useEffect } from "preact/hooks";

export function TocTracker() {
  useEffect(() => {
    const headings = document.querySelectorAll(".prose h2[id], .prose h3[id]");
    if (!headings.length) return;

    const tocLinks = document.querySelectorAll(".toc-link");
    if (!tocLinks.length) return;

    function clearActive() {
      tocLinks.forEach((link) => link.removeAttribute("aria-current"));
    }

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            clearActive();
            const id = entry.target.getAttribute("id");
            const link = document.querySelector(`.toc-link[href="#${id}"]`);
            if (link) link.setAttribute("aria-current", "true");
            break;
          }
        }
      },
      { rootMargin: "-80px 0px -70% 0px", threshold: 0 },
    );

    headings.forEach((h) => observer.observe(h));
    return () => observer.disconnect();
  }, []);

  return null;
}
