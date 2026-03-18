import { useState, useEffect } from "preact/hooks";

const themes = ["dark", "frappe", "light"] as const;
const icons: Record<string, string> = { dark: "\u2600", frappe: "\u25D0", light: "\u263E" };

export function ThemeToggle() {
  const [icon, setIcon] = useState(icons.frappe);

  useEffect(() => {
    const saved = localStorage.getItem("tebian-theme") || "frappe";
    setIcon(icons[saved] || icons.frappe);
  }, []);

  function handleClick() {
    const html = document.documentElement;
    const current = html.getAttribute("data-theme") || "frappe";
    const idx = themes.indexOf(current as (typeof themes)[number]);
    const next = themes[(idx + 1) % themes.length];
    html.setAttribute("data-theme", next);
    localStorage.setItem("tebian-theme", next);
    setIcon(icons[next]);
  }

  return (
    <button class="theme-toggle" onClick={handleClick} title="Toggle theme">
      <span class="theme-icon">{icon}</span>
    </button>
  );
}
