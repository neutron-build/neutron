import "../styles/global.css";
import { Island } from "neutron/client";
import { ThemeToggle } from "../components/ThemeToggle";
import { Nav } from "../components/Nav";
import { Footer } from "../components/Footer";

export function head() {
  return {
    titleTemplate: "%s — Tebian",
    description: "Tebian — The Universal Usability Layer. Stable. Modular. Sovereign.",
    htmlAttrs: { lang: "en", "data-theme": "frappe" },
    headScripts: [
      {
        content: `(function(){var s=localStorage.getItem("tebian-theme")||"frappe";document.documentElement.setAttribute("data-theme",s);var l=document.createElement("link");l.rel="icon";l.type="image/svg+xml";l.href="/favicon.svg";document.head.appendChild(l)})();`,
        id: "theme-init",
      },
    ],
  };
}

export default function Layout({
  children,
}: {
  children: preact.ComponentChildren;
}) {
  return (
    <div class="layout">
      <Island component={ThemeToggle} client="load" id="theme-toggle" />
      <Nav />
      <div class="page-content">{children}</div>
      <Footer />
    </div>
  );
}
