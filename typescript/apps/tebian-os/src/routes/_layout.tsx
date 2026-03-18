import "../styles/global.css";
import { Island } from "neutron/client";
import { ThemeToggle } from "../components/ThemeToggle";

export function head() {
  return {
    titleTemplate: "%s — Tebian",
    description: "Tebian — The Universal Usability Layer. Stable. Modular. Sovereign.",
    htmlAttrs: { lang: "en", "data-theme": "frappe" },
    headScripts: [
      {
        content: `(function(){var s=localStorage.getItem("tebian-theme")||"frappe";document.documentElement.setAttribute("data-theme",s)})();`,
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
    <>
      <Island component={ThemeToggle} client="load" id="theme-toggle" />
      {children}
    </>
  );
}
