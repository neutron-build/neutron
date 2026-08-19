import type { ComponentChildren } from "preact";
import "../styles/docs.css";
import { Island, ViewTransitions } from "@neutron-build/core/client";
import { ThemeToggle } from "../components/ThemeToggle";

export function head() {
  return {
    titleTemplate: "%s — init-smoke-docs",
    description: "init-smoke-docs Documentation",
    link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" },
    htmlAttrs: { lang: "en", "data-theme": "dark" },
    headScripts: [
      {
        content: `(function(){var s=localStorage.getItem("docs-theme")||"dark";document.documentElement.setAttribute("data-theme",s)})();`,
        id: "theme-init",
      },
    ],
  };
}

export default function RootLayout({ children }: { children?: ComponentChildren }) {
  return (
    <div class="docs-app">
      <ViewTransitions />
      <Island component={ThemeToggle} client="load" id="theme-toggle" />
      {children}
    </div>
  );
}
