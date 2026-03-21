import "../styles/docs.css";
import { Island, ViewTransitions } from "neutron/client";
import { ThemeToggle } from "../components/ThemeToggle";

export function head() {
  return {
    titleTemplate: "%s — __PROJECT_NAME__",
    description: "__PROJECT_NAME__ Documentation",
    htmlAttrs: { lang: "en", "data-theme": "dark" },
    headScripts: [
      {
        content: `(function(){var s=localStorage.getItem("docs-theme")||"dark";document.documentElement.setAttribute("data-theme",s)})();`,
        id: "theme-init",
      },
    ],
  };
}

export default function RootLayout({ children }: { children?: unknown }) {
  return (
    <div class="docs-app">
      <ViewTransitions />
      <Island component={ThemeToggle} client="load" id="theme-toggle" />
      {children}
    </div>
  );
}
