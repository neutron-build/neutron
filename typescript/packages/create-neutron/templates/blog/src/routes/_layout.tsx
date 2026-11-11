import type { JsxChildren } from "@neutron-build/core/client";
import "../site.css";

export function head() {
  return {
    defaultTitle: "__PROJECT_NAME__",
    titleTemplate: "%s - __PROJECT_NAME__",
    link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" },
  };
}

export default function Layout(props: { children?: JsxChildren }) {
  return (
    <div className="site">
      <header>
        <a className="name" href="/">__PROJECT_NAME__</a>
      </header>
      <main>{props.children}</main>
    </div>
  );
}
