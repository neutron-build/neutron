import { getCollection } from "neutron/content";
import { buildSidebarTree } from "../../lib/sidebar";
import { Sidebar } from "../../components/Sidebar";
import { Island } from "neutron/client";
import { SidebarToggle } from "../../components/SidebarToggle";
import { Search } from "../../components/Search";

export const config = { mode: "static" };

export async function loader({ request }: { request: Request }) {
  const docs = await getCollection("docs");
  const entries = docs
    .filter((d: any) => !d.data.draft)
    .map((d: any) => ({ slug: d.slug, data: d.data }));

  const url = new URL(request.url);
  const activeSlug = url.pathname.replace(/^\/docs\/?/, "").replace(/\/$/, "") || "index";

  const tree = buildSidebarTree(entries, activeSlug);
  return { entries, tree };
}

export default function DocsLayout({
  data,
  children,
}: {
  data: { entries: any[]; tree: any[] };
  children?: unknown;
}) {
  return (
    <div class="docs-layout">
      <Island component={SidebarToggle} client="load" id="sidebar-toggle" />
      <aside class="docs-sidebar">
        <div class="docs-sidebar-header">
          <a href="/docs" class="docs-sidebar-logo">__PROJECT_NAME__</a>
          <Island component={Search} client="idle" id="docs-search" props={{ entries: data.entries }} />
        </div>
        <nav class="docs-sidebar-nav">
          <Sidebar tree={data.tree} />
        </nav>
      </aside>
      <div class="docs-content">
        {children}
      </div>
    </div>
  );
}
