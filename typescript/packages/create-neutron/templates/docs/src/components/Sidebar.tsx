import { Link } from "neutron/client";
import type { SidebarSection } from "../lib/sidebar";

function SidebarLink(props: {
  slug: string;
  title: string;
  active?: boolean;
}) {
  return (
    <Link
      to={`/docs/${props.slug}`}
      class="docs-sidebar-link"
      aria-current={props.active ? "page" : undefined}
    >
      {props.title}
    </Link>
  );
}

export function Sidebar({ tree }: { tree: SidebarSection[] }) {
  return (
    <>
      {tree.map((section) => {
        if (!section.title) {
          return section.items.map((item) => (
            <SidebarLink
              key={item.slug}
              slug={item.slug}
              title={item.title}
              active={item.active}
            />
          ));
        }

        const hasActive = section.items.some((item) => item.active);

        return (
          <details
            key={section.slug}
            class="docs-sidebar-section"
            open={hasActive || undefined}
          >
            <summary>{section.title}</summary>
            <div class="docs-sidebar-section-items">
              {section.items.map((item) => (
                <SidebarLink
                  key={item.slug}
                  slug={item.slug}
                  title={item.title}
                  active={item.active}
                />
              ))}
            </div>
          </details>
        );
      })}
    </>
  );
}
