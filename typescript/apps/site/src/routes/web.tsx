import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Web Platform - Neutron",
    description: "Neutron's TypeScript web framework and deployment targets.",
  };
}

export default function WebPage() {
  return <ProductOverview
    title="Neutron Web"
    description="The TypeScript and Preact framework for static sites and server-rendered applications."
    category="platform"
    accent="var(--accent-ts)"
    accentRgb="49, 120, 198"
    facts={[
      { label: "Routes", value: "File-based pages, parameters, catch-alls, nested layouts, and error boundaries" },
      { label: "Rendering", value: "Build-time static HTML or request-time app mode, selected per route" },
      { label: "Interaction", value: "Full app hydration or isolated Preact islands on static pages" },
      { label: "Targets", value: "Static, Node, Docker, Cloudflare, and Vercel adapters" },
    ]}
    links={[
      { label: "TypeScript", href: "/typescript" },
      { label: "Quickstart", href: "/docs/getting-started/installation" },
      { label: "Deployment", href: "/docs/deployment/adapters" },
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/typescript" },
    ]}
  />;
}
