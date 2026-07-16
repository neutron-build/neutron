interface DocsLayoutProps {
  children: any;
  title?: string;
  description?: string;
  currentPath?: string;
}

const sidebar = [
  {
    label: "Getting Started",
    items: [
      {
        label: "Installation",
        link: "/docs/getting-started/installation",
      },
      {
        label: "Project Structure",
        link: "/docs/getting-started/project-structure",
      },
      {
        label: "Your First Route",
        link: "/docs/getting-started/your-first-route",
      },
    ],
  },
  {
    label: "Routing",
    items: [
      {
        label: "File Conventions",
        link: "/docs/routing/file-conventions",
      },
      { label: "Static Routes", link: "/docs/routing/static-routes" },
      { label: "App Routes", link: "/docs/routing/app-routes" },
      { label: "Dynamic Routes", link: "/docs/routing/dynamic-routes" },
      { label: "Nested Layouts", link: "/docs/routing/nested-layouts" },
      {
        label: "Error Boundaries",
        link: "/docs/routing/error-boundaries",
      },
    ],
  },
  {
    label: "Data",
    items: [
      { label: "Loaders", link: "/docs/data/loaders" },
      { label: "Actions", link: "/docs/data/actions" },
      { label: "Forms", link: "/docs/data/forms" },
      { label: "Revalidation", link: "/docs/data/revalidation" },
      { label: "Type Safety", link: "/docs/data/type-safety" },
    ],
  },
  {
    label: "Data Layer",
    items: [
      { label: "Overview", link: "/docs/data-layer/overview" },
      { label: "Dragonfly", link: "/docs/data-layer/dragonfly" },
      {
        label: "Database & Drizzle",
        link: "/docs/data-layer/database",
      },
      { label: "Caching Loaders", link: "/docs/data-layer/caching" },
    ],
  },
  {
    label: "Rendering",
    items: [
      {
        label: "Static Rendering",
        link: "/docs/rendering/static-rendering",
      },
      {
        label: "Server-Side Rendering",
        link: "/docs/rendering/server-side-rendering",
      },
      { label: "Islands", link: "/docs/rendering/islands" },
      {
        label: "View Transitions",
        link: "/docs/rendering/view-transitions",
      },
    ],
  },
  {
    label: "Content",
    items: [
      {
        label: "Content Collections",
        link: "/docs/content/content-collections",
      },
      { label: "Schemas", link: "/docs/content/schemas" },
      { label: "Markdown & MDX", link: "/docs/content/markdown-mdx" },
    ],
  },
  {
    label: "Middleware",
    items: [
      {
        label: "Global Middleware",
        link: "/docs/middleware/global-middleware",
      },
      {
        label: "Route Middleware",
        link: "/docs/middleware/route-middleware",
      },
      {
        label: "Request Context",
        link: "/docs/middleware/request-context",
      },
    ],
  },
  {
    label: "Deployment",
    items: [
      { label: "Adapters", link: "/docs/deployment/adapters" },
      { label: "Node.js", link: "/docs/deployment/node" },
      { label: "Cloudflare", link: "/docs/deployment/cloudflare" },
      { label: "Vercel", link: "/docs/deployment/vercel" },
      { label: "Docker", link: "/docs/deployment/docker" },
      {
        label: "Static Hosting",
        link: "/docs/deployment/static-hosting",
      },
    ],
  },
  {
    label: "Configuration",
    items: [
      {
        label: "neutron.config.ts",
        link: "/docs/configuration/neutron-config",
      },
      { label: "TypeScript", link: "/docs/configuration/typescript" },
      {
        label: "Environment Variables",
        link: "/docs/configuration/environment-variables",
      },
    ],
  },
  {
    label: "API Reference",
    items: [
      { label: "Route Exports", link: "/docs/api/route-exports" },
      { label: "Components", link: "/docs/api/components" },
      { label: "Hooks", link: "/docs/api/hooks" },
      {
        label: "Server Utilities",
        link: "/docs/api/server-utilities",
      },
      { label: "Content API", link: "/docs/api/content-api" },
    ],
  },
  {
    label: "Verification",
    items: [
      { label: "Overview", link: "/docs/verification/overview" },
      { label: "Kani", link: "/docs/verification/kani" },
      { label: "Shuttle", link: "/docs/verification/shuttle" },
      { label: "Verus", link: "/docs/verification/verus" },
      { label: "Lean 4", link: "/docs/verification/lean4" },
      { label: "Quint", link: "/docs/verification/quint" },
    ],
  },
  {
    label: "Nucleus",
    items: [
      { label: "Overview", link: "/docs/nucleus/overview" },
      { label: "Quick Start", link: "/docs/nucleus/quickstart" },
      { label: "SQL", link: "/docs/nucleus/sql" },
      { label: "Key-Value", link: "/docs/nucleus/key-value" },
      { label: "Vector Search", link: "/docs/nucleus/vector" },
      { label: "Documents", link: "/docs/nucleus/document" },
      { label: "Graph", link: "/docs/nucleus/graph" },
      { label: "Full-Text Search", link: "/docs/nucleus/fulltext" },
      { label: "Time Series", link: "/docs/nucleus/timeseries" },
      { label: "Columnar", link: "/docs/nucleus/columnar" },
      { label: "Blob Storage", link: "/docs/nucleus/blob" },
      { label: "Datalog", link: "/docs/nucleus/datalog" },
      { label: "Streams", link: "/docs/nucleus/streams" },
      { label: "Geospatial", link: "/docs/nucleus/geo" },
      { label: "Configuration", link: "/docs/nucleus/configuration" },
      { label: "PubSub", link: "/docs/nucleus/pubsub" },
      {
        label: "Change Data Capture",
        link: "/docs/nucleus/cdc",
      },
      { label: "Security", link: "/docs/nucleus/security" },
      { label: "Replication", link: "/docs/nucleus/replication" },
    ],
  },
  {
    label: "Rust Framework",
    items: [
      { label: "Overview", link: "/docs/rust/overview" },
      { label: "Quick Start", link: "/docs/rust/quickstart" },
      { label: "Routing", link: "/docs/rust/routing" },
      { label: "Middleware", link: "/docs/rust/middleware" },
      { label: "Authentication", link: "/docs/rust/authentication" },
      { label: "Database", link: "/docs/rust/database" },
      { label: "WebSocket & SSE", link: "/docs/rust/realtime" },
      { label: "Crates", link: "/docs/rust/crates" },
      { label: "Deployment", link: "/docs/rust/deployment" },
    ],
  },
  {
    label: "Go Framework",
    items: [
      { label: "Overview", link: "/docs/go/overview" },
      { label: "Quick Start", link: "/docs/go/quickstart" },
      { label: "Routing", link: "/docs/go/routing" },
      { label: "Middleware", link: "/docs/go/middleware" },
      { label: "Database", link: "/docs/go/database" },
      { label: "Real-Time", link: "/docs/go/realtime" },
      { label: "Authentication", link: "/docs/go/authentication" },
      { label: "Deployment", link: "/docs/go/deployment" },
    ],
  },
  {
    label: "Python Framework",
    items: [
      { label: "Overview", link: "/docs/python/overview" },
      { label: "Quick Start", link: "/docs/python/quickstart" },
      { label: "Routing", link: "/docs/python/routing" },
      { label: "Database", link: "/docs/python/database" },
      { label: "Middleware", link: "/docs/python/middleware" },
      { label: "Real-Time", link: "/docs/python/realtime" },
      { label: "Deployment", link: "/docs/python/deployment" },
    ],
  },
  {
    label: "Elixir Framework",
    items: [
      { label: "Overview", link: "/docs/elixir/index" },
      { label: "Getting Started", link: "/docs/elixir/getting-started" },
      { label: "Nucleus", link: "/docs/elixir/nucleus" },
      { label: "Real-Time", link: "/docs/elixir/realtime" },
    ],
  },
  {
    label: "CLI",
    items: [
      { label: "Overview", link: "/docs/cli/overview" },
      { label: "Commands", link: "/docs/cli/commands" },
    ],
  },
  {
    label: "Studio",
    items: [
      { label: "Overview", link: "/docs/studio/overview" },
      { label: "Query Editor", link: "/docs/studio/query-editor" },
      {
        label: "Schema Designer",
        link: "/docs/studio/schema-designer",
      },
    ],
  },
  {
    label: "Native (iOS/Android)",
    items: [
      { label: "Overview", link: "/docs/native/overview" },
      { label: "Components", link: "/docs/native/components" },
      { label: "Routing", link: "/docs/native/routing" },
      { label: "NeutronWind", link: "/docs/native/styling" },
      { label: "Turbo Modules", link: "/docs/native/turbomodules" },
    ],
  },
  {
    label: "Zig",
    items: [
      { label: "Overview", link: "/docs/zig/overview" },
      { label: "Quickstart", link: "/docs/zig/quickstart" },
      { label: "Layers", link: "/docs/zig/layers" },
      { label: "Nucleus Client", link: "/docs/zig/nucleus-client" },
    ],
  },
  {
    label: "Julia",
    items: [{ label: "Overview", link: "/docs/julia/overview" }],
  },
  {
    label: "Mojo",
    items: [{ label: "Overview", link: "/docs/mojo/overview" }],
  },
];

export default function DocsLayout({
  children,
  title,
  description,
  currentPath = "",
}: DocsLayoutProps) {
  return (
    <div class="docs-container container">
        <aside class="sidebar">
          {sidebar.map((section) => (
            <div class="sidebar-section" key={section.label}>
              <h3>{section.label}</h3>
              <ul>
                {section.items.map((item) => (
                  <li key={item.link}>
                    <a
                      href={item.link}
                      class={
                        currentPath === item.link ? "active" : undefined
                      }
                    >
                      {item.label}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </aside>
        <main id="main-content" class="docs-content">
          <div class="content-wrapper">{children}</div>
        </main>
    </div>
  );
}
