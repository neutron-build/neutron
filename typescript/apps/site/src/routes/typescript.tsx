import Terminal from "../components/Terminal";
import CodeBlock from "../components/CodeBlock";
import FeatureGrid from "../components/FeatureGrid";

export function head() {
  return {
    title: "TypeScript - Neutron",
    description: "Neutron's Preact framework for static pages and interactive applications.",
  };
}

export default function TypeScriptPage() {
  return (
    <main id="main-content">
      <section class="hero">
        <div class="container container--narrow">
          <p class="hero__flagship" data-animate>TypeScript framework</p>
          <h1 class="hero__title" data-animate>Build-time pages and server-rendered apps, route by route.</h1>
          <div class="hero__lines" data-animate>
            <p>Static HTML is the default. App routes add request-time rendering and client navigation; static pages can add interactive islands.</p>
          </div>
          <div class="hero__install" data-animate>
            <Terminal command="npm create neutron@latest" />
          </div>
        </div>
      </section>

      <section class="modes">
        <div class="container container--code">
          <h2 class="section-label">The central idea</h2>
          <div class="modes__grid">
            <div class="modes__col">
              <CodeBlock filename="routes/about.tsx">
                <pre><code>{`export const config = { mode: "static" };

export default function About() {
  return <h1>About</h1>;
}`}</code></pre>
              </CodeBlock>
              <span class="modes__label">Static HTML</span>
            </div>
            <div class="modes__divider"></div>
            <div class="modes__col">
              <CodeBlock filename="routes/dashboard.tsx">
                <pre><code>{`import { useLoaderData } from "@neutron-build/core";

export const config = { mode: "app" };

export async function loader() {
  return { user: "Alice" };
}

export default function Dashboard() {
  const data = useLoaderData<typeof loader>();
  return <h1>Welcome, {data.user}</h1>;
}`}</code></pre>
              </CodeBlock>
              <span class="modes__label">Interactive route</span>
            </div>
          </div>
        </div>
      </section>

      <section class="features">
        <div class="container container--narrow">
          <h2 class="section-label">Included</h2>
        </div>
        <FeatureGrid columns={2} accentRgb="49, 120, 198">
          <div class="feature-card">
            <div class="feature-card__title">File-based routing</div>
            <div class="feature-card__desc">Static, dynamic, and catch-all routes with nested layouts and error boundaries.</div>
          </div>
          <div class="feature-card">
            <div class="feature-card__title">Loaders and actions</div>
            <div class="feature-card__desc">Server-side reads and mutations colocated with the route that uses them.</div>
          </div>
          <div class="feature-card">
            <div class="feature-card__title">Selective hydration</div>
            <div class="feature-card__desc">Static routes can hydrate individual Preact islands when interaction is needed.</div>
          </div>
          <div class="feature-card">
            <div class="feature-card__title">Static and server adapters</div>
            <div class="feature-card__desc">Build for static hosting or a supported server runtime from the same route model.</div>
          </div>
        </FeatureGrid>
        <div class="container container--narrow language-overview__links">
          <a href="/docs/getting-started/installation">Quickstart</a>
          <a href="/docs">Documentation</a>
          <a href="https://github.com/neutron-build/neutron/tree/main/typescript">Source</a>
        </div>
      </section>
    </main>
  );
}
