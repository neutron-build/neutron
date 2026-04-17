import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Web - Neutron",
    description: "Deploy the same Neutron app to the edge, a Node server, serverless functions, or a static CDN. Four adapters, one codebase, one build command.",
  };
}

export default function WebPage() {
  return (
    <ProductPage
      title="Neutron Web"
      description="Four deploy targets, one codebase. Edge, Node/Bun, serverless, static &mdash; the adapter is one line in your config."
      category="platform"
      status="available"
      accent="var(--accent-ts)"
      heroAccentRgb="49, 120, 198"
      heroTagline="Build once. Deploy where it fits."
      stats={[
        { value: '4', label: 'Deploy Targets' },
        { value: '1 line', label: 'Config Change' },
        { value: 'Vite', label: 'Same Build Tool' },
        { value: 'SSR+Static', label: 'Per-Route' },
      ]}
    >
      <section>
        <h2>The deploy target isn't a rewrite.</h2>
        <p>Most frameworks assume one infrastructure. Neutron Web assumes the opposite: that you'll prototype locally, ship the first version to a static CDN for $0, move to edge when latency matters, and eventually land on a Node box for WebSocket fan-out &mdash; and you shouldn't have to rewrite anything. The only thing that changes is a single line in <code>neutron.config.ts</code>.</p>
      </section>

      <CodeBlock filename="neutron.config.ts" annotation="Swap adapters in one line. Everything else stays the same.">
        <pre><code>{`import { defineConfig, adapterStatic, adapterNode, adapterCloudflare, adapterVercel } from "@neutron-build/core";

export default defineConfig({
  runtime: "preact",
  // adapter: adapterStatic({ precompress: true }),
  // adapter: adapterNode({ port: 8080 }),
  adapter: adapterCloudflare({ compatibilityFlags: ["nodejs_compat"] }),
  // adapter: adapterVercel({ regions: ["iad1"] }),
});`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={2} accentRgb="49, 120, 198">
        <div class="feature-card">
          <div class="feature-card__title">Edge</div>
          <div class="feature-card__desc">Cloudflare Workers, Vercel Edge, Deno Deploy, Fastly. Instant cold start, global deploy, KV &amp; D1 bindings where available.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Node / Bun</div>
          <div class="feature-card__desc">Long-running servers for full WebSocket and SSE support, connection pooling to Nucleus, and anything that outgrows a cold-start budget.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Serverless</div>
          <div class="feature-card__desc">AWS Lambda, Vercel Functions, Netlify Functions. Pay-per-request auto-scaling. Per-route split between static and function targets.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Static</div>
          <div class="feature-card__desc">Cloudflare Pages, Netlify, GitHub Pages, S3 + CloudFront. Pre-rendered HTML with islands that hydrate &mdash; zero server cost.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['Target', 'Cold start', 'Global latency', 'WebSockets', 'Nucleus access', 'Sweet spot']}
        rows={[
          ['Static', 'N/A', '<20ms (CDN)', 'No', 'Build-time only', 'Marketing sites, docs, blogs'],
          ['Edge', '~0ms', '<20ms worldwide', 'Durable Objects', 'Via connection pooler', 'Dynamic pages, global APIs'],
          ['Node / Bun', 'Always on', '50–200ms (1 region)', 'Full support', 'Direct pool', 'Long-running APIs, real-time'],
          ['Serverless', '50–200ms', '50–200ms', 'Limited', 'Pooler recommended', 'Spiky workloads, webhooks'],
        ]}
        highlightColumn={4}
        accentRgb="49, 120, 198"
      />

      <section>
        <h2>Hybrid by default.</h2>
        <p>The same app can split itself across targets. Static-render the marketing pages to a CDN, ship the app shell to the edge, keep the WebSocket server on Node. Per-route <code>config = &#123; mode: "static" &#125;</code> vs <code>&#123; mode: "app" &#125;</code> controls this, and the deploy step emits the right artifact for each target.</p>

        <CodeBlock filename="src/routes/docs/[...slug].tsx" annotation="Static-rendered doc page.">
          <pre><code>{`export const config = { mode: "static" };
// Pre-rendered to HTML at build. Zero JS. CDN-served.`}</code></pre>
        </CodeBlock>

        <CodeBlock filename="src/routes/chat.tsx" annotation="App-mode WebSocket handler.">
          <pre><code>{`export const config = { mode: "app", runtime: "node" };
// Long-running, keeps WebSocket connections open, hydrates in the browser.`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>Any web app where you don't yet know which deploy target will win. Marketing sites that need to be free. APIs that need to be fast. Apps that need WebSockets. Things that start static and grow into something else &mdash; without rewriting the router, the data layer, or the build.</p>

        <h3>Why per-route adapters?</h3>
        <p>Because the answer to "edge or server" is rarely the whole app. Product pages belong on a CDN; the checkout belongs on a server. Docs belong static; search belongs at the edge. Neutron builds all of them from one source and hands your deploy target exactly what it knows how to run.</p>

        <h3>Part of a bigger system</h3>
        <p>Whatever target you pick, your app reads from Nucleus through the same wire protocol. Add a Rust service for hot paths, a Python MCP server for AI tools, a Go worker for jobs &mdash; all reading the same database. Neutron Web is one client among many.</p>
      </section>
    </ProductPage>
  );
}
