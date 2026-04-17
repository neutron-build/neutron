import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Studio - Neutron",
    description: "Visual database manager for all 14 Nucleus data models. SQL browser, graph explorer, vector visualizer, time-series charts, and more &mdash; in one window.",
  };
}

export default function StudioPage() {
  return (
    <ProductPage
      title="Nucleus Studio"
      description="One visual surface for all 14 Nucleus data models. SQL tables, graph explorers, vector visualizers, time-series charts, document browsers &mdash; no more switching between pgAdmin, Neo4j Browser, and a Redis CLI."
      category="tool"
      status="available"
      accent="var(--accent-studio)"
      heroAccentRgb="236, 72, 153"
      heroTagline="See your data. All fourteen models."
      stats={[
        { value: '14', label: 'Model Browsers' },
        { value: 'Preact', label: 'SPA Frontend' },
        { value: '17', label: 'MCP Tools' },
        { value: 'Dogfood', label: 'Built with Neutron' },
      ]}
    >
      <section>
        <h2>One window for every data model.</h2>
        <p>Nucleus supports fourteen data models in a single engine. Studio gives each one a real UI: tables for SQL, force-directed graphs for edges, scatter plots for embeddings, charts for time-series, key browsers for KV, map views for geo. Connect once, browse everything &mdash; no switching apps, no re-entering credentials.</p>
      </section>

      <FeatureGrid columns={3} accentRgb="236, 72, 153">
        <div class="feature-card">
          <div class="feature-card__title">SQL browser</div>
          <div class="feature-card__desc">Table explorer, query editor with EXPLAIN ANALYZE, visual query plans, slow-query timeline. Edit rows in place with type-safe validation.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Vector visualizer</div>
          <div class="feature-card__desc">Similarity search UI, 2D/3D projection of embeddings (UMAP / t-SNE), HNSW graph traversal, k-NN inspector.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Graph explorer</div>
          <div class="feature-card__desc">Force-directed layout, Cypher-style query input, shortest-path visualizer, neighborhood expansion by click.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Time-series charts</div>
          <div class="feature-card__desc">Interactive plots with rolling aggregations, retention policy config, downsampling previews, alert rule tester.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Document browser</div>
          <div class="feature-card__desc">Collection view with JSON editor, schema inference, validator output, and diff-on-save.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">KV browser</div>
          <div class="feature-card__desc">Key scan with glob patterns, TTL management, atomic increments, tombstone view.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Full-text search</div>
          <div class="feature-card__desc">Query tester with BM25 score breakdown, analyzer inspector, relevance tuning presets.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Geo map</div>
          <div class="feature-card__desc">Leaflet-backed map with spatial query builder, geofence drawing, and nearest-neighbor inspection.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Pub/Sub monitor</div>
          <div class="feature-card__desc">Topic list with live message feed, filter by pattern, ack/nack for testing.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Blob browser</div>
          <div class="feature-card__desc">Content-addressed file browser, preview for common MIMEs, dedup stats, orphan cleanup.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Streams viewer</div>
          <div class="feature-card__desc">Append-only log with seek-to-offset, consumer group lag visualizer, event replay panel.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Columnar analytics</div>
          <div class="feature-card__desc">OLAP-style browser with compression ratio per column, SIMD scan monitor, roll-up builder.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Datalog IDE</div>
          <div class="feature-card__desc">Rule editor with live fact browser, recursive query tracer, derivation inspector.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">CDC timeline</div>
          <div class="feature-card__desc">Change-stream viewer with capture config, event timeline, downstream subscriber health.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Visual schema designer</div>
          <div class="feature-card__desc">Cross-model canvas for designing tables, graphs, and vector indexes together. Emit TypeScript / Rust types &amp; migrations.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Ships with an MCP server.</h2>
        <p>Studio includes a built-in MCP server with 17 tools for LLM clients &mdash; schema introspection, query execution, index recommendations, migration generation. Point Claude Desktop at it and your LLM can browse and reason over your database without an integration layer.</p>

        <CodeBlock filename="~/.config/claude-desktop/mcp.json">
          <pre><code>{`{
  "mcpServers": {
    "nucleus": {
      "command": "neutron",
      "args": ["studio", "--mcp", "--stdio"]
    }
  }
}`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>Built with Neutron</h3>
        <p>Studio is a Preact + signals SPA served by a Go embedded server (shared with the CLI) that talks to Nucleus. It's the reference proof that the Neutron stack handles a real, complex app &mdash; file-based routing, typed loaders, islands for interactive panels, and the same pgwire client every other Neutron SDK uses.</p>

        <h3>How to run it</h3>
        <p><code>neutron studio</code> from any project, or <code>neutron studio --url postgres://host/db</code> pointed at any Nucleus instance. Opens at <code>http://localhost:7000</code> by default.</p>

        <h3>Part of a bigger system</h3>
        <p>Studio is the visual counterpart to the CLI, the SDK clients, and the MCP server. Whatever language writes to Nucleus, Studio can browse it. Whatever LLM you connect to MCP, it reasons over the same schema Studio is showing you.</p>
      </section>
    </ProductPage>
  );
}
