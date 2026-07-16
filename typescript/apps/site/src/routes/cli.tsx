import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "CLI - Neutron",
    description: "One Go binary, 16 top-level commands. Scaffold, develop, migrate, generate, manage Nucleus, launch Studio, and serve MCP tools.",
  };
}

export default function CliPage() {
  return (
    <ProductPage
      title="Neutron CLI"
      description="One 20 MB Go binary that handles scaffolding, dev servers, migrations, code generation, Nucleus, Studio, native and desktop workflows, and MCP."
      category="tool"
      status="available"
      accent="var(--accent-ts)"
      heroAccentRgb="49, 120, 198"
      heroTagline="One binary. Sixteen commands. Every language."
      stats={[
        { value: '16', label: 'Top-Level Commands' },
        { value: '20 MB', label: 'Single Binary' },
        { value: 'Go', label: 'Cross-Compiled' },
        { value: '0', label: 'Dependencies' },
      ]}
    >
      <section>
        <h2>Install once. Run anything.</h2>
        <p>A single <code>neutron</code> binary handles every SDK in the ecosystem. It detects the project language from its standard manifest and delegates development to the appropriate toolchain. It also manages Nucleus, migrations, Studio, generated database types, native and desktop workflows, and MCP.</p>
      </section>

      <CodeBlock filename="terminal">
        <pre><code>{`$ neutron new my-app --lang typescript # scaffold a project
$ neutron dev                          # HMR dev server
$ neutron migrate                      # apply pending migrations
$ neutron migrate down                 # roll back one migration
$ neutron generate --table users --lang ts

$ neutron db start                     # start local Nucleus
$ neutron db reset                     # wipe and recreate the database
$ neutron studio                       # standalone Studio server
$ neutron mcp                          # MCP server over stdio

$ neutron desktop dev                # Tauri desktop dev
$ neutron native run ios             # build + run on iOS sim
$ neutron native run android         # build + run on Android emulator`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="49, 120, 198">
        <div class="feature-card">
          <div class="feature-card__title">Scaffold</div>
          <div class="feature-card__desc"><code>neutron new</code> scaffolds TypeScript, Python, Go, Rust, Zig, and Julia projects with native manifests and starter source.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Dev server</div>
          <div class="feature-card__desc">Vite-backed HMR for TypeScript, cargo-watch for Rust, air for Go, <code>uvicorn --reload</code> for Python. Same <code>neutron dev</code> in each.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Migrations</div>
          <div class="feature-card__desc">Generate, apply, and roll back schema changes across all 14 Nucleus models. Up/down files, SQL or TypeScript.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Typed generation</div>
          <div class="feature-card__desc"><code>neutron generate</code> reads database tables and emits typed Go, TypeScript, Rust, Python, Elixir, or Zig models.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Studio launcher</div>
          <div class="feature-card__desc"><code>neutron studio</code> boots the visual database manager against your local Nucleus. Browse all 14 data models; no install step.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">MCP server</div>
          <div class="feature-card__desc"><code>neutron mcp</code> exposes 19 tools over stdio or HTTP &mdash; database ops plus docs search. OpenAI-compatible, <code>--dump-schema</code> for AI context priming.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Built in Go for a reason.</h2>
        <p>One static binary, cross-compiled to every OS and arch we support. No Node runtime to bring. No Python interpreter to pin. Install with a single curl, or grab a release from GitHub. The CLI is the same size and shape on Linux, macOS, Windows, and ARM &mdash; and it's how your CI servers will talk to Neutron too.</p>

        <CodeBlock filename="install">
          <pre><code>{`# Download a prebuilt archive from GitHub Releases, or build from source:
git clone https://github.com/neutron-build/neutron.git
cd neutron/cli
make install

# TypeScript projects use the separate framework CLI:
npm install -D @neutron-build/cli
npx neutron-ts --help`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>Every day of working with Neutron. Local dev, CI pipelines, deploy scripts, one-off migrations, Studio sessions, MCP integrations with Claude or ChatGPT. If it's Neutron-shaped, it runs through <code>neutron</code>.</p>

        <h3>Why one CLI?</h3>
        <p>Because a language-agnostic ecosystem needs a language-agnostic front door. If every SDK had its own CLI you'd be memorizing seven sets of flags; instead, the Go binary delegates to whatever the underlying language expects, and you only learn one surface.</p>

        <h3>Part of a bigger system</h3>
        <p>The CLI knows about every other Neutron component: it can scaffold a Rust service that talks to a Nucleus cluster via <code>neutron-nucleus</code>, spin up Studio against it, and deploy both through the same build pipeline. One tool for the whole stack.</p>
      </section>
    </ProductPage>
  );
}
