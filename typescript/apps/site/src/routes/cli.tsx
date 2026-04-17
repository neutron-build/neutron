import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "CLI - Neutron",
    description: "One Go binary, 17 commands. Scaffold, dev, build, migrate, deploy, studio, and MCP &mdash; all from neutron. 21 MB, cross-platform, zero dependencies.",
  };
}

export default function CliPage() {
  return (
    <ProductPage
      title="Neutron CLI"
      description="One 21 MB Go binary that handles scaffolding, dev servers, database migrations, builds, deploys, Studio, and MCP. Auto-detects your project's language and delegates to the right sub-toolchain."
      category="tool"
      status="available"
      accent="var(--accent-ts)"
      heroAccentRgb="49, 120, 198"
      heroTagline="One binary. Seventeen commands. Every language."
      stats={[
        { value: '17', label: 'Commands' },
        { value: '21 MB', label: 'Single Binary' },
        { value: 'Go', label: 'Cross-Compiled' },
        { value: '0', label: 'Dependencies' },
      ]}
    >
      <section>
        <h2>Install once. Run anything.</h2>
        <p>A single <code>neutron</code> binary handles every SDK in the ecosystem. It detects your project's language from <code>neutron.config.*</code> and invokes the right tool underneath &mdash; Vite for TypeScript, Cargo for Rust, Go's build system for Go, setuptools for Python, <code>zig build</code> for Zig, and so on. You don't learn seven command-line surfaces; you learn one.</p>
      </section>

      <CodeBlock filename="terminal">
        <pre><code>{`$ neutron new my-app                 # scaffold from 20+ templates
$ neutron dev                        # HMR dev server
$ neutron build                      # production build, all targets
$ neutron preview                    # serve the built output

$ neutron db migrate                 # apply pending migrations
$ neutron db studio                  # launch visual database manager
$ neutron db reset                   # wipe and re-seed

$ neutron deploy --target cloudflare # deploy to any configured adapter
$ neutron studio                     # standalone Studio server
$ neutron mcp --stdio                # MCP server for AI clients

$ neutron desktop dev                # Tauri desktop dev
$ neutron native run ios             # build + run on iOS sim
$ neutron native run android         # build + run on Android emulator`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="49, 120, 198">
        <div class="feature-card">
          <div class="feature-card__title">Scaffold</div>
          <div class="feature-card__desc"><code>neutron new</code> with 20+ templates: web app, API, full-stack, mobile (Expo Go), desktop (Tauri), worker, static site, documentation.</div>
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
          <div class="feature-card__title">Multi-target build</div>
          <div class="feature-card__desc">One command emits artifacts for every adapter your config declares: edge worker, Node server, static bundle, Lambda zip, Tauri installer.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Studio launcher</div>
          <div class="feature-card__desc"><code>neutron studio</code> boots the visual database manager against your local Nucleus. Browse all 14 data models; no install step.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">MCP server</div>
          <div class="feature-card__desc"><code>neutron mcp</code> exposes 17 tools over stdio or HTTP. OpenAI-compatible, <code>--dump-schema</code> for AI context priming.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Built in Go for a reason.</h2>
        <p>One static binary, cross-compiled to every OS and arch we support. No Node runtime to bring. No Python interpreter to pin. Install with a single curl, or grab a release from GitHub. The CLI is the same size and shape on Linux, macOS, Windows, and ARM &mdash; and it's how your CI servers will talk to Neutron too.</p>

        <CodeBlock filename="install">
          <pre><code>{`# macOS / Linux
curl -fsSL https://neutron.build/install.sh | sh

# Homebrew
brew install neutron-build/tap/neutron

# Windows
winget install neutron.neutron

# Or grab a release binary from GitHub
`}</code></pre>
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
