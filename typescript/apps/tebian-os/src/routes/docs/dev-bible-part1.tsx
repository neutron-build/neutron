import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Developer's Bible: Part 1 — Tebian" };
}

export default function DevBiblePart1() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Developer's Bible: Part 1</h1>
          <p class="meta">Zero-Config Toolchains: Mastering Rust, Go, Node, and Python Isolation.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Philosophy of Clean Hosts</h2>
            <p>The cardinal sin of development is "Global Installation." When you run <code>sudo npm install -g</code> or <code>pip install --user</code>, you are creating a "Dependency Hell" that will eventually break your system. In Tebian, we follow the <strong>Clean Host</strong> doctrine: Your global OS should only contain the kernel, the compositor, and the container engine.</p>

            <p>This treatise explains how to manage complex developer toolchains in user-space, ensuring that Project A's Node 18 dependency never conflicts with Project B's Node 20 requirement.</p>
          </section>

          <section class="mise-en-place">
            <h2>1. Mise: The Polyglot Version Manager</h2>
            <p>Historically, developers used separate tools for every language: <code>nvm</code> for Node, <code>pyenv</code> for Python, <code>rustup</code> for Rust, <code>goenv</code> for Go. This is bloat. Tebian standardizes on <strong>Mise</strong> (formerly RTX), a single, Rust-based binary that manages <em>all</em> your runtimes.</p>

            <h3>The .mise.toml Config</h3>
            <p>With Mise, you define your project's environment in a simple TOML file. When you <code>cd</code> into the directory, Mise automatically loads the correct versions into your <code>PATH</code>.</p>
            <pre><code>{`[tools]
node = "20.11.0"
python = "3.12"
rust = "1.76"
go = "1.22"

[env]
NODE_ENV = "development"
DATABASE_URL = "postgres://localhost:5432/mydb"`}</code></pre>
            <p>This file is committed to Git. When a new developer pulls your repo on a Tebian machine, they simply run <code>mise install</code>, and they have an identical environment to yours. Zero friction.</p>
          </section>

          <section class="direnv-automation">
            <h2>2. Direnv: The Shell Auto-Loader</h2>
            <p>Mise handles the binaries, but <strong>Direnv</strong> handles the context. It hooks into your shell (Bash/Zsh) and executes a <code>.envrc</code> script whenever you enter a directory. We use this to automatically activate Python virtual environments or load secrets.</p>

            <h3>The Python Workflow</h3>
            <p>Forget <code>source venv/bin/activate</code>. In Tebian, you create a <code>.envrc</code> file with one line:</p>
            <pre><code>layout python</code></pre>
            <p>The moment you <code>cd</code> into the folder, Direnv creates the venv (if missing) and activates it. Your prompt changes to show the active environment. When you <code>cd ..</code> out, it deactivates. It is seamless, C-level automation.</p>
          </section>

          <section class="rust-c-interop">
            <h2>3. Rust and C Interoperability</h2>
            <p>For systems programming, you need more than just a compiler; you need libraries (`openssl`, `libssl-dev`, `pkg-config`). On a standard distro, installing these globally can break system packages. Tebian solves this with <strong>Dev Containers.</strong></p>

            <p>We provide a <code>devcontainer.json</code> template that spins up a pure Debian container with all the necessary C headers pre-installed. Your editor (Neovim/VSCode) connects to this container. You are compiling code <em>inside</em> the container, but editing it on your host. Your host OS stays clean; your build environment is reproducible.</p>
          </section>

          <section class="node-isolation">
            <h2>4. The Node.js Trap (npm vs pnpm)</h2>
            <p>Node.js is notorious for consuming disk space. A standard <code>node_modules</code> folder can be 500MB. If you have 20 projects, that's 10GB of duplicated dependencies. Tebian defaults to <strong>pnpm</strong> (Performant NPM).</p>

            <p><code>pnpm</code> uses a global content-addressable store. If you use React 18 in 100 projects, it is stored on disk exactly <em>once</em>. It links the dependency into your project using hardlinks. This saves massive amounts of SSD space and makes installations instant.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Reproducible Workstation</h2>
            <p>By using Mise, Direnv, and pnpm, you turn your workstation into a stateless engine. You can wipe your machine, reinstall Tebian, pull your Git repos, and be back to work in 5 minutes. The configuration lives in the code, not in the OS. This is the definition of a Sovereign Developer.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
