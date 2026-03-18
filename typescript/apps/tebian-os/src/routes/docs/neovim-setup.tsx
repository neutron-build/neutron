import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Neovim Manual — Tebian" };
}

export default function NeovimSetup() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Neovim Manual</h1>
          <p class="meta">Replacing VSCode with Lua-based Neovim: Zero-Latency Development.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Problem with Electron (VSCode)</h2>
            <p>VSCode is the most popular editor in the world, but it has a fundamental flaw: it is an <strong>Electron</strong> app. Electron is a bundled Chromium browser. Every time you open VSCode, you are starting a web browser engine. This uses hundreds of megabytes of RAM and introduces a subtle input latency between your fingers and the screen.</p>

            <p>For a developer who spends 8-12 hours a day in an editor, this latency adds up. <strong>Neovim</strong>, written in C and configured in Lua, is the high-performance alternative.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The C-Based Engine</h3>
              <p>Neovim is a fork of Vim that prioritizes <strong>Asynchronous</strong> operations and modern <strong>Lua</strong> configuration. It is a single, compact C binary that starts in milliseconds.</p>
              <ul>
                <li><strong>Zero Startup Lag:</strong> Neovim opens instantly, even with complex configs.</li>
                <li><strong>Low Memory:</strong> An entire project open in Neovim uses ~50MB of RAM. VSCode uses 1GB+.</li>
                <li><strong>GPU Acceleration:</strong> In Tebian, Neovim runs inside <strong>Kitty</strong>, a GPU-accelerated terminal. Every keystroke is rendered at 144Hz+.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The LSP Architecture</h3>
              <p>VSCode's "IntelliSense" (autocompletion, go-to-definition) is powered by the **Language Server Protocol (LSP)**. Neovim has a <strong>Native LSP Client</strong> built in C. You get the same world-class completions for Rust, Go, Python, and TypeScript with zero framework overhead.</p>
              <ul>
                <li><strong>Mason:</strong> One-click installer for language servers inside Neovim.</li>
                <li><strong>nvim-cmp:</strong> Blazing fast autocompletion.</li>
                <li><strong>Telescope:</strong> Highly extensible fuzzy-finder for files and code.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. LazyVim: The "One-Click" Setup</h3>
              <p>Configuring Neovim from scratch can take weeks. Tebian's "Dev Mode" includes a pre-configured <strong>LazyVim</strong> setup. It's an opinionated, high-performance "distribution" of Neovim that feels like an IDE out of the box.</p>
              <ul>
                <li><strong>Lazy.nvim:</strong> Plugin manager that loads plugins only when needed.</li>
                <li><strong>Treesitter:</strong> Modern, fast syntax highlighting.</li>
                <li><strong>Which-Key:</strong> Displays available keybindings as you type.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. The "Learning Curve" Myth</h3>
              <p>Most developers avoid Vim because of the "Modal" editing (Normal/Insert/Visual). While it takes a week to learn the "H/J/K/L" keys, the speed gains are permanent. You will never have to reach for your mouse again.</p>
              <ul>
                <li><strong>VimTutor:</strong> Built-in 15-minute interactive tutorial.</li>
                <li><strong>Cheat Sheet:</strong> Tebian includes a "Vim Cheat Sheet" in the Fuzzel menu.</li>
                <li><strong>Muscle Memory:</strong> Once you learn Neovim, you will feel slow in every other editor.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Neovim on Tebian?</h2>
            <p>Neovim is the ultimate "Tebian Editor." It is minimal, modular, and written in C. It respects your hardware. When you combine Neovim's efficiency with Tebian's "Ghost Mode" (no OS distractions), you reach a level of <strong>Developer Flow</strong> that is impossible on any other platform.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
