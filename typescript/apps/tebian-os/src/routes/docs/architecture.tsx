import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Architecture — Tebian" };
}

export default function Architecture() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Documentation</span>
          <h1>Architecture</h1>
          <p class="meta">Complexity is a choice. We choose zero.</p>
        </header>
        <article class="content">
          <section class="bloat-vs-logic">
            <h2>The Comparison</h2>
            <div class="comparison-grid">
              <div class="comp-box traditional">
                <h3>Traditional Distro</h3>
                <ul>
                  <li>Forked kernel</li>
                  <li>Patched desktop environment</li>
                  <li>Custom repo servers</li>
                  <li>Branding assets everywhere</li>
                  <li>System-wide hooks</li>
                  <li>Complex installer (20+ questions)</li>
                </ul>
                <p class="result">Result: Massive maintenance, inevitable bugs.</p>
              </div>
              <div class="comp-box tebian">
                <h3>Tebian</h3>
                <ul>
                  <li>Stock Debian kernel</li>
                  <li>Pure upstream packages</li>
                  <li>Uses official Debian repos</li>
                  <li>Zero branding in system files</li>
                  <li>Everything in <code>~/Tebian/</code></li>
                  <li>One question: Desktop? Y/n</li>
                </ul>
                <p class="result">Result: Zero maintenance, rock solid stability.</p>
              </div>
            </div>
          </section>

          <section class="layers">
            <h2>The Stack</h2>
            <div class="stack-diagram">
              <div class="stack-layer t">
                <span class="layer-name">Tebian</span>
                <span class="layer-desc">1 Folder. 1 Script. The Interface.</span>
              </div>
              <div class="stack-layer d">
                <span class="layer-name">Debian</span>
                <span class="layer-desc">50,000 Packages. Stability. The Foundation.</span>
              </div>
              <div class="stack-layer l">
                <span class="layer-name">Linux</span>
                <span class="layer-desc">The Kernel. Hardware abstraction.</span>
              </div>
            </div>
            <p>Each layer removes a level of complexity. We don't reinvent the wheel; we just give you the steering wheel.</p>
          </section>

          <section class="math">
            <h2>The 3-Package Math</h2>
            <p>A functional desktop doesn't require thousands of packages. Tebian's core is built on three high-performance C binaries:</p>
            <div class="math-grid">
              <div class="math-box">
                <span class="math-pkg">Sway</span>
                <span class="math-desc">Compositor (C)</span>
              </div>
              <span class="math-plus">+</span>
              <div class="math-box">
                <span class="math-pkg">Fuzzel</span>
                <span class="math-desc">Launcher & UI (C)</span>
              </div>
              <span class="math-plus">+</span>
              <div class="math-box">
                <span class="math-pkg">NetworkManager</span>
                <span class="math-desc">Connectivity (C)</span>
              </div>
            </div>
            <p class="math-total">Total: <strong>~16MB</strong> installed. Zero background bloat. Zero Python/JS overhead.</p>
          </section>

          <section class="one-folder">
            <h2>The One Folder Rule</h2>
            <p>Tebian follows a strict rule: <strong>Do not touch the base system.</strong></p>
            <p>Every configuration, every script, and every Tebian-specific asset lives in <code>~/Tebian/</code> (or symlinked from it). If you delete that folder, you are back to a pure, stock Debian system instantly. No traces. No scars.</p>
            <div class="safety-net">
              <h3>Ricing without Killing</h3>
              <p>On Arch or Void, a bad rice can brick your system. On Tebian, your rices are isolated to user-space. Experiment, break things, and push the limits—the Debian foundation underneath remains untouchable. If you mess up, just revert the folder.</p>
            </div>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
