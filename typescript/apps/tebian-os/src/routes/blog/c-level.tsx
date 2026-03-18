import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The C-Level Philosophy — Tebian" };
}

export default function CLevel() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Philosophy</span>
          <h1>The C-Level Philosophy</h1>
          <p class="meta">February 19, 2026 &bull; 8 min read</p>
        </header>

        <article class="content">
          <p class="lead">Most modern operating systems are built on layers of abstraction. In Tebian, we peel those layers back until we hit C. If it doesn't talk directly to the hardware, it's a layer of friction.</p>

          <h2>Why C Matters</h2>
          <p>In the world of 2026, most software is bloated. We've traded memory efficiency for developer convenience. Modern desktops use multi-gigabyte frameworks just to show you a taskbar. This is a betrayal of the hardware.</p>

          <p>Tebian's "Core 3" (Sway, Fuzzel, Mako) are written in C. This isn't because we're nostalgic; it's because C is <strong>deterministic</strong>. It doesn't have a garbage collector that pauses your CPU. It doesn't have a startup lag. It is the language of the kernel.</p>

          <h2>The Layers of Waste</h2>
          <p>Every time you add a layer of abstraction, you lose power. Here is how a "Universal" desktop usually looks compared to Tebian:</p>

          <div class="comparison">
            <div class="stack">
              <h3>Traditional Desktop</h3>
              <ul>
                <li>User Action</li>
                <li>JavaScript / Python Runtime</li>
                <li>Desktop Environment Framework</li>
                <li>Window Manager</li>
                <li>Compositor</li>
                <li>Kernel</li>
                <li>Hardware</li>
              </ul>
            </div>
            <div class="stack tebian">
              <h3>Tebian Desktop</h3>
              <ul>
                <li>User Action</li>
                <li><strong>C Binary (Sway / Fuzzel)</strong></li>
                <li>Kernel</li>
                <li>Hardware</li>
              </ul>
            </div>
          </div>

          <h2>Performance as a Feature</h2>
          <p>When your desktop environment is just a set of C binaries, "performance" stops being a metric and starts being a constant. There are no background tasks indexing your files for advertisement purposes. There is zero telemetry.</p>

          <p>This "C-Level" approach is what allows us to run on a Raspberry Pi Zero or a $5,000 workstation with the same level of responsiveness. We've removed the noise.</p>

          <h2>One Menu to Rule Them All</h2>
          <p>The universal interface for Tebian is <strong>Fuzzel</strong>. It's a tiny, C-based app launcher. It is the bridge between you and the OS. By funneling all configuration (WiFi, Bluetooth, Displays, Updates) through one simple C-based menu, we've reduced the "cognitive load" of computing.</p>

          <p>You shouldn't have to learn how to use your OS. Your OS should learn to get out of your way.</p>

          <h2>Stability Through Simplicity</h2>
          <p>We use <strong>Debian Stable</strong> because "change" is the enemy of "work." When you use Tebian, you're not just getting a rice; you're getting a commitment. A commitment that your computer will work exactly the same way today as it will in two years.</p>

          <p>We've done the work to strip everything else away. All that's left is you and your machine. C-level, zero-fork, pure Debian.</p>
        </article>
      </main>
    </PageShell>
  );
}
