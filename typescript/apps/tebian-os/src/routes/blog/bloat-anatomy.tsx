import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Anatomy of Bloat — Tebian" };
}

export default function BloatAnatomy() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Technical</span>
          <h1>The Anatomy of Bloat</h1>
          <p class="meta">February 20, 2026 &bull; 15 min read</p>
        </header>

        <article class="content">
          <p class="lead">We often hear that software is getting slower. It isn't. Hardware is getting faster, but software is getting <em>exponentially</em> heavier. This is the anatomy of a modern operating system's waste.</p>

          <h2>1. The Telemetry Trap</h2>
          <p>In Windows 11 (and macOS), roughly 30% of your background CPU cycles are spent on tasks that do not benefit you. They benefit the vendor. These are:</p>
          <ul>
            <li><strong>Experience Hosts:</strong> Sending usage data to Redmond/Cupertino.</li>
            <li><strong>Indexers:</strong> Scanning your hard drive to suggest "relevant ads" or "search results."</li>
            <li><strong>Updaters:</strong> Checking for new versions of apps you didn't even open.</li>
          </ul>
          <p>In Tebian, these simply do not exist. There is no telemetry daemon. There is no indexer. There is no updater unless you type <code>update-all</code>.</p>

          <h2>2. The Framework Tax</h2>
          <p>Modern apps are built on heavy frameworks like Electron (Chromium) or Python. While convenient for developers, they are catastrophic for memory usage. A simple "Calendar" app on GNOME might consume 400MB of RAM because it is essentially running a web browser.</p>

          <p>Tebian's core utilities (Fuzzel, Sway, Mako) are written in C. They use <code>libc</code> and talk directly to the kernel. A C binary for a menu system uses <strong>Kilobytes</strong>, not Megabytes. This is why a Tebian desktop idles at 300MB of RAM, while a GNOME desktop idles at 1.5GB.</p>

          <h2>3. The Context Switch Cost</h2>
          <p>Every time your CPU switches from one task to another, it incurs a "Context Switch" penalty. It has to flush the cache, save the registers, and load new data. In a bloated OS with 200 background processes, your CPU is constantly context-switching. It never gets into a "flow state."</p>

          <p>Tebian runs fewer than 20 processes on a fresh boot. This means your CPU spends almost 99% of its time executing <em>your</em> code (the game, the compile job, the render), and less than 1% managing the OS itself. This is "Zero Friction."</p>

          <h2>4. The Visual Noise</h2>
          <p>Bloat isn't just invisible processes. It's visible noise. Animations, blur effects, transparency, rounded corners&mdash;these all require GPU cycles. In a compositor like Hyprland (Omarchy) or Mutter (GNOME), the GPU is constantly redrawing the screen even when nothing is happening.</p>

          <p>Tebian's "Stealth Glass" philosophy means we use minimal effects. We prioritize <strong>Zero Latency</strong> over "smoothness." When you press a key, the character appears instantly. There is no "fade in." There is just action.</p>

          <h2>Conclusion: Reclaiming the Machine</h2>
          <p>The Anatomy of Bloat reveals a simple truth: Modern operating systems treat your hardware as a resource to be harvested. Tebian treats your hardware as a tool to be wielded. By removing the layers of abstraction, telemetry, and visual noise, we give you back the machine you paid for.</p>
        </article>
      </main>
    </>
  );
}
