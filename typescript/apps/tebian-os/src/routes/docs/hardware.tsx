import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Hardware — Tebian" };
}

export default function Hardware() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Documentation</span>
          <h1>Hardware</h1>
          <p class="meta">It just works. No manual driver hunting required.</p>
        </header>
        <article class="content">
          <section class="auto-detect">
            <p>Tebian is designed to be hardware-agnostic. Our <code>bootstrap</code> and <code>desktop</code> scripts automatically detect your machine's capabilities and configure the system for peak performance.</p>

            <div class="hardware-grid">
              <div class="hw-card">
                <h3>Graphics (Nvidia/Intel/AMD)</h3>
                <p>Auto-detects your GPU. Installs the correct firmware and drivers. configures Vulkan and hardware acceleration out of the box.</p>
              </div>
              <div class="hw-card">
                <h3>HiDPI & Scaling</h3>
                <p>Detects 4K and Retina displays. Sets appropriate Wayland scaling so your interface is readable from the first boot.</p>
              </div>
              <div class="hw-card">
                <h3>Laptop Optimization</h3>
                <p>Configures <code>TLP</code> power profiles, multi-touch gestures, and backlight controls automatically for ThinkPads, MacBooks, and more.</p>
              </div>
              <div class="hw-card">
                <h3>CPU Microcode</h3>
                <p>Identifies Intel vs AMD architecture and pulls the latest security and performance microcode updates from Debian's non-free-firmware repos.</p>
              </div>
            </div>
          </section>

          <section class="targets">
            <h2>Optimized Targets</h2>
            <p>We don't just "run" on these platforms; we optimize for them.</p>
            <ul class="hw-list">
              <li><strong>ThinkPads:</strong> Full support for TrackPoints, battery thresholds, and specialized function keys.</li>
              <li><strong>MacBooks (Intel):</strong> Fixed WiFi firmware and Retina scaling configured by default.</li>
              <li><strong>Raspberry Pi 4/5:</strong> Minimal image with hardware-accelerated video decoding and low-latency GPIO access.</li>
              <li><strong>Cloud ARM64:</strong> Leanest possible footprint for high-performance VPS and VM environments.</li>
            </ul>
          </section>
        </article>
      </main>
    </>
  );
}
