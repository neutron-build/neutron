import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Lazarus Manual — Tebian" };
}

export default function LegacyHardware() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Lazarus Manual</h1>
          <p class="meta">Reviving the Dead: How to Run a Modern Desktop on 15-Year-Old Hardware.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most operating systems (Windows 11, macOS) enforce planned obsolescence. They require TPM 2.0, massive RAM, and specific CPUs just to boot. Tebian rejects this. Our C-based core runs on almost anything. This guide explains how to resurrect your old ThinkPad, Netbook, or 2010 MacBook.</p>

            <p>We don't "support" old hardware; we <strong>optimize</strong> for it. Because code efficiency is timeless.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The 16MB Miracle</h3>
              <p>Tebian's base install uses ~16MB of RAM. This is crucial for machines with 2GB or 4GB of memory. While Windows idles at 2.5GB (choking a 4GB laptop), Tebian leaves 3.9GB free for your browser.</p>
              <ul>
                <li><strong>No Electron:</strong> We avoid heavy web-based desktop apps.</li>
                <li><strong>ZRAM:</strong> Compresses RAM data to simulate having 50% more memory.</li>
                <li><strong>Swapiness:</strong> Tuned to avoid disk thrashing on slow HDDs.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The GPU Survival Guide</h3>
              <p>Old GPUs (Intel GMA, Nvidia GT 210) struggle with modern effects. Tebian's "Stealth Glass" UI is designed to run without complex shaders.</p>
              <ul>
                <li><strong>No Blur:</strong> We disable costly blur effects on detected legacy hardware.</li>
                <li><strong>Direct Rendering:</strong> We talk directly to the Mesa drivers.</li>
                <li><strong>Video Decode:</strong> VAAPI hardware acceleration for smooth YouTube on old chips.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. SSD-ifying HDDs</h3>
              <p>If you're stuck with a mechanical hard drive (HDD), modern OSs are painful. Tebian uses <strong>Noatime</strong> and <strong>Write Caching</strong> to minimize disk seeks.</p>
              <ul>
                <li><strong>Noatime:</strong> Disables "access time" writes on every file read.</li>
                <li><strong>BFQ Scheduler:</strong> Prioritizes interactive tasks over background copies.</li>
                <li><strong>Preload:</strong> Learns your usage and loads apps into RAM before you click.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. 32-Bit Support (The Rare Earth)</h3>
              <p>Most distros (Arch, Ubuntu, Fedora) have dropped 32-bit (i386) support. Debian (and Tebian) still supports it. You can run Tebian on a Pentium 4 from 2004.</p>
              <ul>
                <li><strong>Multiarch:</strong> Run 32-bit apps on 64-bit systems seamlessly.</li>
                <li><strong>Legacy Kernel:</strong> We provide kernels that boot on non-PAE CPUs.</li>
                <li><strong>Retro Gaming:</strong> The perfect base for DOSBox and Wine gaming.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why We Don't Throw Things Away</h2>
            <p>E-waste is a global crisis. Every time you throw away a working laptop because "Windows is slow," you are contributing to it. Tebian allows you to keep that machine in service for another decade. It's not just software; it's environmental stewardship through code efficiency.</p>
          </section>
        </article>
      </main>
    </>
  );
}
