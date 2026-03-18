import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Universal Compatibility — Tebian" };
}

export default function UniversalCompatibility() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Hardware Manual</span>
          <h1>Universal Compatibility Manual</h1>
          <p class="meta">The Hardware List Windows and Apple say is 'Too Old'.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Software Choice of Immortality</h2>
            <p>In the world of corporate software, hardware is designed to die. Windows 11 enforces TPM requirements to kill older PCs. macOS drops support for Intel Macs to push Apple Silicon. This is not a technical necessity; it is a financial strategy. Tebian rejects this. We believe that if a CPU can execute an instruction, it is still a computer. Our C-based core is designed for <strong>Immortality.</strong></p>

            <p>This manual provides the technical compatibility list for machines that the world has forgotten, but Tebian has resurrected.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The ThinkPad God-Tier</h3>
              <p>ThinkPads are the primary development platform for the Linux kernel. They have the best driver support in history. Tebian runs flawlessly on almost any ThinkPad made since 2005.</p>
              <ul>
                <li><strong>T420 / T430 / T440p:</strong> The legends. Native kernel support for every button, including the TrackPoint.</li>
                <li><strong>X220 / X230:</strong> The ultimate portable workstation. Tebian's 16MB idle makes these feel faster than modern ultra-books.</li>
                <li><strong>X1 Carbon (All Gens):</strong> Full support for high-res displays and fingerprint readers.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The Intel Mac Classics</h3>
              <p>Don't throw away your 2012-2019 Mac. Tebian includes the proprietary Broadcom and NVIDIA patches required to make these machines fly again.</p>
              <ul>
                <li><strong>MacBook Pro 2012 (Non-Retina):</strong> The last upgradeable Mac. Tebian + SSD = A beast.</li>
                <li><strong>MacBook Pro 2015:</strong> The peak of Mac design. Tebian handles the Retina display perfectly.</li>
                <li><strong>iMac 27" (5K):</strong> Tebian's HiDPI manual makes this the world's best 5K development station.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. The Low-Power Arm64 Fleet</h3>
              <p>Tebian isn't just for PCs. We build first-class Arm64 ISOs for the new generation of hardware.</p>
              <ul>
                <li><strong>Raspberry Pi 4 / 5:</strong> The perfect "Mothership" or "Sovereign Node."</li>
                <li><strong>PinePhone / Pro:</strong> Native mobile support (see the Mobile Bible).</li>
                <li><strong>Apple Silicon (M1/M2/M3):</strong> GPU-accelerated desktop support via the Asahi kernel.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. The "Lazarus" Edge Cases</h3>
              <p>Machines with less than 2GB of RAM or 32-bit CPUs. These are "unusable" on Windows, but Tebian specializes in them.</p>
              <ul>
                <li><strong>Intel Atom Netbooks:</strong> Revive the 2009 Netbook era for distraction-free writing.</li>
                <li><strong>Old Dell Latitude / HP EliteBooks:</strong> Robust business machines that can still serve as reliable servers.</li>
                <li><strong>Custom 32-bit (i386) Industrial PCs:</strong> Maintain critical infrastructure without the modern bloat.</li>
              </ul>
            </div>
          </div>

          <section class="driver-philosophy">
            <h2>Why Tebian has better Drivers</h2>
            <p>Windows relies on manufacturers to provide drivers. When a manufacturer stops making money on an old device, they stop updating the driver. The hardware "dies" because the software link is broken. Linux is different. Drivers are built into the <strong>Kernel.</strong> Because the community maintains the kernel, a driver written in 2008 still works in 2026. Tebian leverages this collective memory to ensure your hardware never truly dies.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Stop the E-Waste</h2>
            <p>The "Slow PC" is a lie. You don't have a slow PC; you have an inefficient OS. By choosing Tebian, you are opting out of the planned obsolescence cycle. You are choosing to keep your hardware, your money, and your sovereignty. One ISO. One menu. Universal compatibility.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
