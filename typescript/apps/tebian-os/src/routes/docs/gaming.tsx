import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Gaming Manual — Tebian" };
}

export default function Gaming() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Gaming Manual</h1>
          <p class="meta">Ultimate Performance: From ISO to 4K 144Hz Gaming.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most gamers avoid Linux because they believe it's "hard." We've simplified it. Tebian's "Gaming Mode" handles the entire driver stack (NVIDIA/AMD) and provides a "Safe Escape" to Windows for anti-cheat games.</p>

            <p>We don't use emulation. We use <strong>Direct Kernel Access</strong> and <strong>Vulkan</strong> to give you native-level speed on every title.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The GPU Stack</h3>
              <p>Tebian's "Hardware Detect" menu is built to identify your GPU and install the non-free firmware automatically. Whether it's the NVIDIA proprietary driver or the open-source AMD Mesa stack, it's one click away.</p>
              <ul>
                <li><strong>NVIDIA:</strong> Proprietary drivers + Wayland support.</li>
                <li><strong>AMD:</strong> Native kernel support + Mesa Vulkan.</li>
                <li><strong>Intel:</strong> Hardware acceleration for mobile gaming.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The Software Arsenal</h3>
              <p>Tebian comes with pre-configured setups for the four horsemen of Linux gaming:</p>
              <ul>
                <li><strong>Steam:</strong> Full Proton support for Windows games.</li>
                <li><strong>Heroic:</strong> The open-source launcher for Epic and GOG.</li>
                <li><strong>Lutris:</strong> The universal manager for all game sources.</li>
                <li><strong>GameMode:</strong> Feral Interactive's performance daemon.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Performance Monitoring</h3>
              <p>Tebian's "Performance" menu includes <strong>MangoHud</strong> and <strong>Glow</strong> (the CLI system monitor). We provide a unified way to see your FPS, GPU temps, and frame-times in every game.</p>
              <ul>
                <li><strong>Halt Mode:</strong> Suspends background tasks during gaming.</li>
                <li><strong>ZRAM:</strong> Optimized memory compression for low-RAM machines.</li>
                <li><strong>CPU Microcode:</strong> Security patches that don't hurt FPS.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. The Anti-Cheat Escape</h3>
              <p>Some games (Valorant, Destiny 2, etc.) use kernel-level anti-cheat that only works on Windows. We don't fight it—we bypass it. Tebian provides a guided "Safe Dual-boot" setup so you can have native Windows for those specific games and Tebian for everything else.</p>
              <ul>
                <li><strong>Safe Partitioning:</strong> Shrink your Windows drive without data loss.</li>
                <li><strong>GRUB Magic:</strong> One menu at boot to pick your OS.</li>
                <li><strong>Shared Storage:</strong> Access your game files across both operating systems.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Tebian Beats Windows for Gaming</h2>
            <p>Windows uses 2-3GB of RAM just to sit at the desktop. Tebian uses 16-32MB. That's 2-3GB of RAM you get back for your games. In the world of high-performance gaming, those megabytes matter.</p>

            <p>With <strong>Zero Background Processes</strong>, you don't have to worry about a Windows Update starting in the middle of a match. You are the root. You own the CPU.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
