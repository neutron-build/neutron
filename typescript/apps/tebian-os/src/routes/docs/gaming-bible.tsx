import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Gaming Bible — Tebian" };
}

export default function GamingBible() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Gaming Bible</h1>
          <p class="meta">From DXVK Mathematics to Kernel Frame-Timing: The Ultimate Linux Gaming Treatise.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Paradigm Shift: Console-Grade Desktop</h2>
            <p>For twenty years, Linux gaming was a game of "cat and mouse." We were emulating Windows calls, fighting with Wine prefixes, and begging developers for native ports. In 2026, the era of "begging" is over. Through the success of the Steam Deck and the maturity of the Vulkan ecosystem, Linux has become the <strong>Reference Platform</strong> for high-performance gaming. Tebian is the weaponization of that platform.</p>

            <p>This treatise breaks down the C-level stack that makes Tebian faster than Windows for 90% of modern titles. We don't use "Compatibility Layers"; we use <strong>Translation Proxies</strong> that speak directly to the GPU.</p>
          </section>

          <section class="dxvk-vulkan">
            <h2>1. The Mathematics of DXVK: DirectX to Vulkan</h2>
            <p>Most modern games are written for DirectX 11 or 12. Windows executes these calls through its own closed-source drivers. Tebian uses <strong>DXVK (DirectX-over-Vulkan)</strong>. This is not emulation. It is a translation of the DirectX state machine into Vulkan commands.</p>

            <h3>How it works at the C-Level</h3>
            <p>When a game issues a <code>Draw()</code> call in DirectX, DXVK intercepts that call and translates it into a Vulkan <code>vkCmdDraw()</code>. Because Vulkan is a lower-level API than DirectX 11, it allows for more efficient multi-threading. While Windows chokes on single-threaded DirectX dispatching, Tebian's DXVK can spread that load across all CPU cores. This results in <strong>lower CPU overhead</strong> and higher minimum 1% lows.</p>

            <ul>
              <li><strong>Shader Pre-Caching:</strong> Tebian's Steam integration pre-compiles shaders before you launch the game. This eliminates the "Shader Stutter" common on Windows.</li>
              <li><strong>VKD3D:</strong> For DirectX 12, we use VKD3D-Proton, which maps DX12's heavy multi-threading directly to Vulkan's queue architecture.</li>
            </ul>
          </section>

          <section class="kernel-timing">
            <h2>2. Kernel Frame-Timing: Eliminating Jitter</h2>
            <p>The difference between "60 FPS" and "Smooth 60 FPS" is <strong>Frame-Time Consistency.</strong> If your frames arrive at 16.6ms, 12ms, then 20ms, you will experience "stutter" even if the average FPS is high. Windows 11 is notorious for "Background Jitter"—background services checking for updates or sending telemetry interrupt the CPU every few milliseconds.</p>

            <p>Tebian's <strong>GameMode</strong> daemon, written in C, performs a surgical strike on the kernel when you launch a game:</p>
            <ul>
              <li><strong>CPU Governor:</strong> It sets the CPU governor to <code>performance</code>, preventing the CPU from "downclocking" during quiet moments in a game.</li>
              <li><strong>I/O Priority:</strong> It grants the game process <code>SCHED_ISO</code> (isochronous) priority, ensuring that even if a background disk write happens, the game's frame-dispatch remains uninterrupted.</li>
              <li><strong>GPU Power States:</strong> It locks your NVIDIA or AMD GPU into its highest P-State, ensuring consistent clock speeds.</li>
            </ul>
          </section>

          <section class="anti-cheat">
            <h2>3. The Anti-Cheat Frontier: Sovereignty vs. Slavery</h2>
            <p>The final boss of Linux gaming is <strong>Kernel-Level Anti-Cheat (KLAC)</strong>. Games like Valorant, Destiny 2, and Modern Warfare use drivers that sit in "Ring 0"—the same level as your OS kernel. They have total access to your hardware and your data. From a security perspective, these are "Authorized Rootkits."</p>

            <h3>The Tebian Approach: The Safe Dual-Boot</h3>
            <p>We believe your OS kernel belongs to you, not to a game developer. We refuse to compromise the security of Tebian by allowing proprietary rootkits into our kernel. Instead, we provide the <strong>Windows Escape Hatch.</strong></p>

            <p>During setup, Tebian guides you through a safe, non-destructive partition of your SSD. You keep a minimal Windows install specifically for KLAC games. Tebian's <strong>GRUB</strong> menu allows you to switch in seconds. You are not "dual-booting distros"; you are isolating corporate-controlled software from your sovereign workstation.</p>
          </section>

          <section class="mangohud">
            <h2>4. Real-Time Telemetry: MangoHud and Glow</h2>
            <p>A professional gamer needs data. Tebian comes pre-configured with <strong>MangoHud</strong>, a C-based overlay that provides more data than MSI Afterburner ever could. You can see your VRAM usage, your frame-times (in graph form), your GPU power draw, and your CPU thermals—all rendered via Vulkan so they don't impact FPS.</p>

            <p>For system-wide monitoring, we use <strong>Glow</strong> (the CLI monitor). It reads directly from <code>/proc</code> and <code>/sys</code>, giving you a C-level view of your hardware health without the overhead of a GUI dashboard.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Only Gaming OS You Need</h2>
            <p>Tebian isn't a "Linux for Gamers." It is a high-performance C-based foundation that happens to be the best gaming platform in the world. By removing the telemetry, the frameworks, and the background noise of Windows, we give you 100% of the hardware you paid for. One ISO. One menu. Native speed. This is the end of the "Gaming on Linux" myth. It is just gaming. And it is better here.</p>
          </section>
        </article>
      </main>
    </>
  );
}
