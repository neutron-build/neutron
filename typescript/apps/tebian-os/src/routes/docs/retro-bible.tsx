import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Retro Bible — Tebian" };
}

export default function RetroBible() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Retro Bible</h1>
          <p class="meta">Input Latency and CRT Shaders: The C-Level Guide to High-Fidelity Emulation.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Philosophy of Preservation</h2>
            <p>Retro gaming is often treated as a "casual" hobby. At Tebian, we treat it as **Digital Archeology.** Emulating a console from 1994 isn't about playing a game; it's about accurately recreating a historical state-machine. To do this correctly, you must eliminate the latency added by modern operating systems.</p>

            <p>This manual provides the technical roadmap for "Cycle-Accurate" emulation on Tebian, focusing on kernel-level input handling and Vulkan-based CRT simulation.</p>
          </section>

          <section class="input-latency">
            <h2>1. The War on Input Lag: Evdev and Poll-Rates</h2>
            <p>The "feel" of a retro game is defined by the time it takes for a button press to become an action on screen. Windows and standard Linux DEs introduce significant lag through their "Input Stacks." Tebian bypasses these layers.</p>

            <h3>The Evdev Advantage</h3>
            <p>Tebian maps controllers directly to the <strong>evdev</strong> (event device) kernel interface. We use a C-based wrapper that reads raw HID reports from your USB or Bluetooth controller. By setting the kernel's USB polling rate to 1000Hz (1ms), we ensure your inputs are processed before the next frame is even rendered.</p>
            <ul>
              <li><strong>Real-time Priority:</strong> We grant the <code>retroarch</code> process <code>SCHED_FIFO</code> priority, ensuring it is never interrupted by background tasks.</li>
              <li><strong>Wait-for-Vsync:</strong> We use the Wayland <code>presentation-time</code> protocol to synchronize game frames perfectly with your monitor's refresh rate.</li>
            </ul>
          </section>

          <section class="shader-math">
            <h2>2. The Visual Soul: CRT-Geom and Vulkan</h2>
            <p>Retro games were designed for CRTs, not LCDs. CRTs have phosphor glow, scanlines, and unique color blending. Playing a 240p game on a 4K LCD looks "wrong" because it is too sharp. Tebian uses <strong>Vulkan-based Shaders</strong> to simulate the CRT experience.</p>

            <h3>Phosphor Mathematics</h3>
            <p>We use the <strong>Mega Bezel</strong> and <strong>CRT-Guest-Advanced</strong> shader presets. These are complex C-like (HLSL/GLSL) programs that calculate light diffusion and beam-convergence in real-time on your GPU. The result is a display that looks like glass—not because it's sharp, but because it has the depth and warmth of a tube television.</p>
          </section>

          <section class="emulationstation">
            <h2>3. The Interface: EmulationStation on Sway</h2>
            <p>Tebian's "Retro Rig" uses <strong>EmulationStation-DE</strong> as its primary shell. We've optimized it to run as a native Wayland client. There is no "Desktop" in the background. When you enter Retro Mode, your machine becomes a console.</p>
            <ul>
              <li><strong>Zero-Gaps:</strong> Sway is configured to remove all window borders and gaps in full-screen retro sessions.</li>
              <li><strong>State Management:</strong> We use C-scripts to handle <code>save-state</code> syncing to your Mothership via Syncthing. Play on your PC, pick up on your laptop.</li>
            </ul>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Timeless Computing</h2>
            <p>The Retro Bible proves that "Legacy" doesn't mean "Old." By applying modern C-level engineering to classic software, we preserve the art of the past with the performance of the future. One ISO. One menu. Infinite classics.</p>
          </section>
        </article>
      </main>
    </>
  );
}
