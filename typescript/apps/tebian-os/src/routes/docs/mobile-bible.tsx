import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Mobile Bible — Tebian" };
}

export default function MobileBible() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Mobile Bible</h1>
          <p class="meta">Convergence Unleashed: Running a C-Based Linux Stack on Mobile Hardware.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Convergence Vision</h2>
            <p>The "Smartphone" is the most successful surveillance device in history. Both iOS and Android are built on layers of telemetry, closed-source blobs, and "Walled Garden" philosophies. Tebian Mobile is the antithesis. We believe that your phone should be nothing more than a pocket-sized "node" in your Fleet—running the same C binaries, the same kernel, and the same sovereignty as your workstation.</p>

            <p>This manual provides the C-level roadmap for hardware convergence: making your phone your primary computer.</p>
          </section>

          <section class="kernel-drivers">
            <h2>1. The Kernel Bridge: Mainlining vs. Downstream</h2>
            <p>Most Android phones run "Downstream Kernels"—Frankenstein versions of Linux heavily patched by manufacturers (Qualcomm, Samsung) that never make it back to the official Linux tree. These kernels are dead ends. Tebian Mobile focuses on **Mainline Linux.**</p>

            <h3>The PostmarketOS Foundation</h3>
            <p>We leverage the work of the <strong>postmarketOS</strong> community, integrating their mainline-kernel recipes into our Debian base. This allows Tebian to run on devices like the PinePhone Pro and Librem 5 with a near-stock kernel.</p>
            <ul>
              <li><strong>DRM/KMS:</strong> We use the Direct Rendering Manager for Wayland acceleration on mobile GPUs (Mali/Adreno).</li>
              <li><strong>Libinput Mobile:</strong> Custom configurations for touch-gestures and palm-rejection at the C-level.</li>
              <li><strong>ModemManager:</strong> Managing 4G/5G data streams as a standard systemd service.</li>
            </ul>
          </section>

          <section class="interface-war">
            <h2>2. UI Layers: Phosh vs. Plasma Mobile</h2>
            <p>A mobile OS needs a "Shell." While our desktop uses Sway, a phone needs touch-centric window management. Tebian supports the two most stable C/C++ mobile shells.</p>

            <h3>Phosh (Phone Shell)</h3>
            <p>Written in C and based on GNOME technologies, Phosh is the "Sway of phones." It is stable, modular, and works perfectly with Wayland. It uses <strong>Phoc</strong> as its compositor, which shares the same <strong>wlroots</strong> foundation as Sway.</p>

            <h3>The Convergence Logic</h3>
            <p>When you plug your Tebian phone into a monitor (via USB-C Alt-Mode), the OS detects the change. Through a C-script we call <code>tebian-dock</code>, the UI shifts from Phosh to a full Sway session. This is **True Convergence**: One device, one OS, two bodies.</p>
          </section>

          <section class="app-isolation">
            <h2>3. Software: Waydroid and Native Stacks</h2>
            <p>The "App Gap" is the primary reason people fear mobile Linux. Tebian solves this using <strong>Waydroid</strong>. Waydroid runs a full Android container (LineageOS) inside your Linux kernel. It is not emulation; it uses the same kernel syscalls as the host.</p>
            <ul>
              <li><strong>Zero-Latency Apps:</strong> Android apps run at native hardware speed.</li>
              <li><strong>Integration:</strong> Android notifications and files are mapped to the Tebian host.</li>
              <li><strong>Isolation:</strong> Keep your banking and surveillance apps (Instagram/WhatsApp) inside the Waydroid container while your host remains pure.</li>
            </ul>
          </section>

          <section class="power-management">
            <h2>4. Mobile Power: The Battle for Deep Sleep</h2>
            <p>Mobile devices live and die by their "Standby Time." In Android, this is handled by "Doze." In Tebian, we use <strong>eg25-manager</strong> and custom <strong>udev</strong> rules to manage the modem's power state. We prioritize <code>suspend-to-ram</code> (S3) while maintaining the ability to wake up for an incoming call—a C-level balancing act of hardware interrupts.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Last Phone You'll Buy</h2>
            <p>Tebian Mobile isn't a "mobile OS." It is Tebian on a smaller screen. By reclaiming the modem, the kernel, and the UI, you turn your phone from a tracking device into a sovereign tool. It is the pocket-sized Mothership. One ISO. One menu. Total convergence.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
