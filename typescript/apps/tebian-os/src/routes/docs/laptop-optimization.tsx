import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Laptop Optimization Manual — Tebian" };
}

export default function LaptopOptimization() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Laptop Optimization Manual</h1>
          <p class="meta">Battery, Brightness, and Sleep: Maximizing Portability on Tebian.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most Linux "Desktop Environments" are built for PCs first. They have poor power management, leading to high battery drain and laptops that "don't sleep" correctly. Tebian is built for the **Mobile Professional**. This guide explains how to get 10+ hours of battery life and perfect "Lid Close" behavior on your laptop.</p>

            <p>We use a C-based stack for power management: <strong>TLP, Powertop, and SwayIdle</strong>, ensuring your hardware is only active when you are.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The Power Governor (TLP)</h3>
              <p>Tebian's "Laptop Mode" includes a pre-configured <strong>TLP</strong> setup. It's an automated background daemon that manages your CPU's P-states and C-states based on whether you are plugged in or on battery.</p>
              <ul>
                <li><strong>Auto-Governor:</strong> Switches between <code>powersave</code> (battery) and <code>performance</code> (AC) automatically.</li>
                <li><strong>SATA Power:</strong> Puts your SSD into a low-power state when idle.</li>
                <li><strong>PCIe ASPM:</strong> Advanced State Power Management for your network and GPU.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Brightness & Backlight</h3>
              <p>Tebian includes <code>brightnessctl</code>, a lightweight C binary that talks directly to the kernel's backlight device. We've mapped it to your laptop's function keys and our "Screen" menu.</p>
              <ul>
                <li><strong>Hardware Access:</strong> No complex D-Bus calls. Direct <code>/sys/class/backlight</code> writes.</li>
                <li><strong>Smooth Dimming:</strong> We pre-configure <code>swayidle</code> to dim the screen before locking to save power.</li>
                <li><strong>No Flicker:</strong> We ensure your PWM (Pulse Width Modulation) is configured for flicker-free brightness.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Sleep & Lid Behavior</h3>
              <p>The "Lid Close" bug is the most common Linux laptop complaint. Tebian solves this with a pre-configured <strong>Systemd-Logind</strong> and <strong>SwayLock</strong> setup. When you close the lid, your session is locked and the system enters <code>suspend-to-ram</code> (S3) instantly.</p>
              <ul>
                <li><strong>Instant Lock:</strong> We lock the screen <em>before</em> the CPU suspends.</li>
                <li><strong>Deep Sleep:</strong> We enable the <code>deep</code> sleep state (mem) over the shallower <code>s2idle</code>.</li>
                <li><strong>Wakeup:</strong> Instant wake on lid open, with zero screen flickering.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Hardware Killswitches</h3>
              <p>Tebian's "Status Bar" includes a real-time monitor for your hardware killswitches. If you have a ThinkPad or a Purism laptop, you can see if your WiFi or Bluetooth is physically disabled.</p>
              <ul>
                <li><strong>Radio Radio:</strong> We use <code>nmcli radio</code> and <code>rfkill</code> to ensure your hardware is completely powered off, not just "soft-blocked."</li>
                <li><strong>Bluetooth OFF:</strong> Bluetooth is physically powered off by default in Tebian to save 5-10% of battery.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Laptops on Tebian?</h2>
            <p>By using a minimal C-based compositor (Sway) instead of a heavy DE (GNOME), you are already saving 10-20% of your battery. Your CPU doesn't have to redraw a clock or a panel every second. When you combine this with our "Laptop Mode" optimizations, you get a machine that outlasts any Windows or macOS laptop. One ISO. One menu. All-day battery.</p>
          </section>
        </article>
      </main>
    </>
  );
}
