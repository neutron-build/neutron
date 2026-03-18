import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Input Latency Manual — Tebian" };
}

export default function InputLatencyManual() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Technical Manual</span>
          <h1>The Input Latency Manual</h1>
          <p class="meta">Kernel-level Input Tuning: Reclaiming the 1ms Response Time.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Silent Performance Killer</h2>
            <p>Most users focus on "FPS" or "RAM," but the single biggest factor in how a computer "feels" is <strong>Input Latency.</strong> This is the time it takes for a button press on your mouse or keyboard to be registered by the kernel, processed by the compositor, and rendered on the screen. In Windows and macOS, this stack is bloated with legacy drivers and polling bottlenecks. Tebian optimizes the input path at the C-level.</p>
          </section>

          <section class="usb-polling">
            <h2>1. USB Polling Rates: 125Hz to 1000Hz</h2>
            <p>By default, many Linux kernels poll USB devices at 125Hz (8ms delay). For a gamer or a professional developer, this is an eternity. Tebian's boot configuration includes the <code>usbhid.mousepoll=1</code> parameter, forcing the kernel to poll your HID devices at 1000Hz (1ms delay) out of the box.</p>
            <ul>
              <li><strong>Interrupt Coalescing:</strong> We disable kernel-level coalescing for input devices to ensure every packet is processed the moment it arrives.</li>
              <li><strong>XHCI Optimization:</strong> We tune the USB 3.0 controller parameters to minimize the overhead of the "Handshake" protocol.</li>
            </ul>
          </section>

          <section class="evdev-tuning">
            <h2>2. Evdev: The Direct Path</h2>
            <p>Tebian uses <strong>Libinput</strong> talking directly to the <strong>evdev</strong> kernel interface. We've removed the intermediate X11 translation layers that introduce "Jitter" into mouse movement. When you move your mouse in Tebian, you are seeing the raw hardware events mapped directly to the Wayland coordinate system.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Instant Machine</h2>
            <p>The Input Latency Manual is about making your hardware honest. By removing the software bottlenecks, we ensure that your machine responds at the speed of your reflexes. One ISO. One menu. Zero lag.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
