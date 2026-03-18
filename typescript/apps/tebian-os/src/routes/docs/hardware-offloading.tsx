import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Hardware Offloading — Tebian" };
}

export default function HardwareOffloading() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Technical Manual</span>
          <h1>Hardware Offloading Manual</h1>
          <p class="meta">Making the GPU do the CPU's work: VA-API, NVENC, and GLSL.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Efficiency of Dedicated Silicon</h2>
            <p>Your CPU is a general-purpose processor. It is good at many things, but it is inefficient at repetitive tasks like video decoding or text rendering. Your GPU, however, has dedicated silicon for these tasks. Tebian prioritizes <strong>Hardware Offloading</strong> to ensure your CPU is free for complex logic while your GPU handles the pixels. This manual provides the technical setup for system-wide acceleration.</p>
          </section>

          <section class="video-decoding">
            <h2>1. Video Decoding: VA-API and VDPAU</h2>
            <p>When you watch a 4K video in a browser, your CPU can easily hit 100% usage, causing heat and battery drain. Tebian enables <strong>Hardware Video Acceleration</strong> by default.</p>
            <ul>
              <li><strong>VA-API:</strong> The standard for Intel and AMD GPUs. We pre-configure Firefox and Chromium to use the <code>iHD</code> or <code>radeonsi</code> drivers.</li>
              <li><strong>VDPAU:</strong> The legacy standard for NVIDIA, now superseded by NVDEC. We use the <code>nvidia-vaapi-driver</code> wrapper to allow NVIDIA users to get 4K hardware acceleration in Wayland browsers.</li>
            </ul>
          </section>

          <section class="terminal-acceleration">
            <h2>2. Terminal Acceleration: Kitty and GLSL</h2>
            <p>Most terminals (like GNOME Terminal or Xterm) render text on the CPU. This introduces a "Lag" between your typing and the screen. Tebian uses <strong>Kitty</strong>, a C-based terminal that renders every character on the GPU using OpenGL shaders.</p>
            <p>This results in <strong>Zero Input Latency.</strong> Even when catting a 100MB log file, Kitty remains responsive because the CPU is only sending pointers to the GPU.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Balanced Machine</h2>
            <p>Hardware Offloading is the secret to Tebian's speed. By ensuring that every piece of silicon is doing what it was designed for, we create a system that is both powerful and cool. One ISO. One menu. Total efficiency.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
