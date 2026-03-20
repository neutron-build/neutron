import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The HiDPI & 4K Manual — Tebian" };
}

export default function Hidpi4kManual() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The HiDPI & 4K Manual</h1>
          <p class="meta">Glass-Sharp Displays: Configuring 4K, Retina, and HiDPI Scaling.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>In the "Old Linux" world (X11), 4K scaling was a nightmare of blurry text and misaligned icons. Wayland and <strong>Sway</strong> have fixed this at the C-level. Tebian provides a "one-click" setup for <strong>HiDPI Scaling</strong> that ensures every pixel is sharp, from your terminal to your web browser.</p>

            <p>This guide explains how to configure Tebian for high-resolution displays (Retina, 4K monitors, and modern laptop screens).</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. Output Scaling (Sway)</h3>
              <p>Sway's native scaling is superior to traditional OS scaling because it happens in the <strong>Compositor</strong>. We pre-configure the <code>output * scale 2</code> command for detected 4K displays.</p>
              <ul>
                <li><strong>Integer Scaling:</strong> We recommend 2x (200%) scaling for 4K displays to maintain sharp, non-interpolated pixels.</li>
                <li><strong>Fractional Scaling:</strong> Sway supports 1.25x, 1.5x, and 1.75x, though integer scaling is always "cleaner" for the GPU.</li>
                <li><strong>Per-Monitor:</strong> You can set different scales for different monitors (e.g., 2x for the laptop, 1x for the external monitor) with zero flicker.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Toolkit Scaling (GDK & QT)</h3>
              <p>Sway's output scaling only handles the window management. To get your actual apps (like Firefox or PCManFM) to look sharp, you need to set the <strong>GDK_SCALE</strong> and <strong>QT_SCALE_FACTOR</strong> environment variables.</p>
              <ul>
                <li><strong>GDK_SCALE=2:</strong> Sharpens GTK-based apps (Firefox, LibreOffice).</li>
                <li><strong>QT_SCALE_FACTOR=2:</strong> Sharpens QT-based apps (VLC, Bitwig).</li>
                <li><strong>Tebian Profile:</strong> We pre-populate these in <code>~/.profile</code> during the HiDPI setup.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Font Rendering (Pango)</h3>
              <p>Sharp displays require sharp fonts. Tebian uses <strong>Pango</strong> and <strong>Freetype</strong> with custom font-config settings to ensure your text is clear and readable on any resolution.</p>
              <ul>
                <li><strong>Hinting:</strong> We enable <code>slight</code> hinting for the best balance between sharpness and character shape.</li>
                <li><strong>Anti-Aliasing:</strong> Subpixel rendering (RGBA) is enabled by default.</li>
                <li><strong>Nerd Fonts:</strong> We use JetBrains Mono Nerd Font as our system font for its exceptional 4K clarity.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Browser Hardware Acceleration</h3>
              <p>On 4K displays, the browser has to render 4x as many pixels as on 1080p. To keep it smooth, you <strong>must</strong> have GPU hardware acceleration enabled. Tebian's "HiDPI Setup" automatically configures <strong>VA-API</strong> for your browser.</p>
              <ul>
                <li><strong>Zero Frame Drops:</strong> Smooth 4K YouTube playback on any hardware.</li>
                <li><strong>GPU Rendering:</strong> Offloads text rendering and CSS animations to your NVIDIA/AMD card.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why HiDPI on Tebian?</h2>
            <p>Because our compositor (Sway) is written in C and our theme (Stealth Glass) is minimal, there is no "Performance Penalty" for 4K scaling. You get the world's sharpest display experience with the world's fastest UI. One ISO. One menu. Glass-sharp productivity.</p>
          </section>
        </article>
      </main>
    </>
  );
}
