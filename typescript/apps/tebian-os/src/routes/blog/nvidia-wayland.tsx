import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "NVIDIA on Wayland — Tebian" };
}

export default function NvidiaWayland() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Hardware</span>
          <h1>NVIDIA on Wayland: Taming the Beast</h1>
          <p class="meta">February 20, 2026 &bull; 18 min read</p>
        </header>
        <article class="content">
          <p class="lead">For years, the phrase "NVIDIA on Linux" was a warning. It meant screen tearing, flickering, and endless configuration files. On Wayland, it was considered a disaster. But in 2026, on Tebian, NVIDIA is a <strong>Performance King.</strong></p>

          <h2>The Proprietary Driver Paradox</h2>
          <p>Most Linux purists argue for open-source drivers. We agree in principle, but for NVIDIA, the <strong>Proprietary Driver</strong> is the only way to get full performance and support for features like DLSS, Ray Tracing, and NVENC. However, installing these drivers on a standard distro often leads to "Dependency Hell."</p>

          <p>Tebian's "Hardware Detect" system is built on a fundamental C-level logic: it identifies your GPU model (via <code>lspci</code>), checks the version against the NVIDIA 555+ driver series (the one that fixed Wayland), and installs it directly from the <strong>non-free-firmware</strong> repository.</p>

          <h2>The Wayland Fix (GBM vs EGLStreams)</h2>
          <p>Wayland used to fail on NVIDIA because NVIDIA insisted on its own "EGLStreams" protocol while everyone else (Intel/AMD) used "GBM." In 2026, NVIDIA has finally embraced GBM. This means Wayland is now as stable on NVIDIA as it is on AMD.</p>

          <p>However, you still need the right <strong>Kernel Parameters</strong>. Tebian handles these automatically during the "NVIDIA Setup" menu: it sets <code>nvidia-drm.modeset=1</code>, configures the <code>WLR_NO_HARDWARE_CURSORS=1</code> environment variable, and ensures your kernel modules are loaded correctly.</p>

          <h2>The Performance Advantage</h2>
          <p>Why use NVIDIA on Tebian? Because our compositor, <strong>Sway</strong>, is written in C and is hyper-efficient. It doesn't have the "Composition Overlays" that slow down GNOME or the "Elastic Animations" that flicker on NVIDIA in Hyprland. You get a solid, 144Hz+ experience without the visual artifacts.</p>

          <h2>Gaming & Beyond</h2>
          <p>With NVIDIA's proprietary driver, Tebian becomes a powerhouse for <strong>Gamer Rig</strong> and <strong>Creative Rig</strong> configurations. You get native-speed Vulkan for Steam games and full hardware acceleration for video editing in Kdenlive or 3D rendering in Blender.</p>

          <p>We've tamed the beast. One ISO. One menu. All the GPU power you paid for.</p>
        </article>
      </main>
    </>
  );
}
