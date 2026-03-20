import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The macOS Escape Manual — Tebian" };
}

export default function MacosEscapeManual() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Migration Manual</span>
          <h1>The macOS Escape Manual</h1>
          <p class="meta">Reclaiming your Hardware from the Walled Garden.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Gilded Cage</h2>
            <p>Apple marketing has spent decades convincing creatives and developers that macOS is the only platform for serious work. They've built a "Gilded Cage": a beautiful interface, seamless integration, and high-quality hardware—all tied to a system that is increasingly hostile to user sovereignty. From the T2 chip to the mandatory iCloud login, macOS is no longer your OS; it is Apple's firmware for your life.</p>

            <p>This manual provides the technical roadmap for "Escaping" macOS and moving to Tebian, with zero loss in productivity. We focus on <strong>Workflow Emulation</strong> and <strong>Virtualization.</strong></p>
          </section>

          <section class="hardware-reclamation">
            <h2>1. Hardware Reclamation: Installing Tebian on Mac</h2>
            <p>Most Mac users don't realize their Intel or Apple Silicon hardware can run Linux faster and cooler than macOS. Tebian's support for <strong>Arm64</strong> and <strong>x86_64</strong> makes it the perfect replacement for any Mac from 2012 to 2026.</p>

            <h3>The T2 and M-Series Challenge</h3>
            <p>Apple uses custom silicon to lock down their machines. On Intel Macs with the T2 chip, Tebian includes the necessary kernel patches to handle the internal SSD and keyboard/trackpad out of the box. For <strong>Apple Silicon (M1/M2/M3)</strong>, we leverage the Asahi Linux kernel patches, providing a native, GPU-accelerated Wayland experience on the world's most efficient hardware.</p>
            <ul>
              <li><strong>Retina Scaling:</strong> Tebian's HiDPI manual ensures your display is exactly as sharp as it was on macOS.</li>
              <li><strong>Trackpad Flow:</strong> We use <code>libinput</code> with custom curves to match the "feel" of the Mac trackpad, including multi-touch gestures.</li>
            </ul>
          </section>

          <section class="app-emulation">
            <h2>2. The "macOS Apps on Linux" Question</h2>
            <p>The biggest hurdle to leaving macOS is the software. "How do I run Xcode?" "What about Photoshop?" Tebian's answer is **Architecture Isolation.**</p>

            <h3>The Xcode Solution (OSX-KVM)</h3>
            <p>You don't need macOS to be your host OS to build for iOS. Tebian's <strong>Virtualization Menu</strong> provides a one-click setup for <strong>OSX-KVM.</strong> This runs a full, hardware-accelerated version of macOS in a window on Tebian. You get native-speed access to Xcode, the App Store, and iCloud. You use macOS as a <em>tool</em>, not as a master.</p>

            <h3>The Creative Suite (Darling & Wine)</h3>
            <p>For standard Mac apps, we use <strong>Darling</strong>—a translation layer (like Wine, but for macOS binaries). While still maturing, Darling can run many CLI and simple GUI Mac tools natively on Tebian. For the Adobe suite, we recommend our <em>Creative Bible</em> workflow: using native Linux powerhouses like <strong>DaVinci Resolve, Bitwig, and Blender</strong> that out-render their Mac counterparts.</p>
          </section>

          <section class="workflow-sync">
            <h2>3. Workflow Sync: Spotlight to Fuzzel</h2>
            <p>The macOS "Spotlight" (Cmd+Space) is the soul of the Mac workflow. Tebian replaces this with <strong>Fuzzel.</strong> Our Fuzzel configuration is mapped to <code>Super+D</code> (or Cmd+Space if you prefer) and provides a faster, C-based search for apps, files, and system settings.</p>
            <ul>
              <li><strong>Stealth UI:</strong> Just like macOS, Tebian's "Ghost Mode" keeps the screen clean. No taskbars, no clutter.</li>
              <li><strong>T-Link Mesh:</strong> Replaces AirDrop and Handoff with a sovereign, WireGuard-based mesh that works on any hardware, not just Apple products.</li>
            </ul>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Sovereign Creative</h2>
            <p>The macOS Escape Manual is about moving from "Consumption" to "Production." You don't need Apple's permission to use your hardware. By moving to Tebian, you get the performance of Linux with the elegance of a Mac, and the freedom of a sovereign individual. One ISO. One menu. Total independence.</p>
          </section>
        </article>
      </main>
    </>
  );
}
