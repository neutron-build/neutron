import "../styles/docs-hub.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Guides" };
}

export default function Docs() {
  return (
    <>
      <main>
        <h1 class="page-title">Guides</h1>
        <p class="subtitle">Authoritative technical specifications. Pure instruction.</p>

        <div class="manual-hub">
          <section class="hub-section">
            <h2>Definitive Bibles</h2>
            <div class="manual-grid protocol-grid">
              <a href="/docs/gaming-bible" class="protocol-card"><span class="category">Gaming</span><h3>The Gaming Bible</h3><p>Mathematics of DXVK, kernel-level frame-timing, and anti-cheat theory.</p></a>
              <a href="/docs/creative-bible" class="protocol-card"><span class="category">Creation</span><h3>The Creative Bible</h3><p>PipeWire graph routing, OBS Wayland capture, and pro-audio excellence.</p></a>
              <a href="/docs/dev-bible-part1" class="protocol-card"><span class="category">Development</span><h3>The Dev Bible: Foundation</h3><p>Mise, Direnv, Rust/C interop, and clean host philosophy.</p></a>
              <a href="/docs/dev-bible-part2" class="protocol-card"><span class="category">Development</span><h3>The Dev Bible: Engine</h3><p>Building a zero-latency IDE with Neovim, Treesitter, and LSP.</p></a>
              <a href="/docs/ai-bible-part1" class="protocol-card"><span class="category">Intelligence</span><h3>The AI Bible: Training</h3><p>Fine-tuning Llama 3 on consumer hardware using QLoRA.</p></a>
              <a href="/docs/mobile-bible" class="protocol-card"><span class="category">Mobility</span><h3>The Mobile Bible</h3><p>Mainline Linux on phones, convergence, and pocket-sized sovereignty.</p></a>
              <a href="/docs/retro-bible" class="protocol-card"><span class="category">Preservation</span><h3>The Retro Bible</h3><p>Cycle-accurate emulation, input lag reduction, and CRT shaders.</p></a>
              <a href="/docs/crypto-bible" class="protocol-card"><span class="category">Finance</span><h3>The Crypto Bible</h3><p>Full nodes, cold storage, and air-gapped security protocols.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Performance &amp; Hardware</h2>
            <div class="protocol-grid">
              <a href="/docs/input-latency-manual" class="protocol-card"><span class="category">Latency</span><h3>Input Latency Manual</h3><p>1000Hz USB polling and kernel-level HID tuning.</p></a>
              <a href="/docs/kernel-tuning-manual" class="protocol-card"><span class="category">Tuning</span><h3>Kernel Tuning Manual</h3><p>Sysctl optimization, I/O scheduling, and custom Zen kernels.</p></a>
              <a href="/docs/hardware-offloading" class="protocol-card"><span class="category">Efficiency</span><h3>Hardware Offloading</h3><p>GPU-accelerated decoding, encoding, and terminal rendering.</p></a>
              <a href="/docs/laptop-optimization" class="protocol-card"><span class="category">Portability</span><h3>Laptop Manual</h3><p>TLP, battery-tuning, and perfect lid-sleep behavior.</p></a>
              <a href="/docs/legacy-hardware" class="protocol-card"><span class="category">Revival</span><h3>Old Hardware Guide</h3><p>Reviving 15-year-old hardware at the C-level.</p></a>
              <a href="/docs/hidpi-4k-manual" class="protocol-card"><span class="category">Display</span><h3>4K &amp; HiDPI Manual</h3><p>Wayland scaling and sharp font configuration.</p></a>
              <a href="/docs/low-latency-living" class="protocol-card"><span class="category">Speed</span><h3>Low-Latency Living</h3><p>System-wide latency reduction for real-time workflows.</p></a>
              <a href="/docs/direct-kernel-capture" class="protocol-card"><span class="category">Capture</span><h3>Direct Kernel Capture</h3><p>Kernel-level screen and audio capture pipelines.</p></a>
              <a href="/docs/hardware" class="protocol-card"><span class="category">Reference</span><h3>Hardware</h3><p>Supported hardware and driver compatibility.</p></a>
              <a href="/docs/binary-stability-manual" class="protocol-card"><span class="category">Stability</span><h3>Binary Stability</h3><p>ABI guarantees and reproducible builds.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Security &amp; Systems</h2>
            <div class="protocol-grid">
              <a href="/docs/security-hardening-bible" class="protocol-card"><span class="category">Hardening</span><h3>Security Hardening</h3><p>AppArmor, memory isolation, and kernel defense.</p></a>
              <a href="/docs/fortress-network" class="protocol-card"><span class="category">Anonymity</span><h3>Fortress Network</h3><p>WireGuard mesh, Tor services, and I2P routing.</p></a>
              <a href="/docs/un-tebian-guide" class="protocol-card"><span class="category">Freedom</span><h3>The Un-Tebian Guide</h3><p>Deterministic reversion to pure Debian without data loss.</p></a>
              <a href="/docs/tebian-protocol" class="protocol-card"><span class="category">Reference</span><h3>Tebian Protocol</h3><p>Complete cheat-sheet for keybinds and scripts.</p></a>
              <a href="/docs/privacy-hardening" class="protocol-card"><span class="category">Privacy</span><h3>The Digital Fortress</h3><p>Full-stack privacy hardening and threat mitigation.</p></a>
              <a href="/docs/control" class="protocol-card"><span class="category">System</span><h3>Control Center</h3><p>Centralized system management and configuration.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Architecture &amp; Interop</h2>
            <div class="protocol-grid">
              <a href="/docs/architecture" class="protocol-card"><span class="category">Structural</span><h3>System Architecture</h3><p>The "Debian + 1 Folder" design philosophy.</p></a>
              <a href="/docs/distrobox-mastery" class="protocol-card"><span class="category">Interop</span><h3>Distrobox Mastery</h3><p>AUR on Debian without the Arch fragility.</p></a>
              <a href="/docs/macos-escape-manual" class="protocol-card"><span class="category">Migration</span><h3>macOS Escape Manual</h3><p>Reclaiming Apple hardware from the Walled Garden.</p></a>
              <a href="/docs/mint-to-debian-migration" class="protocol-card"><span class="category">Migration</span><h3>Mint-to-Debian</h3><p>Moving from the wrapper to the source.</p></a>
              <a href="/docs/container-management" class="protocol-card"><span class="category">Containers</span><h3>Container Management</h3><p>Headless container orchestration and management.</p></a>
              <a href="/docs/macos-vm" class="protocol-card"><span class="category">Virtualization</span><h3>macOS VM Manual</h3><p>One-click OSX-KVM setup with USB passthrough.</p></a>
              <a href="/docs/compatibility" class="protocol-card"><span class="category">Compatibility</span><h3>Compatibility</h3><p>Software and hardware compatibility reference.</p></a>
              <a href="/docs/universal-compatibility" class="protocol-card"><span class="category">Interop</span><h3>Universal Compatibility</h3><p>Running anything on Debian through layers and containers.</p></a>
              <a href="/docs/declarative-fatigue" class="protocol-card"><span class="category">Philosophy</span><h3>Declarative Fatigue</h3><p>Why NixOS-style config management is overengineered.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Software &amp; Tools</h2>
            <div class="protocol-grid">
              <a href="/docs/gaming" class="protocol-card"><span class="category">Gaming</span><h3>The Gaming Manual</h3><p>GPU stack, Steam, Proton, Heroic, and performance monitoring.</p></a>
              <a href="/docs/audio-production" class="protocol-card"><span class="category">Audio</span><h3>The Audiophile's Handbook</h3><p>PipeWire, JACK, and professional audio on Linux.</p></a>
              <a href="/docs/neovim-setup" class="protocol-card"><span class="category">Editor</span><h3>The Neovim Manual</h3><p>Terminal-first editing with LSP, Treesitter, and plugins.</p></a>
              <a href="/docs/local-ai-manual" class="protocol-card"><span class="category">AI</span><h3>The Local AI Manual</h3><p>Ollama, Llama, and local inference on your hardware.</p></a>
              <a href="/docs/web-kiosk" class="protocol-card"><span class="category">Kiosk</span><h3>The Unbreakable Web Kiosk</h3><p>Locked-down single-purpose browser machines.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Sovereignty &amp; Networking</h2>
            <div class="protocol-grid">
              <a href="/docs/self-hosting-manual" class="protocol-card"><span class="category">Self-Hosting</span><h3>The Self-Hosting Manual</h3><p>Syncthing, Vaultwarden, Matrix, and Caddy reverse proxy.</p></a>
              <a href="/docs/t-link-manual" class="protocol-card"><span class="category">Networking</span><h3>The T-Link Manual</h3><p>Tailscale mesh networking across the Fleet.</p></a>
            </div>
          </section>
        </div>
      </main>
    </>
  );
}
