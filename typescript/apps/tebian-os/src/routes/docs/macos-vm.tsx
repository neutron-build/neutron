import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The macOS VM Manual — Tebian" };
}

export default function MacosVm() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The macOS VM Manual</h1>
          <p class="meta">Ultimate Performance: Xcode, App Store, and Apple Services on Tebian.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most iOS developers believe they <strong>must</strong> buy a Mac to build apps. We've proven otherwise. Tebian's "OSX-KVM" setup provides a near-native macOS environment inside a high-performance virtual machine. This isn't a "Hackintosh"; it's a <strong>Kernel-level Virtual Machine (KVM)</strong> running with hardware-assisted virtualization.</p>

            <p>You get full access to <strong>Xcode, the App Store, and iCloud services</strong> without leaving your Tebian desktop. This is the ultimate "Dev Rig" for the 2026 Sovereign Developer.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The KVM Stack</h3>
              <p>Tebian's "Virtualization" menu handles the complex installation of the QEMU/KVM stack automatically. We pre-configure the bridge utilities and user groups so you don't have to touch a config file.</p>
              <ul>
                <li><strong>QEMU/KVM:</strong> High-performance hardware virtualization.</li>
                <li><strong>Virt-Manager:</strong> A GUI for managing your virtual machines.</li>
                <li><strong>Bridge-Utils:</strong> Network bridging for native-speed internet.</li>
                <li><strong>Libvirt:</strong> The industry-standard virtualization API.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The "One-Click" Setup</h3>
              <p>Tebian's <code>Setup macOS VM</code> script does the heavy lifting for you. It clones the OSX-KVM repository, fetches the base system directly from Apple servers, and converts it into a bootable QCOW2 image.</p>
              <ul>
                <li><strong>Automated Fetching:</strong> Get the latest macOS installer securely.</li>
                <li><strong>Image Conversion:</strong> DMG to RAW to QCOW2, handled in C.</li>
                <li><strong>Disk Creation:</strong> 64GB+ virtual disk pre-allocated for Xcode.</li>
                <li><strong>OpenCore Integration:</strong> The most stable bootloader for macOS VMs.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. GPU Passthrough (Optional)</h3>
              <p>For those who need 100% graphics performance (Blender or Metal development), Tebian provides an advanced <strong>PCI Passthrough</strong> guide. You can dedicate a secondary GPU (like an AMD RX 580) directly to the macOS VM.</p>
              <ul>
                <li><strong>Isolated IRQs:</strong> Prevent host-guest hardware conflicts.</li>
                <li><strong>OVMF Firmware:</strong> Unified Extensible Firmware Interface support.</li>
                <li><strong>Native Acceleration:</strong> Metal works at 99.9% of native speed.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>4. The Sovereignty Factor</h3>
              <p>Why use a VM instead of a Mac? Because on Tebian, you own the snapshot. You can duplicate your entire dev environment, test risky updates, and revert in seconds. No T2 chips. No locked-down hardware.</p>
              <ul>
                <li><strong>Easy Backups:</strong> Copy one 64GB file to an external drive.</li>
                <li><strong>Security:</strong> Isolate your "Corporate" dev work from your "Personal" system.</li>
                <li><strong>Portability:</strong> Move your macOS VM to any other Tebian machine.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>How to Launch</h2>
            <p>Once you've run the setup via <code>tebian-settings</code>, launching macOS is as simple as pressing <code>Super + D</code> and selecting "Launch macOS." Tebian handles the KVM bridge and starts the OpenCore bootloader automatically.</p>

            <p>The screen belongs to you. The code belongs to you. The OS is just the interface.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
