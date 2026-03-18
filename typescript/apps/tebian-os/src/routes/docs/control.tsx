import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Control Center — Tebian" };
}

export default function Control() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Documentation</span>
          <h1>Control Center</h1>
          <p class="meta">One interface. Full sovereignty.</p>
        </header>
        <article class="content">
          <section class="fuzzel-power">
            <p>Tebian replaces bloated "Settings" apps with a single, fast, keyboard-centric menu powered by <code>fuzzel</code>. Everything you need to manage your machine is reachable in milliseconds via <code>Super + D</code>.</p>

            <div class="control-grid">
              <div class="control-box">
                <h3>System Update</h3>
                <p>A unified script that updates Apt, Cargo, Bun, and Flatpak in one pass.</p>
              </div>
              <div class="control-box">
                <h3>WiFi & Network</h3>
                <p>Fast connection management with signal strength and T-Link fleet mesh support.</p>
              </div>
              <div class="control-box">
                <h3>Themes & UI</h3>
                <p>Switch between Glass, Solid, Cyber, and Paper themes instantly without restarting.</p>
              </div>
              <div class="control-box">
                <h3>Performance</h3>
                <p>Toggle ZRAM, GameMode, and specialized power profiles on the fly.</p>
              </div>
              <div class="control-box">
                <h3>Security</h3>
                <p>One-tap firewall (ufw) hardening and connection auditing.</p>
              </div>
              <div class="control-box">
                <h3>VMs & Virtualization</h3>
                <p>One-click setup for macOS (OSX-KVM), Windows 11, and Linux virtualization.</p>
              </div>
              <div class="control-box">
                <h3>Containers & Stacks</h3>
                <p>Instantly spawn Arch (AUR), Alpine, or Nix environments via Distrobox.</p>
              </div>
              <div class="control-box">
                <h3>Gaming & Software</h3>
                <p>Toggle Steam, GameMode, and specialized driver stacks from a single menu.</p>
              </div>
              <div class="control-box">
                <h3>Power Menu</h3>
                <p>Clean, fast Suspend, Reboot, and Shutdown triggers.</p>
              </div>
            </div>
          </section>

          <section class="modular">
            <h2>Modular by Design</h2>
            <p>The Control Center is not a single monolith. It is a collection of high-efficiency bash scripts and C binaries. This means it is easily extensible and uses near-zero RAM when not in use.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
