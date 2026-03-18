import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Compatibility — Tebian" };
}

export default function Compatibility() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Documentation</span>
          <h1>Compatibility</h1>
          <p class="meta">The OS that runs other OSs.</p>
        </header>
        <article class="content">
          <section class="meta-layer">
            <p>Tebian isn't just a Linux distribution; it's a bridge. We recognize that sometimes you need tools from other ecosystems. Instead of switching machines, we bring those ecosystems to you.</p>

            <div class="compat-grid">
              <div class="compat-card">
                <h3>t-mac</h3>
                <p>One-click macOS VM setup (OSX-KVM). Access Xcode, iOS development, and the App Store with full USB passthrough and iCloud support.</p>
                <code class="compat-cmd">fuzzel &rarr; Virtualization &rarr; Setup macOS</code>
              </div>
              <div class="compat-card">
                <h3>t-win</h3>
                <p>Guided dual-boot partitioning for Anti-Cheat games and proprietary software that won't run in a VM. Safe, automated, and GRUB-integrated.</p>
                <code class="compat-cmd">fuzzel &rarr; Virtualization &rarr; Setup Windows</code>
              </div>
              <div class="compat-card">
                <h3>t-droid</h3>
                <p>Run Android apps natively on your desktop via Waydroid. No emulation, just containerized performance for your mobile-only tools.</p>
                <code class="compat-cmd">fuzzel &rarr; Software &rarr; Android Mode</code>
              </div>
            </div>
          </section>

          <section class="philosophy">
            <h2>No Compromise</h2>
            <p>Most Linux users feel forced to keep a second "sacrificial" machine for work or gaming. Tebian eliminates this. We prioritize virtualization and dual-boot logic so you can have 100% compatibility without ever sacrificing your privacy or stability.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
