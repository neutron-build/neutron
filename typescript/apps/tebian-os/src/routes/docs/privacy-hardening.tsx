import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Digital Fortress — Tebian" };
}

export default function PrivacyHardening() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Digital Fortress</h1>
          <p class="meta">Military-Grade Privacy: UFW, Fail2Ban, and Zero Telemetry.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Your operating system is the last line of defense between your private data and the surveillance capitalism of the internet. Windows and macOS have become double agents—they protect you from hackers, but sell you to advertisers. Tebian is loyal only to you.</p>

            <p>This guide explains the "Fortress" stack pre-configured in Tebian's "Security Mode."</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The Firewall (UFW)</h3>
              <p>Tebian uses <strong>Uncomplicated Firewall (UFW)</strong>, a wrapper for the Linux kernel's Netfilter. In "Hardened Mode," it blocks all incoming connections by default.</p>
              <ul>
                <li><strong>Default Deny:</strong> No one can connect to your machine unless you explicitly allow it.</li>
                <li><strong>Port Knocking:</strong> Advanced users can set up secret sequences to open SSH ports.</li>
                <li><strong>AppArmor:</strong> Mandatory Access Control that confines programs to a limited set of resources.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The Bouncer (Fail2Ban)</h3>
              <p>If you run a server or SSH on your desktop, you are under attack. Bots are constantly guessing your password. <strong>Fail2Ban</strong> watches your logs. If an IP fails to log in 3 times, it is banned permanently.</p>
              <ul>
                <li><strong>SSH Guard:</strong> Bans brute-force attacks instantly.</li>
                <li><strong>Web Guard:</strong> Protects your self-hosted services (Nextcloud, Matrix).</li>
                <li><strong>Jail Time:</strong> Configurable ban duration (1 hour to forever).</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Zero Telemetry</h3>
              <p>This is the most important security feature. Tebian has <strong>Zero Telemetry</strong>. We don't collect crash reports. We don't collect usage data. We don't know who you are.</p>
              <ul>
                <li><strong>No Account Required:</strong> You don't need an email to use your OS.</li>
                <li><strong>Local Search:</strong> Fuzzel searches your drive, not the web.</li>
                <li><strong>Private DNS:</strong> By default, we use privacy-respecting resolvers (Quad9/Cloudflare) over ISP DNS.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Disk Encryption (LUKS)</h3>
              <p>If your laptop is stolen, your data is gone. Unless it is encrypted. The Tebian installer (Debian Installer) offers Full Disk Encryption (LUKS) with a single checkbox.</p>
              <ul>
                <li><strong>AES-256:</strong> Military-grade encryption standard.</li>
                <li><strong>Pre-Boot Auth:</strong> You must enter a password before the OS even loads.</li>
                <li><strong>Swap Encryption:</strong> Prevents secrets from leaking into swap space.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Privacy is a Right</h2>
            <p>In a world where every click is tracked, having a "silent" operating system is a radical act. Tebian doesn't just protect you from external threats; it protects you from the OS itself. We believe that privacy is not a setting you toggle; it is the default state of a sovereign machine.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
