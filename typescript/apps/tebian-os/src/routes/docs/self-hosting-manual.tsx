import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Self-Hosting Manual — Tebian" };
}

export default function SelfHostingManual() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Self-Hosting Manual</h1>
          <p class="meta">Building the Mothership: Syncthing, Vaultwarden, and Matrix Setup.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most self-hosting tutorials are too complex. They require you to manually manage Docker, Nginx, and SSL. Tebian's "Mothership Mode" provides a pre-configured, hardened stack for the three essential pillars of digital sovereignty. This guide explains how to deploy and manage them with our one-click "Control Center."</p>

            <p>We use **Podman** for rootless, daemonless container execution, ensuring your host OS (Debian) remains 100% clean and secure.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. File Sync (Syncthing)</h3>
              <p>Syncthing is a P2P folder synchronization tool. It doesn't use a central server; it connects your devices directly using a secure, encrypted mesh. In Tebian, we pre-configure the local discovery and file-watching daemons.</p>
              <ul>
                <li><strong>P2P Mesh:</strong> Direct communication between PC, Phone, and Tablet.</li>
                <li><strong>Encryption:</strong> All data is encrypted in transit using TLS 1.3.</li>
                <li><strong>Versioning:</strong> Keep "trash" backups of deleted or modified files for 30 days.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Secrets (Vaultwarden)</h3>
              <p>Vaultwarden is a lightweight implementation of Bitwarden written in <strong>Rust</strong>. It is 100% compatible with official Bitwarden apps but uses 95% less RAM. It's the perfect "Mothership" service.</p>
              <ul>
                <li><strong>Rust-Powered:</strong> Zero-cost abstraction, high-performance security.</li>
                <li><strong>API Compatible:</strong> Works with official Bitwarden browser extensions and mobile apps.</li>
                <li><strong>Zero-Knowledge:</strong> Only you have the master key. Even if your server is stolen, your vault is safe.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Communication (Matrix)</h3>
              <p>Matrix is an open protocol for secure, decentralized chat. Tebian includes a pre-configured <strong>Synapse</strong> server (or the lighter <strong>Conduit</strong> written in Rust) and the <strong>Element</strong> web client.</p>
              <ul>
                <li><strong>Decentralized:</strong> Federation with other Matrix servers (like Mozilla or KDE).</li>
                <li><strong>End-to-End Encryption:</strong> All chats and calls are private by default.</li>
                <li><strong>Bridges:</strong> Connect your Matrix account to Telegram, WhatsApp, and Discord using <code>mautrix</code> bridges.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Reverse Proxy (Caddy)</h3>
              <p>To access your services securely over the internet, Tebian uses <strong>Caddy</strong>. It's a modern, C-based web server that handles SSL certificates (Let's Encrypt) automatically. No manual configuration required.</p>
              <ul>
                <li><strong>Automatic HTTPS:</strong> Caddy fetches and renews SSL certs for your domain.</li>
                <li><strong>Modern Defaults:</strong> HTTP/3 and TLS 1.3 by default.</li>
                <li><strong>Simple Config:</strong> Our one-click setup handles the <code>Caddyfile</code> for you.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Self-Host on Tebian?</h2>
            <p>Tebian's stability (Debian Stable base) makes it the perfect host for a "Mothership." Your server won't break on an update, and our Podman-based isolation ensures your apps are secure. You get a "Cloud" experience with the security of a fortress. One ISO. One menu. One Mothership.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
