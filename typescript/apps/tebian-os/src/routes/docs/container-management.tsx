import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Headless Container Management — Tebian" };
}

export default function ContainerManagement() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>Headless Container Management</h1>
          <p class="meta">Podman, Docker, and Distrobox without the Desktop Bloat.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>In most operating systems, running containers requires a "Desktop App" (Docker Desktop, Podman Desktop). These apps are Electron-based, use massive amounts of RAM, and add an unnecessary layer of complexity between you and the engine. In Tebian, we use the <strong>Headless CLI</strong> engines directly.</p>

            <p>This guide explains how to manage your development environments and services using raw, high-performance container engines.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The Podman Advantage</h3>
              <p>Podman is the "Docker alternative" with a key difference: it is <strong>Daemonless</strong>. Unlike Docker, it doesn't run a background process as root. It's just a tool that starts containers.</p>
              <ul>
                <li><strong>Rootless:</strong> Run containers as your normal user for better security.</li>
                <li><strong>Docker Alias:</strong> Simply <code>alias docker=podman</code>. Same commands, better engine.</li>
                <li><strong>Zero-Overhead:</strong> No background daemons consuming RAM.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Distrobox (The AUR Bridge)</h3>
              <p>This is Tebian's "Secret Weapon." Distrobox lets you run any Linux distribution (Arch, Alpine, Fedora) inside a container, but it <strong>integrates</strong> them into your host desktop. You can run Arch AUR apps as if they were native to Tebian.</p>
              <ul>
                <li><strong>Arch on Debian:</strong> Get the latest AUR packages without the fragility of an Arch host.</li>
                <li><strong>Isolation:</strong> Keep your main OS clean while you test messy dependencies.</li>
                <li><strong>Performance:</strong> It's a container, not a VM. There is zero performance penalty.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Docker-Compose for Services</h3>
              <p>For complex, multi-container stacks (like a database + web server), Tebian's "Container Mode" includes a pre-configured <code>docker-compose</code> setup that talks directly to the <strong>Docker.io</strong> engine.</p>
              <ul>
                <li><strong>Systemd Integration:</strong> Auto-start your containers on boot.</li>
                <li><strong>Networking:</strong> Pre-configured bridges for container-to-host communication.</li>
                <li><strong>Volumes:</strong> Secure, local storage for your database data.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. The "No GUI" Policy</h3>
              <p>Why avoid the GUI? Because GUIs lie to you. They hide the configuration files and the error logs. When a container fails in a GUI, you get a generic "Error." When you use the CLI, you get the raw <strong>STDOUT</strong>. In Tebian, we teach you how to be the root of your containers.</p>
              <ul>
                <li><strong>CLI Tools:</strong> <code>ctop</code> for container monitoring, <code>lazydocker</code> for terminal TUI management.</li>
                <li><strong>Direct Logs:</strong> <code>podman logs -f [container]</code>. See the truth in real-time.</li>
                <li><strong>Pure Performance:</strong> No GUI means more RAM for your containers.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Containers on Tebian?</h2>
            <p>Tebian's stability (Debian base) makes it the perfect host for containers. Your host never changes, but your containers can be anything. It's the ultimate "Dev/Ops" balance: a rock-solid foundation with the freedom to run any software in a sandbox.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
