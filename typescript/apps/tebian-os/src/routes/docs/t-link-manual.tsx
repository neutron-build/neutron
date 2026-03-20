import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The T-Link Manual — Tebian" };
}

export default function TLinkManual() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The T-Link Manual</h1>
          <p class="meta">Connecting the Fleet: Tailscale, Headscale, and Mesh Networking.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>A Fleet is only as strong as its connections. Traditionally, connecting devices across different networks (home, office, mobile) required complex VPNs, port forwarding, and static IPs. <strong>T-Link</strong> simplifies this using a mesh network based on <strong>WireGuard</strong>. This guide explains how to connect your PCs, laptops, servers, and phones into a single private mesh.</p>

            <p>We provide both the standard <strong>Tailscale</strong> option and the self-hosted <strong>Headscale</strong> option for those who want 100% control over their coordination server.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The T-Link Engine (WireGuard)</h3>
              <p>T-Link is built on <strong>WireGuard</strong>, the fastest and most secure VPN protocol in the world. It is a kernel-level module that provides encrypted P2P tunnels between all your devices.</p>
              <ul>
                <li><strong>P2P Mesh:</strong> Your devices connect directly to each other without a central bottleneck.</li>
                <li><strong>Encrypted:</strong> All traffic is encrypted using modern cryptography (ChaCha20).</li>
                <li><strong>Fast:</strong> WireGuard is significantly faster than OpenVPN or IPsec.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Tailscale (The Standard)</h3>
              <p>Tailscale is the easiest way to start your Fleet. It uses a hosted coordination server to handle the identity and NAT traversal for you. Tebian's "T-Link Menu" includes one-click installation and login.</p>
              <ul>
                <li><strong>MagicDNS:</strong> Access your devices by name (e.g., <code>laptop</code>, <code>server</code>).</li>
                <li><strong>NAT Traversal:</strong> Connect through firewalls and coffee shop WiFis without a public IP.</li>
                <li><strong>Identity:</strong> Use your existing Google, GitHub, or Microsoft account for login.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Headscale (The Mothership)</h3>
              <p>For those who want 100% sovereignty, Tebian includes a <strong>Headscale</strong> setup. It is a self-hosted implementation of the Tailscale coordination server. You can run it on your Tebian server (The Mothership) and own the entire network.</p>
              <ul>
                <li><strong>No Central Server:</strong> You don't need a Tailscale account. You are the provider.</li>
                <li><strong>Total Control:</strong> You manage the ACLs, the keys, and the device lists.</li>
                <li><strong>Free Forever:</strong> No device limits. No subscription fees.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Exit Nodes (The Travel Shield)</h3>
              <p>When you are on insecure public WiFi, you can use T-Link's <strong>Exit Node</strong> feature. It routes all your internet traffic through your Tebian "Mothership" at home, providing a secure tunnel and a familiar IP address.</p>
              <ul>
                <li><strong>Secure Browsing:</strong> Encrypt your coffee shop traffic.</li>
                <li><strong>Home IP:</strong> Access geo-locked content as if you were at home.</li>
                <li><strong>Always On:</strong> Keep your T-Link active for constant protection.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why T-Link on Tebian?</h2>
            <p>By integrating mesh networking into the core of your OS, Tebian turns your machines into a single, unified workspace. Your files are always available. Your SSH sessions are always secure. Your "One Brain" lives on every "Body" in your Fleet. One ISO. One menu. One T-Link Mesh.</p>
          </section>
        </article>
      </main>
    </>
  );
}
