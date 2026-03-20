import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "One Brain, Many Bodies — Tebian" };
}

export default function OneBrainManyBodies() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Philosophy</span>
          <h1>One Brain, Many Bodies: The Fleet Philosophy</h1>
          <p class="meta">February 20, 2026 &bull; 12 min read</p>
        </header>
        <article class="content">
          <p class="lead">Most people see their PC, their laptop, and their server as three different machines with three different identities. Tebian sees them as one "Fleet." Your OS shouldn't be a machine; it should be an identity that lives on any hardware you touch.</p>

          <h2>The Fragmentation Problem</h2>
          <p>In the traditional world, your PC runs Windows, your laptop runs macOS, and your Raspberry Pi runs some flavor of Linux. They have different shortcuts, different configurations, and different ways of working. This is a massive waste of cognitive bandwidth. Every time you switch devices, your brain has to "context shift."</p>

          <p>Tebian's "Fleet" philosophy is built on <strong>Total Consistency</strong>. Whether you are on a $5,000 Threadripper workstation, a 10-year-old ThinkPad, or a $35 Raspberry Pi, the experience is exactly the same. The shortcuts (<code>Super+D</code>), the themes (Stealth Glass), and the tools (Fuzzel) are identical.</p>

          <h2>The T-Link Mesh (Tailscale/Headscale)</h2>
          <p>A Fleet is only as strong as its connections. Tebian includes <strong>T-Link</strong>, a pre-configured mesh network based on <strong>Tailscale</strong> (or our self-hosted <strong>Headscale</strong> option). It creates a "Private Cloud" where all your devices can see each other as if they were on the same physical router, no matter where they are in the world.</p>

          <p>You can SSH from your laptop in a coffee shop to your workstation at home without opening a single port on your router. You can sync your files, share your clipboard, and even stream your "Mothership" desktop to your phone—all securely encrypted via WireGuard.</p>

          <h2>The T-Sync Concept</h2>
          <p>What makes it "One Brain"? <strong>Configuration Sync.</strong> Through our upcoming <code>t-sync</code> utility, your dotfiles, your shell history, and your system settings are versioned and synced across all your nodes. If you change a shortcut on your PC, it updates on your laptop. If you install a tool on your workstation, it's ready on your Raspberry Pi. Your OS becomes a "portable soul" that can inhabit any body.</p>

          <h2>Why Arm64 and x86 Compatibility Matters</h2>
          <p>Tebian is one of the only "Universal" OSs that provides first-class support for both x86 (Intel/AMD) and Arm64 (Raspberry Pi/Apple Silicon). We build our ISOs for both architectures simultaneously. This means you can have a high-performance PC and a low-power Pi 5 server running <em>the exact same Tebian code</em>. They are just different bodies for the same brain.</p>

          <h2>Conclusion: The Future of Computing</h2>
          <p>The era of "The PC" is over. The era of "The Fleet" is here. By choosing Tebian, you aren't just picking an operating system for one computer; you are choosing a unified digital identity that respects your focus and follows you anywhere. One ISO. One menu. One Fleet. Infinite bodies.</p>
        </article>
      </main>
    </>
  );
}
