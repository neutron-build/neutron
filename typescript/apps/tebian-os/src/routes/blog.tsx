import "../styles/blog-hub.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Philosophy" };
}

export default function Blog() {
  return (
    <>
      <main>
        <h1 class="page-title">Philosophy</h1>
        <p class="subtitle">Essays on digital ethics, technical debt, and the C-level soul of the OS.</p>

        <div class="journal-hub">
          <section class="hub-section">
            <h2>The Foundation</h2>
            <div class="journal-grid pillar-grid">
              <a href="/manifesto" class="journal-card pillar-card">
                <span class="category">Core</span>
                <h2>The Manifesto</h2>
                <p>Sovereignty through minimalism. Power through the Fleet. The Law of Zero.</p>
              </a>
              <a href="/reasoning" class="journal-card pillar-card">
                <span class="category">Core</span>
                <h2>Technical Reasoning</h2>
                <p>The technical "Why" behind our 3-package C-based foundation.</p>
              </a>
              <a href="/honors" class="journal-card pillar-card">
                <span class="category">Core</span>
                <h2>The Honors</h2>
                <p>Respecting the survivors. Why we stand on the shoulders of giants.</p>
              </a>
            </div>
          </section>

          <section class="hub-section">
            <h2>System &amp; Interface</h2>
            <div class="journal-grid">
              <a href="/blog/animation-fallacy" class="journal-card"><span class="category">Interface</span><h2>Animation Fallacy</h2><p>Why "Smoothness" is a lie used to hide system latency. Choosing response over aesthetics.</p></a>
              <a href="/blog/art-of-the-shell" class="journal-card"><span class="category">Mastery</span><h2>Art of the Shell</h2><p>The philosophical defense of the Command Line Interface as the ultimate UI.</p></a>
              <a href="/blog/ghost-ui" class="journal-card"><span class="category">Focus</span><h2>The Ghost UI</h2><p>Invisible until needed. Reclaiming the pixels for your work.</p></a>
              <a href="/blog/shell-vs-ide" class="journal-card"><span class="category">Workflow</span><h2>The Shell vs. The IDE</h2><p>Why the terminal is an engine and the IDE is a restrictive framework.</p></a>
              <a href="/blog/de-is-dead" class="journal-card"><span class="category">Architecture</span><h2>Death of the DE</h2><p>Desktop environments are obsolete. Compositors replaced them.</p></a>
              <a href="/blog/bloat-anatomy" class="journal-card"><span class="category">Analysis</span><h2>The Anatomy of Bloat</h2><p>Why modern software wastes resources despite faster hardware.</p></a>
              <a href="/blog/c-level" class="journal-card"><span class="category">Philosophy</span><h2>The C-Level Philosophy</h2><p>Building systems close to the hardware. Why C still matters.</p></a>
              <a href="/blog/rust-c-synthesis" class="journal-card"><span class="category">Engineering</span><h2>The Rust &amp; C Synthesis</h2><p>Combining C's performance with Rust's safety for robust systems.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Migration &amp; Economics</h2>
            <div class="journal-grid">
              <a href="/blog/windows-exodus" class="journal-card"><span class="category">Migration</span><h2>The Windows Exodus</h2><p>Why 2026 is the year to leave Redmond. Data-harvesting and legacy debt.</p></a>
              <a href="/blog/myth-of-native-speed" class="journal-card"><span class="category">Hardware</span><h2>Myth of Native Speed</h2><p>Technical proof: Why Linux is the faster OS for your Mac hardware.</p></a>
              <a href="/blog/chromeos-prison" class="journal-card"><span class="category">System</span><h2>The ChromeOS Prison</h2><p>Reclaiming the lobotomized machine from Google's verified boot cage.</p></a>
              <a href="/blog/snap-trap" class="journal-card"><span class="category">Distribution</span><h2>The Snap Trap</h2><p>Why Ubuntu lost its way. The danger of centralized package control.</p></a>
              <a href="/blog/upgrade" class="journal-card"><span class="category">Performance</span><h2>The 10-Minute Upgrade</h2><p>Your OS is the bottleneck, not your hardware. Optimize before you buy.</p></a>
              <a href="/blog/adobe-refugee" class="journal-card"><span class="category">Creative</span><h2>The Adobe Refugee</h2><p>Professional creative workflows without the subscription.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Distro Wars</h2>
            <div class="journal-grid">
              <a href="/blog/debian-vs-arch-ubuntu" class="journal-card"><span class="category">Comparison</span><h2>Why Debian Won</h2><p>And why Arch lost. Stability vs bleeding edge vs corporate.</p></a>
              <a href="/blog/fragility-of-the-roll" class="journal-card"><span class="category">Stability</span><h2>Fragility of the Roll</h2><p>Why Arch Linux is a part-time job. Rolling releases break.</p></a>
              <a href="/blog/forced-upgrades" class="journal-card"><span class="category">Maintenance</span><h2>The Forced Upgrade</h2><p>Fedora's 6-month cycle is a maintenance nightmare.</p></a>
              <a href="/blog/kali-fallacy" class="journal-card"><span class="category">Security</span><h2>The Kali Fallacy</h2><p>You don't need a "Hacker OS" to be a hacker.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Privacy &amp; Psychology</h2>
            <div class="journal-grid">
              <a href="/blog/privacy-performance" class="journal-card"><span class="category">Efficiency</span><h2>Privacy as Performance</h2><p>How removing telemetry daemons reclaim CPU cycles and battery life.</p></a>
              <a href="/blog/death-of-distro-hop" class="journal-card"><span class="category">Psychology</span><h2>End of the Distro Hop</h2><p>How unopinionated infrastructure ends the search for the "perfect" Linux.</p></a>
              <a href="/blog/lazarus-engine" class="journal-card"><span class="category">Survival</span><h2>The Lazarus Engine</h2><p>The math of 16MB RAM and the battle against planned obsolescence.</p></a>
              <a href="/blog/myth-of-security" class="journal-card"><span class="category">Security</span><h2>The Myth of Security</h2><p>Why adding security software often makes your computer less safe.</p></a>
              <a href="/blog/privacy-of-silence" class="journal-card"><span class="category">Telemetry</span><h2>The Privacy of Silence</h2><p>Zero telemetry. Not reduced — zero.</p></a>
            </div>
          </section>

          <section class="hub-section">
            <h2>Sovereignty &amp; The Fleet</h2>
            <div class="journal-grid">
              <a href="/blog/sovereignty" class="journal-card"><span class="category">Philosophy</span><h2>Digital Sovereignty</h2><p>Own your data. Own your infrastructure. Own your future.</p></a>
              <a href="/blog/mothership" class="journal-card"><span class="category">Self-Hosting</span><h2>The Mothership</h2><p>Self-hosting for the rest of us. Your cloud, your rules.</p></a>
              <a href="/blog/one-brain-many-bodies" class="journal-card"><span class="category">Fleet</span><h2>One Brain, Many Bodies</h2><p>Your OS isn't a machine. It's a fleet.</p></a>
              <a href="/blog/local-intelligence" class="journal-card"><span class="category">AI</span><h2>Local Intelligence</h2><p>Your AI should be a binary on your machine, not an API call.</p></a>
              <a href="/blog/sovereign-enterprise" class="journal-card"><span class="category">Enterprise</span><h2>The Sovereign Enterprise</h2><p>Workstations built on user control, not corporate surveillance.</p></a>
              <a href="/blog/sovereign-gamer" class="journal-card"><span class="category">Gaming</span><h2>The Sovereign Gamer</h2><p>Desktop gaming with SteamOS philosophy, minus the bloat.</p></a>
              <a href="/blog/death-of-gaming-pc" class="journal-card"><span class="category">Hardware</span><h2>Death of the Gaming PC</h2><p>Your next console is a Tebian rig.</p></a>
              <a href="/blog/nvidia-wayland" class="journal-card"><span class="category">Drivers</span><h2>NVIDIA on Wayland</h2><p>Taming the beast. NVIDIA finally works on Wayland.</p></a>
            </div>
          </section>
        </div>
      </main>
    </>
  );
}
