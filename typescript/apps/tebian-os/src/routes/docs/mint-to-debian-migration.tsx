import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Mint-to-Debian Migration — Tebian" };
}

export default function MintToDebianMigration() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Migration Manual</span>
          <h1>The Mint-to-Debian Migration Manual</h1>
          <p class="meta">Keeping the Simplicity, Removing the Wrapper.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Beginner's Paradox</h2>
            <p>Linux Mint is often the first stop for users leaving Windows. It provides a familiar "Cinnamon" desktop and a curated experience. But Mint is a wrapper around a wrapper (Ubuntu, which is a wrapper around Debian). This multi-layered architecture introduces lag, delays security updates, and adds unnecessary complexity. Tebian provides the <strong>Mint-like Simplicity</strong> on a <strong>Debian-direct foundation.</strong></p>

            <p>This manual explains how to migrate your workflow from Mint to Tebian while maintaining the ease of use you've come to expect.</p>
          </section>

          <section class="removing-the-wrapper">
            <h2>1. Why Remove the Wrapper?</h2>
            <p>When you use Mint, you are relying on three separate teams to keep your OS secure: the Debian team, the Canonical (Ubuntu) team, and the Mint team. If a security vulnerability is found in a core library, it must pass through all three layers before it hits your machine. In Tebian, we go <strong>Direct to Source.</strong></p>
            <ul>
              <li><strong>Direct Updates:</strong> Security patches land on Tebian the moment they are released by the Debian Security Team.</li>
              <li><strong>Performance Gains:</strong> By removing the "Cinnamon" overhead and using the C-based Sway core, your machine uses 80% less RAM at idle.</li>
              <li><strong>Pure Repositories:</strong> No "Mint-specific" repositories that can conflict with standard Debian packages.</li>
            </ul>
          </section>

          <section class="workflow-translation">
            <h2>2. Workflow Translation: Cinnamon to Sway</h2>
            <p>The transition from a "Start Menu" (Mint) to a "One Menu" (Tebian) is easier than you think. In Mint, you search for apps via the bottom-left menu. In Tebian, you press <code>Super+D</code> and use <strong>Fuzzel.</strong></p>

            <h3>Software Management</h3>
            <p>Mint users love the "Software Manager." Tebian provides a faster, text-based alternative in our <strong>Control Center.</strong> From the "Software" menu, you can install Steam, Flatpaks, and dev tools with a single click. It's the same power, without the slow GUI.</p>
          </section>

          <section class="data-transfer">
            <h2>3. Safe Data Transfer</h2>
            <p>Moving from Mint to Tebian is non-destructive for your files. Because both use the same <strong>FHS (Filesystem Hierarchy Standard)</strong>, you can simply back up your <code>/home</code> directory and drop it into Tebian. All your browser profiles, documents, and downloads will remain exactly where they were.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Graduation to the Source</h2>
            <p>Moving from Mint to Tebian is a "Graduation." You are moving from a system that manages you to a system that empowers you. You keep the stability and the friendliness, but you gain the speed and sovereignty of a direct Debian foundation. One ISO. One menu. Direct power.</p>
          </section>
        </article>
      </main>
    </>
  );
}
