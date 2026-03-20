import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Declarative Fatigue — Tebian" };
}

export default function DeclarativeFatigue() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Technical Manual</span>
          <h1>The Declarative Fatigue Manual</h1>
          <p class="meta">Why NixOS is too much work for a working human.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Hype of Immutability</h2>
            <p>In 2026, <strong>NixOS</strong> is the darling of the dev-ops world. It promises "Reproducible Systems" and "Atomic Rollbacks" through a single configuration file. It sounds like magic. But for most users, NixOS leads to <strong>Declarative Fatigue</strong>: a state where you spend more time fighting with the Nix language than actually using your computer. Tebian provides the <strong>Reproducibility</strong> of NixOS with the <strong>Familiarity</strong> of Debian.</p>

            <p>This manual provides the technical critique of the declarative model and explains how Tebian's "Pillar Isolation" is a more human-centric solution.</p>
          </section>

          <section class="the-nix-trap">
            <h2>1. The Nix Trap: Learning a Language to Use an OS</h2>
            <p>To configure NixOS, you must learn the <strong>Nix Language.</strong> It is a functional, lazy language with a steep learning curve. Want to install a package? You must edit a <code>.nix</code> file and rebuild. Want to change a shortcut? Edit the file and rebuild. This introduces a "Thinking Barrier" between the user and the system.</p>

            <h3>The Tebian Alternative</h3>
            <p>Tebian uses standard Bash and TOML for its scripts and configs. You don't need a degree in computer science to understand <code>tebian-settings</code>. It is a set of transparent scripts that you can read, edit, and run in real-time. We achieve "Reproducibility" by keeping our entire OS logic in a single folder (<code>~/Tebian</code>) that you can copy to any other machine.</p>
          </section>

          <section class="fhs-compliance">
            <h2>2. FHS Compliance: The World is not Nix</h2>
            <p>NixOS rejects the <strong>FHS (Filesystem Hierarchy Standard)</strong>. It doesn't have <code>/usr/bin</code> or <code>/lib</code> in the traditional sense. Everything lives in the <code>/nix/store</code>. This means that 90% of the world's software—which assumes an FHS layout—will not run on NixOS without custom "packaging" or "wrappers."</p>

            <p>Tebian is 100% <strong>FHS Compliant.</strong> Because we are pure Debian underneath, every binary, every script, and every proprietary tool (like Steam or Discord) works exactly as the developer intended. We don't break the world to fix the OS.</p>
          </section>

          <section class="maintenance-overhead">
            <h2>3. Maintenance Overhead</h2>
            <p>When something goes wrong in NixOS, you have to debug the configuration logic. When something goes wrong in Tebian, you debug the <strong>System.</strong> Because Tebian follows the standard Linux paradigms, the skills you learn are transferable to every other Linux system in the world. NixOS skills only work on NixOS.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Simple is Better than Sophisticated</h2>
            <p>The Declarative Fatigue Manual proves that "Statelessness" doesn't require a complex functional language. By using Debian as our base and isolating our configs in <code>~/Tebian</code>, we provide a system that is easy to move, easy to backup, and—most importantly—easy to use. One ISO. One menu. Zero declarative fatigue.</p>
          </section>
        </article>
      </main>
    </>
  );
}
