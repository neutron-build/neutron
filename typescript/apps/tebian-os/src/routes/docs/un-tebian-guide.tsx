import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Un-Tebian Guide — Tebian" };
}

export default function UnTebianGuide() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Un-Tebian Guide</h1>
          <p class="meta">The Reversible OS: Reverting to Pure Debian without Data Loss.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Philosophy of Zero Lock-In</h2>
            <p>The greatest fear of any power user is "OS Lock-In." You install a custom distro, you spend months configuring it, and then the maintainer disappears, or the project shifts direction, and you are stuck with a "fork" that has no future. <strong>Tebian is not a fork.</strong> It is a usability layer on top of Debian. Our most important feature is that you can delete us.</p>

            <p>This guide provides a C-level technical breakdown of how Tebian isolates its components and the exact, step-by-step process to revert your machine to a 100% pure, stock Debian installation without losing a single personal file. This is the ultimate "Sovereign" guarantee.</p>
          </section>

          <section class="technical-isolation">
            <h2>1. Architectural Isolation: The ~/Tebian/ Pillar</h2>
            <p>Most distros scatter their configurations across <code>/etc</code>, <code>/usr/share</code>, and <code>/var</code>. If you want to "uninstall" their changes, you have to perform a surgical strike on your entire filesystem. Tebian uses a <strong>Pillar Isolation</strong> model. Everything that makes Tebian "Tebian" is stored in one place: <code>~/Tebian/</code>.</p>

            <p>When you run the <code>desktop.sh</code> installer, it doesn't "overwrite" your Debian system. It symlinks. Your <code>~/.config/sway/config</code> is not a file; it is a pointer to <code>~/Tebian/configs/sway/config</code>. This design choice is fundamental to our "Zero Lock-In" promise. By breaking the symlinks, you effectively "turn off" Tebian.</p>

            <h3>The Symlink Map</h3>
            <p>During a standard install, Tebian creates the following pointers:</p>
            <ul>
              <li><strong>Config:</strong> <code>~/.config/[app]</code> -&gt; <code>~/Tebian/configs/[app]</code></li>
              <li><strong>Scripts:</strong> <code>~/.local/bin/tebian-*</code> -&gt; <code>~/Tebian/scripts/tebian-*</code></li>
              <li><strong>Assets:</strong> <code>~/.local/share/backgrounds/tebian</code> -&gt; <code>~/Tebian/assets/wallpapers</code></li>
            </ul>
            <p>Because the actual files live in <code>/opt</code>, your <code>$HOME</code> directory remains "clean" in the eyes of the Debian package manager (dpkg).</p>
          </section>

          <section class="reversion-process">
            <h2>2. The Reversion Process: Step-by-Step</h2>
            <p>If you decide to return to pure Debian, the process is deterministic and safe. We have automated this in <code>uninstall.sh</code>, but for the "Universal" reader, here is the manual breakdown of what happens under the hood.</p>

            <h3>Step A: Breaking the Pointers</h3>
            <p>The first step is to remove the symlinks. This is a non-destructive action. It doesn't delete the Tebian code; it just stops your user session from using it. We use the <code>find</code> command to locate and remove any link pointing to <code>~/Tebian</code>.</p>
            <pre><code>find ~ -maxdepth 3 -type l -lname '~/Tebian/*' -delete</code></pre>
            <p>At this moment, if you were to logout and back in, your Sway session would load the default Debian configs (or fail if they don't exist), but your data remains untouched.</p>

            <h3>Step B: Restoring the Defaults</h3>
            <p>Tebian makes very few changes to system-wide files in <code>/etc</code>, but we do touch <code>/etc/apt/sources.list</code> to ensure you have access to <code>non-free-firmware</code>. To revert, we restore the stock Debian sources list. This ensures that every future <code>apt update</code> is talking only to official Debian servers.</p>

            <h3>Step C: Package Purging</h3>
            <p>Tebian installs exactly 3-10 base packages (sway, fuzzel, mako, etc.). To revert, we perform a <code>purge</code>. In the Debian world, <code>purge</code> is more powerful than <code>remove</code>; it deletes the binaries AND the system-wide configuration files.</p>
            <pre><code>sudo apt purge sway fuzzel mako kitty waybar wob brightnessctl</code></pre>
            <p>Because these were official Debian packages to begin with, the system removes them cleanly. There are no "Tebian-specific" orphans left behind.</p>
          </section>

          <section class="data-integrity">
            <h2>3. Data Integrity: Your Home is Yours</h2>
            <p>The most critical part of the "Un-Tebian" philosophy is that we <strong>never</strong> touch your personal data. Your <code>Documents</code>, <code>Pictures</code>, <code>Downloads</code>, and <code>Projects</code> are yours. Tebian doesn't use a custom home structure. We don't hide your data in "Tebian Cloud" folders. We use the standard XDG User Directories.</p>

            <p>When you uninstall Tebian, your <code>Projects/</code> folder (where your code lives) and your <code>Archive/</code> (where your backups live) are completely ignored by the uninstaller. This is the difference between a "Distro" and a "Layer." A distro owns your partitions; a layer only owns its config.</p>
          </section>

          <section class="c-level-audit">
            <h2>4. The C-Level Audit: Verifying the Reversion</h2>
            <p>For the skeptical developer, how do you verify that Tebian is truly gone? You perform a system audit. Because we don't fork the kernel and we don't use a custom repo, there are no "foreign" packages to find. You can run:</p>
            <pre><code>apt-forktracer</code></pre>
            <p>This tool will show you any package that doesn't match the official Debian repositories. On a reverted Tebian system, this list will be empty. You are now running 100% pure Debian Stable.</p>

            <h3>The Persistence of Firmware</h3>
            <p>One caveat: If you used Tebian to install proprietary drivers (NVIDIA or Intel/AMD firmware), those files will remain in <code>/lib/firmware</code>. We choose to keep these during a standard uninstall because they are necessary for your hardware to function. If you want a 100% "Free Software" system (FSF style), you must manually remove the <code>non-free-firmware</code> packages.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Freedom is the Feature</h2>
            <p>Tebian is designed to be the "Greatest OS" because it respects your right to leave. We believe that if we provide enough value—speed, stability, and a beautiful C-based interface—you won't <em>want</em> to leave. But if you do, the door is wide open. You aren't "switching" back to Debian; you were always there. We just provided the view.</p>
          </section>
        </article>
      </main>
    </>
  );
}
