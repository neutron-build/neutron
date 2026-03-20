import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Distrobox Mastery — Tebian" };
}

export default function DistroboxMastery() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Advanced Manual</span>
          <h1>The Distrobox Mastery Manual</h1>
          <p class="meta">AUR on Debian: Arch Performance without the Arch Fragility.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Best of All Worlds</h2>
            <p>In the Linux world, you are usually forced to choose: the stability of Debian or the massive software availability of the Arch User Repository (AUR). Tebian ends this conflict. Using <strong>Distrobox</strong>, we allow you to run any Linux distribution inside a container while integrating it seamlessly into your host desktop. This is the "God-Tier" of Linux configuration.</p>

            <p>This manual explains how to use Distrobox to access the AUR, test Fedora tools, or run legacy Ubuntu binaries—all while keeping your Tebian host session 100% stable.</p>
          </section>

          <section class="technical-foundation">
            <h2>1. The Foundation: Podman and Namespace Isolation</h2>
            <p>Distrobox is not a Virtual Machine. It is a wrapper for <strong>Podman</strong> (or Docker) that uses Linux <strong>Namespaces</strong> to share the host's kernel, hardware, and home directory. This means there is <strong>Zero Performance Overhead.</strong> A program running in an Arch container on Tebian is as fast as if it were running on raw Arch.</p>

            <h3>Rootless execution</h3>
            <p>Tebian pre-configures Podman to run in <strong>Rootless Mode.</strong> This means your containers don't have administrative access to your host OS. If a malicious package in the AUR tries to wipe your drive, it is trapped inside the container's namespace. This is "Sovereign Isolation."</p>
          </section>

          <section class="aur-on-debian">
            <h2>2. The AUR on Debian: A Step-by-Step Guide</h2>
            <p>The most common use for Distrobox on Tebian is accessing the AUR. Here is the technical workflow:</p>
            <ol>
              <li><strong>Create:</strong> <code>distrobox create --name arch --image archlinux:latest</code></li>
              <li><strong>Enter:</strong> <code>distrobox enter arch</code></li>
              <li><strong>Setup:</strong> Inside the container, you install <code>yay</code> or <code>paru</code> just like you would on Arch.</li>
              <li><strong>Export:</strong> <code>distrobox-export --app [appname]</code>. This creates a <code>.desktop</code> file on your Tebian host.</li>
            </ol>
            <p>Now, when you press <code>Super+D</code> in Tebian, your Arch-based app appears in the menu. It opens in a window just like a native app. You have the AUR, but you still have the "Rock" foundation of Debian.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The End of Distro Hopping</h2>
            <p>Distrobox Mastery means you never have to "Distro Hop" again. If you want a feature from Fedora, you spin up a container. If you want a tool from Kali, you spin up a container. Tebian is the <strong>Universal Host</strong> that manages the Fleet. One ISO. One menu. Every Linux app in existence.</p>
          </section>
        </article>
      </main>
    </>
  );
}
