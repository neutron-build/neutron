import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Death of the DE — Tebian" };
}

export default function DeIsDead() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Minimalism</span>
          <h1>The Death of the Desktop Environment</h1>
          <p class="meta">February 20, 2026 &bull; 12 min read</p>
        </header>

        <article class="content">
          <p class="lead">The "Desktop Environment" is a legacy concept from the 1990s. In 2026, the DE is dead. The future belongs to the <strong>Compositor</strong>. This is why Tebian rejects GNOME, KDE, and XFCE in favor of a pure, C-based Sway core.</p>

          <h2>What is a Desktop Environment (DE)?</h2>
          <p>A "DE" is a collection of dozens of different programs bundled together: a window manager, a panel, a notification daemon, a file manager, a settings app, a login screen, and thousands of icons. While convenient, they are inherently bloated. They assume you want everything they have. They are an "All-You-Can-Eat" buffet where you only need a single plate.</p>

          <p>Ubuntu (Omakub) uses <strong>GNOME</strong>. GNOME is a monolithic DE. It is an opinionated ecosystem that is hard to strip down. It's a "black box" that consumes nearly 2GB of RAM just to show you the desktop. It is a product of "Experience Design," not "Engineering Design."</p>

          <h2>The Compositor Solution (Sway)</h2>
          <p>A <strong>Compositor</strong>, on the other hand, is a single program that does only what is necessary: it talks to the Wayland protocol, manages window placement, and renders frames. That's it.</p>

          <p>Tebian uses <strong>Sway</strong>. Sway is a "Wayland Compositor." It is a single C binary that manages your windows with mathematical precision. Because it doesn't "bundle" a panel or a settings app, we are free to add only what is necessary. We add <strong>Fuzzel</strong> for menus, <strong>Mako</strong> for notifications, and <strong>Status.sh</strong> for information. These are all separate, small, C-based components.</p>

          <h2>The Modular Advantage</h2>
          <p>By using a compositor instead of a DE, Tebian is <strong>Modular</strong>. If you don't want notifications, you don't run <code>mako</code>. If you don't want a bar, you don't run <code>status.sh</code>. In a DE like GNOME, you can't "uninstall" the panel without breaking the whole system. In Tebian, every component is optional. This is how we achieve a 16MB base desktop.</p>

          <h2>The Tiling Tipping Point</h2>
          <p>DEs (GNOME/macOS) focus on "floating" windows. You spend half your day moving, resizing, and clicking on window borders. This is a waste of human-CPU cycles. Tebian (like Omarchy) is a <strong>Tiling</strong> compositor by default. Windows are arranged in a grid automatically. They fill the screen. They don't overlap. You spend 0% of your time "managing" windows and 100% of your time "using" them.</p>

          <p>However, unlike Omarchy (which uses the complex, animation-heavy Hyprland), Tebian uses <strong>Sway</strong>. Sway is the "i3" of Wayland. It is rock-solid, predictable, and blindingly fast. It doesn't wobble. It doesn't bounce. It just works.</p>

          <h2>Conclusion: The "Invisible" OS</h2>
          <p>The "Death of the DE" is really the "Birth of the Invisible OS." When you remove the DE, you remove the branding, the bloat, and the barriers. All that remains is a compositor that handles the hardware and lets you do your work. That is the Tebian promise.</p>
        </article>
      </main>
    </PageShell>
  );
}
