import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Tebian Protocol — Tebian" };
}

export default function TebianProtocol() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Tebian Protocol</h1>
          <p class="meta">A complete technical reference for the World's Greatest OS.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>One Manual to Rule the Machine</h2>
            <p>Tebian is designed to be self-documenting. However, for the professional user who needs a quick reference for our C-level optimizations and custom scripts, we provide the <strong>Tebian Protocol.</strong> This is the definitive cheat sheet for managing your sovereign workstation.</p>
          </section>

          <section class="keybindings">
            <h2>1. Keybindings: The Sway Control</h2>
            <p>Tebian standardizes on the <code>Super</code> (Mod4) key. Here are the essential shortcuts:</p>
            <ul>
              <li><strong>[ Super + Enter ]</strong> - Launch Kitty (GPU-accelerated terminal).</li>
              <li><strong>[ Super + D ]</strong> - Launch Fuzzel (The universal app/settings menu).</li>
              <li><strong>[ Super + Shift + Q ]</strong> - Kill the focused window.</li>
              <li><strong>[ Super + Space ]</strong> - Toggle Floating/Tiling mode.</li>
              <li><strong>[ Super + R ]</strong> - Enter Resize mode (use arrow keys).</li>
              <li><strong>[ Super + V ]</strong> - Open Clipboard History (Cliphist).</li>
              <li><strong>[ Print ]</strong> - Snapshot (Regional to clipboard).</li>
            </ul>
          </section>

          <section class="scripts">
            <h2>2. The Tebian Scripts: ~/Tebian/scripts</h2>
            <p>Our custom logic lives in <code>~/Tebian/scripts/</code>. You can call these from anywhere.</p>
            <ul>
              <li><code>update-all</code> - Unifies apt and flatpak updates.</li>
              <li><code>tebian-settings</code> - Opens the Fuzzel-based Control Center.</li>
              <li><code>status.sh</code> - The zero-dependency bar script.</li>
              <li><code>tebian-theme [name]</code> - Hot-swaps the system aesthetics.</li>
            </ul>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Mastery through Reference</h2>
            <p>The Tebian Protocol is your guide to the metal. By internalizing these commands and locations, you move from a "user" to an "operator." One ISO. One menu. Total command. Welcome to the Protocol.</p>
          </section>
        </article>
      </main>
    </>
  );
}
