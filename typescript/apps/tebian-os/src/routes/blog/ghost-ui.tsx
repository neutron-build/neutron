import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Ghost UI — Tebian" };
}

export default function GhostUi() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Philosophy</span>
          <h1>The Ghost UI (Stealth Glass)</h1>
          <p class="meta">February 20, 2026 &bull; 10 min read</p>
        </header>

        <article class="content">
          <p class="lead">The best interface is no interface. If your OS is constantly reminding you of its existence with flashy animations, massive taskbars, and pop-ups, it is failing. The Ghost UI (Stealth Glass) is Tebian's answer to the "Candy UI" trend.</p>

          <h2>The Problem with Candy (Omarchy/Hyprland)</h2>
          <p>In the "ricing" community (and systems like Omarchy), the goal is often to make the desktop look like a sci-fi movie. Windows fly in with elastic ease. Borders glow with RGB cycles. Everything is a spectacle.</p>

          <p>But spectacle is distraction. Every time a window "wobbles" on open, your brain has to process that movement. Every time a notification slides in with a bounce, your focus is broken. Over an 8-hour workday, these micro-distractions accumulate into cognitive fatigue.</p>

          <p><strong>Tebian's Approach:</strong> We use Sway, configured for <strong>Zero Animation</strong>. Windows appear instantly. Workspaces switch instantly. The interface is not a performance; it is a tool.</p>

          <h2>The Stealth Bar (Ghost Mode)</h2>
          <p>Traditional operating systems (Windows, macOS, Ubuntu/Omakub) dedicate a permanent strip of pixels to the "Taskbar" or "Dock." This is wasted screen real estate. Why do you need to see a clock when you are in a deep coding session? Why do you need to see a WiFi icon when you are editing a video?</p>

          <p>Tebian hides the bar by default. It is "Stealth." When you need information (Time, Battery, WiFi), you hold the <code>Super</code> key. The bar appears. You release the key. It vanishes.</p>

          <p>This "Ghost Mode" means that 100% of your pixels belong to your work. Whether it's a 13-inch laptop or a 4K monitor, the OS gets out of the way.</p>

          <h2>The Glass Aesthetic</h2>
          <p>When the UI <em>does</em> appear (menus, terminals), we use a subtle transparency (Glass). This isn't just for looks. It allows you to maintain context. You can open a terminal over a webpage and still see the content underneath. You never lose your place.</p>

          <p>The "Stealth Glass" philosophy is simple: Be invisible until needed, and be transparent when present.</p>

          <h2>Conclusion: Reclaiming Your Attention</h2>
          <p>Your attention is the most valuable resource you have. Don't let your operating system steal it with candy. Choose a Ghost UI that respects your focus and disappears into the background. The only thing you should see on your screen is your work.</p>
        </article>
      </main>
    </>
  );
}
