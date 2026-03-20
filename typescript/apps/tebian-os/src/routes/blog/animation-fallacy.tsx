import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Fallacy of the Smooth Animation" };
}

export default function AnimationFallacy() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Philosophy</span>
          <h1>The Fallacy of the Smooth Animation</h1>
          <p class="meta">February 20, 2026 &bull; 20 min read</p>
        </header>

        <article class="content">
          <p class="lead">Modern operating systems are obsessed with "Smoothness." Windows 11, macOS, and even many Linux desktops (like Hyprland) dedicate massive amounts of GPU power to elastic window animations, rounded corners, and blur effects. They call this "User Experience." In Tebian, we call it <strong>Visual Friction.</strong></p>

          <h2>1. The Mathematics of Latency</h2>
          <p>Every time a window "fades in" or "slides out," your operating system is introducing a delay between your command and the result. Even if the animation only takes 200ms, that is 200ms where you are waiting for the OS to finish its performance. In high-frequency work—coding, editing, or gaming—these 200ms increments add up to hundreds of hours of lost productivity over a year.</p>
          <p>Tebian's C-based core (Sway) is configured for <strong>Zero Animation.</strong> When you press a key to open a terminal, it appears in the next frame buffer update—usually within 7ms on a 144Hz display. This isn't just "faster"; it is a different psychological state. It is the difference between "Managing a Computer" and "Thinking through a Machine."</p>

          <h2>2. The GPU's Real Job</h2>
          <p>Your GPU has a finite number of compute cycles per second. Every cycle spent calculating the Gaussian blur of a background window is a cycle stolen from your actual workload. If you are a video editor or a 3D artist, you want 100% of your VRAM and compute units dedicated to your render, not to your taskbar.</p>
          <p>By using a "Stealth Glass" UI, Tebian offloads all non-essential tasks from the GPU. We use the GPU for what it's good at—rendering text at 4K and hardware-accelerating video—while keeping the window management logic at the C-level in the compositor.</p>

          <h2>Conclusion: Choosing Response over Aesthetics</h2>
          <p>The "Smooth Animation" is a trick designed to hide system lag. If your OS is fast enough, you don't need animations to bridge the gap. Tebian is built for the user who values <strong>Response</strong> over <strong>Aesthetics.</strong> We don't want your computer to look like a movie; we want it to work like an extension of your mind. One ISO. One menu. Zero animations.</p>
        </article>
      </main>
    </>
  );
}
