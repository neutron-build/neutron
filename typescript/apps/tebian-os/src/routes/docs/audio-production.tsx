import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Audiophile's Handbook — Tebian" };
}

export default function AudioProduction() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Audiophile's Handbook</h1>
          <p class="meta">Replacing CoreAudio with PipeWire: Pro Audio on Linux.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Myth of "Mac Only" Audio</h2>
            <p>For decades, musicians and producers have been told that "CoreAudio" (macOS) is the only way to get low-latency, glitch-free sound. In 2026, this is false. Tebian uses <strong>PipeWire</strong>, a next-generation multimedia server that matches or beats CoreAudio in latency and stability.</p>

            <p>This guide explains how to configure Tebian for professional audio work (DAW, live performance, podcasting) without the "Apple Tax."</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. PipeWire: The New Standard</h3>
              <p>PipeWire unifies consumer audio (PulseAudio) and pro audio (JACK) into a single, seamless graph. On Tebian, it comes pre-configured with a low-latency profile.</p>
              <ul>
                <li><strong>Unified Graph:</strong> Route any app's audio to any other app (e.g., Spotify into a DAW).</li>
                <li><strong>Realtime Priority:</strong> Tebian grants realtime privileges to the audio group out of the box.</li>
                <li><strong>Quantum Size:</strong> Dynamically adjusts buffer sizes (64/128/256 samples) based on load.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The DAW Setup (Bitwig/Reaper)</h3>
              <p>Linux has native support for world-class DAWs. Tebian's "Creative Mode" includes one-click installers for the best in the business.</p>
              <ul>
                <li><strong>Bitwig Studio:</strong> The Ableton alternative with first-class Linux support.</li>
                <li><strong>Reaper:</strong> The ultra-lightweight, infinitely customizable DAW.</li>
                <li><strong>Ardour:</strong> The open-source Pro Tools alternative.</li>
                <li><strong>Yabridge:</strong> Run Windows VST plugins seamlessly inside Linux DAWs.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Low-Latency Kernel Tuning</h3>
              <p>Tebian includes a <code>performance</code> governor script in the "Settings" menu. This ensures your CPU doesn't downclock during a recording session, preventing "xruns" (audio dropouts).</p>
              <ul>
                <li><strong>CPU Governor:</strong> Locks cores to max frequency.</li>
                <li><strong>IRQ Threading:</strong> Prioritizes audio hardware interrupts over network/disk.</li>
                <li><strong>Memlock Limit:</strong> Allows audio apps to lock memory pages to prevent swapping.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Hardware Compatibility</h3>
              <p>Most USB audio interfaces are "Class Compliant," meaning they work instantly on Linux without drivers. However, some proprietary devices require specific kernels.</p>
              <ul>
                <li><strong>Works Great:</strong> Focusrite Scarlett, Behringer, RME (Class Compliant Mode), Motu M-Series.</li>
                <li><strong>Avoid:</strong> UAD Apollo (Thunderbolt requires specific tweaking), Antelope Audio.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why PipeWire Wins</h2>
            <p>On macOS, routing audio between apps (e.g., capturing a Zoom call into Logic) requires paid third-party tools like Loopback. On Tebian with PipeWire, this is built-in. You can draw a virtual cable from any output to any input using a graph tool like <strong>qpwgraph</strong>.</p>

            <p>It is the ultimate routing freedom. And it costs $0.</p>
          </section>
        </article>
      </main>
    </>
  );
}
