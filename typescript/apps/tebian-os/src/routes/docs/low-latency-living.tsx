import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Low-Latency Living — Tebian" };
}

export default function LowLatencyLiving() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Technical Manual</span>
          <h1>The Low-Latency Living Manual</h1>
          <p class="meta">Why Windows is the worst platform for Pro Audio.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The ASIO Nightmare</h2>
            <p>Pro audio on Windows has relied on the <strong>ASIO</strong> protocol since the 1990s. While functional, ASIO is a hack: it allows a single app to bypass the Windows audio mixer to achieve low latency. This means if your DAW is open, you often can't hear audio from your browser or other apps. It is a "Monopolistic" audio model. Tebian uses <strong>PipeWire</strong>, a C-based multimedia server that provides pro-level latency while allowing every app to share the hardware.</p>

            <p>This manual explains why PipeWire is the superior architecture for modern music production and how to tune Tebian for zero-glitch recording.</p>
          </section>

          <section class="pipewire-vs-asio">
            <h2>1. PipeWire vs. ASIO: The Architectural War</h2>
            <p>ASIO is synchronous and blocking. PipeWire is <strong>Asynchronous and Graph-based.</strong> In Tebian, every audio stream is a node in a real-time graph. You can route your microphone through a noise-gate plugin and then into <em>both</em> your DAW and your Discord call simultaneously. This is impossible on Windows without expensive third-party tools like Virtual Audio Cable.</p>

            <h3>Zero-Copy DMA</h3>
            <p>Tebian's PipeWire implementation uses <strong>Direct Memory Access (DMA)</strong> to pass audio buffers between the hardware and your apps. There is no CPU-heavy "copying" of data. This allows you to run buffer sizes as low as 64 samples (1.3ms latency) without the CPU spikes that cause audio "pops" and "clicks" on Windows.</p>
          </section>

          <section class="kernel-realtime">
            <h2>2. Real-time Kernel Tuning</h2>
            <p>For pro audio, the Linux kernel must be tuned to prioritize audio interrupts. Tebian handles this in our "Creative Mode" setup:</p>
            <ul>
              <li><strong>Threaded IRQs:</strong> We move hardware interrupts into their own threads so they can be prioritized by the scheduler.</li>
              <li><strong>RT-Priority:</strong> We grant the PipeWire daemon and your DAW <code>realtime</code> privileges (RR/FIFO scheduling).</li>
              <li><strong>Locked Memory:</strong> We allow audio apps to lock their memory in RAM, preventing the kernel from "swapping" them to disk during a recording.</li>
            </ul>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Future of Sound</h2>
            <p>The Low-Latency Living Manual proves that the "Mac only" or "Windows ASIO" era is over. By using a C-based, graph-driven audio engine on a stable kernel, you get a more flexible, faster, and more reliable studio. One ISO. One menu. Total sonic freedom.</p>
          </section>
        </article>
      </main>
    </>
  );
}
