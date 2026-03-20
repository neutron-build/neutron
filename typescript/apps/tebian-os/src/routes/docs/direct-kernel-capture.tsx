import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Direct Kernel Capture — Tebian" };
}

export default function DirectKernelCapture() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Technical Manual</span>
          <h1>The Direct Kernel Capture Manual</h1>
          <p class="meta">The technical reason OBS is better on Tebian.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Bottleneck of Screen Capture</h2>
            <p>In Windows and macOS, screen capture (OBS) is a high-overhead task. The OS has to copy frames from the GPU to the CPU, encode them, and then often copy them back. This leads to high CPU usage and dropped frames during recording. Tebian uses <strong>Direct Kernel Capture (PipeWire DMA-BUF)</strong> to achieve zero-overhead screen recording.</p>

            <p>This manual explains the C-level magic that allows you to record 4K 60FPS video with almost 0% CPU impact.</p>
          </section>

          <section class="dma-buf-logic">
            <h2>1. The Logic of DMA-BUF</h2>
            <p>DMA-BUF (Direct Memory Access Buffer) is a Linux kernel feature that allows different hardware drivers (GPU and CPU) to share memory buffers. In Tebian, when your screen is rendered by <strong>Sway</strong>, the frame sits in a buffer on your GPU. Using PipeWire, we pass a "Pointer" to that buffer directly to OBS.</p>

            <h3>Zero-Copy Recording</h3>
            <p>Because OBS has a pointer to the existing GPU memory, it doesn't need to "Capture" or "Copy" the image. It simply reads the data that is already there. This is <strong>Zero-Copy Architecture.</strong> While Windows is busy moving gigabytes of pixel data per second, Tebian is just moving pointers. This results in the lowest possible recording latency in the world.</p>
          </section>

          <section class="encoding-acceleration">
            <h2>2. Hardware Encoding: NVENC and AMF</h2>
            <p>Tebian's <em>Hardware Detect</em> system ensures that your GPU's dedicated encoding chips are active. We pre-configure OBS to use <strong>NVENC</strong> (NVIDIA) or <strong>AMF</strong> (AMD) for H.264 and AV1 encoding.</p>
            <ul>
              <li><strong>Low Latency:</strong> Encoding happens on a dedicated piece of silicon, leaving your CPU free for the game or app.</li>
              <li><strong>AV1 Support:</strong> Tebian is ready for the future of streaming with native AV1 hardware support on modern cards.</li>
            </ul>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Ultimate Streamer Foundation</h2>
            <p>The Direct Kernel Capture Manual proves that Linux isn't just "good enough" for streamers—it is the <strong>Superior Engineering Choice.</strong> By using kernel-level buffer sharing and hardware-native encoding, you get a smoother stream and a faster PC. One ISO. One menu. Pro-grade recording.</p>
          </section>
        </article>
      </main>
    </>
  );
}
