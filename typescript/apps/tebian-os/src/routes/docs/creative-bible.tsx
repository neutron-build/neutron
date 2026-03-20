import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Creative Bible — Tebian" };
}

export default function CreativeBible() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Creative Bible</h1>
          <p class="meta">PipeWire Graph Theory, OBS Wayland Mastery, and Pro-Audio Excellence.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Creative Sovereign: Beyond the Mac</h2>
            <p>The "Mac for Creatives" trope is a marketing success, but a technical failure. In 2026, macOS is a locked-down, telemetry-heavy environment that forces you to buy proprietary hardware for professional tasks. Tebian is the alternative. We don't just "support" creative tools; we provide a high-performance C-based foundation that allows you to out-render and out-route any Mac.</p>

            <p>This treatise breaks down the technical stack for professional video editors, audio engineers, and streamers. We focus on <strong>Zero-Latency Routing</strong> and <strong>Direct Kernel Access.</strong></p>
          </section>

          <section class="pipewire-graph">
            <h2>1. PipeWire Graph Theory: Unlimited Audio Routing</h2>
            <p>The single greatest advantage of Tebian for creators is <strong>PipeWire.</strong> Written in C, PipeWire unifies audio and video streams into a single, real-time graph. In macOS, you need third-party apps like Loopback to route audio between applications. In Tebian, this is a core system feature.</p>

            <h3>The Logic of the Node</h3>
            <p>Every audio source (a browser, a DAW, a microphone) is a <strong>Node</strong> in the PipeWire graph. Every output (headphones, OBS, a recording app) is a <strong>Port.</strong> Using tools like <code>qpwgraph</code> or <code>helvum</code>, you can draw virtual cables between any node and any port. This allows for complex setups that are impossible on other OSs:</p>
            <ul>
              <li><strong>Voice Isolation:</strong> Route your mic through a noise-reduction plugin (like RNNoise) before it ever hits Discord.</li>
              <li><strong>System Capture:</strong> Capture only the audio from a specific browser tab into OBS, ignoring your system notifications.</li>
              <li><strong>Low-Latency Pro Audio:</strong> Run your DAW (Bitwig/Reaper) with 128-sample buffers while simultaneously playing a video in the background—with zero crackling.</li>
            </ul>
          </section>

          <section class="obs-wayland">
            <h2>2. OBS on Wayland: Direct Capture Excellence</h2>
            <p>Screen recording on Linux used to be a mess of "X11 grab" issues. On Wayland, and specifically on Tebian's Sway core, we use <strong>PipeWire DMA-BUF</strong> for screen capture. This is the "God-Tier" of screen recording.</p>

            <h3>The C-Level Advantage</h3>
            <p>When you record your screen in OBS on Tebian, the frames are passed directly from the compositor (Sway) to OBS via a memory buffer. There is zero "copying" of the frame data between the CPU and GPU. This results in <strong>0% CPU usage for the capture itself.</strong> You can record 4K 60FPS gameplay with zero impact on your game's performance.</p>
            <ul>
              <li><strong>V4L2 Virtual Camera:</strong> Tebian includes the <code>v4l2loopback</code> kernel module pre-configured. You can turn your OBS scene into a "Virtual Webcam" for Zoom or Teams with a single click.</li>
              <li><strong>Hardware Encoding:</strong> We pre-configure OBS to use NVIDIA's NVENC or AMD's AMF encoders, ensuring your GPU handles the heavy lifting of video compression.</li>
            </ul>
          </section>

          <section class="v4l2-kernel">
            <h2>3. The Camera Stack: V4L2 and Libcamera</h2>
            <p>Professional creators often use high-end DSLRs as webcams. Windows and macOS require messy proprietary drivers. Tebian uses the native Linux <strong>V4L2 (Video4Linux2)</strong> subsystem. It is a stable, C-based interface that treats every video device as a simple file stream in <code>/dev/video*</code>.</p>

            <p>For modern laptop cameras (like those in the Surface or ThinkPad X1), we use <strong>Libcamera.</strong> It provides a modern, asynchronous API that allows for complex post-processing (autofocus, white balance) to happen at the kernel level, ensuring your image is always crisp.</p>
          </section>

          <section class="color-management">
            <h2>4. Color Management and HiDPI</h2>
            <p>For visual artists, color accuracy is non-negotiable. Tebian on Wayland supports <strong>ICC Profiles</strong> at the compositor level. You can load your monitor's calibration profile into Sway, ensuring that what you see in Blender or GIMP is exactly what will be printed.</p>

            <p>Combined with our <strong>HiDPI Scaling</strong> (as detailed in the 4K Manual), Tebian provides a "Retina-Grade" experience on any high-resolution monitor. No blurry icons. No misaligned pixels. Just glass-sharp precision.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Ultimate Creative Engine</h2>
            <p>The Creative Bible is about one thing: <strong>Removing the middleman.</strong> We don't want you to fight with your OS to get your audio routed or your screen captured. We provide the C-based infrastructure that gets out of the way and lets you create. One ISO. One menu. Total creative freedom. This is why the world's best creators are moving to Tebian. It's not just a tool; it's an engine for your art.</p>
          </section>
        </article>
      </main>
    </>
  );
}
