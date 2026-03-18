import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Privacy of Silence — Tebian" };
}

export default function PrivacyOfSilence() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Privacy</span>
          <h1>The Privacy of Silence (Zero Telemetry)</h1>
          <p class="meta">February 20, 2026 &bull; 12 min read</p>
        </header>
        <article class="content">
          <p class="lead">Most modern operating systems are "Talkative." They are constantly sending data to their creators. Windows 11 sends telemetry to Redmond. macOS sends telemetry to Cupertino. Even some Linux distros (Ubuntu/Fedora) send crash reports and usage data. Tebian is Silent.</p>

          <h2>The Cost of "Talkative" OSs</h2>
          <p>Telemetry isn't just a privacy issue; it's a <strong>Performance Penalty.</strong> Every time your OS "sends a report," it consumes CPU cycles, RAM, and network bandwidth. In an OS with hundreds of background processes, these "Talkative" tasks add up. This is the cause of "Background Noise" on a fresh boot.</p>

          <p>When you use Tebian, you are performing a software "Vow of Silence." We have removed the telemetry daemons. We don't have a "Crash Reporter" that starts a 50MB background process. We don't have a "Usage Statistics" service. We don't even have a "Software Store" that checks for updates without your permission.</p>

          <h2>Why "Zero Data" is the Only True Privacy</h2>
          <p>Most OSs offer "Privacy Settings" where you can "opt-out" of telemetry. This is a false choice. The code for the telemetry is still there. The data is still collected locally. It is just "not sent." This is not privacy; it is a toggle that can be reset by any update.</p>

          <p>Tebian's "Zero Data" policy is different. We don't have a toggle because we don't have the code. We build on <strong>Debian Stable</strong>, which is already minimal, and we strip away the rest. If there is no code to collect the data, the data cannot exist. This is the C-Level Privacy fundamental: Privacy isn't a setting; it's an architectural choice.</p>

          <h2>The Silence of Your Hard Drive</h2>
          <p>A "Talkative" OS is also a "Noisy" OS for your hard drive. Constant indexing, logging, and reporting means your SSD is constantly being written to. This reduces the lifespan of your drive and slows down your system. In Tebian, the only thing being written to your disk is <strong>your data.</strong></p>

          <h2>Conclusion: Reclaiming the Silence</h2>
          <p>Your computer should be a quiet space for your thoughts. It should not be a corporate observation post. By choosing a silent operating system, you are reclaiming your focus, your performance, and your privacy. One ISO. One menu. Zero Telemetry. The silence is the feature.</p>
        </article>
      </main>
    </PageShell>
  );
}
