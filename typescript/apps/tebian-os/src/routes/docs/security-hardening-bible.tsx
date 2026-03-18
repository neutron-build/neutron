import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Security Hardening Bible — Tebian" };
}

export default function SecurityHardeningBible() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Security Hardening Bible</h1>
          <p class="meta">Kernel-Level Protection: Hardening the Foundation of your OS.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Science of Defense</h2>
            <p>Most "Security Software" is reactive: it waits for an attack and then tries to stop it. Tebian's security is **Proactive and Structural.** We don't just "run an antivirus"; we harden the kernel and the filesystem so that entire classes of exploits become impossible. This bible provides the technical procedures for military-grade OS hardening.</p>

            <p>We focus on the three pillars of structural defense: <strong>Kernel Hardening, Mandatory Access Control (MAC), and Memory Isolation.</strong></p>
          </section>

          <section class="kernel-hardening">
            <h2>1. Kernel Hardening: The Sysctl Manual</h2>
            <p>The Linux kernel has hundreds of parameters that control networking, memory management, and process execution. By default, these are tuned for compatibility. Tebian's "Hardened Mode" tunes them for **Defense.**</p>

            <h3>The Sysctl Config</h3>
            <p>We provide a pre-configured <code>99-tebian-hardened.conf</code> that applies the following C-level protections:</p>
            <ul>
              <li><strong>ASLR (Address Space Layout Randomization):</strong> We set <code>kernel.randomize_va_space=2</code> to ensure that memory addresses are unpredictable, defeating "Buffer Overflow" exploits.</li>
              <li><strong>Network Stack Hardening:</strong> We disable ICMP redirects and source-routing to prevent "Man-in-the-Middle" attacks.</li>
              <li><strong>Unprivileged BPF:</strong> We set <code>kernel.unprivileged_bpf_disabled=1</code> to prevent users from executing complex kernel-level scripts that could leak data.</li>
            </ul>
          </section>

          <section class="apparmor-logic">
            <h2>2. AppArmor: Confining the Applications</h2>
            <p>Most exploits happen because an application (like a web browser) has too much access to the system. If your browser is compromised, the attacker can see your <code>/home</code> directory. Tebian uses <strong>AppArmor</strong> to prevent this.</p>

            <h3>The Profile Strategy</h3>
            <p>Every critical app in Tebian has an AppArmor profile. This profile is a set of rules that tells the kernel: "This browser is ONLY allowed to read its own config and write to the Downloads folder." It cannot see your SSH keys. It cannot see your banking files. Even if the browser is "Hacked," the attacker is trapped inside a tiny, digital room.</p>
          </section>

          <section class="memory-isolation">
            <h2>3. Memory Isolation: Defeating Rowhammer and Meltdown</h2>
            <p>Modern CPUs have hardware flaws that can leak data between processes. Tebian includes kernel patches and boot parameters (like <code>pti=on</code> and <code>spectre_v2=on</code>) to mitigate these risks. We prioritize <strong>Data Integrity</strong> over the 2-3% performance hit these mitigations might cause.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Silent Guardian</h2>
            <p>True security isn't a pop-up window or a scan. It is a set of silent, mathematical rules enforced by the kernel. By following the Security Hardening Bible, you turn your Tebian machine into a vault. One ISO. One menu. Absolute protection.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
