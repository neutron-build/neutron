import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Kernel Tuning Manual — Tebian" };
}

export default function KernelTuningManual() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Kernel Tuning Manual</h1>
          <p class="meta">Peak Performance: Latency, Scheduling, and Sysctl Optimization.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most operating systems use a "One Size Fits All" kernel. They prioritize "General Purpose" workloads, leading to high latency and sluggishness during heavy tasks. Tebian is built for <strong>Performance</strong>. This guide explains how to tune the Linux kernel for zero-latency desktop and gaming performance.</p>

            <p>We use a set of **Sysctl** and **Kernel Parameters** that prioritize interactive tasks (like your mouse and keyboard) over background maintenance.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The CPU Governor</h3>
              <p>Tebian's "Performance Mode" includes a pre-configured <strong>CPU Governor</strong> setup. We switch your CPU from <code>powersave</code> to <code>performance</code> or <code>schedutil</code> based on load.</p>
              <ul>
                <li><strong>Performance Governor:</strong> Locks your CPU to its maximum frequency, eliminating the delay of frequency scaling.</li>
                <li><strong>Schedutil:</strong> Uses the kernel's scheduler data to adjust frequency more intelligently than the legacy <code>ondemand</code>.</li>
                <li><strong>Intel P-States:</strong> We optimize the P-State driver for modern Intel CPUs.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. Memory & Swappiness</h3>
              <p>Tebian optimizes how your system uses its RAM. By default, Linux is too aggressive about using "Swap" (disk space) even when you have free RAM. We change this at the C-level.</p>
              <ul>
                <li><strong>Vm.Swappiness=10:</strong> Tells the kernel to use your physical RAM as much as possible before touching the disk.</li>
                <li><strong>ZRAM:</strong> We enable compressed RAM swap to simulate having 50% more memory with zero latency.</li>
                <li><strong>Dirty Cache:</strong> We increase the dirty cache limits to allow for smoother background writes.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Latency & Scheduling</h3>
              <p>Tebian uses the <strong>BFQ (Budget Fair Queuing)</strong> I/O scheduler for mechanical drives and the <strong>Kyber</strong> scheduler for SSDs. These are tuned for "interactive" performance, meaning your app won't hang when your system is busy.</p>
              <ul>
                <li><strong>PREEMPT_DYNAMIC:</strong> We use the dynamic preemption kernel to switch between <code>low-latency</code> and <code>throughput</code> modes at runtime.</li>
                <li><strong>Network Buffers:</strong> We increase the TCP buffer sizes for smoother, faster internet.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Zen & XanMod Kernels</h3>
              <p>For those who need the absolute best, Tebian provides a one-click setup for the <strong>XanMod</strong> or <strong>Zen</strong> kernels. These are community-tuned kernels that include patches for better gaming and desktop responsiveness.</p>
              <ul>
                <li><strong>Zen Kernel:</strong> The official kernel for many performance distros (like Garuda).</li>
                <li><strong>XanMod Kernel:</strong> A highly optimized, stable kernel with advanced scheduling.</li>
                <li><strong>Liquorix:</strong> The best kernel for ultra-low latency audio and gaming.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Kernel Tuning on Tebian?</h2>
            <p>Because our OS is minimal (16MB base), kernel tuning has a **Magnified Effect**. When the kernel is only managing a few processes, every optimization goes directly into your app's performance. You aren't just "tuning a machine"; you are perfecting a tool. One ISO. One menu. Peak performance.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
