import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Lazarus Engine — Tebian" };
}

export default function LazarusEngine() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Minimalism</span>
          <h1>The Lazarus Engine: The 16MB Miracle</h1>
          <p class="meta">February 20, 2026 &bull; 25 min read</p>
        </header>

        <article class="content">
          <p class="lead">In the tech world, "Planned Obsolescence" is a multi-billion dollar business. Microsoft and Apple want you to believe that a laptop from 2012 is "dead." They design their modern operating systems (Windows 11, macOS Sequoia) to choke old hardware until it is unusable. <strong>Tebian is the Lazarus Engine.</strong> We prove that a 15-year-old machine isn't just "usable"&mdash;it's fast. And we do it by hitting the 16MB RAM threshold.</p>

          <h2>1. The Math of 16MB</h2>
          <p>When we say Tebian idles at 16MB of RAM, we aren't using "tricks." We are using <strong>C-Level Fundamentals.</strong> A standard Ubuntu (Omakub) install idles at 1.5GB. A standard Arch (Omarchy) install idles at 600MB. Why the gap?</p>

          <p>Most desktops are built on <strong>Frameworks.</strong> GNOME is built on GTK and GJS (Gnome JavaScript). KDE is built on Qt and QML. Every time you start a desktop environment, you are starting a massive virtual machine that interprets your UI in real-time. This virtual machine consumes hundreds of megabytes of RAM before you even open a browser.</p>

          <p>Tebian's "Core 3"&mdash;<strong>Sway, Fuzzel, and Mako</strong>&mdash;are all written in C. They are compiled directly to machine code. They don't have a "runtime." They talk directly to the Linux kernel via <strong>Syscalls</strong>. A C binary that shows a menu uses <strong>Kilobytes</strong> of RAM, not Megabytes. When you add up the memory usage of our compositor (Sway), our launcher (Fuzzel), and our status bar (Status.sh), the total is roughly 16-20MB. This leaves 99% of your RAM for your actual work.</p>

          <h2>2. The Wayland Advantage (No X11 Bloat)</h2>
          <p>Traditional "Lightweight" distros (Lubuntu, AntiX, Puppy Linux) are still stuck on the 1980s <strong>X11</strong> display server. X11 is a massive, complex, and insecure protocol that requires a background daemon (Xorg) that is always running and consuming RAM.</p>

          <p>Tebian is <strong>Wayland-Native.</strong> We use the <strong>wlroots</strong> library (written in C) to talk directly to the kernel's DRM (Direct Rendering Manager). There is no "Display Server" in the middle. The compositor (Sway) <em>is</em> the display server. By removing the X11 layer, we've removed a massive source of input latency and memory consumption. This is why Tebian feels "snappy" even on a dual-core Pentium.</p>

          <h2>3. Kernel Paging and ZRAM Optimization</h2>
          <p>For machines with 2GB or 4GB of RAM (common in the "Lazarus" category), the biggest bottleneck is <strong>Disk Thrashing.</strong> When your RAM fills up, the OS starts moving data to the hard drive (Swap). On a 15-year-old HDD, this is the "Death Spiral" that makes a PC feel slow.</p>

          <p>Tebian uses <strong>ZRAM</strong> by default on detected legacy hardware. ZRAM creates a compressed "Virtual Drive" inside your physical RAM. When the kernel needs to move data, it doesn't send it to the slow HDD; it compresses it (using the <strong>zstd</strong> algorithm) and keeps it in the fast RAM. This effectively "expands" a 4GB laptop into a 6GB machine with zero hardware cost.</p>
          <ul>
            <li><strong>Swappiness=10:</strong> Tells the kernel to use RAM as much as possible before touching the disk.</li>
            <li><strong>VFS Cache Pressure:</strong> Tuned to keep file metadata in RAM for faster browsing.</li>
            <li><strong>Noatime:</strong> Disables "Access Time" writes to the disk, reducing wear on old HDDs/SSDs.</li>
          </ul>

          <h2>4. The 32-Bit (i386) Survival Guide</h2>
          <p>Most modern distros (Ubuntu, Arch, Fedora) have abandoned 32-bit hardware. They want you to throw away your 2005 Netbooks and Pentium 4 towers. Tebian, standing on the shoulders of <strong>Debian Stable</strong>, continues to support the i386 architecture. We provide kernels and binaries that boot on non-PAE CPUs.</p>

          <p>The Lazarus Engine proves that "obsolescence" is a software choice, not a hardware reality. A Pentium 4 with 2GB of RAM is perfectly capable of running a modern, secure browser and an office suite&mdash;as long as the OS isn't stealing all the resources.</p>

          <h2>5. Reviving the 2012 MacBook</h2>
          <p>The 2012 MacBook Pro is the "God-Tier" of legacy hardware. It has a beautiful screen, a great keyboard, and a metal body, but macOS has abandoned it. On Tebian, a 2012 MacBook isn't just "okay"&mdash;it's faster than a brand-new $300 Windows laptop.</p>

          <p>We've pre-configured the <strong>Broadcom WiFi drivers</strong> and the <strong>Retina (HiDPI) scaling</strong> specifically for these machines. You flash the ISO, you boot, and you have a 2026-ready workstation in a 2012 body.</p>

          <h2>Conclusion: Reclaiming the Machine</h2>
          <p>The Lazarus Engine isn't just about "saving money." It's about <strong>Efficiency as Art.</strong> By stripping away the layers of abstraction, telemetry, and visual noise, we've created a system that respects the hardware it runs on. Whether it's a $5,000 server or a $5 yard-sale laptop, Tebian provides the same "C-Level" speed. One ISO. One menu. Total resurrection.</p>
        </article>
      </main>
    </>
  );
}
