import "../styles/honors.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Honors" };
}

export default function Honors() {
  return (
    <>
      <main>
        <section class="shrine">
          <h2 class="shrine-title">The Foundation</h2>
          <p class="shrine-sub">The world runs on these three. So do we.</p>
          <div class="shrine-grid">
            <div class="shrine-card">
              <h2 class="shrine-gnu">GNU</h2>
              <p>The userspace that makes Linux livable. The GPL that keeps it free.</p>
              <a href="https://www.gnu.org" target="_blank" class="shrine-btn shrine-btn-gnu">gnu.org</a>
            </div>
            <div class="shrine-card">
              <h2 class="shrine-linux">Linux</h2>
              <p>The kernel. The foundation. Everything runs on it.</p>
              <a href="https://kernel.org" target="_blank" class="shrine-btn shrine-btn-linux">kernel.org</a>
            </div>
            <div class="shrine-card">
              <h2 class="shrine-debian">Debian</h2>
              <p>30+ years. Universal. Stable. Free.</p>
              <a href="https://debian.org" target="_blank" class="shrine-btn shrine-btn-debian">debian.org</a>
            </div>
          </div>
        </section>

        <section class="survivors">
          <h2>Survivors</h2>
          <p>Still here. Still irreplaceable.</p>
          <div class="survivor-grid">
            <a href="https://www.gentoo.org" target="_blank" class="survivor-card">
              <span class="survivor-name">Gentoo</span>
              <span class="survivor-desc">Compiles for anything. The deepest way to learn Linux.</span>
            </a>
            <a href="https://alpinelinux.org" target="_blank" class="survivor-card">
              <span class="survivor-name">Alpine</span>
              <span class="survivor-desc">musl. 5MB. Container standard.</span>
            </a>
            <a href="https://tails.net" target="_blank" class="survivor-card">
              <span class="survivor-name">Tails</span>
              <span class="survivor-desc">Amnesic. RAM-only. Zero trace.</span>
            </a>
            <a href="https://www.openbsd.org" target="_blank" class="survivor-card">
              <span class="survivor-name">OpenBSD</span>
              <span class="survivor-desc">Pledge, unveil, pf. The most audited codebase on earth. Never compromised.</span>
            </a>
            <div class="survivor-divider">Available via Tebian Menu &rarr; Virtualization &amp; dual boot</div>
            <div class="survivor-vm-row">
              <a href="https://www.apple.com/macos/" target="_blank" class="survivor-card survivor-special">
                <span class="survivor-name">macOS</span>
                <span class="survivor-desc">The only way to run Xcode. Proprietary, but worth keeping close.</span>
              </a>
              <a href="https://www.microsoft.com/windows/" target="_blank" class="survivor-card survivor-special">
                <span class="survivor-name">Windows</span>
                <span class="survivor-desc">Anti-cheat games. Proprietary software. Still the only option sometimes.</span>
              </a>
            </div>
          </div>
        </section>

        <section class="infra">
          <h2>Server Management</h2>
          <p>Manage what runs, what stores, and what ships.</p>
          <div class="survivor-grid">
            <a href="https://www.proxmox.com" target="_blank" class="survivor-card">
              <span class="survivor-name">Proxmox</span>
              <span class="survivor-desc">Bare-metal Type-1 hypervisor. VMs, LXC, clustering, HA.</span>
            </a>
            <a href="https://www.truenas.com/truenas-community-edition/" target="_blank" class="survivor-card">
              <span class="survivor-name">TrueNAS</span>
              <span class="survivor-desc">ZFS storage OS. Now also runs Docker apps. The sovereign homelab foundation.</span>
            </a>
            <a href="https://teploy.com" target="_blank" class="survivor-card">
              <span class="survivor-name">Teploy</span>
              <span class="survivor-desc">App deployments made easy. SSH-native CLI with a live state dashboard — ship code and see your fleet.</span>
            </a>
          </div>
        </section>

        <section class="embedded">
          <h2>Embedded &amp; Real-Time</h2>
          <p>Not Linux. Not general purpose. Built for one job, perfectly.</p>
          <div class="survivor-grid">
            <a href="https://zephyrproject.org" target="_blank" class="survivor-card">
              <span class="survivor-name">Zephyr</span>
              <span class="survivor-desc">Linux Foundation RTOS. The modern embedded standard.</span>
            </a>
            <a href="https://www.freertos.org" target="_blank" class="survivor-card">
              <span class="survivor-name">FreeRTOS</span>
              <span class="survivor-desc">The microcontroller OS. Runs on everything from Arduino to AWS IoT.</span>
            </a>
            <a href="https://sel4.systems" target="_blank" class="survivor-card">
              <span class="survivor-name">seL4</span>
              <span class="survivor-desc">Formally verified microkernel. Mathematically proven correct.</span>
            </a>
            <a href="https://www.rtems.org" target="_blank" class="survivor-card">
              <span class="survivor-name">RTEMS</span>
              <span class="survivor-desc">Real-time OS. Used in space missions and safety-critical systems.</span>
            </a>
          </div>
        </section>

        <section class="killed">
          <h2>The Killed</h2>
          <p>Distros we replaced. And why they died.</p>
          <div class="killed-grid">
            <div class="killed-card"><span class="killed-name">Ubuntu</span><span class="killed-reason">Debian + Snap + Canonical. We skip the middleman.</span></div>
            <div class="killed-card"><span class="killed-name">Arch</span><span class="killed-reason">Rolling = breaking. Stability wins. AUR via Distrobox.</span></div>
            <div class="killed-card"><span class="killed-name">Fedora</span><span class="killed-reason">6-month forced upgrades. Corporate backing. No thanks.</span></div>
            <div class="killed-card"><span class="killed-name">Enterprise Linux</span><span class="killed-reason">RHEL, Rocky, Alma — sell liability, not technology.</span></div>
            <div class="killed-card"><span class="killed-name">Mint</span><span class="killed-reason">Ubuntu + theme. We go straight to Debian.</span></div>
            <div class="killed-card"><span class="killed-name">Pop!_OS</span><span class="killed-reason">Gaming stack + GNOME. Install via Tebian Menu instead.</span></div>
            <div class="killed-card"><span class="killed-name">Manjaro</span><span class="killed-reason">Arch with training wheels. Still breaks.</span></div>
            <div class="killed-card"><span class="killed-name">EndeavourOS</span><span class="killed-reason">Arch installer with a GUI. Still Arch underneath.</span></div>
            <div class="killed-card"><span class="killed-name">FreeBSD</span><span class="killed-reason">ZFS, DTrace, jails — Linux absorbed them all. Even TrueNAS moved on.</span></div>
            <div class="killed-card"><span class="killed-name">Kali</span><span class="killed-reason">Just tools. apt install nmap metasploit. Done.</span></div>
            <div class="killed-card"><span class="killed-name">Parrot</span><span class="killed-reason">Kali with a theme. Same tools, more bloat.</span></div>
            <div class="killed-card"><span class="killed-name">NixOS</span><span class="killed-reason">Git is your reproducibility. Declarative config is a full-time job. Not FHS compliant.</span></div>
            <div class="killed-card"><span class="killed-name">Guix</span><span class="killed-reason">Same idea as Nix but in Lisp. Twice as academic, half as useful.</span></div>
            <div class="killed-card"><span class="killed-name">openSUSE</span><span class="killed-reason">YaST, Zypper, BTRFS by default. Different for different's sake.</span></div>
            <div class="killed-card"><span class="killed-name">Void</span><span class="killed-reason">musl + minimal? That's Alpine. runit is cool, not enough.</span></div>
            <div class="killed-card"><span class="killed-name">Solus</span><span class="killed-reason">Independent distro. Tiny team. Uncertain future.</span></div>
            <div class="killed-card"><span class="killed-name">Zorin</span><span class="killed-reason">Ubuntu + Windows theme. We don't pretend.</span></div>
            <div class="killed-card"><span class="killed-name">elementary</span><span class="killed-reason">Ubuntu + macOS theme. Same trick, different skin.</span></div>
            <div class="killed-card"><span class="killed-name">MX Linux</span><span class="killed-reason">Debian + XFCE + tools. Just use Debian.</span></div>
            <div class="killed-card"><span class="killed-name">antiX</span><span class="killed-reason">MX without systemd. We respect the cause, not the result.</span></div>
            <div class="killed-card"><span class="killed-name">Deepin</span><span class="killed-reason">Pretty DE. Chinese telemetry concerns. Pass.</span></div>
            <div class="killed-card"><span class="killed-name">Qubes</span><span class="killed-reason">For one-off anonymity, use Tails. For a hardened daily driver, harden Debian.</span></div>
            <div class="killed-card"><span class="killed-name">Whonix</span><span class="killed-reason">Two-VM Tor overhead. Tails does it cleaner.</span></div>
            <div class="killed-card"><span class="killed-name">Tiny Core</span><span class="killed-reason">Alpine is 5MB and actually usable. This is a curiosity.</span></div>
            <div class="killed-card"><span class="killed-name">Puppy</span><span class="killed-reason">RAM-based. Old hardware. Old everything.</span></div>
            <div class="killed-card"><span class="killed-name">TempleOS</span><span class="killed-reason">Not Linux. God's temple. Rest in peace, Terry. The temple still stands somewhere in Tebian.</span></div>
          </div>
        </section>
      </main>
    </>
  );
}
