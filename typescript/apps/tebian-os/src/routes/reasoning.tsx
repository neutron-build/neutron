import "../styles/reasoning.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Reasoning" };
}

export default function Reasoning() {
  return (
    <>
      <main>
        <article class="reasoning">
          <h1>Reasoning</h1>
          <p class="lead">Every decision has a reason. Here they are.</p>

          <blockquote class="epigraph">
            <p>"Perfection is achieved not when there is nothing more to add, but when there is nothing left to take away."</p>
            <cite>— Antoine de Saint-Exup&eacute;ry, <em>Terre des hommes</em> (Wind, Sand and Stars, 1939)</cite>
          </blockquote>

          <section>
            <h2>Why Debian?</h2>
            <p>30+ years of stability. No corporate agenda. The universal base.</p>
            <p>Ubuntu, Mint, Pop, Kali — all downstream of Debian. We go to the source.</p>
            <p>Rolling releases (Arch) break. Debian doesn't. Stability is a feature.</p>
          </section>

          <section>
            <h2>Why Our Own Installer?</h2>
            <p>Calamares pulls 1.4GB of KDE/Qt dependencies. The Debian installer looks like 1998.</p>
            <p>We built a whiptail TUI: Catppuccin-themed, ~0 bytes overhead, supports dual-boot, LUKS encryption, and three install modes.</p>
            <p>The installer is a bash script. It uses <code>debootstrap</code>, <code>parted</code>, <code>cryptsetup</code>, and <code>grub</code>. All standard Debian tools. No custom binaries.</p>
          </section>

          <section>
            <h2>Why Three Modes?</h2>
            <p><strong>Desktop:</strong> Sway + fuzzel + kitty + audio. On first login, choose Base (tiling) or Desktop (familiar with file manager, screenshots, etc).</p>
            <p><strong>Secure Server:</strong> SSH + UFW firewall + fail2ban + monitoring. Hardened out of the box.</p>
            <p><strong>Bare Server:</strong> SSH + pure Debian. Nothing else. You build what you need.</p>
            <p>One ISO. Three outcomes. No wasted packages.</p>
          </section>

          <section>
            <h2>Why Sway + fuzzel + NetworkManager?</h2>
            <p><strong>Sway:</strong> i3-compatible, Wayland-native, stable. No GNOME bloat. No KDE complexity.</p>
            <p><strong>fuzzel:</strong> One binary. App launcher + settings menu. The user's interface to everything.</p>
            <p><strong>NetworkManager:</strong> Universal connectivity. WiFi setup from within fuzzel.</p>
            <p>All three are <strong>written in C</strong>, minimal, and fast. No JavaScript overhead. No Python startup lag. This is the 3-package math that equals a 16MB base desktop.</p>
          </section>

          <section>
            <h2>Why One ISO?</h2>
            <p>Most distros provide 20+ ISOs (Desktop, Server, Minimal, etc). We provide one. ~800MB.</p>
            <p>The installer asks: Desktop or Server? Desktop installs the GUI stack. Server installs SSH and optionally hardens. Both use <code>debootstrap</code> — clean install, zero leftovers from the live session.</p>
            <p>Smaller than Arch. Smaller than Ubuntu. Smaller than Fedora. With a prettier installer than all of them.</p>
          </section>

          <section>
            <h2>Why gtkgreet + gtklock?</h2>
            <p><strong>gtkgreet:</strong> GTK3 greeter for greetd, running inside cage. Auto-remembers last user. Custom CSS — minimal, themed to match.</p>
            <p><strong>gtklock:</strong> Lock screen with custom CSS. Same aesthetic. Minimal, matches the theme.</p>
            <p>No GDM. No SDDM. No LightDM. Two tiny GTK apps that do one thing each.</p>
          </section>

          <section>
            <h2>Why a Custom Status Bar?</h2>
            <p>Waybar and polybar fork 10-20 processes per second. We wrote a pure bash status bar.</p>
            <p>It reads <code>/sys</code> and <code>/proc</code> directly — zero forks for brightness, battery, memory, GPU. One fork every 5 seconds for volume (<code>wpctl</code>). One fork every 30 seconds for disk (<code>df</code>).</p>
            <p>Auto-detects hardware at startup: backlight, battery, WiFi, Bluetooth, GPU type. Shows only what exists. Bluetooth icon hidden when off.</p>
            <p>~0.1% CPU. 234 lines of bash.</p>
          </section>

          <section>
            <h2>Why Dual-Boot?</h2>
            <p>Anti-cheat games don't work on Linux. Some software requires Windows.</p>
            <p>The installer detects Windows, offers "Install alongside" — shrinks the NTFS partition, creates Tebian partitions, sets up GRUB with os-prober. Safe, guided, reversible.</p>
            <p>Not emulation — native Windows when you need it.</p>
          </section>

          <section>
            <h2>Why LUKS Encryption?</h2>
            <p>Laptops get stolen. Disks get sold. Encryption should be one checkbox, not a 40-step guide.</p>
            <p>The installer offers it as a yes/no. Sets up crypttab, initramfs, GRUB — all automatic. Enter a passphrase at boot. Done.</p>
          </section>

          <section>
            <h2>Why One Folder?</h2>
            <p><code>~/Tebian/</code> contains everything Tebian-specific.</p>
            <p>Delete it = pure Debian. Keep it = working desktop.</p>
            <p>No scattered config. No hidden hooks. One folder, one undo command.</p>
          </section>

          <section>
            <h2>Why No Repo?</h2>
            <p>We don't fork packages. We don't patch software.</p>
            <p>Debian maintains 50,000 packages. We maintain one script.</p>
            <p>Updates come from Debian. Security comes from Debian. We ride for free.</p>
          </section>

          <section>
            <h2>Why No Docs/Manual?</h2>
            <p>The system is the manual.</p>
            <p>One question at boot. Everything else is fuzzel. Keybinds shown in-menu.</p>
            <p>If you need a manual, the system is too complex.</p>
          </section>

          <section>
            <h2>Why macOS VM?</h2>
            <p>iOS development requires Xcode. Xcode requires macOS.</p>
            <p>One-click OSX-KVM setup: USB passthrough, shared folders, iCloud valid.</p>
            <p>No second machine. No Hackintosh. Just a VM when you need it.</p>
          </section>

          <section>
            <h2>Why Waydroid?</h2>
            <p>Android apps on desktop. Containerized, not emulated.</p>
            <p>Run phone apps in a window. Same experience, bigger screen.</p>
          </section>

          <section>
            <h2>Why The Fleet?</h2>
            <p>One config across PC, Pi, server, phone.</p>
            <p>T-Link connects them (Tailscale mesh). T-Sync keeps configs aligned.</p>
            <p>Your OS isn't a machine — it's a fleet.</p>
          </section>

          <section>
            <h2>Why Local AI?</h2>
            <p>Ollama runs Llama, Mistral, etc. on your hardware.</p>
            <p>No API keys. No cloud. No data leaving your machine.</p>
            <p>AI is a tool for empowerment, not surveillance.</p>
          </section>

          <section>
            <h2>Why Stealth Glass?</h2>
            <p>The default theme is Glass — catppuccin mocha with blur. Bar visible, minimal, informational.</p>
            <p>Want less? Switch to Minimal feel — bar goes invisible, hold Super to peek. The screen belongs to your work.</p>
            <p>11 themes. 3 feels. All switchable from the fuzzel menu in seconds.</p>
          </section>

          <section>
            <h2>Why The Mothership?</h2>
            <p>Self-hosted cloud: Syncthing (files), Vaultwarden (passwords), Matrix (chat).</p>
            <p>If it's on someone else's computer, it isn't yours.</p>
          </section>

          <section>
            <h2>Why Kill 200 Distros?</h2>
            <p>Most distros are: base + packages + theme + branding.</p>
            <p>That's not a distro. That's a post-install script with a logo.</p>
            <p>Tebian exposes this. We're honest about what we are.</p>
          </section>

          <section>
            <h2>Why Keep 3 Survivors?</h2>
            <p>Only three distros do something Tebian can't:</p>
            <ul>
              <li><strong>Alpine:</strong> musl, 5MB containers, embedded. Different base entirely.</li>
              <li><strong>Tails:</strong> Amnesic, RAM-only. Power off = zero trace. True deniability.</li>
              <li><strong>Proxmox:</strong> Datacenter hypervisor. 100s of VMs, clustering, HA.</li>
            </ul>
            <p>Everything else is Debian with extra steps. We respect genuine difference. We kill redundancy.</p>
          </section>

          <section>
            <h2>Why Server = Pure Debian?</h2>
            <p>Server users don't need a desktop. The installer uses <code>debootstrap</code> — only server packages touch the disk.</p>
            <p><strong>Bare Server:</strong> SSH + standard system utilities. You build the rest.</p>
            <p><strong>Secure Server:</strong> SSH + UFW + fail2ban + monitoring. Production-ready in 10 minutes.</p>
            <p>No GUI packages installed. No leftover configs. Just Debian.</p>
          </section>

          <section>
            <h2>Why Kill macOS and Windows?</h2>
            <p>They're not Linux. But Tebian kills them anyway.</p>
            <p><strong>macOS:</strong> Need Xcode? Run it in a VM. One-click setup. No second machine.</p>
            <p><strong>Windows:</strong> Anti-cheat games? The installer detects Windows and offers safe dual-boot. Native when you need it, one reboot away.</p>
            <p>Don't switch OS. Make them come to you.</p>
          </section>
        </article>
      </main>
    </>
  );
}
