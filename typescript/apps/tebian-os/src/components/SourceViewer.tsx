import { useState } from "preact/hooks";

const files: Record<string, string> = {
  "install.sh": `#!/bin/bash
# Usage: curl -sL tebian.org/install | bash
# For ARM / Pi / existing Debian installs

set -euo pipefail

TEBIAN_DIR="$HOME/Tebian"
TEBIAN_REPO="https://github.com/tebian-os/tebian.git"

echo ""
echo "  ┌───────────────┐"
echo "  │  T E B I A N  │"
echo "  └───────────────┘"
echo ""

if [ ! -f /etc/debian_version ]; then
    echo "Error: Tebian requires a Debian-based system"
    exit 1
fi

if ! command -v git &>/dev/null; then
    sudo apt update -qq && sudo apt install -y -qq git
fi

git clone --depth 1 "$TEBIAN_REPO" "$TEBIAN_DIR"
exec bash "$TEBIAN_DIR/bootstrap.sh"`,

  "tebian-installer": `#!/bin/bash
# Tebian Installer — runs from live USB
# Catppuccin-themed whiptail TUI

# Catppuccin Mocha theme
export NEWT_COLORS='
root=white,black
border=cyan,black
title=cyan,black
window=white,black
button=black,cyan
actbutton=white,blue
listbox=white,black
actlistbox=black,cyan
'

# Step 1: Select disk
DISK=$(whiptail --menu "Select disk" 18 70 10 \\
    /dev/sda "500GB SSD" \\
    /dev/nvme0n1 "1TB NVMe" \\
    3>&1 1>&2 2>&3)

# Step 2: Partition strategy
# Detects Windows → offers "Install alongside"
MODE=$(whiptail --menu "Partition" 18 70 10 \\
    alongside "Install alongside Windows" \\
    erase "Erase disk" \\
    manual "Manual (advanced)" \\
    3>&1 1>&2 2>&3)

# Step 3: Optional LUKS encryption
whiptail --yesno "Enable encryption?" 8 40

# Step 4: User + hostname
USERNAME=$(whiptail --inputbox "Username:" 8 40 \\
    3>&1 1>&2 2>&3)

# Step 5: Desktop or Server?
MODE=$(whiptail --menu "System mode" 18 70 10 \\
    desktop "Tebian Desktop (Sway + audio)" \\
    server  "Server (headless)" \\
    3>&1 1>&2 2>&3)

# Server submenu
# server-bare:   SSH + pure Debian
# server-secure: SSH + UFW + fail2ban + monitoring

# Step 6: debootstrap → chroot → GRUB → done
debootstrap trixie /mnt http://deb.debian.org/debian
# ... install packages, configure, reboot`,

  "desktop.sh": `#!/bin/bash
# Tebian Base Installer (V3.0)
# Installs minimum GUI: Sway, fuzzel, kitty, pipewire, greetd
# Desktop extras installed by tebian-onboard if user picks "Desktop"

set -euo pipefail

sudo apt update

# Base packages — minimum for sway + onboard fuzzel menu
sudo apt install -y \\
    sway swaybg \\
    fuzzel \\
    kitty \\
    pipewire wireplumber pipewire-pulse \\
    fonts-noto fonts-jetbrains-mono \\
    network-manager \\
    curl \\
    greetd gtkgreet cage libnotify-bin

# Copy configs
mkdir -p ~/.config/sway ~/.config/kitty ~/.config/fuzzel
cp ~/Tebian/configs/sway/config ~/.config/sway/
cp ~/Tebian/configs/themes/glass/kitty.conf ~/.config/kitty/
cp ~/Tebian/configs/themes/glass/fuzzel.ini ~/.config/fuzzel/

# Install scripts to ~/.local/bin/
cp ~/Tebian/scripts/* ~/.local/bin/
chmod +x ~/.local/bin/*

# Enable graphical login
sudo systemctl enable greetd

echo "Done. Reboot for graphical login."`,

  "status.sh": `#!/bin/bash
# Zero-Fork Status Bar — pure bash, reads /sys and /proc
# Auto-detects: backlight, battery, WiFi, Bluetooth, GPU

# Hardware detection (once at startup)
BACKLIGHT_DEV=$(ls /sys/class/backlight/ | head -1)
WIFI_IF=$(ls /sys/class/net/ | grep -E '^wl' | head -1)
BAT_DEV=$(ls /sys/class/power_supply/ | grep -i bat | head -1)

while true; do
    # 1s: time + capslock
    printf -v date_str '%(%a %d %b %H:%M)T' -1

    # 5s: brightness, memory, GPU, volume
    if [ $((counter % 5)) -eq 0 ]; then
        read -r cur < /sys/class/backlight/$BACKLIGHT_DEV/actual_brightness
        read -r max < /sys/class/backlight/$BACKLIGHT_DEV/max_brightness
        bright=$((cur * 100 / max))
        vol_raw=$(wpctl get-volume @DEFAULT_AUDIO_SINK@)
    fi

    # 10s: WiFi signal strength
    # 30s: disk usage, bluetooth, battery

    echo "$wifi | $vol | $bright | $bat | $date_str"
    sleep 1
done`,
};

const fileNames = Object.keys(files);

export function SourceViewer() {
  const [activeFile, setActiveFile] = useState(fileNames[0]);

  const code = files[activeFile];
  const lines = code.split("\n");

  return (
    <div class="editor">
      <div class="tabs">
        {fileNames.map((file) => (
          <button
            key={file}
            class={`tab ${file === activeFile ? "active" : ""}`}
            onClick={() => setActiveFile(file)}
          >
            {file}
          </button>
        ))}
      </div>
      <div class="code-container">
        <div class="line-numbers">
          {lines.map((_, i) => (
            <span key={i}>{i + 1}</span>
          ))}
        </div>
        <pre class="code">
          <code>{code}</code>
        </pre>
      </div>
    </div>
  );
}
