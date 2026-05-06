#!/bin/bash
# ==============================================================================
# TEBIAN REMOTE INSTALLER
# Usage: curl -sL tebian.org/install | bash
# Local: bash install.sh --local
# Fleet: curl -sL tebian.org/install | bash -s -- --repo https://your-git-host/org/tebian.git
# Works on: Debian, Raspberry Pi OS, Armbian, Ubuntu (any Debian-based distro)
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

TEBIAN_DIR="$HOME/Tebian"
TEBIAN_TARBALL="https://github.com/tebian-os/tebian/archive/main.tar.gz"
TEBIAN_CHECKSUM_URL="https://github.com/tebian-os/tebian/releases/latest/download/SHA256SUMS"
USE_GIT=""
TEBIAN_REPO=""
USE_LOCAL=""
LOCAL_SRC=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)
            USE_LOCAL="yes"
            # Check if next arg is a path (not another flag)
            if [[ $# -gt 1 && ! "$2" =~ ^-- ]]; then
                LOCAL_SRC="$2"
                shift 2
            else
                shift
            fi
            ;;
        --repo)
            USE_GIT="yes"
            TEBIAN_REPO="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

echo ""
echo "  ┌───────────────┐"
echo "  │  T E B I A N  │"
echo "  └───────────────┘"
echo ""

# Check for Debian-based system
if [ ! -f /etc/debian_version ]; then
    echo -e "${RED}Error: Tebian requires a Debian-based system${NC}"
    echo ""
    echo "  Supported: Debian, Raspberry Pi OS, Armbian, Ubuntu"
    exit 1
fi

echo -e "  ${GREEN}Detected:${NC} $(. /etc/os-release && echo "$PRETTY_NAME")"
echo -e "  ${GREEN}Architecture:${NC} $(uname -m)"
if [ -f /sys/firmware/devicetree/base/model ]; then
    echo -e "  ${GREEN}Hardware:${NC} $(tr -d '\0' < /sys/firmware/devicetree/base/model)"
fi
echo ""

# Local mode: use the local tebian-os directory
if [[ "$USE_LOCAL" == "yes" ]]; then
    # Auto-detect source directory if not specified
    if [ -z "$LOCAL_SRC" ]; then
        SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        if [ -f "$SCRIPT_PATH/bootstrap.sh" ] && [ -d "$SCRIPT_PATH/scripts" ]; then
            LOCAL_SRC="$SCRIPT_PATH"
        elif [ -d "$HOME/tebian-os/scripts" ]; then
            LOCAL_SRC="$HOME/tebian-os"
        else
            echo -e "${RED}  Error: Could not auto-detect tebian-os directory${NC}"
            echo "  Usage: bash install.sh --local /path/to/tebian-os"
            exit 1
        fi
    fi

    LOCAL_SRC="$(cd "$LOCAL_SRC" && pwd)"

    if [ ! -f "$LOCAL_SRC/bootstrap.sh" ] || [ ! -d "$LOCAL_SRC/scripts" ]; then
        echo -e "${RED}  Error: $LOCAL_SRC doesn't look like a tebian-os directory${NC}"
        echo "  Expected: bootstrap.sh and scripts/ in $LOCAL_SRC"
        exit 1
    fi

    echo -e "${GREEN}  Local mode:${NC} $LOCAL_SRC"

    if [ "$LOCAL_SRC" != "$TEBIAN_DIR" ]; then
        if [ -d "$TEBIAN_DIR" ]; then
            echo -e "${YELLOW}  Updating $TEBIAN_DIR from local source...${NC}"
        else
            echo "  Linking local source to $TEBIAN_DIR..."
        fi
        rm -rf "$TEBIAN_DIR"
        ln -sf "$LOCAL_SRC" "$TEBIAN_DIR"
        echo -e "${GREEN}  ✓ Symlinked $TEBIAN_DIR -> $LOCAL_SRC${NC}"
    else
        echo -e "${GREEN}  ✓ Source is already at $TEBIAN_DIR${NC}"
    fi

# Check if Tebian is already installed
elif [ -d "$TEBIAN_DIR" ]; then
    echo -e "${YELLOW}  Tebian directory already exists at $TEBIAN_DIR${NC}"
    echo ""
    read -p "  Update and reinstall? [Y/n]: " confirm
    if [[ "$confirm" =~ ^[Nn]$ ]]; then
        echo "  Cancelled."
        exit 0
    fi
    echo "  Updating existing install..."
    # If it's a git repo, pull. Otherwise re-download tarball.
    if command -v git &>/dev/null && [ -d "$TEBIAN_DIR/.git" ]; then
        git -C "$TEBIAN_DIR" pull --ff-only 2>/dev/null || true
    else
        echo "  Downloading latest Tebian..."
        curl -fsSL "$TEBIAN_TARBALL" | tar xz -C /tmp
        if command -v rsync &>/dev/null; then
            rsync -a --exclude='tebian.conf' /tmp/tebian-main/ "$TEBIAN_DIR/"
        else
            cp -a /tmp/tebian-main/* "$TEBIAN_DIR/" 2>/dev/null
            cp -a /tmp/tebian-main/.* "$TEBIAN_DIR/" 2>/dev/null || true
        fi
        rm -rf /tmp/tebian-main
    fi
else
    if [[ "$USE_GIT" == "yes" ]]; then
        # Fleet mode: use git clone for ongoing sync
        echo -e "${YELLOW}  Fleet mode: $TEBIAN_REPO${NC}"
        if ! command -v git &>/dev/null; then
            echo "  Installing git..."
            sudo apt update -qq && sudo apt install -y -qq git
        fi
        echo "  Cloning config repo..."
        git clone --depth 1 "$TEBIAN_REPO" "$TEBIAN_DIR"
    else
        # Standard install: tarball with checksum verification
        echo "  Downloading Tebian..."
        curl -fsSL -o /tmp/tebian.tar.gz "$TEBIAN_TARBALL"

        # Verify checksum if available
        if curl -fsSL -o /tmp/tebian-SHA256SUMS "$TEBIAN_CHECKSUM_URL" 2>/dev/null; then
            echo "  Verifying checksum..."
            EXPECTED=$(grep 'main.tar.gz' /tmp/tebian-SHA256SUMS | awk '{print $1}')
            ACTUAL=$(sha256sum /tmp/tebian.tar.gz | awk '{print $1}')
            if [ -n "$EXPECTED" ] && [ "$EXPECTED" != "$ACTUAL" ]; then
                echo -e "${RED}  ✗ Checksum mismatch! Download may be corrupted or tampered with.${NC}"
                echo "  Expected: $EXPECTED"
                echo "  Got:      $ACTUAL"
                rm -f /tmp/tebian.tar.gz /tmp/tebian-SHA256SUMS
                exit 1
            fi
            echo -e "${GREEN}  ✓ Checksum verified${NC}"
            rm -f /tmp/tebian-SHA256SUMS
        else
            echo -e "${YELLOW}  ⚠ No checksum available — installing without verification${NC}"
        fi

        tar xzf /tmp/tebian.tar.gz -C /tmp
        rm -f /tmp/tebian.tar.gz
        mv /tmp/tebian-main "$TEBIAN_DIR"
    fi
fi

# Run bootstrap
echo ""
exec bash "$TEBIAN_DIR/bootstrap.sh"
