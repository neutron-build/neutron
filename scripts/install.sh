#!/bin/sh
# Neutron CLI installer.
#
# Usage:   curl -fsSL https://get.neutron.build | sh
#
# Environment variables:
#   NEUTRON_INSTALL_DIR   target directory (default: $HOME/.local/bin)
#   NEUTRON_VERSION       pin a specific version (e.g. "0.1.0"); default: latest
#
# Source: https://github.com/neutron-build/neutron/blob/main/scripts/install.sh
# Hosted at: https://get.neutron.build (served as a static file by Caddy on OVH)

set -eu

OWNER="neutron-build"
REPO="neutron"
BIN="neutron"
INSTALL_DIR="${NEUTRON_INSTALL_DIR:-$HOME/.local/bin}"

# ---- detect platform ------------------------------------------------------

case "$(uname -s)" in
  Darwin) OS="darwin" ;;
  Linux)  OS="linux" ;;
  *)
    echo "neutron: unsupported OS: $(uname -s)" >&2
    echo "         supported: macOS, Linux. See https://neutron.build/docs/cli" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  ARCH="amd64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *)
    echo "neutron: unsupported architecture: $(uname -m)" >&2
    echo "         supported: x86_64, arm64. See https://neutron.build/docs/cli" >&2
    exit 1
    ;;
esac

# ---- resolve version ------------------------------------------------------

if [ -n "${NEUTRON_VERSION:-}" ]; then
  VERSION="$NEUTRON_VERSION"
else
  # Find the most recent release whose tag starts with cli/v
  VERSION=$(curl -fsSL "https://api.github.com/repos/${OWNER}/${REPO}/releases" 2>/dev/null \
    | grep -oE '"tag_name":[[:space:]]*"cli/v[^"]+"' \
    | head -1 \
    | sed -E 's/.*"cli\/v([^"]+)".*/\1/')
  if [ -z "$VERSION" ]; then
    echo "neutron: no published CLI release found." >&2
    echo "         See https://github.com/${OWNER}/${REPO}/releases" >&2
    echo "         Or install via npm:  npx @neutron-build/cli --help" >&2
    exit 1
  fi
fi

ASSET="neutron_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/${OWNER}/${REPO}/releases/download/cli/v${VERSION}/${ASSET}"

echo "Installing neutron CLI v${VERSION} (${OS}/${ARCH})"
echo "  from ${URL}"

# ---- download + extract + install ----------------------------------------

TMP=$(mktemp -d 2>/dev/null || mktemp -d -t neutron)
trap 'rm -rf "$TMP"' EXIT INT TERM

if ! curl -fsSL "$URL" | tar -xz -C "$TMP"; then
  echo "neutron: download or extract failed for ${ASSET}" >&2
  echo "         verify the release exists at https://github.com/${OWNER}/${REPO}/releases/tag/cli/v${VERSION}" >&2
  exit 1
fi

if [ ! -f "${TMP}/${BIN}" ]; then
  echo "neutron: archive did not contain ./${BIN} — bug, please report." >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
mv "${TMP}/${BIN}" "${INSTALL_DIR}/${BIN}"
chmod +x "${INSTALL_DIR}/${BIN}"

echo ""
echo "  installed: ${INSTALL_DIR}/${BIN}"

# ---- PATH hint ------------------------------------------------------------

case ":${PATH}:" in
  *":${INSTALL_DIR}:"*)
    echo "  ready:  ${BIN} --version"
    ;;
  *)
    echo ""
    echo "  ${INSTALL_DIR} is not on your PATH. Add it to your shell profile:"
    echo "    export PATH=\"${INSTALL_DIR}:\$PATH\""
    ;;
esac
