#!/bin/sh
# Neutron CLI installer. Usage: curl -fsSL https://get.neutron.build | sh
# NEUTRON_VERSION pins a release; NEUTRON_INSTALL_DIR defaults to ~/.local/bin.
set -eu

fail() { echo "neutron: $*" >&2; exit 1; }
REPO=neutron-build/neutron
INSTALL_DIR=${NEUTRON_INSTALL_DIR:-"$HOME/.local/bin"}
case "$(uname -s)" in
    Darwin) os=darwin ;;
    Linux) os=linux ;;
    MINGW*|MSYS*|CYGWIN*) fail "Windows is supported through WSL; run this installer inside its Linux shell" ;;
    *) fail "supported operating systems: macOS and Linux (Windows through WSL)" ;;
esac
case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) fail "supported architectures: x86_64 and arm64" ;;
esac
if command -v sha256sum >/dev/null 2>&1; then
    hash_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    hash_tool=shasum
else
    fail "SHA-256 verification requires sha256sum or shasum; nothing was installed"
fi
version=${NEUTRON_VERSION:-}
if [ -z "$version" ]; then
    releases=$(curl --proto '=https' --tlsv1.2 -fsSL "https://api.github.com/repos/$REPO/releases") || fail "cannot read releases"
    version=$(printf '%s\n' "$releases" | grep -oE '"tag_name"[[:space:]]*:[[:space:]]*"cli/v[^"]+"' | head -1 | sed -E 's/.*"cli\/v([^"]+)"/\1/')
fi
case "$version" in
    ''|*[!0-9A-Za-z.+-]*|[!0-9]*) fail "no valid CLI release version; set NEUTRON_VERSION to a published version" ;;
esac
archive=neutron_${version}_${os}_${arch}.tar.gz
base=https://github.com/$REPO/releases/download/cli/v$version
task_tmp=$(mktemp -d)
trap 'rm -rf "$task_tmp"' EXIT HUP INT TERM
curl --proto '=https' --tlsv1.2 -fsSL -o "$task_tmp/$archive" "$base/$archive" || fail "archive download failed"
curl --proto '=https' --tlsv1.2 -fsSL -o "$task_tmp/checksums.txt" "$base/checksums.txt" || fail "checksum download failed"
expected=$(awk -v name="$archive" '$2 == name || $2 == "*" name {print $1}' "$task_tmp/checksums.txt")
[ "${#expected}" -eq 64 ] || fail "expected exactly one SHA-256 checksum for $archive"
case "$expected" in *[!0-9a-fA-F]*) fail "invalid SHA-256 checksum for $archive" ;; esac
if [ "$hash_tool" = sha256sum ]; then
    actual=$(sha256sum "$task_tmp/$archive" | awk '{print $1}')
else
    actual=$(shasum -a 256 "$task_tmp/$archive" | awk '{print $1}')
fi
[ "$actual" = "$expected" ] || fail "checksum mismatch; nothing was installed"
# Release archives contain exactly one regular file, named neutron.
entries=$(tar -tzf "$task_tmp/$archive") || fail "invalid archive"
[ "$entries" = neutron ] || fail "archive must contain only neutron"
tar -xzf "$task_tmp/$archive" -C "$task_tmp" || fail "archive extraction failed"
[ -f "$task_tmp/neutron" ] && [ ! -L "$task_tmp/neutron" ] || fail "archive binary is not a regular file"
mkdir -p "$INSTALL_DIR"
staged=$(mktemp "$INSTALL_DIR/.neutron-install.XXXXXX")
trap 'rm -rf "$task_tmp"; rm -f "$staged"' EXIT HUP INT TERM
cp "$task_tmp/neutron" "$staged"
chmod 755 "$staged"
mv -f "$staged" "$INSTALL_DIR/neutron"
printf 'Installed neutron CLI %s to %s/neutron\nRun: neutron version\n' "$version" "$INSTALL_DIR"
