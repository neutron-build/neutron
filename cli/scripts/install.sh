#!/bin/sh
# Neutron CLI installer — https://neutron.build
# Usage: curl -fsSL https://get.neutron.build | sh
set -e

REPO="neutron-build/neutron"
INSTALL_DIR="${NEUTRON_INSTALL_DIR:-/usr/local/bin}"
BINARY="neutron"

main() {
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)

    case "$arch" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *) echo "Error: unsupported architecture $arch"; exit 1 ;;
    esac

    case "$os" in
        darwin|linux) ;;
        *) echo "Error: unsupported OS $os (use WSL on Windows)"; exit 1 ;;
    esac

    # Find the latest cli/v* release
    echo "Finding latest Neutron CLI release..."
    tag=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases" \
        | grep -o '"tag_name": *"cli/v[^"]*"' \
        | head -1 \
        | sed 's/"tag_name": *"cli\/v\(.*\)"/\1/')

    if [ -z "$tag" ]; then
        echo "Error: could not find a CLI release"
        exit 1
    fi

    echo "Latest version: v${tag}"

    archive="neutron_${tag}_${os}_${arch}.tar.gz"
    url="https://github.com/${REPO}/releases/download/cli/v${tag}/${archive}"
    checksums_url="https://github.com/${REPO}/releases/download/cli/v${tag}/checksums.txt"

    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT

    echo "Downloading ${archive}..."
    curl -fsSL -o "${tmpdir}/${archive}" "$url"

    # Verify checksum
    echo "Verifying checksum..."
    curl -fsSL -o "${tmpdir}/checksums.txt" "$checksums_url"
    expected=$(grep "$archive" "${tmpdir}/checksums.txt" | awk '{print $1}')

    if [ -n "$expected" ]; then
        if command -v sha256sum >/dev/null 2>&1; then
            actual=$(sha256sum "${tmpdir}/${archive}" | awk '{print $1}')
        elif command -v shasum >/dev/null 2>&1; then
            actual=$(shasum -a 256 "${tmpdir}/${archive}" | awk '{print $1}')
        else
            echo "Warning: no sha256 tool found, skipping verification"
            actual="$expected"
        fi

        if [ "$actual" != "$expected" ]; then
            echo "Error: checksum mismatch"
            echo "  expected: $expected"
            echo "  actual:   $actual"
            exit 1
        fi
        echo "Checksum OK"
    else
        echo "Warning: checksum not found for ${archive}, skipping verification"
    fi

    # Extract
    echo "Extracting..."
    tar -xzf "${tmpdir}/${archive}" -C "$tmpdir"

    # Install
    if [ -w "$INSTALL_DIR" ]; then
        mv "${tmpdir}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
    else
        echo "Installing to ${INSTALL_DIR} (requires sudo)..."
        sudo mv "${tmpdir}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
    fi
    chmod +x "${INSTALL_DIR}/${BINARY}"

    echo ""
    echo "Neutron CLI v${tag} installed to ${INSTALL_DIR}/${BINARY}"
    echo ""
    echo "Get started:"
    echo "  neutron new my-app --lang typescript"
    echo "  neutron doctor"
}

main
