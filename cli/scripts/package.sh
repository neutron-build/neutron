#!/bin/sh
# Build and package the CLI the way a cli/v* release publishes it.
#
#   scripts/package.sh <version> <goos> <goarch> [outdir]
#
# Writes neutron_<version>_<goos>_<goarch>.tar.gz to outdir (default: the
# current directory): one `neutron` binary at the archive root, the shape
# install.sh downloads and extracts. The embedded Studio comes from
# internal/studio/dist, which must hold a current Studio build (see
# internal/studio/embed.go). The release workflow and the ORM artifact gate
# (.github/workflows/orm-artifacts.yml) both package through this script.
set -eu

if [ $# -lt 3 ]; then
    echo "usage: $0 <version> <goos> <goarch> [outdir]" >&2
    exit 2
fi
version=$1
goos=$2
goarch=$3
outdir=$(cd "${4:-.}" && pwd)

cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

GOOS=$goos GOARCH=$goarch CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X github.com/neutron-build/neutron/cli/cmd.version=${version}" \
    -o "$tmp/neutron" .

archive="neutron_${version}_${goos}_${goarch}.tar.gz"
tar czf "$outdir/$archive" -C "$tmp" neutron
echo "$outdir/$archive"
