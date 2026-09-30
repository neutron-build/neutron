#!/bin/sh
# Prepare a remote Docker build context from the canonical installer.
set -eu
host_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
mkdir -p "$host_dir/dist"
cp "$host_dir/../../scripts/install.sh" "$host_dir/dist/install.sh"
cmp "$host_dir/../../scripts/install.sh" "$host_dir/dist/install.sh"
echo "Prepared get.neutron.build/install.sh from scripts/install.sh"
