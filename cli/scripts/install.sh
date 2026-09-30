#!/bin/sh
# Repository entry point; the hosted installer is scripts/install.sh.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec sh "$script_dir/../../scripts/install.sh" "$@"
