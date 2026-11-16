#!/usr/bin/env bash
# Provenance (card A-1a): nucleus/v2/scripts/check-api-surface.sh at 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b.
# Standard-tier code may use only the allowlisted public API of the core crates
# nucleus-store, nucleus-env, nucleus-am and nucleus-txn (PLAN-v3 §6.1). Allowlist: api-allowlist.txt.
#
# Scope: every .rs file under crates/ except the core crates below and the
# paths in CORE_PATHS. New crates are in scope by default. Renamed
# dependencies on the core crates are rejected because the lint matches crate names.
# The path parsing lives in xtask/src/api_surface.rs.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=core-paths-lib.sh
. "$here/core-paths-lib.sh"
cd "$here/.."

# Other premium code (catalog, embedded/src/txn.rs, ...) is exempt through
# CORE_PATHS only, so the S-editable rest of those crates is still linted.
core_crates="nucleus-store nucleus-env nucleus-am nucleus-txn"
entries=$(core_entries <CORE_PATHS)

files=()
manifests=(Cargo.toml)
for dir in crates/*/; do
  crate=$(basename "$dir")
  case " $core_crates " in *" $crate "*) continue ;; esac
  manifests+=("$dir/Cargo.toml")
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    is_core "$f" "$entries" >/dev/null || files+=("$f")
  done <<EOF
$(find "${dir%/}" -name '*.rs' -type f | LC_ALL=C sort)
EOF
done

if grep -nE 'package[[:space:]]*=[[:space:]]*"nucleus-(store|env|am|txn)"' "${manifests[@]}"; then
  echo "api-surface: renamed core-crate dependencies are not allowed" >&2
  exit 1
fi

if [ ${#files[@]} -eq 0 ]; then
  echo "api-surface: no files in scope"
  exit 0
fi
cargo run -q -p xtask -- api-surface api-allowlist.txt "${files[@]}"
