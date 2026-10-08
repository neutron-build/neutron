#!/usr/bin/env bash
# All runnable safety models and invariants are declared in the checked manifest.
set -euo pipefail
python3 "$(dirname "$0")/manifest.py" run
