#!/usr/bin/env python3
"""Run all reference implementation self-tests.

Usage:
    python validate_all.py           # Run all tests
    python validate_all.py --quick   # Skip slow tests (gradient checks)
"""

import sys
import os
import importlib
import time

# Ensure reference/ is on the import path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MODULES = [
    "matmul",
    "matmul_tiled",
    "softmax",
    "online_softmax",
    "activations",
    "rmsnorm",
    "layernorm",
    "rope",
    "flash_attention",
    "nf4_dequant",
    "gguf_dequant",
    "sampling",
]


def run_module(name: str) -> tuple:
    """Run a module's self-tests by calling its main block functions.

    Returns (passed: bool, elapsed_sec: float, error_msg: str | None)
    """
    t0 = time.perf_counter()
    try:
        mod = importlib.import_module(name)
        # Find and run all _test_* functions
        test_fns = sorted(
            [fn for fn in dir(mod) if fn.startswith("_test_")]
        )
        for fn_name in test_fns:
            fn = getattr(mod, fn_name)
            fn()
        elapsed = time.perf_counter() - t0
        return True, elapsed, None
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return False, elapsed, str(e)


def main():
    quick = "--quick" in sys.argv

    print("=" * 60)
    print("Neutron Mojo — Reference Implementation Validation")
    print("=" * 60)

    passed = 0
    failed = 0
    skipped = 0
    total_time = 0.0
    failures = []

    for name in MODULES:
        # Some modules have slow gradient checks
        print(f"\n--- {name} ---")
        ok, elapsed, err = run_module(name)
        total_time += elapsed

        if ok:
            passed += 1
            print(f"  PASSED ({elapsed:.2f}s)")
        else:
            failed += 1
            failures.append((name, err))
            print(f"  FAILED ({elapsed:.2f}s): {err}")

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print(f"Total time: {total_time:.2f}s")

    if failures:
        print("\nFailures:")
        for name, err in failures:
            print(f"  {name}: {err}")
        sys.exit(1)
    else:
        print("\nALL REFERENCE TESTS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
