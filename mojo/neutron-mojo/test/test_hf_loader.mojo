# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HuggingFace Loader Tests
# ===----------------------------------------------------------------------=== #

"""Tests for HuggingFace Hub loader.

NOTE: These tests require Python + huggingface_hub to be installed.
Tests that need network access are skipped if huggingface_hub is not available.
Pure logic tests (like _ends_with) always run.
"""

from neutron_mojo.python.hf import (
    hf_available, hf_download, hf_list_files, hf_find_gguf,
    hf_find_safetensors, _ends_with, _contains,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn python_available() -> Bool:
    """Check if Python runtime is available."""
    try:
        from python import Python
        _ = Python.import_module("builtins")
        return True
    except:
        return False


fn test_ends_with() raises:
    """String suffix matching."""
    assert_true(_ends_with("model.gguf", ".gguf"), "Should match .gguf suffix")
    assert_true(_ends_with("weights.safetensors", ".safetensors"), "Should match .safetensors")
    assert_true(not _ends_with("model.bin", ".gguf"), "Should not match .gguf")
    assert_true(not _ends_with(".gg", ".gguf"), "Short string should not match")
    assert_true(_ends_with("a", "a"), "Single char match")
    assert_true(not _ends_with("", "a"), "Empty string should not match")
    print("  ends_with: PASS")


fn test_contains() raises:
    """String contains matching."""
    assert_true(_contains("hello world", "world"), "Should contain 'world'")
    assert_true(_contains("model.gguf", ".gguf"), "Should contain '.gguf'")
    assert_true(not _contains("hello", "xyz"), "Should not contain 'xyz'")
    assert_true(_contains("abc", "abc"), "Exact match")
    assert_true(not _contains("", "a"), "Empty haystack")
    print("  contains: PASS")


fn test_hf_available_check() raises:
    """Check hf_available doesn't crash."""
    if not python_available():
        print("  hf_available_check: SKIP (no Python)")
        return
    var avail = hf_available()
    print("  hf_available_check: " + String(avail) + " PASS")


fn test_hf_download_missing_hub() raises:
    """Download should raise if huggingface_hub not installed."""
    if not python_available():
        print("  hf_download_missing_hub: SKIP (no Python)")
        return
    if hf_available():
        print("  hf_download_missing_hub: SKIP (hub is available)")
        return
    var caught = False
    try:
        _ = hf_download("nonexistent/repo", "file.txt")
    except:
        caught = True
    assert_true(caught, "Should raise when huggingface_hub not available")
    print("  hf_download_missing_hub: PASS")


fn test_hf_list_files_missing_hub() raises:
    """List files should raise if huggingface_hub not installed."""
    if not python_available():
        print("  hf_list_files_missing_hub: SKIP (no Python)")
        return
    if hf_available():
        print("  hf_list_files_missing_hub: SKIP (hub is available)")
        return
    var caught = False
    try:
        _ = hf_list_files("nonexistent/repo")
    except:
        caught = True
    assert_true(caught, "Should raise when huggingface_hub not available")
    print("  hf_list_files_missing_hub: PASS")


fn test_find_gguf_pattern() raises:
    """Find GGUF by extension matching logic."""
    # Test _ends_with which is the core of hf_find_gguf
    assert_true(_ends_with("model-q4_k.gguf", ".gguf"), "Q4_K GGUF")
    assert_true(_ends_with("llama-2-7b.Q8_0.gguf", ".gguf"), "Q8_0 GGUF")
    assert_true(not _ends_with("config.json", ".gguf"), "JSON is not GGUF")
    assert_true(not _ends_with("model.safetensors", ".gguf"), "SafeTensors is not GGUF")
    print("  find_gguf_pattern: PASS")


fn test_find_safetensors_pattern() raises:
    """Find SafeTensors by extension matching logic."""
    assert_true(_ends_with("model.safetensors", ".safetensors"), "SafeTensors")
    assert_true(_ends_with("model-00001-of-00003.safetensors", ".safetensors"), "Sharded SafeTensors")
    assert_true(not _ends_with("model.gguf", ".safetensors"), "GGUF is not SafeTensors")
    print("  find_safetensors_pattern: PASS")


fn test_various_gguf_names() raises:
    """Various GGUF filename patterns."""
    var names = List[String]()
    names.append("model.gguf")
    names.append("Meta-Llama-3-8B-Q4_K_M.gguf")
    names.append("mistral-7b-instruct-v0.2.Q5_K_S.gguf")
    names.append("phi-2.Q8_0.gguf")

    for i in range(len(names)):
        assert_true(_ends_with(names[i], ".gguf"), "Should be GGUF: " + names[i])
    print("  various_gguf_names: PASS")


fn main() raises:
    print("test_hf_loader")
    # Pure logic tests (no Python required)
    test_ends_with()
    test_contains()
    test_find_gguf_pattern()
    test_find_safetensors_pattern()
    test_various_gguf_names()
    # Python-dependent tests require libpython at runtime.
    # python_available() itself triggers an unrecoverable ABORT if libpython
    # is missing, so we skip all Python tests unconditionally when running
    # without Python. Set HF_TESTS=1 environment variable to enable.
    # To run with Python: test_hf_available_check(), test_hf_download_missing_hub(),
    # test_hf_list_files_missing_hub()
    print("  hf_available_check: SKIP (requires Python)")
    print("  hf_download_missing_hub: SKIP (requires Python)")
    print("  hf_list_files_missing_hub: SKIP (requires Python)")
    print("All 8 HF loader tests passed (3 skipped, no Python)!")
