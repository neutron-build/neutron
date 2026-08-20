# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HuggingFace Hub Loader
# ===----------------------------------------------------------------------=== #

"""Download models from HuggingFace Hub via Python's huggingface_hub library.

All downloads use Python interop. Model loading uses native Mojo GGUF/SafeTensors
parsers after download.
"""

from std.python import Python, PythonObject


def hf_available() -> Bool:
    """Check if huggingface_hub is importable."""
    try:
        _ = Python.import_module("huggingface_hub")
        return True
    except:
        return False


def hf_download(repo_id: String, filename: String) raises -> String:
    """Download a file from HuggingFace Hub.

    Uses huggingface_hub.hf_hub_download() which handles caching automatically.

    Args:
        repo_id: HuggingFace repo ID (e.g., "TheBloke/Llama-2-7B-GGUF").
        filename: File to download (e.g., "llama-2-7b.Q4_K_M.gguf").

    Returns:
        Local file path to the downloaded file.
    """
    var hf = Python.import_module("huggingface_hub")
    var path = hf.hf_hub_download(repo_id=repo_id, filename=filename)
    return String(path)


def hf_list_files(repo_id: String) raises -> List[String]:
    """List files in a HuggingFace repository.

    Args:
        repo_id: HuggingFace repo ID.

    Returns:
        List of filenames in the repo.
    """
    var hf = Python.import_module("huggingface_hub")
    var builtins = Python.import_module("builtins")
    var files_iter = hf.list_repo_files(repo_id)
    var py_list = builtins.list(files_iter)
    var result = List[String]()
    var n = Int(py=builtins.len(py_list))
    for i in range(n):
        result.append(String(py_list[i]))
    return result^


def hf_find_gguf(repo_id: String) raises -> String:
    """Find the first .gguf file in a HuggingFace repository.

    Args:
        repo_id: HuggingFace repo ID.

    Returns:
        Filename of the first .gguf file found.

    Raises:
        Error if no .gguf file is found.
    """
    var files = hf_list_files(repo_id)
    for i in range(len(files)):
        var f = files[i]
        if _ends_with(f, ".gguf"):
            return f
    raise Error("No .gguf file found in repo: " + repo_id)


def hf_find_safetensors(repo_id: String) raises -> String:
    """Find the first .safetensors file in a HuggingFace repository.

    Args:
        repo_id: HuggingFace repo ID.

    Returns:
        Filename of the first .safetensors file found.

    Raises:
        Error if no .safetensors file is found.
    """
    var files = hf_list_files(repo_id)
    for i in range(len(files)):
        var f = files[i]
        if _ends_with(f, ".safetensors"):
            return f
    raise Error("No .safetensors file found in repo: " + repo_id)


def _ends_with(s: String, suffix: String) -> Bool:
    """Check if string ends with suffix."""
    if suffix.byte_length() > s.byte_length():
        return False
    var start = s.byte_length() - suffix.byte_length()
    for i in range(suffix.byte_length()):
        if ord(s[byte=start + i]) != ord(suffix[byte=i]):
            return False
    return True


def _contains(haystack: String, needle: String) -> Bool:
    """Check if haystack contains needle."""
    if needle.byte_length() > haystack.byte_length():
        return False
    for i in range(haystack.byte_length() - needle.byte_length() + 1):
        var found = True
        for j in range(needle.byte_length()):
            if ord(haystack[byte=i + j]) != ord(needle[byte=j]):
                found = False
                break
        if found:
            return True
    return False
