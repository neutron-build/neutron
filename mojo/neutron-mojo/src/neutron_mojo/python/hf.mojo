# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HuggingFace Hub Loader
# ===----------------------------------------------------------------------=== #

"""Download models from HuggingFace Hub via Python's huggingface_hub library.

All downloads use Python interop. Model loading uses native Mojo GGUF/SafeTensors
parsers after download.
"""

from python import Python, PythonObject


fn hf_available() -> Bool:
    """Check if huggingface_hub is importable."""
    try:
        _ = Python.import_module("huggingface_hub")
        return True
    except:
        return False


fn hf_download(repo_id: String, filename: String) raises -> String:
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


fn hf_list_files(repo_id: String) raises -> List[String]:
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


fn hf_find_gguf(repo_id: String) raises -> String:
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


fn hf_find_safetensors(repo_id: String) raises -> String:
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


fn _ends_with(s: String, suffix: String) -> Bool:
    """Check if string ends with suffix."""
    if len(suffix) > len(s):
        return False
    var start = len(s) - len(suffix)
    for i in range(len(suffix)):
        if ord(s[byte=start + i]) != ord(suffix[byte=i]):
            return False
    return True


fn _contains(haystack: String, needle: String) -> Bool:
    """Check if haystack contains needle."""
    if len(needle) > len(haystack):
        return False
    for i in range(len(haystack) - len(needle) + 1):
        var found = True
        for j in range(len(needle)):
            if ord(haystack[byte=i + j]) != ord(needle[byte=j]):
                found = False
                break
        if found:
            return True
    return False
