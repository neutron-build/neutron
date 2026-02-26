# ===----------------------------------------------------------------------=== #
# Neutron Mojo — HuggingFace Auto-Load Pipeline
# ===----------------------------------------------------------------------=== #

"""One-call HuggingFace model loading: download + parse + build Model.

Uses HF download for GGUF files and native Mojo parsers for loading.
"""

from neutron_mojo.python.hf import hf_available, hf_download, hf_find_gguf
from neutron_mojo.nn.model import Model
from neutron_mojo.nn.tokenizer import BPETokenizer, load_gguf_tokenizer
from neutron_mojo.model.weight_reader import load_gguf_model
from neutron_mojo.io.gguf import parse_gguf_file


struct HFLoadResult(Movable):
    """Result of loading a model from HuggingFace Hub."""
    var model: Model
    var tokenizer: BPETokenizer
    var model_path: String

    fn __init__(out self, var model: Model, var tokenizer: BPETokenizer, model_path: String):
        self.model = model^
        self.tokenizer = tokenizer^
        self.model_path = model_path

    fn __moveinit__(out self, deinit other: Self):
        self.model = other.model^
        self.tokenizer = other.tokenizer^
        self.model_path = other.model_path^


fn hf_load_model(repo_id: String) raises -> Model:
    """Download and load a GGUF model from HuggingFace.

    Finds the first .gguf file in the repo, downloads it,
    and parses it into a Model.

    Args:
        repo_id: HuggingFace repo ID (e.g., "TheBloke/TinyLlama-1.1B-GGUF").

    Returns:
        Loaded Model with weights populated.
    """
    var filename = hf_find_gguf(repo_id)
    var path = hf_download(repo_id, filename)
    return load_gguf_model(path)


fn hf_load_tokenizer(repo_id: String) raises -> BPETokenizer:
    """Download and load a tokenizer from a GGUF file on HuggingFace.

    Args:
        repo_id: HuggingFace repo ID.

    Returns:
        BPETokenizer loaded from the GGUF file's metadata.
    """
    var filename = hf_find_gguf(repo_id)
    var path = hf_download(repo_id, filename)
    var gguf = parse_gguf_file(path)

    var scores = List[Float64]()
    var bos = 1
    var eos = 2
    if "tokenizer.ggml.bos_token_id" in gguf.metadata_int:
        bos = gguf.metadata_int["tokenizer.ggml.bos_token_id"]
    if "tokenizer.ggml.eos_token_id" in gguf.metadata_int:
        eos = gguf.metadata_int["tokenizer.ggml.eos_token_id"]

    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=bos, eos_id=eos,
    )
    tok.unk_id = 0
    return tok^


fn hf_auto_load(repo_id: String) raises -> HFLoadResult:
    """Download, load, and return both model and tokenizer from HuggingFace.

    Args:
        repo_id: HuggingFace repo ID.

    Returns:
        HFLoadResult containing Model and BPETokenizer.
    """
    var filename = hf_find_gguf(repo_id)
    var path = hf_download(repo_id, filename)

    var model = load_gguf_model(path)

    var gguf = parse_gguf_file(path)
    var scores = List[Float64]()
    var bos = 1
    var eos = 2
    if "tokenizer.ggml.bos_token_id" in gguf.metadata_int:
        bos = gguf.metadata_int["tokenizer.ggml.bos_token_id"]
    if "tokenizer.ggml.eos_token_id" in gguf.metadata_int:
        eos = gguf.metadata_int["tokenizer.ggml.eos_token_id"]

    var tok = load_gguf_tokenizer(
        gguf.token_vocab, scores, gguf.token_merges,
        bos_id=bos, eos_id=eos,
    )
    tok.unk_id = 0

    return HFLoadResult(model^, tok^, path)
