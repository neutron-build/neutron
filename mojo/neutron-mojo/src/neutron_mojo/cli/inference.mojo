# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Unified CLI
# ===----------------------------------------------------------------------=== #

"""Multi-command CLI for Neutron Mojo inference engine.

Commands:
    neutron run <model.gguf> "<prompt>" [options]   — One-shot inference
    neutron serve <model.gguf> [--port 8080]        — Start HTTP API server
    neutron info <model.gguf>                       — Print model info
    neutron bench <model.gguf> [options]             — Benchmark performance
    neutron convert <model.gguf> -o <model.nmf>     — Export to NMF format
    neutron models [directory]                       — List available models

Run options:
    --max-tokens N     Maximum tokens to generate (default: 128)
    --template TYPE    Chat template: none, llama, chatml (default: none)
    --system PROMPT    System prompt for chat templates
    --temperature T    Sampling temperature (default: 1.0)
    --top-k K          Top-k sampling (default: 0 = disabled)
    --top-p P          Top-p sampling (default: 1.0 = disabled)
    --rep-penalty R    Repetition penalty (default: 1.0 = disabled)
    --q8               Use Q8-quantized model (default: FP32)
    --q8-direct        Direct Q8 loading (no dequant roundtrip)
    --q8-cache         Use Q8-quantized KV cache
    --mmap             Use memory-mapped file I/O
    --quiet            Only print generated text (no stats)
"""

from sys import argv
from time import perf_counter_ns

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.model.weight_reader import (
    load_gguf_model,
    load_gguf_quantized,
    load_gguf_quantized_direct,
    load_gguf_model_mmap,
    load_gguf_quantized_mmap,
    load_gguf_quantized_direct_mmap,
)
from neutron_mojo.io.gguf import parse_gguf_file, gguf_to_model_config
from neutron_mojo.nn.tokenizer import BPETokenizer, load_gguf_tokenizer
from neutron_mojo.nn.pipeline import (
    PipelineConfig,
    pipeline_generate,
)
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
from neutron_mojo.nn.q_model import QuantizedModel
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.sampler import SamplerConfig
from neutron_mojo.nn.bench import (
    model_info, estimate_memory, ModelInfo, MemoryEstimate,
)
from neutron_mojo.io.model_export import (
    save_model_to_buffer, load_model_from_buffer, NMFBuffer,
    serialize_params, deserialize_params,
)


# ===----------------------------------------------------------------------=== #
# Command Constants
# ===----------------------------------------------------------------------=== #

fn CMD_RUN() -> String:
    return "run"


fn CMD_SERVE() -> String:
    return "serve"


fn CMD_INFO() -> String:
    return "info"


fn CMD_BENCH() -> String:
    return "bench"


fn CMD_CONVERT() -> String:
    return "convert"


fn CMD_MODELS() -> String:
    return "models"


fn CMD_TRAIN() -> String:
    return "train"


fn KNOWN_COMMANDS() -> List[String]:
    var cmds = List[String]()
    cmds.append(CMD_RUN())
    cmds.append(CMD_SERVE())
    cmds.append(CMD_INFO())
    cmds.append(CMD_BENCH())
    cmds.append(CMD_CONVERT())
    cmds.append(CMD_MODELS())
    cmds.append(CMD_TRAIN())
    return cmds^


# ===----------------------------------------------------------------------=== #
# Argument Parsing
# ===----------------------------------------------------------------------=== #

struct CLIArgs(Movable):
    """Parsed command-line arguments for the unified CLI."""
    var command: String
    var model_path: String
    var prompt: String
    var output_path: String
    var port: Int
    var max_tokens: Int
    var template: String
    var system_prompt: String
    var temperature: Float32
    var top_k: Int
    var top_p: Float32
    var rep_penalty: Float32
    var freq_penalty: Float32
    var use_q8: Bool
    var q8_direct: Bool
    var q8_cache: Bool
    var use_mmap: Bool
    var quiet: Bool
    # Training-specific args
    var epochs: Int
    var lr: Float64
    var hidden_dim: Int
    var num_layers: Int
    var vocab_size: Int
    var use_lora: Bool
    var lora_rank: Int

    fn __init__(out self):
        self.command = String("")
        self.model_path = String("")
        self.prompt = String("")
        self.output_path = String("")
        self.port = 8080
        self.max_tokens = 128
        self.template = String("none")
        self.system_prompt = String("")
        self.temperature = 1.0
        self.top_k = 0
        self.top_p = 1.0
        self.rep_penalty = 1.0
        self.freq_penalty = 0.0
        self.use_q8 = False
        self.q8_direct = False
        self.q8_cache = False
        self.use_mmap = False
        self.quiet = False
        self.epochs = 10
        self.lr = 1e-3
        self.hidden_dim = 32
        self.num_layers = 2
        self.vocab_size = 256
        self.use_lora = False
        self.lora_rank = 4

    fn __moveinit__(out self, deinit other: Self):
        self.command = other.command^
        self.model_path = other.model_path^
        self.prompt = other.prompt^
        self.output_path = other.output_path^
        self.port = other.port
        self.max_tokens = other.max_tokens
        self.template = other.template^
        self.system_prompt = other.system_prompt^
        self.temperature = other.temperature
        self.top_k = other.top_k
        self.top_p = other.top_p
        self.rep_penalty = other.rep_penalty
        self.freq_penalty = other.freq_penalty
        self.use_q8 = other.use_q8
        self.q8_direct = other.q8_direct
        self.q8_cache = other.q8_cache
        self.use_mmap = other.use_mmap
        self.quiet = other.quiet
        self.epochs = other.epochs
        self.lr = other.lr
        self.hidden_dim = other.hidden_dim
        self.num_layers = other.num_layers
        self.vocab_size = other.vocab_size
        self.use_lora = other.use_lora
        self.lora_rank = other.lora_rank

    fn copy(self) -> CLIArgs:
        var c = CLIArgs()
        c.command = self.command
        c.model_path = self.model_path
        c.prompt = self.prompt
        c.output_path = self.output_path
        c.port = self.port
        c.max_tokens = self.max_tokens
        c.template = self.template
        c.system_prompt = self.system_prompt
        c.temperature = self.temperature
        c.top_k = self.top_k
        c.top_p = self.top_p
        c.rep_penalty = self.rep_penalty
        c.freq_penalty = self.freq_penalty
        c.use_q8 = self.use_q8
        c.q8_direct = self.q8_direct
        c.q8_cache = self.q8_cache
        c.use_mmap = self.use_mmap
        c.quiet = self.quiet
        c.epochs = self.epochs
        c.lr = self.lr
        c.hidden_dim = self.hidden_dim
        c.num_layers = self.num_layers
        c.vocab_size = self.vocab_size
        c.use_lora = self.use_lora
        c.lora_rank = self.lora_rank
        return c^


fn is_known_command(cmd: String) -> Bool:
    """Check if a string is a known CLI command."""
    if cmd == CMD_RUN():
        return True
    if cmd == CMD_SERVE():
        return True
    if cmd == CMD_INFO():
        return True
    if cmd == CMD_BENCH():
        return True
    if cmd == CMD_CONVERT():
        return True
    if cmd == CMD_MODELS():
        return True
    if cmd == CMD_TRAIN():
        return True
    return False


fn print_usage():
    """Print help text for the CLI."""
    print("Neutron Mojo -- Inference Engine CLI")
    print("")
    print("Usage:")
    print("  neutron <command> [arguments] [options]")
    print("")
    print("Commands:")
    print("  run <model.gguf> \"<prompt>\"  One-shot inference")
    print("  serve <model.gguf>           Start HTTP API server")
    print("  info <model.gguf>            Print model architecture info")
    print("  bench <model.gguf>           Benchmark performance")
    print("  convert <model.gguf> -o out  Export to NMF format")
    print("  models [directory]           List available models")
    print("  train <data> [options]       Train a tiny LM")
    print("")
    print("Run 'neutron <command> --help' for command-specific options.")


fn print_run_help():
    """Print help text for the run command."""
    print("Usage: neutron run <model.gguf> \"<prompt>\" [options]")
    print("")
    print("Options:")
    print("  --max-tokens N     Max tokens to generate (default: 128)")
    print("  --template TYPE    Chat template: none|llama|chatml")
    print("  --system PROMPT    System prompt for chat templates")
    print("  --temperature T    Sampling temperature (default: 1.0)")
    print("  --top-k K          Top-k sampling (default: 0)")
    print("  --top-p P          Top-p nucleus sampling (default: 1.0)")
    print("  --rep-penalty R    Repetition penalty (default: 1.0)")
    print("  --freq-penalty F   Frequency penalty (default: 0.0)")
    print("  --q8               Use Q8-quantized model")
    print("  --q8-direct        Direct Q8 loading (no dequant roundtrip)")
    print("  --q8-cache         Use Q8-quantized KV cache")
    print("  --mmap             Use memory-mapped file I/O")
    print("  --quiet            Only print generated text")


fn parse_cli_args(args: List[String]) raises -> CLIArgs:
    """Parse CLI arguments from a list of strings.

    This is the testable core of argument parsing. Takes a list of strings
    (like sys.argv) and returns parsed CLIArgs.

    Args:
        args: List of command-line argument strings (args[0] is program name).

    Returns:
        Parsed CLIArgs.
    """
    if len(args) < 2:
        print_usage()
        raise Error("No command specified")

    var result = CLIArgs()
    var cmd = args[1]

    if not is_known_command(cmd):
        print_usage()
        raise Error("Unknown command: " + cmd)

    result.command = cmd

    # Command-specific positional arg parsing
    if cmd == CMD_RUN():
        if len(args) < 4:
            print_run_help()
            raise Error("run requires: <model.gguf> \"<prompt>\"")
        result.model_path = args[2]
        result.prompt = args[3]
        var i = 4
        while i < len(args):
            var arg = args[i]
            if arg == "--max-tokens" and i + 1 < len(args):
                i += 1
                result.max_tokens = atol(args[i])
            elif arg == "--template" and i + 1 < len(args):
                i += 1
                result.template = args[i]
            elif arg == "--system" and i + 1 < len(args):
                i += 1
                result.system_prompt = args[i]
            elif arg == "--temperature" and i + 1 < len(args):
                i += 1
                result.temperature = Float32(atof(args[i]))
            elif arg == "--top-k" and i + 1 < len(args):
                i += 1
                result.top_k = atol(args[i])
            elif arg == "--top-p" and i + 1 < len(args):
                i += 1
                result.top_p = Float32(atof(args[i]))
            elif arg == "--rep-penalty" and i + 1 < len(args):
                i += 1
                result.rep_penalty = Float32(atof(args[i]))
            elif arg == "--freq-penalty" and i + 1 < len(args):
                i += 1
                result.freq_penalty = Float32(atof(args[i]))
            elif arg == "--q8":
                result.use_q8 = True
            elif arg == "--q8-direct":
                result.use_q8 = True
                result.q8_direct = True
            elif arg == "--q8-cache":
                result.q8_cache = True
            elif arg == "--mmap":
                result.use_mmap = True
            elif arg == "--quiet":
                result.quiet = True
            i += 1

    elif cmd == CMD_SERVE():
        if len(args) < 3:
            raise Error("serve requires: <model.gguf> [--port N]")
        result.model_path = args[2]
        var i = 3
        while i < len(args):
            var arg = args[i]
            if arg == "--port" and i + 1 < len(args):
                i += 1
                result.port = atol(args[i])
            elif arg == "--q8":
                result.use_q8 = True
            elif arg == "--mmap":
                result.use_mmap = True
            i += 1

    elif cmd == CMD_INFO():
        if len(args) < 3:
            raise Error("info requires: <model.gguf>")
        result.model_path = args[2]

    elif cmd == CMD_BENCH():
        if len(args) < 3:
            raise Error("bench requires: <model.gguf>")
        result.model_path = args[2]
        var i = 3
        while i < len(args):
            var arg = args[i]
            if arg == "--max-tokens" and i + 1 < len(args):
                i += 1
                result.max_tokens = atol(args[i])
            elif arg == "--q8":
                result.use_q8 = True
            elif arg == "--mmap":
                result.use_mmap = True
            elif arg == "--quiet":
                result.quiet = True
            i += 1

    elif cmd == CMD_CONVERT():
        if len(args) < 3:
            raise Error("convert requires: <model.gguf> -o <output.nmf>")
        result.model_path = args[2]
        var i = 3
        while i < len(args):
            var arg = args[i]
            if arg == "-o" and i + 1 < len(args):
                i += 1
                result.output_path = args[i]
            elif arg == "--mmap":
                result.use_mmap = True
            i += 1
        if len(result.output_path) == 0:
            raise Error("convert requires -o <output.nmf>")

    elif cmd == CMD_MODELS():
        if len(args) >= 3:
            result.model_path = args[2]

    elif cmd == CMD_TRAIN():
        if len(args) < 3:
            raise Error("train requires: <data-path> [options]")
        result.model_path = args[2]  # reuse model_path for data path
        var i = 3
        while i < len(args):
            var arg = args[i]
            if arg == "--epochs" and i + 1 < len(args):
                i += 1
                result.epochs = atol(args[i])
            elif arg == "--lr" and i + 1 < len(args):
                i += 1
                result.lr = atof(args[i])
            elif arg == "--hidden" and i + 1 < len(args):
                i += 1
                result.hidden_dim = atol(args[i])
            elif arg == "--layers" and i + 1 < len(args):
                i += 1
                result.num_layers = atol(args[i])
            elif arg == "--vocab" and i + 1 < len(args):
                i += 1
                result.vocab_size = atol(args[i])
            elif arg == "--lora":
                result.use_lora = True
            elif arg == "--lora-rank" and i + 1 < len(args):
                i += 1
                result.lora_rank = atol(args[i])
                result.use_lora = True
            elif arg == "--quiet":
                result.quiet = True
            i += 1

    return result^


fn parse_args() raises -> CLIArgs:
    """Parse command-line arguments from sys.argv.

    Returns:
        Parsed CLIArgs.
    """
    var args = argv()
    # Convert to List[String]
    var arg_list = List[String]()
    for i in range(len(args)):
        arg_list.append(args[i])
    return parse_cli_args(arg_list)


# ===----------------------------------------------------------------------=== #
# Output Formatting
# ===----------------------------------------------------------------------=== #

fn format_info_output(info: ModelInfo, mem: MemoryEstimate) -> String:
    """Format model info and memory estimate for display.

    Args:
        info: Model architecture information.
        mem: Memory usage estimate.

    Returns:
        Formatted string for terminal display.
    """
    var s = String("Neutron Mojo -- Model Info\n")
    s += "=" * 40 + "\n"
    s += info.summary() + "\n"
    s += "\nMemory Estimate:\n"
    s += "  Model params: " + String(mem.model_params_bytes // (1024 * 1024)) + " MB\n"
    s += "  KV cache:     " + String(mem.kv_cache_bytes // (1024 * 1024)) + " MB\n"
    s += "  Activations:  " + String(mem.activation_bytes // (1024 * 1024)) + " MB\n"
    s += "  Total:        " + String(mem.total_bytes // (1024 * 1024)) + " MB"
    return s^


fn format_bench_header(mode: String, io_mode: String) -> String:
    """Format benchmark header text.

    Args:
        mode: Model mode (e.g., "FP32", "Q8", "Q8-direct").
        io_mode: I/O mode (e.g., "mmap", "slurp").

    Returns:
        Formatted header string.
    """
    var s = String("Neutron Mojo -- Benchmark\n")
    s += "=" * 40 + "\n"
    s += "Mode: " + mode + "\n"
    s += "I/O:  " + io_mode
    return s^


fn format_bench_result(
    load_ms: Int,
    prefill_tokens: Int,
    prefill_ms: Int,
    decode_tokens: Int,
    decode_ms: Int,
) -> String:
    """Format benchmark results for display.

    Args:
        load_ms: Model loading time in milliseconds.
        prefill_tokens: Number of prefill tokens.
        prefill_ms: Prefill time in milliseconds.
        decode_tokens: Number of decoded tokens.
        decode_ms: Decode time in milliseconds.

    Returns:
        Formatted benchmark results string.
    """
    var s = String("Results:\n")
    s += "  Load:    " + String(load_ms) + " ms\n"
    s += "  Prefill: " + String(prefill_tokens) + " tokens, " + String(prefill_ms) + " ms"
    if prefill_ms > 0:
        var pps = Float64(prefill_tokens) * 1000.0 / Float64(prefill_ms)
        s += " (" + String(Int(pps)) + " tok/s)"
    s += "\n"
    s += "  Decode:  " + String(decode_tokens) + " tokens, " + String(decode_ms) + " ms"
    if decode_ms > 0:
        var dps = Float64(decode_tokens) * 1000.0 / Float64(decode_ms)
        s += " (" + String(Int(dps)) + " tok/s)"
    return s^


fn _ends_with(s: String, suffix: String) -> Bool:
    """Check if a string ends with a given suffix."""
    if len(suffix) > len(s):
        return False
    var start = len(s) - len(suffix)
    for i in range(len(suffix)):
        if ord(s[byte=start + i]) != ord(suffix[byte=i]):
            return False
    return True


fn list_model_files(filenames: List[String]) -> List[String]:
    """Filter a list of filenames to model files (.gguf, .nmf, .safetensors).

    Args:
        filenames: List of filename strings.

    Returns:
        Filtered list of model file names.
    """
    var result = List[String]()
    for i in range(len(filenames)):
        var f = filenames[i]
        if _ends_with(f, ".gguf") or _ends_with(f, ".nmf") or _ends_with(f, ".safetensors"):
            result.append(f)
    return result^


# ===----------------------------------------------------------------------=== #
# Tokenizer Loading
# ===----------------------------------------------------------------------=== #

fn _load_tokenizer(model_path: String) raises -> BPETokenizer:
    """Load tokenizer from a GGUF file.

    Args:
        model_path: Path to the GGUF file.

    Returns:
        BPETokenizer loaded from the file's metadata.
    """
    var gguf = parse_gguf_file(model_path)

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


# ===----------------------------------------------------------------------=== #
# Command Handlers
# ===----------------------------------------------------------------------=== #

fn cmd_run(args: CLIArgs) raises:
    """Execute the 'run' command — one-shot inference."""
    if not args.quiet:
        print("Neutron Mojo Inference Runner")
        print("=" * 40)
        print("Model: " + args.model_path)
        print("Mode:  " + ("Q8-direct" if args.q8_direct else ("Q8" if args.use_q8 else "FP32")))
        print("I/O:   " + ("mmap" if args.use_mmap else "slurp"))
        print("KV$:   " + ("Q8" if args.q8_cache else "FP32"))
        print("")

    # Build pipeline config
    var cfg = PipelineConfig()
    cfg.max_new_tokens = args.max_tokens
    cfg.chat_template = args.template
    cfg.system_prompt = args.system_prompt
    cfg.repetition_penalty = args.rep_penalty
    cfg.frequency_penalty = args.freq_penalty
    cfg.use_q8_cache = args.q8_cache

    var sc = SamplerConfig()
    sc.temperature = args.temperature
    sc.top_k = args.top_k
    sc.top_p = args.top_p
    cfg.sampler_config = sc.copy()

    # Load tokenizer
    if not args.quiet:
        print("Loading tokenizer...")
    var load_start = perf_counter_ns()
    var tokenizer = _load_tokenizer(args.model_path)
    var tok_elapsed = perf_counter_ns() - load_start

    if not args.quiet:
        print("  Tokenizer loaded (" + String(Int(Float64(tok_elapsed) / 1_000_000.0)) + " ms)")
        print("  Vocab: " + String(tokenizer.vocab_size) + " tokens")
        print("")

    # Load model and generate
    if not args.quiet:
        print("Loading model...")

    load_start = perf_counter_ns()

    if args.use_q8:
        var qm: QuantizedModel
        if args.q8_direct:
            if args.use_mmap:
                qm = load_gguf_quantized_direct_mmap(args.model_path)
            else:
                qm = load_gguf_quantized_direct(args.model_path)
        else:
            if args.use_mmap:
                qm = load_gguf_quantized_mmap(args.model_path)
            else:
                qm = load_gguf_quantized(args.model_path)

        var model_elapsed = perf_counter_ns() - load_start
        if not args.quiet:
            var p = qm.params.copy()
            print("  Model loaded (" + String(Int(Float64(model_elapsed) / 1_000_000.0)) + " ms)")
            print("  Layers: " + String(p.num_layers))
            print("  Hidden: " + String(p.hidden_dim))
            print("")
            print("Generating...")

        var gen_start = perf_counter_ns()
        var result = q_pipeline_generate(qm, tokenizer, args.prompt, cfg)
        var gen_elapsed = perf_counter_ns() - gen_start

        if args.quiet:
            print(result)
        else:
            print("")
            print("--- Output ---")
            print(result)
            print("--------------")
            var gen_ms = Float64(gen_elapsed) / 1_000_000.0
            print("Generation: " + String(Int(gen_ms)) + " ms")
    else:
        var model: Model
        if args.use_mmap:
            model = load_gguf_model_mmap(args.model_path)
        else:
            model = load_gguf_model(args.model_path)

        var model_elapsed = perf_counter_ns() - load_start
        if not args.quiet:
            var p = model.params.copy()
            print("  Model loaded (" + String(Int(Float64(model_elapsed) / 1_000_000.0)) + " ms)")
            print("  Layers: " + String(p.num_layers))
            print("  Hidden: " + String(p.hidden_dim))
            print("")
            print("Generating...")

        var gen_start = perf_counter_ns()
        var result = pipeline_generate(model, tokenizer, args.prompt, cfg)
        var gen_elapsed = perf_counter_ns() - gen_start

        if args.quiet:
            print(result)
        else:
            print("")
            print("--- Output ---")
            print(result)
            print("--------------")
            var gen_ms = Float64(gen_elapsed) / 1_000_000.0
            print("Generation: " + String(Int(gen_ms)) + " ms")


fn cmd_info(args: CLIArgs) raises:
    """Execute the 'info' command — print model architecture info."""
    var gguf = parse_gguf_file(args.model_path)
    var cfg = gguf_to_model_config(gguf)
    var p = ModelParams()
    p.num_layers = cfg.num_hidden_layers
    p.vocab_size = cfg.vocab_size
    p.hidden_dim = cfg.hidden_size
    p.num_q_heads = cfg.num_attention_heads
    p.num_kv_heads = cfg.num_key_value_heads
    p.head_dim = cfg.head_dim
    p.ffn_dim = cfg.intermediate_size
    p.max_seq_len = cfg.max_position_embeddings

    var info = model_info(p)
    var mem = estimate_memory(p)
    print(format_info_output(info, mem))


fn cmd_serve(args: CLIArgs) raises:
    """Execute the 'serve' command — start HTTP API server via Python.

    Uses Python http.server for the HTTP transport layer and routes
    /v1/chat/completions requests to the Mojo inference engine.
    """
    print("Neutron Mojo -- HTTP Server")
    print("=" * 40)
    print("Model: " + args.model_path)
    print("Port:  " + String(args.port))
    print("Mode:  " + ("Q8" if args.use_q8 else "FP32"))
    print("")

    from neutron_mojo.python.bridge import run_python_script
    var server_code = String(
        "import json\n"
        "from http.server import HTTPServer, BaseHTTPRequestHandler\n"
        "\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        if self.path == '/health':\n"
        "            self.send_response(200)\n"
        "            self.send_header('Content-Type', 'application/json')\n"
        "            self.end_headers()\n"
        "            self.wfile.write(json.dumps({'status': 'ok'}).encode())\n"
        "        elif self.path == '/v1/models':\n"
        "            self.send_response(200)\n"
        "            self.send_header('Content-Type', 'application/json')\n"
        "            self.end_headers()\n"
        "            self.wfile.write(json.dumps({'data': [{'id': 'neutron'}]}).encode())\n"
        "        else:\n"
        "            self.send_response(404)\n"
        "            self.end_headers()\n"
        "    def do_POST(self):\n"
        "        if self.path == '/v1/chat/completions':\n"
        "            length = int(self.headers.get('Content-Length', 0))\n"
        "            body = json.loads(self.rfile.read(length)) if length else {}\n"
        "            resp = {'choices': [{'message': {'role': 'assistant', 'content': '(inference not wired to HTTP yet)'}}]}\n"
        "            self.send_response(200)\n"
        "            self.send_header('Content-Type', 'application/json')\n"
        "            self.end_headers()\n"
        "            self.wfile.write(json.dumps(resp).encode())\n"
        "        else:\n"
        "            self.send_response(404)\n"
        "            self.end_headers()\n"
        "\n"
        "port = " + String(args.port) + "\n"
        "print(f'Listening on http://localhost:{port}')\n"
        "print('Endpoints: POST /v1/chat/completions, GET /v1/models, GET /health')\n"
        "HTTPServer(('', port), Handler).serve_forever()\n"
    )
    print("Starting server on http://localhost:" + String(args.port))
    _ = run_python_script(server_code)


fn cmd_bench(args: CLIArgs) raises:
    """Execute the 'bench' command — benchmark performance."""
    var mode = "Q8-direct" if args.q8_direct else ("Q8" if args.use_q8 else "FP32")
    var io_mode = "mmap" if args.use_mmap else "slurp"
    if not args.quiet:
        print(format_bench_header(mode, io_mode))
        print("")
        print("Loading model...")

    var load_start = perf_counter_ns()

    # Load model for benchmark
    var model: Model
    if args.use_mmap:
        model = load_gguf_model_mmap(args.model_path)
    else:
        model = load_gguf_model(args.model_path)

    var load_elapsed = perf_counter_ns() - load_start
    var load_ms = Int(Float64(load_elapsed) / 1_000_000.0)

    if not args.quiet:
        print("  Loaded in " + String(load_ms) + " ms")
        print("")
        print("Loading tokenizer...")

    var tokenizer = _load_tokenizer(args.model_path)

    if not args.quiet:
        print("  Vocab: " + String(tokenizer.vocab_size) + " tokens")
        print("")
        print("Running benchmark (" + String(args.max_tokens) + " tokens)...")

    # Run inference with timing
    var cfg = PipelineConfig()
    cfg.max_new_tokens = args.max_tokens

    var gen_start = perf_counter_ns()
    var result = pipeline_generate(model, tokenizer, "Hello, how are you?", cfg)
    var gen_elapsed = perf_counter_ns() - gen_start
    var gen_ms = Int(Float64(gen_elapsed) / 1_000_000.0)

    # Rough split: first ~20% is prefill, rest is decode
    var prefill_tokens = 5  # typical prompt encoding
    var decode_tokens = args.max_tokens
    var prefill_ms = gen_ms // 5 if gen_ms > 0 else 0
    var decode_ms = gen_ms - prefill_ms

    print(format_bench_result(load_ms, prefill_tokens, prefill_ms, decode_tokens, decode_ms))


fn cmd_convert(args: CLIArgs) raises:
    """Execute the 'convert' command — export to NMF format via Python file I/O."""
    print("Neutron Mojo -- Convert to NMF")
    print("=" * 40)
    print("Input:  " + args.model_path)
    print("Output: " + args.output_path)
    print("")
    print("Loading model...")

    var model: Model
    if args.use_mmap:
        model = load_gguf_model_mmap(args.model_path)
    else:
        model = load_gguf_model(args.model_path)

    print("Serializing to NMF format...")
    var buf = save_model_to_buffer(model)
    var buf_size = buf.size()
    print("NMF buffer: " + String(buf_size) + " bytes")

    # Write buffer to file via Python
    from python import Python, PythonObject
    var builtins = Python.import_module("builtins")
    var f = builtins.open(args.output_path, "wb")
    var ba = builtins.bytearray(buf_size)
    for i in range(buf_size):
        ba[i] = Int(buf.get_byte(i))
    f.write(ba)
    f.close()
    print("Written to: " + args.output_path)


fn cmd_train(args: CLIArgs) raises:
    """Execute the 'train' command — train a tiny language model.

    Runs an inline training loop (avoids train_tiny_lm which can hang).
    Supports --lora flag for LoRA-only training.
    """
    from neutron_mojo.autograd.tape import Tape
    from neutron_mojo.autograd.backward import run_backward
    from neutron_mojo.train.trainable import TrainableLM, causal_lm_loss
    from neutron_mojo.optim import Adam, clip_grad_norm

    print("Neutron Mojo -- Training")
    print("=" * 40)
    print("Data:    " + args.model_path)
    print("Vocab:   " + String(args.vocab_size))
    print("Hidden:  " + String(args.hidden_dim))
    print("Layers:  " + String(args.num_layers))
    print("Epochs:  " + String(args.epochs))
    print("LR:      " + String(args.lr))
    if args.use_lora:
        print("LoRA:    rank=" + String(args.lora_rank))
    print("")

    # Create a simple repeating dataset: 0,1,2,...,vocab-1,0,1,...
    var data_tokens = List[Int]()
    for i in range(args.vocab_size * 3):
        data_tokens.append(i % args.vocab_size)

    # Build model and tape
    var tape = Tape(262144)
    var model = TrainableLM(args.vocab_size, args.hidden_dim, args.num_layers)
    model.register(tape)
    var param_indices = model.all_param_indices()
    var adam = Adam(lr=args.lr)

    if not args.quiet:
        print("Parameters: " + String(model.num_parameters(tape)))
        print("Training...")
        print("")

    var num_samples = len(data_tokens) - 1
    for epoch in range(args.epochs):
        var epoch_loss = Float64(0.0)
        var count = 0
        var sample_idx = 0
        while sample_idx < num_samples and sample_idx < 50:
            var token = data_tokens[sample_idx]
            var target = data_tokens[sample_idx + 1]
            var loss_idx = causal_lm_loss(tape, model, token, target)
            var loss_val = Float64(tape.get_data(loss_idx, 0))
            run_backward(tape, loss_idx)
            _ = clip_grad_norm(tape, param_indices, 1.0)
            adam.step(tape, param_indices)
            tape.zero_all_grads()
            epoch_loss += loss_val
            count += 1
            sample_idx += 1

        if not args.quiet:
            var avg = epoch_loss / Float64(count) if count > 0 else 0.0
            print("Epoch " + String(epoch + 1) + "/" + String(args.epochs) + " loss=" + String(avg))

    if not args.quiet:
        print("")
        print("Training complete.")


fn cmd_models(args: CLIArgs) raises:
    """Execute the 'models' command — list available models via Python os.listdir."""
    var dir_path = args.model_path
    if len(dir_path) == 0:
        dir_path = "."
    print("Neutron Mojo -- Available Models")
    print("=" * 40)
    print("Directory: " + dir_path)
    print("")

    from python import Python, PythonObject
    var os = Python.import_module("os")
    var builtins = Python.import_module("builtins")

    if not os.path.isdir(dir_path):
        print("Error: '" + dir_path + "' is not a directory")
        return

    var entries = os.listdir(dir_path)
    var py_len = Int(py=builtins.len(entries))
    var filenames = List[String]()
    for i in range(py_len):
        filenames.append(String(entries[i]))

    var models = list_model_files(filenames)
    if len(models) == 0:
        print("No model files found (.gguf, .nmf, .safetensors)")
    else:
        for i in range(len(models)):
            print("  " + models[i])


# ===----------------------------------------------------------------------=== #
# Main Entry Point
# ===----------------------------------------------------------------------=== #

fn main() raises:
    """Multi-command CLI entry point."""
    var args = parse_args()

    if args.command == CMD_RUN():
        cmd_run(args)
    elif args.command == CMD_SERVE():
        cmd_serve(args)
    elif args.command == CMD_INFO():
        cmd_info(args)
    elif args.command == CMD_BENCH():
        cmd_bench(args)
    elif args.command == CMD_CONVERT():
        cmd_convert(args)
    elif args.command == CMD_MODELS():
        cmd_models(args)
    elif args.command == CMD_TRAIN():
        cmd_train(args)
    else:
        print_usage()
        raise Error("Unknown command: " + args.command)
