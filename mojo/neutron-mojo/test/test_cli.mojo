# ===----------------------------------------------------------------------=== #
# Neutron Mojo — CLI Unification Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the unified multi-command CLI argument parsing and output formatting."""

from neutron_mojo.cli.inference import (
    CLIArgs,
    parse_cli_args,
    is_known_command,
    format_info_output,
    format_bench_header,
    format_bench_result,
    list_model_files,
)
from neutron_mojo.nn.bench import ModelInfo, MemoryEstimate, model_info, estimate_memory
from neutron_mojo.nn.model import ModelParams


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn contains(haystack: String, needle: String) -> Bool:
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


fn test_known_commands() raises:
    """is_known_command recognizes valid commands."""
    assert_true(is_known_command("run"), "run is known")
    assert_true(is_known_command("serve"), "serve is known")
    assert_true(is_known_command("info"), "info is known")
    assert_true(is_known_command("bench"), "bench is known")
    assert_true(is_known_command("convert"), "convert is known")
    assert_true(is_known_command("models"), "models is known")
    assert_true(not is_known_command("unknown"), "unknown is not known")
    assert_true(not is_known_command(""), "empty is not known")
    print("  known_commands: PASS")


fn test_parse_run_basic() raises:
    """Parse basic 'run' command."""
    var args = List[String]()
    args.append("neutron")
    args.append("run")
    args.append("model.gguf")
    args.append("Hello world")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "run", "Command should be run")
    assert_true(parsed.model_path == "model.gguf", "Model path")
    assert_true(parsed.prompt == "Hello world", "Prompt")
    assert_true(parsed.max_tokens == 128, "Default max_tokens")
    assert_true(parsed.template == "none", "Default template")
    assert_true(parsed.use_q8 == False, "Default no q8")
    assert_true(parsed.use_mmap == False, "Default no mmap")
    print("  parse_run_basic: PASS")


fn test_parse_run_with_options() raises:
    """Parse 'run' with all options."""
    var args = List[String]()
    args.append("neutron")
    args.append("run")
    args.append("llama.gguf")
    args.append("Tell me a joke")
    args.append("--max-tokens")
    args.append("256")
    args.append("--template")
    args.append("llama")
    args.append("--q8-direct")
    args.append("--mmap")
    args.append("--quiet")
    args.append("--q8-cache")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "run", "Command")
    assert_true(parsed.max_tokens == 256, "Max tokens 256")
    assert_true(parsed.template == "llama", "Template llama")
    assert_true(parsed.use_q8 == True, "Q8 enabled by q8-direct")
    assert_true(parsed.q8_direct == True, "Q8 direct")
    assert_true(parsed.use_mmap == True, "Mmap enabled")
    assert_true(parsed.quiet == True, "Quiet enabled")
    assert_true(parsed.q8_cache == True, "Q8 cache enabled")
    print("  parse_run_with_options: PASS")


fn test_parse_serve() raises:
    """Parse 'serve' command with port."""
    var args = List[String]()
    args.append("neutron")
    args.append("serve")
    args.append("model.gguf")
    args.append("--port")
    args.append("3000")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "serve", "Command")
    assert_true(parsed.model_path == "model.gguf", "Model path")
    assert_true(parsed.port == 3000, "Port 3000")
    print("  parse_serve: PASS")


fn test_parse_info() raises:
    """Parse 'info' command."""
    var args = List[String]()
    args.append("neutron")
    args.append("info")
    args.append("model.gguf")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "info", "Command")
    assert_true(parsed.model_path == "model.gguf", "Model path")
    print("  parse_info: PASS")


fn test_parse_convert() raises:
    """Parse 'convert' command with output flag."""
    var args = List[String]()
    args.append("neutron")
    args.append("convert")
    args.append("model.gguf")
    args.append("-o")
    args.append("model.nmf")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "convert", "Command")
    assert_true(parsed.model_path == "model.gguf", "Model path")
    assert_true(parsed.output_path == "model.nmf", "Output path")
    print("  parse_convert: PASS")


fn test_parse_convert_missing_output() raises:
    """Convert without -o should raise."""
    var args = List[String]()
    args.append("neutron")
    args.append("convert")
    args.append("model.gguf")
    var caught = False
    try:
        _ = parse_cli_args(args)
    except:
        caught = True
    assert_true(caught, "Should raise for missing -o")
    print("  parse_convert_missing_output: PASS")


fn test_parse_models() raises:
    """Parse 'models' command with optional directory."""
    var args = List[String]()
    args.append("neutron")
    args.append("models")
    args.append("/path/to/models")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "models", "Command")
    assert_true(parsed.model_path == "/path/to/models", "Directory path")

    # Without directory
    var args2 = List[String]()
    args2.append("neutron")
    args2.append("models")
    var parsed2 = parse_cli_args(args2)
    assert_true(parsed2.command == "models", "Command models")
    assert_true(len(parsed2.model_path) == 0, "No directory specified")
    print("  parse_models: PASS")


fn test_unknown_command_raises() raises:
    """Unknown command should raise error."""
    var args = List[String]()
    args.append("neutron")
    args.append("foobar")
    var caught = False
    try:
        _ = parse_cli_args(args)
    except:
        caught = True
    assert_true(caught, "Should raise for unknown command")
    print("  unknown_command_raises: PASS")


fn test_format_info_output() raises:
    """Format model info output contains key fields."""
    var p = ModelParams()
    p.num_layers = 4
    p.vocab_size = 1000
    p.hidden_dim = 256
    p.num_q_heads = 8
    p.num_kv_heads = 2
    p.head_dim = 32
    p.ffn_dim = 512
    p.max_seq_len = 1024
    var info = model_info(p)
    var mem = estimate_memory(p)
    var output = format_info_output(info, mem)
    assert_true(contains(output, "-- Model Info"), "Has title")
    assert_true(contains(output, "Layers: 4"), "Has layers")
    assert_true(contains(output, "Hidden: 256"), "Has hidden dim")
    assert_true(contains(output, "GQA"), "Has GQA indicator")
    assert_true(contains(output, "Memory Estimate"), "Has memory section")
    assert_true(contains(output, "MB"), "Has MB units")
    print("  format_info_output: PASS")


fn test_format_bench_output() raises:
    """Format benchmark output contains key metrics."""
    var header = format_bench_header("FP32", "slurp")
    assert_true(contains(header, "Benchmark"), "Has title")
    assert_true(contains(header, "FP32"), "Has mode")
    assert_true(contains(header, "slurp"), "Has IO mode")

    var result = format_bench_result(150, 10, 20, 64, 500)
    assert_true(contains(result, "150 ms"), "Has load time")
    assert_true(contains(result, "10 tokens"), "Has prefill tokens")
    assert_true(contains(result, "64 tokens"), "Has decode tokens")
    assert_true(contains(result, "tok/s"), "Has throughput")
    print("  format_bench_output: PASS")


fn test_list_model_files() raises:
    """Filter filenames to model files only."""
    var files = List[String]()
    files.append("model.gguf")
    files.append("config.json")
    files.append("weights.safetensors")
    files.append("README.md")
    files.append("saved.nmf")
    files.append("photo.png")
    var models = list_model_files(files)
    assert_true(len(models) == 3, "Should find 3 model files, got " + String(len(models)))
    assert_true(models[0] == "model.gguf", "First is GGUF")
    assert_true(models[1] == "weights.safetensors", "Second is SafeTensors")
    assert_true(models[2] == "saved.nmf", "Third is NMF")
    print("  list_model_files: PASS")


fn main() raises:
    print("test_cli")
    test_known_commands()
    test_parse_run_basic()
    test_parse_run_with_options()
    test_parse_serve()
    test_parse_info()
    test_parse_convert()
    test_parse_convert_missing_output()
    test_parse_models()
    test_unknown_command_raises()
    test_format_info_output()
    test_format_bench_output()
    test_list_model_files()
    print("All 12 CLI tests passed!")
