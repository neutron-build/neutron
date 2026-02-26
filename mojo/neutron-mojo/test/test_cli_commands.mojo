# ===----------------------------------------------------------------------=== #
# Test — Sprint 71: CLI Commands
# ===----------------------------------------------------------------------=== #

"""Tests for CLI argument parsing and command dispatch."""

from math import abs
from testing import assert_true

from neutron_mojo.cli.inference import (
    CLIArgs, parse_cli_args, is_known_command,
    list_model_files, format_info_output, format_bench_result,
    format_bench_header, _ends_with,
    CMD_RUN, CMD_SERVE, CMD_INFO, CMD_BENCH, CMD_CONVERT, CMD_MODELS, CMD_TRAIN,
)
from neutron_mojo.nn.bench import ModelInfo, MemoryEstimate


fn test_parse_run_command() raises:
    """Parse run command with basic args."""
    var args = List[String]()
    args.append("neutron")
    args.append("run")
    args.append("model.gguf")
    args.append("Hello world")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "run", "command = run")
    assert_true(parsed.model_path == "model.gguf", "model_path")
    assert_true(parsed.prompt == "Hello world", "prompt")
    print("PASS: test_parse_run_command")


fn test_parse_run_with_options() raises:
    """Parse run command with all options."""
    var args = List[String]()
    args.append("neutron")
    args.append("run")
    args.append("model.gguf")
    args.append("prompt")
    args.append("--max-tokens")
    args.append("256")
    args.append("--template")
    args.append("llama")
    args.append("--q8")
    args.append("--quiet")
    var parsed = parse_cli_args(args)
    assert_true(parsed.max_tokens == 256, "max_tokens = 256")
    assert_true(parsed.template == "llama", "template = llama")
    assert_true(parsed.use_q8, "use_q8 = True")
    assert_true(parsed.quiet, "quiet = True")
    print("PASS: test_parse_run_with_options")


fn test_parse_serve_command() raises:
    """Parse serve command."""
    var args = List[String]()
    args.append("neutron")
    args.append("serve")
    args.append("model.gguf")
    args.append("--port")
    args.append("9090")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "serve", "command = serve")
    assert_true(parsed.port == 9090, "port = 9090")
    print("PASS: test_parse_serve_command")


fn test_parse_train_command() raises:
    """Parse train command with training-specific args."""
    var args = List[String]()
    args.append("neutron")
    args.append("train")
    args.append("data.txt")
    args.append("--epochs")
    args.append("20")
    args.append("--lr")
    args.append("0.001")
    args.append("--hidden")
    args.append("64")
    args.append("--layers")
    args.append("4")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "train", "command = train")
    assert_true(parsed.epochs == 20, "epochs = 20")
    assert_true(parsed.hidden_dim == 64, "hidden = 64")
    assert_true(parsed.num_layers == 4, "layers = 4")
    print("PASS: test_parse_train_command")


fn test_parse_train_lora() raises:
    """Parse train command with --lora flag."""
    var args = List[String]()
    args.append("neutron")
    args.append("train")
    args.append("data.txt")
    args.append("--lora")
    args.append("--lora-rank")
    args.append("8")
    var parsed = parse_cli_args(args)
    assert_true(parsed.use_lora, "use_lora = True")
    assert_true(parsed.lora_rank == 8, "lora_rank = 8")
    print("PASS: test_parse_train_lora")


fn test_parse_convert_command() raises:
    """Parse convert command."""
    var args = List[String]()
    args.append("neutron")
    args.append("convert")
    args.append("model.gguf")
    args.append("-o")
    args.append("model.nmf")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "convert", "command = convert")
    assert_true(parsed.output_path == "model.nmf", "output = model.nmf")
    print("PASS: test_parse_convert_command")


fn test_is_known_command() raises:
    """Known command detection."""
    assert_true(is_known_command("run"), "run is known")
    assert_true(is_known_command("serve"), "serve is known")
    assert_true(is_known_command("train"), "train is known")
    assert_true(is_known_command("info"), "info is known")
    assert_true(is_known_command("bench"), "bench is known")
    assert_true(is_known_command("convert"), "convert is known")
    assert_true(is_known_command("models"), "models is known")
    assert_true(not is_known_command("unknown"), "unknown not known")
    print("PASS: test_is_known_command")


fn test_list_model_files() raises:
    """Filter model files from a list."""
    var files = List[String]()
    files.append("model.gguf")
    files.append("readme.txt")
    files.append("weights.safetensors")
    files.append("config.json")
    files.append("model.nmf")
    var models = list_model_files(files)
    assert_true(len(models) == 3, "3 model files")
    print("PASS: test_list_model_files")


fn test_ends_with() raises:
    """String suffix checking."""
    assert_true(_ends_with("model.gguf", ".gguf"), ".gguf suffix")
    assert_true(_ends_with("weights.safetensors", ".safetensors"), ".safetensors suffix")
    assert_true(not _ends_with("model.gguf", ".nmf"), "wrong suffix")
    assert_true(not _ends_with("ab", "abc"), "suffix longer than string")
    print("PASS: test_ends_with")


fn test_format_bench_result() raises:
    """Format benchmark result string."""
    var result = format_bench_result(100, 5, 10, 50, 500)
    assert_true(len(result) > 10, "bench result has content")
    print("PASS: test_format_bench_result")


fn test_default_args() raises:
    """Default CLIArgs values."""
    var a = CLIArgs()
    assert_true(a.port == 8080, "default port")
    assert_true(a.max_tokens == 128, "default max_tokens")
    assert_true(a.temperature == 1.0, "default temperature")
    assert_true(not a.use_lora, "default lora = False")
    assert_true(a.lora_rank == 4, "default lora_rank = 4")
    print("PASS: test_default_args")


fn test_parse_models_with_dir() raises:
    """Parse models command with directory argument."""
    var args = List[String]()
    args.append("neutron")
    args.append("models")
    args.append("/path/to/models")
    var parsed = parse_cli_args(args)
    assert_true(parsed.command == "models", "command = models")
    assert_true(parsed.model_path == "/path/to/models", "dir path")
    print("PASS: test_parse_models_with_dir")


fn main() raises:
    print("=== Sprint 71: CLI Commands Tests ===")
    test_parse_run_command()
    test_parse_run_with_options()
    test_parse_serve_command()
    test_parse_train_command()
    test_parse_train_lora()
    test_parse_convert_command()
    test_is_known_command()
    test_list_model_files()
    test_ends_with()
    test_format_bench_result()
    test_default_args()
    test_parse_models_with_dir()
    print("")
    print("All 12 CLI command tests passed!")
