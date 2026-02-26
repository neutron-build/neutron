#!/usr/bin/env python3
"""
Benchmark: SGLang Llama-3-8B Inference
========================================
Measures SGLang serving performance for Llama-3-8B at various concurrency levels.
This script starts an SGLang server as a subprocess, sends concurrent requests via
the OpenAI-compatible API, and reports throughput metrics.

Setup:
    1. Install SGLang:
       pip install "sglang[all]"

    2. Make sure you have access to the Llama-3-8B model:
       huggingface-cli login
       huggingface-cli download meta-llama/Meta-Llama-3-8B-Instruct

    3. Ensure sufficient GPU memory (~16GB for FP16).

Configuration:
    --model             Model name or path (default: meta-llama/Meta-Llama-3-8B-Instruct)
    --concurrency       Concurrency levels to test (default: 1 16 64)
    --num_requests      Total requests per concurrency level (default: 64)
    --max_tokens        Max tokens to generate per request (default: 128)
    --prompt_len        Approximate input prompt length in tokens (default: 256)
    --port              Port for SGLang server (default: 8401)
    --runs              Number of full runs per concurrency level (default: 5)
    --server_ready_timeout  Seconds to wait for server startup (default: 300)
    --no_server         Skip server startup (use already-running server)
    --tensor_parallel   Tensor parallel size (default: 1)

Metrics:
    tok/sec   - Output tokens per second (aggregate throughput)
    TTFT      - Time to first token in milliseconds
    throughput - Total requests per second

Dependencies:
    pip install "sglang[all]" requests
"""

import argparse
import concurrent.futures
import json
import statistics
import subprocess
import sys
import time

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with:")
    print("  pip install requests")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Prompt generation
# ---------------------------------------------------------------------------
SAMPLE_PROMPT = (
    "You are a helpful AI assistant. Please write a detailed explanation of "
    "how transformer neural networks work, including the attention mechanism, "
    "positional encoding, and the encoder-decoder architecture. Cover the "
    "mathematical foundations and provide intuitive explanations. "
    "Include discussion of multi-head attention, layer normalization, "
    "and feed-forward networks. Explain why transformers have become the "
    "dominant architecture for natural language processing tasks."
)


def make_prompt(target_len):
    """Repeat the sample prompt to approximate target token length."""
    target_chars = target_len * 4
    repeats = max(1, target_chars // len(SAMPLE_PROMPT))
    return (SAMPLE_PROMPT + " ") * repeats


# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------
def start_sglang_server(model, port, tp_size, timeout):
    """Start an SGLang server subprocess and wait until it's ready."""
    cmd = [
        sys.executable, "-m", "sglang.launch_server",
        "--model-path", model,
        "--port", str(port),
        "--tp-size", str(tp_size),
        "--context-length", "4096",
    ]

    print(f"Starting SGLang server: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Wait for server to become ready
    url = f"http://localhost:{port}/health"
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                print(f"SGLang server ready after {time.time()-start:.1f}s")
                return proc
        except requests.ConnectionError:
            pass
        # Also check if process has died
        if proc.poll() is not None:
            print("ERROR: SGLang server process exited unexpectedly.")
            stdout = proc.stdout.read() if proc.stdout else ""
            if stdout:
                print(f"Server output:\n{stdout[-2000:]}")
            sys.exit(1)
        time.sleep(2)

    proc.terminate()
    proc.wait()
    print("ERROR: SGLang server failed to start within timeout.")
    print("Try running the server manually:")
    print(f"  python -m sglang.launch_server --model-path {model} --port {port}")
    sys.exit(1)


def stop_server(proc):
    """Gracefully stop the server subprocess."""
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Request sending
# ---------------------------------------------------------------------------
def send_request(base_url, model, prompt, max_tokens):
    """Send a single completion request and return timing + token count."""
    url = f"{base_url}/v1/completions"
    payload = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "stream": True,
    }

    t_start = time.perf_counter()
    t_first_token = None
    output_tokens = 0

    try:
        with requests.post(url, json=payload, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if not line:
                    continue
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                if line.startswith("data: "):
                    data = line[6:]
                    if data.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        text = chunk["choices"][0].get("text", "")
                        if text and t_first_token is None:
                            t_first_token = time.perf_counter()
                        if text:
                            output_tokens += 1
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
    except Exception as e:
        return None, None, 0, str(e)

    t_end = time.perf_counter()
    total_time = t_end - t_start
    ttft = (t_first_token - t_start) if t_first_token else total_time

    return total_time, ttft, output_tokens, None


def run_concurrent_requests(base_url, model, prompt, max_tokens, num_requests, concurrency):
    """Send requests at the given concurrency level using threads."""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(send_request, base_url, model, prompt, max_tokens)
            for _ in range(num_requests)
        ]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    return results


# ---------------------------------------------------------------------------
# Benchmark driver
# ---------------------------------------------------------------------------
def benchmark_concurrency(base_url, model, prompt, max_tokens, num_requests, concurrency, runs):
    """Run the benchmark multiple times and collect stats."""
    all_tok_sec = []
    all_ttft = []
    all_rps = []

    for run_idx in range(runs):
        t_wall_start = time.perf_counter()
        results = run_concurrent_requests(
            base_url, model, prompt, max_tokens, num_requests, concurrency
        )
        t_wall_end = time.perf_counter()

        wall_time = t_wall_end - t_wall_start
        total_tokens = sum(r[2] for r in results if r[3] is None)
        ttfts = [r[1] for r in results if r[1] is not None and r[3] is None]
        errors = sum(1 for r in results if r[3] is not None)

        if errors > 0:
            print(f"  Run {run_idx+1}: {errors}/{num_requests} requests failed")

        tok_sec = total_tokens / wall_time if wall_time > 0 else 0
        rps = (num_requests - errors) / wall_time if wall_time > 0 else 0
        median_ttft = statistics.median(ttfts) if ttfts else 0

        all_tok_sec.append(tok_sec)
        all_ttft.append(median_ttft)
        all_rps.append(rps)

    return {
        "tok_sec": (statistics.median(all_tok_sec),
                    statistics.stdev(all_tok_sec) if len(all_tok_sec) > 1 else 0),
        "ttft_ms": (statistics.median(all_ttft) * 1000,
                    statistics.stdev(all_ttft) * 1000 if len(all_ttft) > 1 else 0),
        "rps": (statistics.median(all_rps),
                statistics.stdev(all_rps) if len(all_rps) > 1 else 0),
    }


def main():
    parser = argparse.ArgumentParser(description="SGLang Llama-3-8B Benchmark")
    parser.add_argument("--model", default="meta-llama/Meta-Llama-3-8B-Instruct")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 16, 64])
    parser.add_argument("--num_requests", type=int, default=64)
    parser.add_argument("--max_tokens", type=int, default=128)
    parser.add_argument("--prompt_len", type=int, default=256)
    parser.add_argument("--port", type=int, default=8401)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--server_ready_timeout", type=int, default=300)
    parser.add_argument("--no_server", action="store_true",
                        help="Skip server startup; connect to already-running server")
    parser.add_argument("--tensor_parallel", type=int, default=1)
    args = parser.parse_args()

    base_url = f"http://localhost:{args.port}"
    prompt = make_prompt(args.prompt_len)

    print(f"Model: {args.model}")
    print(f"Prompt length: ~{args.prompt_len} tokens, Max output: {args.max_tokens} tokens")
    print(f"Requests per concurrency level: {args.num_requests}")
    print(f"Runs per config: {args.runs}")
    print("=" * 80)

    server_proc = None
    try:
        if not args.no_server:
            try:
                import sglang  # noqa: F401
            except ImportError:
                print("ERROR: SGLang is required. Install with:")
                print('  pip install "sglang[all]"')
                sys.exit(1)
            server_proc = start_sglang_server(
                args.model, args.port, args.tensor_parallel, args.server_ready_timeout
            )
        else:
            # Verify server is reachable
            try:
                requests.get(f"{base_url}/health", timeout=5)
            except requests.ConnectionError:
                print(f"ERROR: No server running at {base_url}. Start one or remove --no_server.")
                sys.exit(1)

        for conc in args.concurrency:
            actual_requests = max(conc, args.num_requests)
            stats = benchmark_concurrency(
                base_url, args.model, prompt, args.max_tokens, actual_requests, conc, args.runs
            )

            tok_med, tok_std = stats["tok_sec"]
            ttft_med, ttft_std = stats["ttft_ms"]
            rps_med, rps_std = stats["rps"]

            print(f"BENCHMARK: sglang_llama3 | concurrency: {conc} | "
                  f"tok_sec: {tok_med:.1f} tok/s (std {tok_std:.1f}) | "
                  f"ttft: {ttft_med:.1f} ms (std {ttft_std:.1f} ms) | "
                  f"throughput: {rps_med:.2f} req/s (std {rps_std:.2f})")

    finally:
        if server_proc is not None:
            print("\nStopping SGLang server...")
            stop_server(server_proc)


if __name__ == "__main__":
    main()
