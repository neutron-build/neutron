#!/usr/bin/env bash
#
# Benchmark: llama.cpp Llama-3-8B
# =================================
# Measures llama.cpp inference performance using llama-bench for both
# quantized (Q4_K_M) and full-precision (F16) models.
#
# Setup:
#   1. Build llama.cpp from source:
#      git clone https://github.com/ggerganov/llama.cpp
#      cd llama.cpp && cmake -B build -DGGML_CUDA=ON && cmake --build build -j
#
#   2. Download GGUF model files:
#      # Q4_K_M quantized (~4.9GB):
#      huggingface-cli download bartowski/Meta-Llama-3-8B-Instruct-GGUF \
#        Meta-Llama-3-8B-Instruct-Q4_K_M.gguf --local-dir ./models
#
#      # F16 full-precision (~16GB):
#      huggingface-cli download bartowski/Meta-Llama-3-8B-Instruct-GGUF \
#        Meta-Llama-3-8B-Instruct-f16.gguf --local-dir ./models
#
#   3. Set environment variables or pass as arguments:
#      LLAMA_BENCH=/path/to/llama.cpp/build/bin/llama-bench
#      MODEL_Q4=/path/to/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf
#      MODEL_F16=/path/to/Meta-Llama-3-8B-Instruct-f16.gguf
#
# Usage:
#   ./llamacpp_llama3.sh [options]
#
# Options:
#   --llama-bench PATH    Path to llama-bench binary
#   --model-q4 PATH       Path to Q4_K_M GGUF model
#   --model-f16 PATH      Path to F16 GGUF model
#   --runs N              Number of runs for median (default: 5)
#   --gpu-layers N        Number of layers to offload to GPU (default: 99, i.e. all)
#   --prompt-lens LENS    Space-separated prompt lengths (default: "128 512 2048")
#   --gen-lens LENS       Space-separated generation lengths (default: "128 512")
#   --skip-f16            Skip F16 benchmarks (saves time/memory)
#
# Metrics:
#   tok/sec - Tokens per second for prompt evaluation and generation.
#
# Dependencies:
#   - llama.cpp built with CUDA support
#   - GGUF model files
#   - Standard Unix tools: bc, awk, sort

set -euo pipefail

# ---- Defaults ----
LLAMA_BENCH="${LLAMA_BENCH:-}"
MODEL_Q4="${MODEL_Q4:-}"
MODEL_F16="${MODEL_F16:-}"
RUNS=5
GPU_LAYERS=99
PROMPT_LENS="128 512 2048"
GEN_LENS="128 512"
SKIP_F16=false

# ---- Parse arguments ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --llama-bench) LLAMA_BENCH="$2"; shift 2 ;;
        --model-q4) MODEL_Q4="$2"; shift 2 ;;
        --model-f16) MODEL_F16="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --gpu-layers) GPU_LAYERS="$2"; shift 2 ;;
        --prompt-lens) PROMPT_LENS="$2"; shift 2 ;;
        --gen-lens) GEN_LENS="$2"; shift 2 ;;
        --skip-f16) SKIP_F16=true; shift ;;
        -h|--help)
            head -50 "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ---- Dependency checks ----
if ! command -v bc &>/dev/null; then
    echo "ERROR: 'bc' is required. Install with your package manager."
    exit 1
fi

if [[ -z "$LLAMA_BENCH" ]]; then
    # Try common locations
    for candidate in \
        "./llama.cpp/build/bin/llama-bench" \
        "$HOME/llama.cpp/build/bin/llama-bench" \
        "$(command -v llama-bench 2>/dev/null || true)"; do
        if [[ -x "$candidate" ]]; then
            LLAMA_BENCH="$candidate"
            break
        fi
    done
fi

if [[ -z "$LLAMA_BENCH" || ! -x "$LLAMA_BENCH" ]]; then
    echo "ERROR: llama-bench not found. Please specify with --llama-bench or LLAMA_BENCH env var."
    echo ""
    echo "Build instructions:"
    echo "  git clone https://github.com/ggerganov/llama.cpp"
    echo "  cd llama.cpp && cmake -B build -DGGML_CUDA=ON && cmake --build build -j"
    exit 1
fi

if [[ -z "$MODEL_Q4" ]]; then
    # Try common locations
    for candidate in \
        "./models/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf" \
        "$HOME/models/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf"; do
        if [[ -f "$candidate" ]]; then
            MODEL_Q4="$candidate"
            break
        fi
    done
fi

if [[ -z "$MODEL_Q4" || ! -f "$MODEL_Q4" ]]; then
    echo "ERROR: Q4_K_M model not found. Please specify with --model-q4 or MODEL_Q4 env var."
    echo ""
    echo "Download with:"
    echo "  huggingface-cli download bartowski/Meta-Llama-3-8B-Instruct-GGUF \\"
    echo "    Meta-Llama-3-8B-Instruct-Q4_K_M.gguf --local-dir ./models"
    exit 1
fi

echo "llama-bench: $LLAMA_BENCH"
echo "Model (Q4_K_M): $MODEL_Q4"
if [[ "$SKIP_F16" == false && -n "$MODEL_F16" && -f "$MODEL_F16" ]]; then
    echo "Model (F16): $MODEL_F16"
fi
echo "Runs per config: $RUNS"
echo "GPU layers: $GPU_LAYERS"
echo "Prompt lengths: $PROMPT_LENS"
echo "Generation lengths: $GEN_LENS"
echo "================================================================================"

# ---- Helper: compute median of an array ----
median() {
    local -a sorted
    IFS=$'\n' sorted=($(sort -g <<< "$*"))
    local n=${#sorted[@]}
    local mid=$(( n / 2 ))
    if (( n % 2 == 1 )); then
        echo "${sorted[$mid]}"
    else
        echo "scale=2; (${sorted[$mid-1]} + ${sorted[$mid]}) / 2" | bc
    fi
}

# ---- Helper: compute standard deviation ----
stddev() {
    local -a vals=("$@")
    local n=${#vals[@]}
    if (( n <= 1 )); then
        echo "0.00"
        return
    fi
    local sum=0
    for v in "${vals[@]}"; do
        sum=$(echo "$sum + $v" | bc -l)
    done
    local mean=$(echo "scale=6; $sum / $n" | bc -l)
    local sq_sum=0
    for v in "${vals[@]}"; do
        local diff=$(echo "$v - $mean" | bc -l)
        sq_sum=$(echo "$sq_sum + ($diff * $diff)" | bc -l)
    done
    echo "scale=2; sqrt($sq_sum / ($n - 1))" | bc -l
}

# ---- Run llama-bench and extract tok/sec ----
# llama-bench outputs CSV-like lines. We parse the tok/sec from its output.
run_bench() {
    local model="$1"
    local quant_label="$2"
    local pp="$3"  # prompt length (0 means skip prompt eval)
    local tg="$4"  # generation length (0 means skip generation)

    local -a pp_results=()
    local -a tg_results=()

    for ((r=1; r<=RUNS; r++)); do
        local output
        output=$("$LLAMA_BENCH" \
            -m "$model" \
            -ngl "$GPU_LAYERS" \
            -p "$pp" \
            -n "$tg" \
            -r 1 \
            2>/dev/null)

        # llama-bench outputs lines like:
        # model | size | ... | test | t/s
        # Parse prompt processing (pp) and text generation (tg) tok/sec
        while IFS= read -r line; do
            if echo "$line" | grep -q "pp[0-9]"; then
                local tps
                tps=$(echo "$line" | awk -F'|' '{print $NF}' | tr -d ' ')
                if [[ -n "$tps" && "$tps" != "t/s" ]]; then
                    pp_results+=("$tps")
                fi
            fi
            if echo "$line" | grep -q "tg[0-9]"; then
                local tps
                tps=$(echo "$line" | awk -F'|' '{print $NF}' | tr -d ' ')
                if [[ -n "$tps" && "$tps" != "t/s" ]]; then
                    tg_results+=("$tps")
                fi
            fi
        done <<< "$output"
    done

    # Report prompt processing results
    if (( ${#pp_results[@]} > 0 )); then
        local med std
        med=$(median "${pp_results[@]}")
        std=$(stddev "${pp_results[@]}")
        echo "BENCHMARK: llamacpp_llama3 | quant: $quant_label | task: prompt_eval | tokens: $pp | tok_sec: $med tok/s (std $std)"
    fi

    # Report text generation results
    if (( ${#tg_results[@]} > 0 )); then
        local med std
        med=$(median "${tg_results[@]}")
        std=$(stddev "${tg_results[@]}")
        echo "BENCHMARK: llamacpp_llama3 | quant: $quant_label | task: generation | tokens: $tg | tok_sec: $med tok/s (std $std)"
    fi
}

# ---- Main benchmark loop ----

# Q4_K_M benchmarks
echo ""
echo "--- Q4_K_M Quantized ---"
for pp in $PROMPT_LENS; do
    for tg in $GEN_LENS; do
        run_bench "$MODEL_Q4" "Q4_K_M" "$pp" "$tg"
    done
done

# F16 benchmarks (optional)
if [[ "$SKIP_F16" == false ]]; then
    if [[ -z "$MODEL_F16" ]]; then
        # Try common locations
        for candidate in \
            "./models/Meta-Llama-3-8B-Instruct-f16.gguf" \
            "$HOME/models/Meta-Llama-3-8B-Instruct-f16.gguf"; do
            if [[ -f "$candidate" ]]; then
                MODEL_F16="$candidate"
                break
            fi
        done
    fi

    if [[ -n "$MODEL_F16" && -f "$MODEL_F16" ]]; then
        echo ""
        echo "--- F16 Full Precision ---"
        for pp in $PROMPT_LENS; do
            for tg in $GEN_LENS; do
                run_bench "$MODEL_F16" "F16" "$pp" "$tg"
            done
        done
    else
        echo ""
        echo "SKIPPED: F16 model not found. Specify with --model-f16 or MODEL_F16 env var."
        echo "Download with:"
        echo "  huggingface-cli download bartowski/Meta-Llama-3-8B-Instruct-GGUF \\"
        echo "    Meta-Llama-3-8B-Instruct-f16.gguf --local-dir ./models"
    fi
fi

echo ""
echo "Done."
