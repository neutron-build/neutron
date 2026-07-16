import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Mojo - Neutron",
    description: "Mojo ML library with SIMD kernels, five quantization formats, and a full inference and training stack. GPU kernels that read like Python. 125 test suites.",
  };
}

export default function MojoPage() {
  return (
    <ProductPage
      title="Neutron Mojo"
      description="GPU kernels, quantized inference, and a training stack in a language that reads like Python and runs like CUDA. Preview-shipped today, stable when Mojo 1.0 lands."
      category="language"
      status="in-progress"
      accent="var(--accent-mojo)"
      heroAccentRgb="168, 85, 247"
      heroTagline="GPU kernels that speak Python."
      stats={[
        { value: 'SIMD', label: 'Kernels' },
        { value: '5', label: 'Quantization Formats' },
        { value: '125', label: 'Test Suites' },
        { value: 'Tensor', label: 'First-Class Type' },
      ]}
    >
      <section>
        <h2>GPU code that a Python programmer can read.</h2>
        <p>Mojo is Modular's language designed to be a superset of Python with the ergonomics of CPython and the speed of CUDA. Neutron Mojo is the ML library on top: SIMD-accelerated kernels, five quantization formats (int4, int8, fp8, fp16, bf16), a tensor type you can differentiate through, and an inference pipeline that doesn't assume you brought PyTorch with you.</p>
        <p>This is a preview. Mojo itself is pre-1.0, so the surface may shift when the language stabilizes. We ship against the current stable Mojo release and bump versions deliberately.</p>
      </section>

      <CodeBlock filename="kernel/gemm.mojo" annotation="SIMD GEMM kernel. Vectorized at comptime, tiled for L1.">
        <pre><code>{`from neutron.tensor import Tensor, DType
from neutron.simd import vectorize, tile

fn gemm[
    dtype: DType, M: Int, N: Int, K: Int
](C: Tensor[dtype, M, N], A: Tensor[dtype, M, K], B: Tensor[dtype, K, N]):
    @parameter
    fn row(m: Int):
        @parameter
        fn col[nelts: Int](n: Int):
            var acc = SIMD[dtype, nelts](0)
            for k in range(K):
                acc += A[m, k] * B[k, n:n+nelts]
            C[m, n:n+nelts] = acc
        vectorize[col, simd_width[dtype]()](N)
    tile[row](M, tile_size=64)`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="168, 85, 247">
        <div class="feature-card">
          <div class="feature-card__title">SIMD kernels</div>
          <div class="feature-card__desc">Vectorized matmul, softmax, layernorm, rotary embeddings, KV cache, attention. All parameterized on dtype and tile size, monomorphized at comptime.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Five quant formats</div>
          <div class="feature-card__desc">int4, int8, fp8 (e4m3 + e5m2), fp16, bf16. Packed and unpacked kernels for each. Mix formats per layer for optimal accuracy/size tradeoff.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Tensor as a type</div>
          <div class="feature-card__desc">Shape, dtype, and device are part of the type. Shape mismatches fail to compile. No runtime shape checks on the hot path.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Inference pipeline</div>
          <div class="feature-card__desc">Load a GGUF or safetensors file, select a quant format, serve. Streaming token generation with paged KV cache.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Training stack</div>
          <div class="feature-card__desc">Autodiff, Adam/AdamW/Lion optimizers, gradient accumulation, mixed-precision training. Enough to fine-tune small models locally.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">125 test suites</div>
          <div class="feature-card__desc">Each kernel verified against a reference NumPy implementation. Numeric tolerance asserted per dtype.</div>
        </div>
      </FeatureGrid>

      <BenchmarkBars
        title="Quantization formats"
        bars={[
          { label: 'int4', value: 'Packed, 2× density vs int8', width: 50, color: '#A855F7' },
          { label: 'int8', value: 'Symmetric and asymmetric', width: 60, color: '#B775F9' },
          { label: 'fp8', value: 'e4m3 + e5m2', width: 75, color: '#C693FB' },
          { label: 'fp16', value: 'Half precision, IEEE 754', width: 85, color: '#D4B1FC' },
          { label: 'bf16', value: 'Brain float, training favorite', width: 95, color: '#E2CFFD' },
        ]}
      />

      <section>
        <h2>Preview, then stable.</h2>
        <p>Mojo is still pre-1.0; the language is evolving every release. Neutron Mojo tracks the stable branch and bumps deliberately when breaking changes land. Expect surface changes until Mojo 1.0 ships &mdash; after that, we commit to semver.</p>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>Model inference on the same machine as your application. Fine-tuning small models on customer data without an external GPU service. SIMD-heavy data transforms that outgrew NumPy. Anywhere you'd reach for CUDA C++ but would rather keep reading Python.</p>

        <h3>Why Mojo?</h3>
        <p>Because it's Python-shaped but compiles through MLIR to the same codegen path as CUDA. Because <code>@parameter</code> and <code>vectorize</code> replace a thousand lines of C++ templates. Because the same kernel definition runs on CPU SIMD, GPU, and TPU with no per-target rewrite.</p>

        <h3>Part of a bigger system</h3>
        <p>Train or fine-tune in Neutron Mojo. Expose the model through Neutron Python's MCP server. Consume from the edge in Neutron TypeScript. Persist training runs, metrics, and model artifacts in Nucleus &mdash; one database, one contract, whether you're shipping a web app or an inference service.</p>
      </section>
    </ProductPage>
  );
}
