import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The AI Bible: Part 1 — Tebian" };
}

export default function AiBiblePart1() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The AI Bible: Part 1</h1>
          <p class="meta">Training the Sovereign Brain: Fine-Tuning Local LLMs on Consumer Hardware.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Difference Between Running and Knowing</h2>
            <p>Running a local LLM (inference) is like reading a book. <strong>Fine-tuning</strong> an LLM is like writing one. Most AI guides stop at "How to chat with Llama 3." Tebian goes further. We provide the tools to teach Llama 3 <em>your</em> code style, <em>your</em> documentation, and <em>your</em> way of thinking.</p>

            <p>This treatise explains the mathematics of <strong>QLoRA (Quantized Low-Rank Adapters)</strong>, a technique that allows you to fine-tune massive 70B parameter models on a single consumer GPU (like an RTX 3090 or 4090). This is the frontier of digital sovereignty.</p>
          </section>

          <section class="qlora-math">
            <h2>1. The Mathematics of QLoRA</h2>
            <p>Training a full model requires updating billions of weights. This usually takes hundreds of gigabytes of VRAM. QLoRA solves this by freezing the main model (in 4-bit quantized mode) and only training a tiny "Adapter" layer on top of it.</p>

            <h3>The Memory Equation</h3>
            <p>With QLoRA, the memory requirement is drastically reduced:</p>
            <ul>
              <li><strong>Base Model (Frozen):</strong> 7B model @ 4-bit = ~5GB VRAM.</li>
              <li><strong>Adapter (Trainable):</strong> 64MB of parameters = ~200MB VRAM.</li>
              <li><strong>Gradients/Optimizer:</strong> ~2GB VRAM.</li>
            </ul>
            <p>Total VRAM: ~8GB. This means you can fine-tune a state-of-the-art model on a standard gaming laptop running Tebian. We provide the <strong>Axolotl</strong> configuration scripts to automate this process.</p>
          </section>

          <section class="dataset-curation">
            <h2>2. Dataset Curation: Garbage In, Garbage Out</h2>
            <p>The secret to a good AI isn't the model; it's the data. Tebian includes tools to convert your existing digital life into a training dataset.</p>
            <ul>
              <li><strong>Git Scraper:</strong> Turn your GitHub repos into `instruction/response` pairs. (e.g., "Write a function to connect to Redis" -&gt; [Your Code]).</li>
              <li><strong>Obsidian/Markdown Parser:</strong> Turn your personal notes into a knowledge graph.</li>
              <li><strong>Chat Log Cleaner:</strong> Sanitize your Signal/Matrix logs to teach the AI your "voice."</li>
            </ul>
            <p>We use <strong>Apache Arrow</strong> format for high-performance data loading, ensuring your GPU isn't waiting on your CPU during training.</p>
          </section>

          <section class="training-loop">
            <h2>3. The Training Loop (Unsloth)</h2>
            <p>Tebian optimizes the training loop using <strong>Unsloth</strong>, a library that rewrites the PyTorch kernels in manual Triton assembly. It makes fine-tuning 2x faster and uses 50% less memory than standard HuggingFace scripts.</p>

            <h3>The Sovereign Workflow</h3>
            <ol>
              <li><strong>Prepare:</strong> Run `tebian-train prepare` to tokenize your dataset.</li>
              <li><strong>Train:</strong> Run `tebian-train start`. Watch the loss curve in real-time via a local TensorBoard instance.</li>
              <li><strong>Merge:</strong> Once trained, merge the adapter back into the base model or keep it separate for runtime loading in Ollama.</li>
            </ol>
          </section>

          <section class="privacy-implication">
            <h2>4. The Privacy Implication</h2>
            <p>Why fine-tune locally? Because if you upload your company's code or your medical records to OpenAI for "fine-tuning," you have lost control of that data. It is now part of their ecosystem.</p>

            <p>When you train locally on Tebian, the weights are yours. The data is yours. The resulting intelligence is a permanent asset that lives on your hard drive. You can copy it to a USB stick, put it in a safe, or deploy it to your air-gapped server. It is <strong>Private Property.</strong></p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: Building the Exocortex</h2>
            <p>The future belongs to those who own their intelligence. By fine-tuning local models, you are building an "Exocortex"—an external extension of your own mind that knows what you know but thinks at the speed of silicon. Tebian is the foundry for this new organ.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
