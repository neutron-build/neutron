import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Local AI Manual — Tebian" };
}

export default function LocalAiManual() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Local AI Manual</h1>
          <p class="meta">Your Private Brain: Ollama and Local LLM Setup.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Most AI tutorials focus on API keys and monthly subscriptions. Tebian's "AI Mode" focuses on your <strong>GPU</strong>. This guide explains how to run Large Language Models (LLMs) like Llama 3 and Mistral locally on your hardware using <strong>Ollama</strong>. No cloud. No telemetry. No censorship.</p>

            <p>We use a C-based runner that talks directly to your CUDA (NVIDIA) or ROCm (AMD) cores, ensuring maximum performance for your private brain.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The Ollama Engine</h3>
              <p>Ollama is a lightweight, C-based runner for LLMs. It handles the quantization and memory management for your models, allowing them to fit into your VRAM.</p>
              <ul>
                <li><strong>Hardware Acceleration:</strong> Auto-detects NVIDIA/AMD GPUs for 100% speed.</li>
                <li><strong>Quantization:</strong> Reduces model size (e.g., 8GB to 4GB) with 99% accuracy.</li>
                <li><strong>REST API:</strong> Allows other apps (like our <code>t-ask</code>) to talk to the model.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The `t-ask` CLI Assistant</h3>
              <p>Tebian includes <code>t-ask</code>, a Go-based (and soon Rust-based) CLI tool that connects your terminal to your local AI. You can summarize files, write code, and answer questions without leaving your shell.</p>
              <ul>
                <li><strong>Piping Support:</strong> <code>cat logs.txt | t-ask "Find the error"</code>.</li>
                <li><strong>System Prompts:</strong> Pre-configured "Developer," "Writer," and "Admin" personas.</li>
                <li><strong>Context Aware:</strong> Remembers your previous questions for a seamless chat experience.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Choosing Your Model</h3>
              <p>Tebian's "AI Menu" provides one-click downloads for the world's most capable local models.</p>
              <ul>
                <li><strong>Llama 3 (Meta):</strong> The current king of open-weight models. Great for general tasks.</li>
                <li><strong>Mistral:</strong> Highly efficient and fast. Perfect for mobile or low-power machines.</li>
                <li><strong>CodeLlama:</strong> Specialized for programming and debugging.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. Memory Management (VRAM)</h3>
              <p>Local AI is memory-intensive. To get the best speed, your model should fit entirely into your GPU's **VRAM**. Tebian's setup script helps you pick the right model size for your hardware.</p>
              <ul>
                <li><strong>4GB VRAM:</strong> Use 3B or smaller models.</li>
                <li><strong>8GB VRAM:</strong> Use 7B or 8B models (Llama 3).</li>
                <li><strong>12GB+ VRAM:</strong> Use 13B models or run multiple small models at once.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why Local AI on Tebian?</h2>
            <p>By running AI as a system utility on a stable Debian base, you turn your computer into a true <strong>Intelligent Workstation</strong>. You aren't just a user of AI; you are the host. It's faster, more private, and completely free. One ISO. One menu. One Private Brain.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
