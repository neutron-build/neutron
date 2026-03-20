import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Sovereignty — Tebian" };
}

export default function Sovereignty() {
  return (
    <>
      <main class="post">
        <header>
          <span class="category">Philosophy</span>
          <h1>Digital Sovereignty</h1>
          <p class="meta">February 20, 2026 &bull; 10 min read</p>
        </header>
        <article class="content">
          <p class="lead">If it's on someone else's computer, it isn't yours.</p>

          <h2>The Mothership</h2>
          <p>Tebian comes ready to host your own cloud. We provide pre-configured stacks for the three pillars of digital independence:</p>

          <div class="sov-grid">
            <div class="sov-box">
              <h3>File Sync</h3>
              <p><strong>Syncthing:</strong> P2P, encrypted file synchronization. No central server. No subscription. Just your data on your devices.</p>
            </div>
            <div class="sov-box">
              <h3>Secrets</h3>
              <p><strong>Vaultwarden:</strong> Self-hosted password management. Your keys stay in your vault, under your control.</p>
            </div>
            <div class="sov-box">
              <h3>Communication</h3>
              <p><strong>Matrix:</strong> Secure, decentralized chat. Escape the walled gardens of corporate messaging platforms.</p>
            </div>
          </div>

          <h2>Local Intelligence</h2>
          <p>AI should be a tool for empowerment, not surveillance. Tebian integrates <strong>Ollama</strong> and our custom <code>t-ask</code> CLI assistant to run LLMs (Llama 3, Mistral) 100% locally on your hardware.</p>
          <ul class="sov-list">
            <li><strong>Zero Data Leakage:</strong> Your prompts never leave your machine.</li>
            <li><strong>Offline First:</strong> Work with AI in any environment, no internet required.</li>
            <li><strong>Private Brain:</strong> Use your own documents and data to train a model that only you can access.</li>
          </ul>

          <p>Tebian isn't just an OS; it's a fortress for your digital life. We provide the tools to own your infrastructure from the kernel to the cloud.</p>
        </article>
      </main>
    </>
  );
}
