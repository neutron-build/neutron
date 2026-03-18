import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Crypto Bible — Tebian" };
}

export default function CryptoBible() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Definitive Manual</span>
          <h1>The Crypto Bible</h1>
          <p class="meta">Financial Sovereignty: From Cold Storage to Full Node Operations.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Financial Fortress: Be the Bank</h2>
            <p>Most "Crypto Users" are not sovereign. They use centralized exchanges (Coinbase, Binance) or software wallets on "talkative" operating systems (Windows, macOS). If you don't own your keys, and you don't run your own node, you are using someone else's bank. Tebian is the foundation for <strong>Total Financial Independence.</strong></p>

            <p>This manual provides the technical blueprint for securing your assets, running your own clearinghouse, and achieving zero-leakage privacy.</p>
          </section>

          <section class="node-operations">
            <h2>1. The Foundation: Running your own Node</h2>
            <p>A node is your connection to the truth. If you use a wallet that connects to a "public server," you are revealing your IP address and every transaction you've ever made to that server. Tebian's "Mothership" is designed to host your private <strong>Bitcoin (Core)</strong> and <strong>Monero</strong> nodes.</p>

            <h3>Storage and Performance</h3>
            <p>Full nodes require 500GB+ of high-performance storage. We use <strong>XFS</strong> partitions with <code>noatime</code> optimization to handle the massive I/O load during Initial Block Download (IBD). By running your node on a Tebian Mothership, you provide a private endpoint for all your devices (Phone, Laptop, Desktop) via the <strong>T-Link Mesh.</strong></p>
          </section>

          <section class="cold-storage">
            <h2>2. The Fortress: Air-Gapped Cold Storage</h2>
            <p>The "Gold Standard" of security is the <strong>Air-Gap.</strong> A computer that has never, and will never, touch the internet. Tebian is uniquely suited for this due to its 16MB footprint and modularity. You can run Tebian on a Raspberry Pi Zero with <strong>no networking hardware</strong> to sign transactions.</p>

            <h3>The Deterministic Logic (BIP-39)</h3>
            <p>We provide C-based tools to generate <strong>BIP-39 Mnemonic Phrases</strong> from raw entropy (dice rolls). In Tebian's "Fortress Mode," you generate your keys on an air-gapped device, export the "Public Key" (xpub) to your online Tebian workstation, and use <strong>Sparrow Wallet</strong> or <strong>Monero-GUI</strong> to track your balance without ever exposing your private keys.</p>
          </section>

          <section class="privacy-privacy">
            <h2>3. Privacy: Tor, I2P, and CoinJoins</h2>
            <p>Financial activity is the most sensitive data you own. Tebian hardens your node's communication using <strong>Tor Hidden Services (.onion)</strong>. This masks your physical location from the global network. For advanced privacy, we integrate <strong>CoinJoin</strong> (via Whirlpool or Samourai) directly into our "Sovereign Workstation" configurations.</p>

            <ul>
              <li><strong>Zero Leakage:</strong> Tebian's firewall (UFW) is pre-configured to only allow Bitcoin/Monero traffic over the Tor interface.</li>
              <li><strong>I2P Support:</strong> For the Monero network, we provide one-click I2P (Invisible Internet Project) integration for even deeper anonymity.</li>
            </ul>
          </section>

          <section class="amnestic-mode">
            <h2>4. The Amnesic Session: Live-RAM Operations</h2>
            <p>For high-risk operations, Tebian supports <strong>Amnesic Mode.</strong> You boot Tebian from a USB stick into a "RAM-only" session. You perform your transaction, sign it, and shut down the machine. Because the OS lived in RAM, no trace of your activity—no logs, no cache, no secrets—ever hits the hard drive. This is the ultimate defense against physical forensics.</p>
          </section>

          <section class="conclusion">
            <h2>Conclusion: The Sovereign Future</h2>
            <p>The Crypto Bible is about taking back the most important power of all: the power to hold value. By using a C-based, sovereign foundation like Tebian, you remove the "Trusted Third Party" from your life. You are the bank. You are the vault. You are the root. One ISO. One menu. Total sovereignty.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
