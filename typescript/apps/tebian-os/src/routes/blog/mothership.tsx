import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Mothership — Tebian" };
}

export default function Mothership() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Infrastructure</span>
          <h1>The Mothership: Self-Hosting for the Rest of Us</h1>
          <p class="meta">February 20, 2026 &bull; 15 min read</p>
        </header>
        <article class="content">
          <p class="lead">We've been conditioned to believe that "The Cloud" is a magical place where our data lives for free. It isn't. The Cloud is just someone else's computer, and they are charging you in privacy, autonomy, and subscription fees. It's time to bring the data home.</p>

          <h2>The Centralization Crisis</h2>
          <p>In 2026, our digital lives are fragmented across silos. Your photos are with Google, your passwords are with Apple, and your chats are with Meta. If any of these companies decide to lock your account—or if their servers go down—your digital life ceases to exist. This is a fragile way to live.</p>

          <p>Tebian's "Mothership" philosophy is about creating your own private cloud. A single machine (or a cluster of them) that handles your files, your secrets, and your communication. No monthly fees. No data harvesting. No "Terms of Service" that can change overnight.</p>

          <h2>Why Self-Hosting used to be Hard</h2>
          <p>Historically, self-hosting required a degree in systems administration. You had to manage Linux servers, configure reverse proxies, handle SSL certificates, and pray that an update didn't break your database. It was a full-time hobby, not a tool for regular people.</p>

          <p>Tebian changes the math. We've applied our "C-Level" simplicity to the server stack. Through our <strong>Control Center</strong>, you can deploy the three pillars of digital independence with a single click. We handle the <code>systemd</code> units, the container networking, and the local DNS. You just use the service.</p>

          <h2>The Three Pillars</h2>
          <ol>
            <li><strong>File Sync (Syncthing):</strong> Imagine Dropbox, but without the company. Syncthing synchronizes your folders across your PC, your phone, and your tablet in real-time. It's P2P and encrypted. Your files never touch a central server.</li>
            <li><strong>Secrets (Vaultwarden):</strong> A self-hosted version of Bitwarden. It stores your passwords, 2FA codes, and credit cards in an encrypted vault that <em>only you</em> have the key to. It's fast, secure, and works on every browser.</li>
            <li><strong>Communication (Matrix):</strong> A decentralized chat protocol. It's like Discord or Slack, but you own the server. You can bridge it to Telegram, WhatsApp, and Signal, bringing all your chats into one "Stealth" interface.</li>
          </ol>

          <h2>Performance as Security</h2>
          <p>Why is a Tebian "Mothership" faster than Google Drive? <strong>Latency.</strong> When you save a file on Google Drive, it has to travel across the internet to a data center, be processed, and then sync back. When you use Syncthing on a Tebian machine in your own home, it moves at the speed of your Local Area Network (LAN). It's the difference between 500ms and 5ms.</p>

          <p>By keeping your data local, you aren't just more private; you are more productive. Your computer stops waiting for the internet to catch up.</p>

          <h2>Conclusion: The Home for Your Data</h2>
          <p>Tebian isn't just an OS for your laptop; it's a foundation for your digital life. The Mothership is the "Brain" of your Fleet. It is the place where your data is safe, stable, and truly yours. It's time to stop renting your digital existence and start owning it.</p>
        </article>
      </main>
    </PageShell>
  );
}
