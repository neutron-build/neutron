import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Unbreakable Web Kiosk — Tebian" };
}

export default function WebKiosk() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Resources</span>
          <h1>The Unbreakable Web Kiosk</h1>
          <p class="meta">Grandma-Proofing your PC: A Zero-Maintenance Browser Experience.</p>
        </header>
        <article class="content">
          <section class="overview">
            <h2>The Strategy</h2>
            <p>Many users don't need a "Desktop Operating System." They only need a **Web Browser**. But traditional OSs (Windows, macOS) are too complex for this simple task. They have pop-ups, update prompts, and thousands of places to click that can lead to a broken system. In Tebian, we can configure a **Web Kiosk** mode that is virtually unbreakable.</p>

            <p>This guide explains how to set up a machine that boots directly into a full-screen, locked-down browser.</p>
          </section>

          <div class="resource-grid">
            <div class="resource-box">
              <h3>1. The "Kiosk" Session</h3>
              <p>Tebian's "Simple Rig" can be configured to start a special Sway session that contains only one window: the web browser. No taskbars. No menus. No exit button.</p>
              <ul>
                <li><strong>Auto-Start:</strong> Boots directly into the browser in under 10 seconds.</li>
                <li><strong>Full-Screen:</strong> The browser fills 100% of the screen. No window borders.</li>
                <li><strong>Locked Down:</strong> Disable keyboard shortcuts like <code>Alt-Tab</code> or <code>Super-D</code> to prevent the user from leaving the browser.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>2. The Choice of Browser</h3>
              <p>For a kiosk, we want stability and privacy. Tebian supports the two most stable browsers in the world.</p>
              <ul>
                <li><strong>Firefox (ESR):</strong> The "Extended Support Release" of Firefox. It only gets security updates, not new features, ensuring it never breaks.</li>
                <li><strong>Chromium (Ungoogled):</strong> The open-source engine behind Chrome, with all the Google telemetry stripped out.</li>
                <li><strong>Hardware Acceleration:</strong> Both are configured to use the GPU for smooth video playback (YouTube/Netflix) on any hardware.</li>
              </ul>
            </div>

            <div class="resource-box">
              <h3>3. Zero-Maintenance Updates</h3>
              <p>A kiosk should never ask the user to "Update." In Tebian, we set up a <strong>Cron Job</strong> that runs <code>apt update && apt upgrade -y</code> in the background at 3:00 AM. The machine stays secure without any human intervention.</p>
              <ul>
                <li><strong>Automatic Reboots:</strong> Optionally reboot once a week at night to apply kernel patches.</li>
                <li><strong>Clean Slate:</strong> Optionally clear the browser cache and history on every reboot for a "fresh" experience.</li>
              </ul>
            </div>

            <div class="resource-box warning">
              <h3>4. The "Safety Net" Partition</h3>
              <p>For those who need a "Rescue" option, Tebian's kiosk setup includes a hidden <strong>Recovery Mode</strong>. By holding a secret key combination during boot, you can enter the standard Tebian desktop to troubleshoot or change settings.</p>
              <ul>
                <li><strong>Secret Key:</strong> A combination of keys (e.g., <code>Shift+Super+R</code>) that only you know.</li>
                <li><strong>Admin Password:</strong> Protect the recovery mode from accidental access.</li>
              </ul>
            </div>
          </div>

          <section class="deep-dive">
            <h2>Why the Kiosk Wins</h2>
            <p>The "Unbreakable Web Kiosk" is the ultimate solution for people who "just want to check their email." By removing the layers of the OS, you remove the chance of user error. It's faster, safer, and completely maintenance-free. It turns a 10-year-old laptop into the world's best Chromebook—without the Google tracking.</p>
          </section>
        </article>
      </main>
    </PageShell>
  );
}
