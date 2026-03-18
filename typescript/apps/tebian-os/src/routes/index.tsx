import { PageShell } from "../components/PageShell";
import { Island } from "neutron/client";
import { DownloadCounter } from "../components/DownloadCounter";
import { CopyCommand } from "../components/CopyCommand";
import "../styles/home.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Tebian — The Last Operating System", titleTemplate: null };
}

export default function Home() {
  return (
    <PageShell hideNav>
      <main>
        <div class="banner">
          <span>v0.1 — Early Release</span>
        </div>

        <div class="content-wrapper">
          <h1 class="logo">TEBIAN</h1>
          <p class="tagline">Linux, perfected. One ISO. Every machine.</p>

          <div class="download-section">
            <div class="dl-wrapper">
              <a href="https://github.com/tebian-os/tebian/releases/latest/download/tebian-pc.iso" class="dl-main" id="dl-btn">
                <span class="dl-text">Download ISO</span>
                <span class="dl-arch">x86_64 ~800MB</span>
              </a>
            </div>
            <Island component={DownloadCounter} client="load" id="dl-counter" />
          </div>

          <p class="install-hint">Flash &rarr; Boot &rarr; Install</p>

          <div class="features">
            <span class="feature">Desktop</span>
            <span class="feature-sep">&middot;</span>
            <span class="feature">Server</span>
            <span class="feature-sep">&middot;</span>
            <span class="feature">Dual Boot</span>
            <span class="feature-sep">&middot;</span>
            <span class="feature">Gaming Ready</span>
            <span class="feature-sep">&middot;</span>
            <span class="feature">Encrypted</span>
          </div>

          <div class="arm-section">
            <p class="arm-label">Raspberry Pi / ARM Board / Apple Silicon VM?</p>
            <p class="arm-hint">
              Install <a href="https://www.raspberrypi.com/software/operating-systems/" target="_blank">Pi OS Lite</a>,{" "}
              <a href="https://www.armbian.com/download/" target="_blank">Armbian</a>, or{" "}
              <a href="https://www.debian.org/distrib/netinst" target="_blank">Debian ARM64</a>, then run:
            </p>
            <Island component={CopyCommand} client="load" id="copy-cmd" />
          </div>

          <div class="screenshot">
            <img src="/screenshot.png" alt="Tebian Desktop" />
          </div>
        </div>

        <div class="page-footer">
          <a href="/source" class="footer-link">Source</a>
          <p class="quote">"Perfection is achieved not when there is nothing more to add, but when there is nothing left to take away."</p>
          <p class="quote-cite">— Saint-Exup&eacute;ry, <em>Wind, Sand and Stars</em> (1939)</p>
          <a href="https://neutron.build" target="_blank" class="built-on-link">neutron.</a>
        </div>
      </main>
    </PageShell>
  );
}
