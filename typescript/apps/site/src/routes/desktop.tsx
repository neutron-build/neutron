import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Desktop - Neutron",
    description: "Desktop apps with Tauri 2.0 and Nucleus embedded. ~10 MB bundles using the system WebView, Rust backend, 12 typed plugins, and a neutron:// protocol bridge.",
  };
}

export default function DesktopPage() {
  return (
    <ProductPage
      title="Neutron Desktop"
      description="Tauri 2.0 with Neutron Rust underneath. System WebView for UI, embedded Nucleus for data, 12 typed plugins for OS integration. ~10 MB bundles, Rust safety, no Chromium."
      category="platform"
      status="in-progress"
      accent="var(--accent-ts)"
      heroAccentRgb="107, 107, 107"
      heroTagline="Desktop apps that don't ship a browser."
      stats={[
        { value: '~10 MB', label: 'Typical Bundle' },
        { value: '12', label: 'Typed Plugins' },
        { value: 'Tauri 2.0', label: 'Runtime' },
        { value: 'Embedded', label: 'Nucleus' },
      ]}
    >
      <section>
        <h2>Electron wrote a browser into your install. Don't.</h2>
        <p>Electron ships a 120 MB Chromium bundle, idles at 200 MB of RAM, and runs your backend on Node. Neutron Desktop uses Tauri 2.0 with your system's native WebView, a Rust backend on the same Neutron framework you'd use for the web, and an embedded Nucleus instance so your app has a real database without requiring one to be running.</p>
      </section>

      <CodeBlock filename="src-tauri/src/main.rs" annotation="Rust backend + Nucleus embedded + typed neutron:// bridge.">
        <pre><code>{`use neutron_desktop::{App, Nucleus};
use neutron::prelude::*;

#[tauri::command]
async fn search(q: String, db: State<'_, NucleusClient>) -> Result<Vec<Hit>, String> {
    let hits = db.vector()
        .search("docs", &embed(&q))
        .k(10)
        .execute()
        .await
        .map_err(|e| e.to_string())?;
    Ok(hits)
}

fn main() {
    App::builder()
        .embedded_nucleus()
        .plugin(neutron_desktop::tray())
        .plugin(neutron_desktop::hotkey())
        .plugin(neutron_desktop::autostart())
        .invoke_handler(tauri::generate_handler![search])
        .run(tauri::generate_context!())
        .expect("failed to start");
}`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="107, 107, 107">
        <div class="feature-card">
          <div class="feature-card__title">System WebView</div>
          <div class="feature-card__desc">WebKit on macOS, WebView2 on Windows, WebKitGTK on Linux. Already installed, already updated, already patched.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Rust backend</div>
          <div class="feature-card__desc">The Neutron Rust framework powers the desktop backend. Memory-safe, no GC pauses, one binary.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Embedded Nucleus</div>
          <div class="feature-card__desc">Ship your app with Nucleus linked in. All 14 data models, persistent across restarts, zero network hops, no server to install.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">12 typed plugins</div>
          <div class="feature-card__desc">Clipboard, shell, tray, hotkeys, autostart, window-state, deep-link, biometrics, updater, notifications, file-system, store.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">neutron:// bridge</div>
          <div class="feature-card__desc">Custom protocol handler for safe file access and IPC. Typed commands from the WebView, zero-copy where possible.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Auto-updates</div>
          <div class="feature-card__desc">Signed deltas delivered from your server, applied without a reinstall. Rollback support on failure.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['Metric', 'Electron', 'Neutron Desktop']}
        rows={[
          ['Typical bundle', '~120 MB', '~10 MB'],
          ['Memory (idle)', '~200 MB', '~30–50 MB'],
          ['Backend', 'Node.js', 'Rust (same Neutron framework)'],
          ['Browser engine', 'Bundled Chromium', 'System WebView'],
          ['Security posture', 'Node.js CVEs', 'Rust memory safety'],
          ['Database', 'BYO', 'Nucleus embedded, 14 models'],
        ]}
        highlightColumn={2}
        accentRgb="107, 107, 107"
      />

      <BenchmarkBars
        title="Why Tauri 2.0"
        bars={[
          { label: 'Bundle', value: '~10× smaller than Electron', width: 95, color: '#6B6B6B' },
          { label: 'Memory', value: '4–6× less at idle', width: 85, color: '#858585' },
          { label: 'Backend', value: 'Rust, memory-safe, no GC pauses', width: 80, color: '#9E9E9E' },
          { label: 'Updates', value: 'System WebView patches itself', width: 72, color: '#B8B8B8' },
          { label: 'IPC', value: 'Typed commands, zero-copy', width: 68, color: '#D1D1D1' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Developer tools (Nucleus Studio itself is built on this stack). Productivity apps where binary size matters. Offline-first apps where the user owns the data. Internal tools that must run on airgapped networks. Anywhere you'd reach for Electron but the 120 MB install gives you pause.</p>

        <h3>Why embed Nucleus?</h3>
        <p>Because a desktop app with a real database beats a file-format war. You get SQL, vector search, full-text, and time-series out of the box, persisting across restarts without a daemon. When users need to sync, replicate to a Nucleus cluster; when they don't, it's just a file on disk.</p>

        <h3>Part of a bigger system</h3>
        <p>The same Preact components render to web, iOS, Android, and desktop. The same Rust backend compiles to a server, a desktop app, or a Tauri sidecar. Your users get native-feeling apps; your code stays one codebase.</p>
      </section>
    </ProductPage>
  );
}
