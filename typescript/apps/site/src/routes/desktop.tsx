import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Desktop - Neutron",
    description: "Tauri and Preact foundations for Neutron desktop applications.",
  };
}

export default function DesktopPage() {
  return <ProductOverview
    title="Neutron Desktop"
    description="A Tauri 2 and Preact foundation for desktop applications using the system webview."
    category="platform"
    status="in-progress"
    accent="var(--accent-desktop)"
    accentRgb="255, 159, 67"
    facts={[
      { label: "Shell", value: "Tauri 2 Rust backend with a Preact client bridge" },
      { label: "Database", value: "Optional embedded Nucleus feature in the desktop core crate" },
      { label: "Modules", value: "Window state, filesystem, tray, clipboard, notifications, updater, and more" },
      { label: "Commands", value: "The universal CLI exposes desktop dev, build, and preview workflows" },
    ]}
    note="Desktop packages and examples are present in the repository but are still evolving as a platform surface."
    links={[
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/desktop" },
      { label: "CLI", href: "/cli" },
    ]}
  />;
}
