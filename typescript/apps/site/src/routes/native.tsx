import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Native - Neutron",
    description: "React Native components, routing, styling, and tooling for iOS and Android.",
  };
}

export default function NativePage() {
  return <ProductOverview
    title="Neutron Native"
    description="A React Native workspace for shared iOS and Android components, navigation, styling, and native modules."
    category="platform"
    status="in-progress"
    accent="var(--accent-native)"
    accentRgb="97, 218, 251"
    facts={[
      { label: "Runtime", value: "React Native 0.76 with React-compatible component APIs" },
      { label: "Packages", value: "Core components, router, navigation, platform APIs, and TurboModules" },
      { label: "Styling", value: "Optional build-time className-to-StyleSheet transforms" },
      { label: "Tooling", value: "Separate native CLI for development, builds, iOS, and Android" },
    ]}
    note="Native is implemented in the monorepo and remains under active development. Check the package and platform requirements before adopting it."
    links={[
      { label: "Documentation", href: "/docs/native/overview" },
      { label: "Components", href: "/docs/native/components" },
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/native" },
    ]}
  />;
}
