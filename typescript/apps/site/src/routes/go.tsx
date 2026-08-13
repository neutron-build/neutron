import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return {
    title: "Go - Neutron",
    description: "Neutron's Go HTTP framework and modular Nucleus client.",
  };
}

export default function GoPage() {
  return <LanguageOverview
    name="Go"
    description="An HTTP framework and modular Nucleus client built around Go's standard library."
    accent="var(--accent-go)"
    accentRgb="0, 173, 216"
    docsHref="/docs/go/overview"
    quickstartHref="/docs/go/quickstart"
    facts={[
      { label: "HTTP", value: "Go 1.22+ net/http ServeMux with composable route groups" },
      { label: "Data", value: "SQL in the core client; other Nucleus models are optional packages" },
      { label: "Contract", value: "RFC 7807 errors, standard middleware order, health checks, and graceful shutdown" },
      { label: "Module", value: "github.com/neutron-build/neutron-go" },
    ]}
  />;
}
