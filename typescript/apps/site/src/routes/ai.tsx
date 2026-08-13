import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "AI - Neutron",
    description: "TypeScript model calls, streaming, structured output, embeddings, and tools.",
  };
}

export default function AIPage() {
  return <ProductOverview
    title="Neutron AI"
    description="A TypeScript SDK for text generation, streaming, structured output, embeddings, and tool execution."
    category="tool"
    accent="var(--accent)"
    accentRgb="0, 229, 160"
    facts={[
      { label: "Calls", value: "generateText, streamText, generateObject, and streamObject" },
      { label: "Providers", value: "OpenAI and Anthropic adapters behind shared model interfaces" },
      { label: "Tools", value: "Schema-defined tools, multi-step execution, and approval decisions" },
      { label: "Additional", value: "Embeddings, event streams, chat state, and optional Preact helpers" },
    ]}
    note="Provider capabilities still vary. The shared API normalizes the SDK surface but does not make every model feature identical."
    links={[
      { label: "Package source", href: "https://github.com/neutron-build/neutron/tree/main/typescript/packages/neutron-ai" },
      { label: "Agents", href: "/agents" },
      { label: "Workflow", href: "/workflow" },
    ]}
  />;
}
