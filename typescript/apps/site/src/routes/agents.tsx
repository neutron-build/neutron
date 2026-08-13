import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Agents - Neutron",
    description: "File-based TypeScript agents built from instructions, tools, AI models, and optional durable workflows.",
  };
}

export default function AgentsPage() {
  return <ProductOverview
    title="Neutron Agents"
    description="File-based agent definitions that compose Neutron AI tools with local, sandboxed, team, and durable execution."
    category="tool"
    accent="var(--accent)"
    accentRgb="0, 229, 160"
    facts={[
      { label: "Authoring", value: "agent.ts definitions, instructions.md prompts, tools, and skills" },
      { label: "Runtime", value: "Single turns, typed tools, local execution, or sandbox execution" },
      { label: "Teams", value: "Pipeline and round-trip policies for multiple agent members" },
      { label: "Durability", value: "Optional adapter onto @neutron-build/workflow" },
    ]}
    links={[
      { label: "Package source", href: "https://github.com/neutron-build/neutron/tree/main/typescript/packages/neutron-agents" },
      { label: "AI SDK", href: "/ai" },
      { label: "Workflow", href: "/workflow" },
    ]}
  />;
}
