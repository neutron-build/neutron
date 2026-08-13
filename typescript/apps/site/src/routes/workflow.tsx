import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Workflow - Neutron",
    description: "Event-sourced TypeScript workflows with suspension, replay, scheduling, and Nucleus storage.",
  };
}

export default function WorkflowPage() {
  return <ProductOverview
    title="Neutron Workflow"
    description="An event-sourced TypeScript workflow engine for recorded steps, suspension, replay, and scheduled resumption."
    category="tool"
    accent="var(--accent)"
    accentRgb="0, 229, 160"
    facts={[
      { label: "Execution", value: "Recorded steps with replay and nondeterminism checks" },
      { label: "Suspension", value: "Sleep, external events, cancellation, and later resumption" },
      { label: "Stores", value: "In-memory EventStore and Nucleus streams adapter" },
      { label: "Operations", value: "Leases, exclusive execution, run index, scheduler, and event HTTP handler" },
    ]}
    note="Durability depends on the configured event store and on placing side effects inside recorded workflow steps."
    links={[
      { label: "Package source", href: "https://github.com/neutron-build/neutron/tree/main/typescript/packages/neutron-workflow" },
      { label: "Agents", href: "/agents" },
      { label: "Nucleus", href: "/nucleus" },
    ]}
  />;
}
