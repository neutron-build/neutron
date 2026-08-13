import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Python - Neutron", description: "Neutron's async Python framework for AI applications." };
}

export default function PythonPage() {
  return <LanguageOverview
    name="Python"
    description="An async application framework built on Starlette and Pydantic, with Nucleus and MCP support."
    accent="var(--accent-python)"
    accentRgb="55, 118, 171"
    docsHref="/docs/python/overview"
    quickstartHref="/docs/python/quickstart"
    facts={[
      { label: "Runtime", value: "Python 3.11+, Starlette, and Uvicorn" },
      { label: "Validation", value: "Pydantic v2 request and configuration models" },
      { label: "Data", value: "Async PostgreSQL-wire access through asyncpg" },
      { label: "Package", value: "neutron-py" },
    ]}
  />;
}
