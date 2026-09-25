import type { components } from "../api";

export const config = { mode: "app" };

// Generated from api/openapi.json by the web-api-types task.
type Item = components["schemas"]["Item"];

// The coordinator injects NEUTRON_SERVICE_API_URL from depends_on = ["api"],
// so this service never hard-codes the API's (assigned) port.
export async function loader() {
  const api = process.env.NEUTRON_SERVICE_API_URL;
  if (!api) throw new Error("NEUTRON_SERVICE_API_URL is not set; run with `neutron dev`");
  const response = await fetch(`${api}/api/items`, { signal: AbortSignal.timeout(2000) });
  if (!response.ok) throw new Error(`API returned ${response.status}`);
  return { items: (await response.json()) as Item[] };
}

export default function Home(props: { data?: { items: Item[] } }) {
  return (
    <section>
      <p>Items served by the Go API:</p>
      <ul>
        {props.data?.items.map((item) => (
          <li key={item.id} data-item={item.name}>{item.name}</li>
        ))}
      </ul>
    </section>
  );
}
