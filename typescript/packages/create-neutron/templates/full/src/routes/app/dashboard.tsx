import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 30, loaderMaxAge: 10 } };

export async function loader({ request }: LoaderArgs) {
  const url = new URL(request.url);
  const region = url.searchParams.get("region") || "us-east-1";
  return {
    region,
    rps: 1200,
    p95: 28,
    loadedAt: new Date().toISOString(),
  };
}

export default function Dashboard(props: {
  data?: { region: string; rps: number; p95: number; loadedAt: string };
}) {
  return (
    <section>
      <h2>Dashboard</h2>
      <p>Region: {props.data?.region}</p>
      <p>RPS: {props.data?.rps}</p>
      <p>p95: {props.data?.p95} ms</p>
      <p>Loaded at {props.data?.loadedAt}</p>
    </section>
  );
}
