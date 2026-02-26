import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 30, loaderMaxAge: 10 } };

export async function loader({ request }: LoaderArgs) {
  const url = new URL(request.url);
  const workspace = url.searchParams.get("workspace") || "main";

  return {
    workspace,
    stats: {
      users: 12,
      projects: 4,
      uptime: "99.99%",
    },
    loadedAt: new Date().toISOString(),
  };
}

export default function Dashboard(props: {
  data?: {
    workspace: string;
    stats: { users: number; projects: number; uptime: string };
    loadedAt: string;
  };
}) {
  return (
    <section>
      <h2>Dashboard</h2>
      <p>Workspace: <strong>{props.data?.workspace}</strong></p>
      <ul>
        <li>Users: {props.data?.stats.users}</li>
        <li>Projects: {props.data?.stats.projects}</li>
        <li>Uptime: {props.data?.stats.uptime}</li>
      </ul>
      <p>Loaded at {props.data?.loadedAt}</p>
    </section>
  );
}
