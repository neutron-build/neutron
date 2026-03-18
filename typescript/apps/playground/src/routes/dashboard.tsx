import type { LoaderArgs } from "neutron";
import { getDataRuntimeSummary } from "../data/runtime.js";

export const config = { mode: "app" };

export async function loader({ params }: LoaderArgs) {
  const runtime = await getDataRuntimeSummary();
  return {
    message: "Hello from the loader!",
    timestamp: new Date().toISOString(),
    runtime,
    items: [
      { id: 1, name: "First Item" },
      { id: 2, name: "Second Item" },
      { id: 3, name: "Third Item" },
    ],
  };
}

interface LoaderData {
  message: string;
  timestamp: string;
  runtime: {
    profile: string;
    databaseProvider: string;
    drivers: {
      database: string;
      cache: string;
      session: string;
      queue: string;
      storage: string;
    };
  };
  items: Array<{ id: number; name: string }>;
}

export default function Dashboard({ data }: { data: LoaderData }) {
  return (
    <div>
      <h1>Dashboard</h1>
      <p>{data?.message}</p>
      <p><small>Loaded at: {data?.timestamp}</small></p>
      <p style={{ color: "#888" }}>
        Data profile: <code>{data?.runtime.profile}</code> | DB provider:{" "}
        <code>{data?.runtime.databaseProvider}</code>
      </p>
      
      <h2>Items</h2>
      <ul>
        {data?.items.map((item) => (
          <li key={item.id}>{item.name}</li>
        ))}
      </ul>
    </div>
  );
}
