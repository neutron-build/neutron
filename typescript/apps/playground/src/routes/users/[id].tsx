import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 30 } };

export function headers() {
  return {
    "Cache-Control": "public, max-age=30",
    Vary: "Accept",
  };
}

export async function loader({ params }: LoaderArgs) {
  const users: Record<string, { id: string; name: string; email: string }> = {
    "1": { id: "1", name: "Alice", email: "alice@example.com" },
    "2": { id: "2", name: "Bob", email: "bob@example.com" },
    "3": { id: "3", name: "Charlie", email: "charlie@example.com" },
  };

  const user = users[params.id];
  
  if (!user) {
    throw new Response("User not found", { status: 404 });
  }

  return { user };
}

interface LoaderData {
  user: { id: string; name: string; email: string };
}

export default function UserPage({ data, params }: { data: LoaderData; params: Record<string, string> }) {
  return (
    <div>
      <h1>User: {data?.user.name}</h1>
      <p>ID: {params?.id}</p>
      <p>Email: {data?.user.email}</p>
      <a href="/users">&larr; Back to users</a>
    </div>
  );
}
