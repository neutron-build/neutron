import React, { useMemo } from "react";
import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 30 } };

export function headers() {
  return {
    "Cache-Control": "public, max-age=30",
    Vary: "Accept",
  };
}

export async function loader({ params }: LoaderArgs) {
  const id = params.id || "0";
  return {
    user: {
      id,
      name: `User ${id}`,
      email: `user${id}@example.com`,
    },
  };
}

interface LoaderData {
  user: {
    id: string;
    name: string;
    email: string;
  };
}

export default function UserPage({ data }: { data: LoaderData }) {
  const title = useMemo(() => `Profile: ${data.user.name}`, [data.user.name]);

  return (
    <section>
      <h1>{title}</h1>
      <p>ID: {data.user.id}</p>
      <p>Email: {data.user.email}</p>
    </section>
  );
}
