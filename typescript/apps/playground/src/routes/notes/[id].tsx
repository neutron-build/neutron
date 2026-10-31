import type { LoaderArgs } from "@neutron-build/core";
import { notFound } from "@neutron-build/core";

export const config = { mode: "app" };

export async function loader({ params }: LoaderArgs) {
  if (params.id !== "welcome") throw notFound();
  return { note: { title: "Welcome note" } };
}

export default function Note({ data }: { data: { note: { title: string } } }) {
  return <h1>{data.note.title}</h1>;
}
