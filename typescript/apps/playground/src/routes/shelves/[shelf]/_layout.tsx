import type { LoaderArgs } from "@neutron-build/core";
import { notFound } from "@neutron-build/core";

export const config = { mode: "app" };

export async function loader({ params }: LoaderArgs) {
  if (params.shelf !== "open") throw notFound();
  return { shelf: params.shelf };
}

export default function Shelf({ data, children }: { data: { shelf: string }; children?: preact.ComponentChildren }) {
  return (
    <section>
      <h2>Shelf {data.shelf}</h2>
      {children}
    </section>
  );
}
