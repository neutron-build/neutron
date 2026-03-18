import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 30 } };

export async function loader({ params }: LoaderArgs) {
  const id = params.id || "1";
  return {
    user: {
      id,
      name: `User ${id}`,
      email: `user${id}@example.com`,
    },
  };
}

export default function UserRoute(props: {
  data?: { user: { id: string; name: string; email: string } };
}) {
  return (
    <section>
      <h2>{props.data?.user.name}</h2>
      <p>ID: {props.data?.user.id}</p>
      <p>Email: {props.data?.user.email}</p>
    </section>
  );
}
