import type { LoaderArgs } from "neutron";

export const config = { mode: "app", cache: { maxAge: 10 } };

export async function loader({ request }: LoaderArgs) {
  const url = new URL(request.url);
  const tokenFromHeader = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
  const tokenFromQuery = url.searchParams.get("token");
  const token = tokenFromHeader || tokenFromQuery;

  if (token !== "demo") {
    return {
      authorized: false,
      reason: "Missing or invalid token. Provide ?token=demo for local testing.",
    };
  }

  return {
    authorized: true,
    account: {
      id: "acct_demo",
      plan: "pro",
    },
    loadedAt: new Date().toISOString(),
  };
}

export default function Protected(props: {
  data?: {
    authorized: boolean;
    reason?: string;
    account?: { id: string; plan: string };
    loadedAt?: string;
  };
}) {
  if (!props.data?.authorized) {
    return (
      <section>
        <h2>Protected</h2>
        <p>{props.data?.reason}</p>
      </section>
    );
  }

  return (
    <section>
      <h2>Protected</h2>
      <p>Account: {props.data?.account?.id}</p>
      <p>Plan: {props.data?.account?.plan}</p>
      <p>Loaded at {props.data?.loadedAt}</p>
    </section>
  );
}
