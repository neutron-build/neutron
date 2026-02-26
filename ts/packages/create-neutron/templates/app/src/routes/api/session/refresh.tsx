import type { ActionArgs } from "neutron";

export const config = { mode: "app" };

export async function action({ request }: ActionArgs) {
  const auth = request.headers.get("authorization") || "";
  const token = auth.replace(/^Bearer\s+/i, "");

  if (token !== "demo") {
    return Response.json(
      { refreshed: false, error: "unauthorized" },
      { status: 401, headers: { "Cache-Control": "no-store" } }
    );
  }

  return Response.json(
    {
      refreshed: true,
      refreshedAt: new Date().toISOString(),
      ttlSec: 3600,
    },
    { headers: { "Cache-Control": "no-store" } }
  );
}

export default function SessionRefreshRoute() {
  return null;
}
