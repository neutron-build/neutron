import { json } from "@remix-run/node";

export async function loader() {
  return json({ ok: false, error: "Method Not Allowed" }, { status: 405 });
}

export async function action({ request }) {
  const authorized = request.headers.get("authorization") === "Bearer valid-token";
  if (!authorized) {
    return json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  return json({
    ok: true,
    refreshed: true,
    token: "valid-token",
  });
}
