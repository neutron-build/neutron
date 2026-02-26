import { json } from "@remix-run/node";
import { revalidateBenchCache } from "../lib/bench-cache";

export async function loader() {
  return json({ ok: false, error: "Method Not Allowed" }, { status: 405 });
}

export async function action() {
  const version = revalidateBenchCache();
  return json({ ok: true, version });
}
