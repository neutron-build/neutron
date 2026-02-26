import { Form } from "neutron/client";
import type { ActionArgs } from "neutron";

export const config = { mode: "app" };

export async function loader() {
  return { alerts: "all" };
}

export async function action({ request }: ActionArgs) {
  const formData = await request.formData();
  const alerts = String(formData.get("alerts") || "all");
  return Response.json({ ok: true, alerts, savedAt: new Date().toISOString() });
}

export default function Settings(props: {
  data?: { alerts: string };
  actionData?: { ok: boolean; alerts: string; savedAt: string };
}) {
  return (
    <section>
      <h2>Settings</h2>
      <Form method="post" style={{ display: "grid", gap: "0.75rem", maxWidth: "360px" }}>
        <label>
          Alerts
          <select name="alerts" defaultValue={props.data?.alerts || "all"}>
            <option value="all">All</option>
            <option value="critical">Critical only</option>
            <option value="none">None</option>
          </select>
        </label>
        <button type="submit">Save</button>
      </Form>

      {props.actionData?.ok ? (
        <p style={{ marginTop: "1rem" }}>
          Saved <strong>{props.actionData.alerts}</strong> at {props.actionData.savedAt}
        </p>
      ) : null}
    </section>
  );
}
