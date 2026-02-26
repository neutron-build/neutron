import { Form } from "neutron/client";
import type { ActionArgs } from "neutron";

export const config = { mode: "app" };

export async function loader() {
  return {
    currentName: "Acme Inc",
    currentTheme: "system",
  };
}

export async function action({ request }: ActionArgs) {
  const formData = await request.formData();
  const name = String(formData.get("name") || "").trim();
  const theme = String(formData.get("theme") || "system").trim();

  return Response.json({
    ok: true,
    saved: {
      name: name || "Unnamed Workspace",
      theme,
    },
    updatedAt: new Date().toISOString(),
  });
}

export default function Settings(props: {
  data?: { currentName: string; currentTheme: string };
  actionData?: {
    ok: boolean;
    saved: { name: string; theme: string };
    updatedAt: string;
  };
}) {
  return (
    <section>
      <h2>Settings</h2>
      <Form method="post" style={{ display: "grid", gap: "0.75rem", maxWidth: "420px" }}>
        <label>
          Workspace Name
          <input
            type="text"
            name="name"
            defaultValue={props.data?.currentName}
            style={{ width: "100%" }}
          />
        </label>
        <label>
          Theme
          <select name="theme" defaultValue={props.data?.currentTheme}>
            <option value="system">System</option>
            <option value="light">Light</option>
            <option value="dark">Dark</option>
          </select>
        </label>
        <button type="submit">Save</button>
      </Form>

      {props.actionData?.ok ? (
        <p style={{ marginTop: "1rem" }}>
          Saved <strong>{props.actionData.saved.name}</strong> ({props.actionData.saved.theme}) at{" "}
          {props.actionData.updatedAt}
        </p>
      ) : null}
    </section>
  );
}
