import type { ActionArgs, LoaderArgs, MiddlewareFn } from "neutron";
import { getDataRuntime, getDataRuntimeSummary } from "../data/runtime.js";

export const middleware: MiddlewareFn = async (request, context, next) => {
  const start = Date.now();
  
  // Add request info to context
  context.requestId = Math.random().toString(36).slice(2);
  context.requestStart = start;

  console.log(`[${new Date().toISOString()}] ${request.method} ${new URL(request.url).pathname}`);

  const response = await next();

  const duration = Date.now() - start;
  console.log(`  → ${response.status} (${duration}ms)`);

  // Add timing header
  const newResponse = new Response(response.body, response);
  newResponse.headers.set("X-Response-Time", `${duration}ms`);
  newResponse.headers.set("X-Request-Id", context.requestId as string);

  return newResponse;
};

export const config = { mode: "app" };

export async function loader(_: LoaderArgs) {
  return await getDataRuntimeSummary();
}

export async function action({ request }: ActionArgs) {
  const formData = await request.formData();
  const intent = String(formData.get("_intent") || "");

  if (intent !== "storage-probe") {
    return { ok: false, message: "Unknown action intent." };
  }

  const runtime = await getDataRuntime();
  const key = `probes/${Date.now()}.txt`;
  const body = new TextEncoder().encode(`probe:${Date.now()}`);

  await runtime.storage.put({
    key,
    body,
    contentType: "text/plain",
  });

  const retrieved = await runtime.storage.get(key);
  await runtime.storage.del(key);

  return {
    ok: true,
    message: retrieved ? "Storage probe succeeded." : "Storage probe returned no object.",
    storageDriver: runtime.drivers.storage,
  };
}

export default function Admin({
  data,
  actionData,
}: {
  data: {
    profile: string;
    databaseProvider: string;
    drivers: {
      database: string;
      cache: string;
      session: string;
      queue: string;
      storage: string;
    };
  };
  actionData?: { ok: boolean; message: string; storageDriver?: string };
}) {
  return (
    <div>
      <h1>Admin Dashboard</h1>
      <p>This page has logging middleware that runs on every request.</p>
      <p>Check the server console for request logs.</p>
      <p>Response headers include X-Response-Time and X-Request-Id.</p>
      <p style={{ color: "#888" }}>
        Data profile: <code>{data.profile}</code> | DB provider: <code>{data.databaseProvider}</code>
      </p>

      <ul>
        <li>Database driver: <code>{data.drivers.database}</code></li>
        <li>Cache driver: <code>{data.drivers.cache}</code></li>
        <li>Session driver: <code>{data.drivers.session}</code></li>
        <li>Queue driver: <code>{data.drivers.queue}</code></li>
        <li>Storage driver: <code>{data.drivers.storage}</code></li>
      </ul>

      <form method="post">
        <input type="hidden" name="_intent" value="storage-probe" />
        <button type="submit">Run Storage Probe</button>
      </form>

      {actionData?.message ? (
        <p style={{ color: actionData.ok ? "#00E5A0" : "#FF4444" }}>
          {actionData.message}
          {actionData.storageDriver ? ` (${actionData.storageDriver})` : ""}
        </p>
      ) : null}
    </div>
  );
}
