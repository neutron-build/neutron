// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { stringify } from "devalue";

const navigation = vi.hoisted(() => ({ navigate: vi.fn(), followDataRedirect: vi.fn() }));
vi.mock("./navigate.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./navigate.js")>()),
  navigate: navigation.navigate,
  followDataRedirect: navigation.followDataRedirect,
}));

import { callServerAction, createServerActionReference, ServerActionError } from "./server-actions.js";

type Win = Window & { __NEUTRON_ROUTER_ACTIVE__?: boolean };

let sent: { url: string; init: RequestInit }[] = [];
function answer(response: Response): void {
  vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
    sent.push({ url, init });
    return response;
  });
}

beforeEach(() => {
  sent = [];
  navigation.navigate.mockReset();
  navigation.followDataRedirect.mockReset();
  window.history.replaceState(null, "", "/todos?tab=open");
  document.head.innerHTML = "";
  (window as Win).__NEUTRON_ROUTER_ACTIVE__ = true;
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (window as Win).__NEUTRON_ROUTER_ACTIVE__;
});

describe("server action client", () => {
  it("posts the id and serialized arguments, with the page's CSRF token, and decodes the result", async () => {
    document.head.innerHTML = '<meta name="csrf-token" content="tok-1">';
    answer(new Response(stringify({ at: new Date(1) }), { headers: { "X-Neutron-Action-Result": "1" } }));
    const action = createServerActionReference("abc", "save");
    expect(action.name).toBe("save");
    expect(await action(1, "two")).toEqual({ at: new Date(1) });
    const [{ url, init }] = sent;
    expect(url).toBe("/_neutron/action");
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Neutron-Action"]).toBe("abc");
    expect(headers["X-Neutron-Data"]).toBe("true");
    expect(headers["x-csrf-token"]).toBe("tok-1");
    expect(init.body).toBe(stringify([1, "two"]));
  });

  it("navigates on a signalled redirect: through the router when it runs, as a document otherwise", async () => {
    answer(new Response(null, { status: 204, headers: { "X-Neutron-Redirect": "/done" } }));
    expect(await callServerAction("abc", "go", [])).toBeUndefined();
    expect(navigation.followDataRedirect).toHaveBeenCalledWith("/done");

    (window as Win).__NEUTRON_ROUTER_ACTIVE__ = false;
    const assign = vi.fn();
    vi.spyOn(window, "location", "get").mockReturnValue({ ...window.location, assign } as Location);
    answer(new Response(null, { status: 204, headers: { "X-Neutron-Redirect": "/elsewhere" } }));
    await callServerAction("abc", "go", []);
    expect(assign).toHaveBeenCalledWith("/elsewhere");
  });

  it("revalidates the page on screen when the response invalidates it", async () => {
    answer(new Response(stringify(1), { headers: { "X-Neutron-Action-Result": "1", "x-neutron-invalidate": "/other" } }));
    await callServerAction("abc", "touch", []);
    expect(navigation.navigate).not.toHaveBeenCalled();

    answer(new Response(stringify(1), { headers: { "X-Neutron-Action-Result": "1", "x-neutron-invalidate": "/other, /todos" } }));
    await callServerAction("abc", "touch", []);
    expect(navigation.navigate).toHaveBeenCalledWith("/todos?tab=open");
  });

  it("rejects with the problem the server answered", async () => {
    answer(
      new Response(JSON.stringify({ type: "x", title: "Conflict", status: 409, detail: "taken" }), {
        status: 409,
        headers: { "Content-Type": "application/problem+json" },
      })
    );
    const error = (await callServerAction("abc", "save", []).catch((e: unknown) => e)) as ServerActionError;
    expect(error).toBeInstanceOf(ServerActionError);
    expect(error.status).toBe(409);
    expect(error.message).toBe('Server action "save" failed (409): taken');
    expect(error.problem?.detail).toBe("taken");
  });

  it("hands back a Response the action answered with itself", async () => {
    const response = new Response("csv,data", { headers: { "Content-Type": "text/csv" } });
    answer(response);
    expect(await callServerAction("abc", "export", [])).toBe(response);
  });

  it("sends FormData and Blob arguments as multipart", async () => {
    answer(new Response(stringify(null), { headers: { "X-Neutron-Action-Result": "1" } }));
    const form = new FormData();
    form.append("name", "Ada");
    await callServerAction("abc", "submit", [form, new Blob(["b"])]);
    const body = sent[0].init.body as FormData;
    expect(body).toBeInstanceOf(FormData);
    expect((sent[0].init.headers as Record<string, string>)["Content-Type"]).toBeUndefined();
    expect(body.get("$f0:name")).toBe("Ada");
    expect(body.get("$b0")).toBeInstanceOf(Blob);
    expect(typeof body.get("$neutron_args")).toBe("string");
  });
});
