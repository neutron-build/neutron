import { describe, it, expect } from "vitest";
import type { LoaderArgs, ActionArgs, AppContext } from "./types.js";

describe("loader types", () => {
  it("LoaderArgs has correct structure", () => {
    const request = new Request("http://localhost/test");
    const params = { id: "123" };
    const context: AppContext = { user: { name: "test" } };

    const args: LoaderArgs = { request, params, context };

    expect(args.request).toBe(request);
    expect(args.params).toBe(params);
    expect(args.context).toBe(context);
  });

  it("loader function can return data", async () => {
    async function loader({ params }: LoaderArgs) {
      return {
        user: { id: params.id, name: "Test User" },
      };
    }

    const result = await loader({
      request: new Request("http://localhost/users/123"),
      params: { id: "123" },
      context: {},
    });

    expect(result).toEqual({
      user: { id: "123", name: "Test User" },
    });
  });

  it("loader can throw Response for errors", async () => {
    async function loader({ params }: LoaderArgs) {
      if (params.id === "not-found") {
        throw new Response("Not found", { status: 404 });
      }
      return { found: true };
    }

    await expect(
      loader({
        request: new Request("http://localhost/users/not-found"),
        params: { id: "not-found" },
        context: {},
      })
    ).rejects.toThrow(Response);
  });
});

describe("action types", () => {
  it("ActionArgs has correct structure", () => {
    const request = new Request("http://localhost/test", { method: "POST" });
    const params = { id: "123" };
    const context: AppContext = { db: {} };

    const args: ActionArgs = { request, params, context };

    expect(args.request).toBe(request);
    expect(args.params).toBe(params);
    expect(args.context).toBe(context);
  });

  it("action can process form data", async () => {
    const formData = new FormData();
    formData.append("name", "Test");
    formData.append("email", "test@example.com");

    async function action({ request }: ActionArgs) {
      const form = await request.formData();
      return {
        name: form.get("name"),
        email: form.get("email"),
      };
    }

    const result = await action({
      request: new Request("http://localhost/test", {
        method: "POST",
        body: formData,
      }),
      params: {},
      context: {},
    });

    expect(result).toEqual({
      name: "Test",
      email: "test@example.com",
    });
  });

  it("action can return success data", async () => {
    async function action({ request }: ActionArgs) {
      return { success: true, id: Date.now() };
    }

    const result = await action({
      request: new Request("http://localhost/test", { method: "POST" }),
      params: {},
      context: {},
    });

    expect(result).toHaveProperty("success", true);
    expect(result).toHaveProperty("id");
  });
});
