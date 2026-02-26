import { describe, it, expect } from "vitest";
import { runMiddlewareChain, composeMiddleware } from "./middleware.js";
import type { MiddlewareFn, AppContext } from "./types.js";

describe("middleware", () => {
  it("runs middleware in order", async () => {
    const order: string[] = [];
    
    const middleware1: MiddlewareFn = async (req, ctx, next) => {
      order.push("1-start");
      const res = await next();
      order.push("1-end");
      return res;
    };
    
    const middleware2: MiddlewareFn = async (req, ctx, next) => {
      order.push("2-start");
      const res = await next();
      order.push("2-end");
      return res;
    };

    const request = new Request("http://localhost/test");
    const context: AppContext = {};
    
    await runMiddlewareChain([middleware1, middleware2], request, context, async () => {
      order.push("handler");
      return new Response("ok");
    });

    expect(order).toEqual(["1-start", "2-start", "handler", "2-end", "1-end"]);
  });

  it("middleware can modify response", async () => {
    const addHeader: MiddlewareFn = async (req, ctx, next) => {
      const res = await next();
      const newRes = new Response(res.body, res);
      newRes.headers.set("X-Custom", "modified");
      return newRes;
    };

    const request = new Request("http://localhost/test");
    const context: AppContext = {};
    
    const response = await runMiddlewareChain(
      [addHeader], 
      request, 
      context, 
      async () => new Response("ok")
    );

    expect(response.headers.get("X-Custom")).toBe("modified");
  });

  it("middleware can short-circuit", async () => {
    let handlerCalled = false;
    
    const authMiddleware: MiddlewareFn = async (req, ctx, next) => {
      if (!ctx.user) {
        return new Response("Unauthorized", { status: 401 });
      }
      return next();
    };

    const request = new Request("http://localhost/test");
    const context: AppContext = {}; // no user
    
    const response = await runMiddlewareChain(
      [authMiddleware], 
      request, 
      context, 
      async () => {
        handlerCalled = true;
        return new Response("ok");
      }
    );

    expect(response.status).toBe(401);
    expect(handlerCalled).toBe(false);
  });

  it("middleware can set context", async () => {
    const setUser: MiddlewareFn = async (req, ctx, next) => {
      ctx.user = { id: "123", name: "Test User" };
      return next();
    };

    let capturedUser: unknown = null;
    
    const request = new Request("http://localhost/test");
    const context: AppContext = {};
    
    await runMiddlewareChain(
      [setUser], 
      request, 
      context, 
      async () => {
        capturedUser = context.user;
        return new Response("ok");
      }
    );

    expect(capturedUser).toEqual({ id: "123", name: "Test User" });
  });

  it("composeMiddleware creates a single function", async () => {
    const order: string[] = [];

    const middleware: MiddlewareFn = async (req, ctx, next) => {
      order.push("m");
      return next();
    };

    const composed = composeMiddleware([middleware]);
    const response = await composed(
      new Request("http://localhost/test"),
      {}
    );

    expect(order).toEqual(["m"]);
    expect(response.status).toBe(404); // default handler returns 404
  });

  it("composeMiddleware returns short-circuit middleware response", async () => {
    const guard: MiddlewareFn = async () => {
      return new Response("Blocked", { status: 403 });
    };

    const composed = composeMiddleware([guard]);
    const response = await composed(
      new Request("http://localhost/test"),
      {}
    );

    expect(response.status).toBe(403);
    expect(await response.text()).toBe("Blocked");
  });

  it("composeMiddleware preserves response modifications from middleware", async () => {
    const addHeader: MiddlewareFn = async (req, ctx, next) => {
      const res = await next();
      res.headers.set("X-Trace", "abc");
      return res;
    };

    const composed = composeMiddleware([addHeader]);
    const response = await composed(
      new Request("http://localhost/test"),
      {}
    );

    expect(response.headers.get("X-Trace")).toBe("abc");
  });
});
