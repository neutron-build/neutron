import * as net from "node:net";
import { describe, expect, it } from "vitest";
import { createServer } from "./index.js";

describe("createServer resolves once the server is listening", () => {
  it("can be closed immediately, and reports the bound port", async () => {
    const running = await createServer({ mode: "api", host: "127.0.0.1", port: 0 });
    const port = Number(new URL(running.url).port);
    expect(port).toBeGreaterThan(0);
    await expect(running.close()).resolves.toBeUndefined();
  });

  it("rejects when the port is taken", async () => {
    const blocker = net.createServer();
    await new Promise<void>((resolve) => blocker.listen(0, "127.0.0.1", resolve));
    const { port } = blocker.address() as net.AddressInfo;
    try {
      await expect(createServer({ mode: "api", host: "127.0.0.1", port })).rejects.toThrow(/EADDRINUSE/);
    } finally {
      await new Promise<void>((resolve) => blocker.close(() => resolve()));
    }
  });
});
