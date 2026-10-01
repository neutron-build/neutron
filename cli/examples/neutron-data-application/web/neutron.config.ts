import { defineConfig } from "@neutron-build/core";
const host = process.env.NEUTRON_HOST;
if (host && !["127.0.0.1", "localhost", "::1"].includes(host)) {
    throw new Error("The operator reference UI requires a loopback host");
}
export default defineConfig({ runtime: "preact", server: { host: "127.0.0.1" } });
