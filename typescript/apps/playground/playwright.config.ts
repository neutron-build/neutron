import { defineConfig, devices } from "@playwright/test";

/**
 * Browser e2e for the playground app.
 *
 * Chromium only, on purpose: this suite verifies framework behaviour (routing,
 * actions, island hydration), not cross-browser rendering, and each additional
 * engine costs a browser download in CI.
 *
 * Requires a production build of the playground (`pnpm build`); `preview`
 * refuses to start without `dist/` and says so.
 */
export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "pnpm run preview",
    url: "http://127.0.0.1:4173/health",
    timeout: 120_000,
    reuseExistingServer: !process.env.CI,
  },
});
