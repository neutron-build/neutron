import { test, expect, type Page } from "@playwright/test";

/**
 * F-P6-5: a loader's notFound() is answered with the app's not-found page
 * (status 404), but the URL still matches the page whose loader refused. The
 * client router used to boot by URL and hydrate that page without its data,
 * so the 404 turned into the layout's error boundary. The not-found page must
 * be the one that hydrates: interactive, with client navigation away from it.
 */

declare global {
  interface Window {
    __NEUTRON_ROUTER_ACTIVE__?: boolean;
    __e2eNavMarker?: string;
  }
}

async function waitForRouter(page: Page): Promise<void> {
  await expect
    .poll(() => page.evaluate(() => window.__NEUTRON_ROUTER_ACTIVE__ === true))
    .toBe(true);
}

function collectPageErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    // The browser logs every 404 response as a failed resource; the 404s
    // here are intended.
    if (message.type() === "error" && !/status of 404/.test(message.text())) errors.push(message.text());
  });
  return errors;
}

async function expectHydratedNotFound(page: Page): Promise<void> {
  await expect(page.getByRole("heading", { name: "Page not found" })).toBeVisible();
  await waitForRouter(page);
  // Only a hydrated not-found page has the click handler.
  const button = page.getByRole("button", { name: /Checked \d+ times/ });
  await expect(button).toHaveText("Checked 0 times");
  await button.click();
  await expect(button).toHaveText("Checked 1 times");
  await expect(page.getByText("Layout Error")).toHaveCount(0);
  await expect(page.getByText("Something went wrong")).toHaveCount(0);
}

test("a page loader's notFound() hydrates the not-found page, and links away client-side", async ({ page }) => {
  const errors = collectPageErrors(page);
  const response = await page.goto("/notes/missing");
  expect(response?.status()).toBe(404);
  await expectHydratedNotFound(page);

  // Client navigation away from it: the document is not replaced.
  await page.evaluate(() => {
    window.__e2eNavMarker = "alive";
  });
  await page.getByRole("link", { name: "Go to todos" }).click();
  await expect(page).toHaveURL(/\/todos$/);
  await expect(page.getByRole("heading", { name: "Todos" })).toBeVisible();
  expect(await page.evaluate(() => window.__e2eNavMarker)).toBe("alive");
  expect(errors).toEqual([]);
});

test("a layout loader's notFound() hydrates the not-found page", async ({ page }) => {
  const errors = collectPageErrors(page);
  const response = await page.goto("/shelves/closed");
  expect(response?.status()).toBe(404);
  await expectHydratedNotFound(page);
  expect(errors).toEqual([]);

  const open = await page.goto("/shelves/open");
  expect(open?.status()).toBe(200);
  await expect(page.getByText("Shelf contents")).toBeVisible();
});

test("a client navigation that hits a loader 404 lands on the not-found page", async ({ page }) => {
  const errors = collectPageErrors(page);
  await page.goto("/todos");
  await expect(page.getByRole("heading", { name: "Todos" })).toBeVisible();
  await waitForRouter(page);

  await page.evaluate(() => {
    const anchor = document.createElement("a");
    anchor.href = "/notes/missing";
    anchor.id = "e2e-missing-note";
    anchor.textContent = "missing note";
    document.body.appendChild(anchor);
  });
  await page.click("#e2e-missing-note");
  await expect(page).toHaveURL(/\/notes\/missing$/);
  await expectHydratedNotFound(page);

  // The page itself still renders where its loader answers.
  await page.goto("/notes/welcome");
  await expect(page.getByRole("heading", { name: "Welcome note" })).toBeVisible();
  expect(errors).toEqual([]);
});
