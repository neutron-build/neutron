// Every screen at desktop and phone size, light and dark: it must fit the screen, and the new ones
// are photographed for review. Then the keyboard: Tab reaches the main controls with a visible ring.
import { saves } from "../../../dist/store.js";
import { DESKTOP, PHONE, scenario } from "../harness.mjs";
import { newPost, VISUAL } from "./editor.mjs";

export const SIZES = [["desktop", DESKTOP], ["phone", PHONE]];

/** Opens a screen at both sizes in both themes, checks it fits, and saves a picture when named. */
export async function everyWay(t, path, name, setup, { sizes = SIZES, state } = {}) {
  for (const [sizeName, size] of sizes) {
    for (const dark of [false, true]) {
      const page = await t.page({ size, dark, state, owner: state === undefined });
      await t.go(page, path);
      if (setup) await setup(page);
      await t.fits(page, `${name ?? path} (${sizeName}, ${dark ? "dark" : "light"})`);
      if (name && t.browserName === "chrome") await t.shot(page, `${name}-${sizeName}-${dark ? "dark" : "light"}`);
      await page.context().close();
    }
  }
}

scenario("screens: every admin screen fits at both sizes, light and dark", async (t) => {
  const { ids } = t.fixture;
  const named = [["/n-admin", "posts"], ["/n-admin/pages", "pages"], ["/n-admin/lists/links", "list-links"], ["/n-admin/lists/gallery", "list-gallery"], ["/n-admin/lists/events", "list-events"], ["/n-admin/lists/team", "list-team"], ["/n-admin/lists/text", "text"], [`/n-admin/posts/${ids.untouched}`, "editor"]];
  for (const [path, name] of named) await everyWay(t, path, name);
  await everyWay(t, "/n-admin/edit", "edit-site", async (page) => {
    await page.frameLocator("[data-edit-frame]").locator("[data-cms-field]").first().waitFor();
  });
  await everyWay(t, "/n-admin/edit", "edit-site-selected", async (page) => {
    await page.frameLocator("[data-edit-frame]").locator("[data-cms-item='team.0'] [data-cms=name]").click({ force: true });
    await page.locator("[data-edit-tools]").getByLabel("Site").waitFor();
  });
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "editor-markdown", async (page) => {
    await page.locator("[data-toggle-meta]").click();
    await page.getByRole("button", { name: "Edit as Markdown" }).click();
    await page.locator("[data-toggle-meta]").click();
  });
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "editor-preview", async (page) => {
    await page.locator("[data-toggle-preview]").click();
    await page.frameLocator("[data-preview]").locator("h1").first().waitFor();
  });
  for (const [sizeName, size] of SIZES) {
    for (const dark of [false, true]) {
      const missing = await t.page({ size, dark });
      t.equal((await missing.goto(`${t.origin}/n-admin/nope`)).status(), 404, "an unknown address is Not found");
      await t.fits(missing, "not found");
      if (t.browserName === "chrome") await t.shot(missing, `not-found-${sizeName}-${dark ? "dark" : "light"}`);
    }
  }
  await everyWay(t, "/n-admin/login", "login", undefined, { state: { cookies: [], origins: [] } });
  await everyWay(t, "/n-admin?q=seed", "posts-search");
  await everyWay(t, `/n-admin/posts/${ids.versioned ?? ids.untouched}/versions`, "versions");
  await everyWay(t, "/n-admin/trash", "trash");
  await everyWay(t, "/n-admin/history", "history");
  const older = saves()[2]?.id;
  await everyWay(t, `/n-admin/history?s=${older}`, "history-selected");
  await everyWay(t, "/n-admin/media", "media");
  await everyWay(t, "/n-admin/media?unused=1", "media-unused");
  await everyWay(t, "/n-admin/account", "account");
  await everyWay(t, `/n-admin/posts/${ids.iframe}`, "editor-embed");
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "image-chooser", async (page) => {
    await page.locator("[data-add-image], [data-add-menu]").filter({ visible: true }).first().click();
    const image = page.locator("dialog[open]").getByRole("button", { name: "Image" });
    if (await image.isVisible().catch(() => false)) await image.click();
    await page.locator("dialog.chooser[open] .chooser-grid img").first().waitFor();
    await page.locator("dialog.chooser[open]").evaluate((dialog) => Promise.all([...dialog.querySelectorAll("img")].map((img) => img.decode().catch(() => {}))));
  });
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "embed-dialog", async (page) => {
    await page.locator("[data-add-embed], [data-add-menu]").filter({ visible: true }).first().click();
    const embed = page.locator("dialog[open]").getByRole("button", { name: "Embed" });
    if (await embed.isVisible().catch(() => false)) await embed.click();
    await page.locator("dialog[open]").getByLabel("Link or embed code").fill("https://youtu.be/dQw4w9WgXcQ");
  });
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "editor-phone-add-menu", async (page) => {
    await page.getByRole("button", { name: "Add", exact: true }).click();
  }, { sizes: [["phone", PHONE]] });
  await everyWay(t, `/n-admin/posts/${ids.untouched}`, "editor-settings", async (page) => {
    await page.locator("[data-toggle-meta]").click();
  });
});

scenario("screens: the login page fits", async (t) => {
  for (const [sizeName, size] of SIZES) {
    const page = await t.page({ size, owner: false });
    await t.go(page, "/n-admin/login");
    await page.getByLabel("Password").fill("wrong one");
    await page.getByRole("button", { name: "Log in" }).click();
    await page.getByRole("alert").waitFor();
    await t.fits(page, `login with an error (${sizeName})`);
  }
});

/** Presses Tab until `match` has focus; returns what was passed on the way. */
async function tabTo(t, page, match, limit = 40) {
  // Safari moves to links and buttons only with Option held, as its own setting does.
  const key = t.browserName === "webkit" ? "Alt+Tab" : "Tab";
  for (let n = 0; n < limit; n++) {
    await page.keyboard.press(key);
    const found = await page.evaluate((match) => {
      const el = document.activeElement;
      return el && (el.textContent?.trim() === match || el.getAttribute("aria-label") === match || el.getAttribute("name") === match) ? true : false;
    }, match);
    if (found) return true;
  }
  return false;
}

const ringed = (page) => page.evaluate(() => {
  const el = document.activeElement;
  const style = getComputedStyle(el);
  return el.matches(":focus-visible") && style.outlineStyle !== "none" && parseFloat(style.outlineWidth) > 0;
});

scenario("keyboard: Tab reaches the main controls with a visible focus ring", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin");
  for (const control of ["Posts", "Media", "Log out", "New post"]) {
    t.check(await tabTo(t, page, control), `Tab reaches ${control} on Posts`);
    t.check(await ringed(page), `${control} shows a focus ring`);
  }
  await page.keyboard.press("Enter");
  await page.waitForURL(/\/posts\/\d+$/);
  for (const control of ["Settings", "Preview", "Publish"]) {
    t.check(await tabTo(t, page, control), `Tab reaches ${control} in the editor`);
    t.check(await ringed(page), `${control} shows a focus ring`);
  }
  await t.go(page, "/n-admin/lists/links");
  for (const control of ["Label", "Remove", "Save", "Add"]) {
    t.check(await tabTo(t, page, control), `Tab reaches ${control} on a list`);
    t.check(await ringed(page), `${control} shows a focus ring`);
  }
  // Settings close on Escape, giving focus back.
  await newPost(t, page);
  await page.locator(VISUAL).click();
  const settings = page.locator("[data-toggle-meta]");
  await settings.focus();
  await page.keyboard.press("Enter");
  t.equal(await settings.getAttribute("aria-expanded"), "true", "Enter opens Settings");
  await page.getByLabel("URL").focus();
  await page.keyboard.press("Escape");
  t.equal(await settings.getAttribute("aria-expanded"), "false", "Escape closes Settings");
  t.check(await settings.evaluate((el) => el === document.activeElement), "and focus goes back to Settings");
});
