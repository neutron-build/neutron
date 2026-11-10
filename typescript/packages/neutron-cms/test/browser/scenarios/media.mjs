import { existsSync, utimesSync } from "node:fs";
import { join } from "node:path";
import { listImages, saveImage } from "../../../dist/media.js";
import { createPost, dataDir, getPost, listItems, savePost, trashPost } from "../../../dist/store.js";
import { imageFile, png } from "../fixture.mjs";
import { pickFiles, scenario } from "../harness.mjs";
import { markdown, newPost, VISUAL } from "./editor.mjs";

const cards = (page) => page.locator(".media-grid > li");
const card = (page, name) => page.locator(".media-grid > li", { has: page.locator(`a[href$="${name}"]`) });
const age = (name, days) => {
  const at = new Date(Date.now() - days * 86_400_000);
  utimesSync(join(dataDir(), "media", name), at, at);
};

scenario("media: listing, where used, upload, unused, delete rules and copy", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/media");
  const total = listImages().length;
  t.check(total > 0, "earlier scenarios left images");
  await t.seeText(page.getByRole("link", { name: /^All \(\d+\)$/ }), `All (${total})`, "All counts every image");
  t.equal(await cards(page).count(), Math.min(total, 60), "each image has a card");
  const where = (await page.locator(".media-uses").allTextContents()).join("\n");
  t.check(/In the post /.test(where), "post images say which post");
  t.check(where.includes("In Gallery"), "gallery images say Gallery");
  t.check(where.includes("In Team"), "Edit site images say Team");
  t.check(where.includes("In the text Hero image"), "text images say which text");
  const teamLink = page.getByRole("link", { name: "In Team" }).first();
  t.equal(await teamLink.getAttribute("href"), "/n-admin/edit", "a list edited on the page links to Edit site");
  t.equal(await page.locator(".media-grid > li", { hasText: "In Gallery" }).first().getByRole("button", { name: "Delete" }).count(), 0, "an image in use has no Delete");

  // Upload two by the button.
  const files = [imageFile(t.fixture.files, "media-1.png"), imageFile(t.fixture.files, "media-2.png")];
  await pickFiles(page, () => page.getByRole("button", { name: "Upload" }).click(), files);
  await page.waitForURL(/added=2/);
  await t.seeText(page.getByRole("status").first(), "2 images added.");
  t.equal(listImages().length, total + 2, "both are stored");
  const [newest, second] = listImages();

  // One only in a trashed post, one old and unused, one fresh and unused.
  const id = createPost();
  savePost(id, { title: "Trashed with an image", description: "", body: `![](/n-admin/media/${second.name})`, slug: "", published_at: null }, getPost(id).updated_at);
  trashPost(id);
  age(newest.name, 3);

  await page.getByRole("link", { name: /^Unused \(\d+\)$/ }).click();
  await page.waitForURL(/unused=1/);
  await t.fits(page, "unused media");
  t.check(await card(page, second.name).count() === 1 && await card(page, newest.name).count() === 1, "the unused filter shows both");
  t.equal(await page.locator(".media-grid").getByText("In the post").count(), 0, "and no image in use");
  await t.seeText(card(page, second.name), "Kept for undo in a post in the Trash");

  // Copy address: to the clipboard, or into a prompt where the clipboard is refused.
  const prompts = [];
  page.on("dialog", (dialog) => prompts.push(dialog.defaultValue()));
  await card(page, newest.name).getByRole("button", { name: "Copy address" }).click();
  await t.eventually(async () => prompts.length > 0 || (await card(page, newest.name).getByRole("button", { name: "Copied" }).count()) > 0, "Copy address copies or shows the address");
  if (prompts.length) t.equal(prompts[0], `${t.origin}/n-admin/media/${newest.name}`, "the prompt holds the full address");
  // WebKit lets a page write the clipboard but not read it back.
  else if (t.browserName === "chrome") t.equal(await page.evaluate(() => navigator.clipboard.readText()), `${t.origin}/n-admin/media/${newest.name}`, "the clipboard holds the full address");

  // Delete all unused leaves images kept for undo and fresh ones.
  const deletable = listImages().filter((image) => image.name === newest.name).length;
  const button = page.getByRole("button", { name: /^Delete all unused \(\d+\)$/ });
  await t.visible(button, "Delete all unused is offered");
  const count = Number((await button.textContent()).match(/\((\d+)\)/)[1]);
  t.check(count >= deletable, "it counts the old unused image");
  page.answers.push(false);
  await button.click();
  await page.waitForTimeout(200);
  t.check(existsSync(join(dataDir(), "media", newest.name)), "cancelling deletes nothing");
  page.answers.push(true);
  await button.click();
  await page.waitForURL(/deleted=\d+/);
  await t.seeText(page.getByRole("status").first(), "deleted.");
  t.check(!existsSync(join(dataDir(), "media", newest.name)), "the old unused image is gone");
  t.check(existsSync(join(dataDir(), "media", second.name)), "the one kept for undo stays");

  // Deleting one kept for undo warns first.
  const messages = [];
  page.on("dialog", (dialog) => messages.push(dialog.message()));
  page.answers.push(true);
  await card(page, second.name).getByRole("button", { name: "Delete" }).click();
  await page.waitForURL(/deleted=1/);
  t.check(messages.some((text) => text.includes("only kept for undo in a post in the Trash")), "the warning says where it is kept");
  t.check(!existsSync(join(dataDir(), "media", second.name)), "and then deletes it");
  t.check(page.url().includes("unused=1"), "back on the unused filter");
});

scenario("media: the image chooser from the post editor and from a list field", async (t) => {
  // The chooser offers images already here, so there must be two.
  while (listImages().length < 2) saveImage(png(64, 64, [listImages().length * 90, 30, 200]));
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Chosen image");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Pick one.");
  const add = page.locator("[data-add-image]");
  await add.focus();
  await page.keyboard.press("Enter");
  const chooser = page.locator("dialog.chooser[open]");
  await t.visible(chooser, "the chooser opens");
  t.check(await chooser.getByRole("button", { name: "Upload" }).evaluate((el) => el === document.activeElement), "Upload has focus");
  t.check(await t.trapped(page, chooser, 40), "Tab never reaches the page behind the chooser");
  await page.keyboard.press("Escape");
  await t.hidden(chooser, "Escape closes it");
  t.check(await add.evaluate((el) => el === document.activeElement), "focus goes back to Add image");

  await add.click();
  await chooser.click({ position: { x: 4, y: 4 } });
  t.check(await chooser.isVisible(), "a click on the chooser's own edge leaves it open");
  await page.mouse.click(4, 400);
  await t.hidden(chooser, "a click on the backdrop closes it");
  await add.click();
  await chooser.getByRole("button", { name: "Cancel" }).click();
  await t.hidden(chooser, "Cancel closes it");
  t.check(!(await markdown(page)).includes("!["), "and adds nothing");

  await add.click();
  const thumb = await chooser.getByRole("button", { name: "Use this image" }).first().locator("img").getAttribute("src");
  await chooser.getByRole("button", { name: "Use this image" }).first().click();
  await t.hidden(chooser, "choosing closes it");
  await t.eventually(async () => /!\[\]\(\/n-admin\/media\/[^)]+\)/.test(await markdown(page)), "the chosen image is in the post");
  t.check((await markdown(page)).includes(thumb.replace("-t.", ".")), "the full image, not its thumbnail");

  // A list's image field.
  await t.go(page, "/n-admin/lists/team");
  const row = page.locator(".rows > .item").first();
  await row.getByRole("group", { name: "Photo" }).getByRole("button").click();
  await t.visible(chooser, "an image field opens the chooser");
  await chooser.getByRole("button", { name: "Use this image" }).nth(1).click();
  await t.eventually(async () => (await row.locator("input[type=hidden]").inputValue()).startsWith("/n-admin/media/"), "the field takes the chosen image");
  await t.seeText(row.getByRole("group", { name: "Photo" }).getByRole("button"), "Replace");
  const chosen = await row.locator("input[type=hidden]").inputValue();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForURL(/saved=1/);
  t.equal(listItems("team")[0].photo, chosen, "and saves it");
});
