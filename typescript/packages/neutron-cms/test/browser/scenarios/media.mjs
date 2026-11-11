import { existsSync, readFileSync, utimesSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { listImages, listMedia, saveImage } from "../../../dist/media.js";
import { createPost, dataDir, getPost, listItems, replaceItems, savePost, trashPost } from "../../../dist/store.js";
import { cms, imageFile, png } from "../fixture.mjs";
import { pickFiles, scenario } from "../harness.mjs";
import { caretAtEnd, markdown, newPost, openSettings, selectText, VISUAL } from "./editor.mjs";

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
  await t.seeText(page.getByRole("status").first(), "2 files added.");
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

scenario("media: a file uploads in parts, links from the editor and downloads from the site", async (t) => {
  const page = await t.page();
  // Three parts' worth, so the pieces have to join up.
  const bytes = randomBytes(9 * 1024 * 1024);
  const path = join(t.fixture.files, "Game Build v2.zip");
  writeFileSync(path, bytes);
  await t.go(page, "/n-admin/media");
  await pickFiles(page, () => page.getByRole("button", { name: "Upload" }).click(), [path]);
  await page.waitForURL(/added=1/, { timeout: 30000 });
  const file = listMedia().find((entry) => entry.file === "game-build-v2.zip");
  t.check(Boolean(file), "the file is in Media under its own name");
  t.equal(file.bytes, bytes.length, "every byte arrived");
  t.check(readFileSync(join(dataDir(), "media", file.name)).equals(bytes), "and in order");
  await t.seeText(card(page, file.name).locator(".media-name"), "game-build-v2.zip");
  await t.fits(page, "Media with a file");
  if (t.browserName === "chrome") await t.shot(page, "media-file-desktop-light");

  // Link the selected words to it from the editor.
  await newPost(t, page);
  await page.locator(VISUAL).click();
  await page.keyboard.type("Get the build here");
  await selectText(page, "build");
  await page.getByRole("button", { name: "Add file" }).click();
  const chooser = page.locator("dialog.chooser[open]");
  await t.visible(chooser.getByRole("button", { name: "Upload" }), "Add file offers an upload");
  await chooser.getByRole("button", { name: /game-build-v2\.zip/ }).click();
  await t.eventually(async () => (await markdown(page)) === `Get the [build](/n-admin/media/${file.name}) here`, "the selected words link to the file");

  // With nothing selected, a new upload goes in under its own name.
  await caretAtEnd(page);
  const notes = join(t.fixture.files, "notes.txt");
  writeFileSync(notes, "hello");
  await pickFiles(page, () => page.getByRole("button", { name: "Add file" }).click().then(() => page.locator("dialog.chooser[open]").getByRole("button", { name: "Upload" }).click()), [notes]);
  await t.eventually(async () => /\[notes\.txt\]\(\/n-admin\/media\/[a-f0-9]{32}-notes\.txt\)/.test(await markdown(page)), "an uploaded file is linked by name");

  // A visitor gets a download, not a page.
  const response = await page.request.get(new URL(`/n-admin/media/${file.name}`, page.url()).href);
  t.equal(response.status(), 200, "the file is served");
  t.equal(response.headers()["content-disposition"], 'attachment; filename="game-build-v2.zip"', "as a download");
});

scenario("media: a file field in a list and on the site, filled from Media", async (t) => {
  const page = await t.page();
  const file = listMedia().find((entry) => entry.file === "game-build-v2.zip");
  const address = `/n-admin/media/${file.name}`;

  // On the list's own page: Upload offers the files already here.
  await t.go(page, "/n-admin/lists/downloads");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  const row = page.locator(".item").last();
  await row.getByLabel("Name").fill("Mac build");
  await row.getByRole("button", { name: "Upload" }).click();
  await page.locator("dialog.chooser[open]").getByRole("button", { name: /game-build-v2\.zip/ }).click();
  await t.eventually(async () => (await row.getByRole("textbox", { name: "File" }).inputValue()) === address, "the field holds the file's address");
  await t.seeText(page.locator("[data-state]"), "Unsaved changes");
  await t.fits(page, "a list with a file field");
  if (t.browserName === "chrome") await t.shot(page, "list-file-field-desktop-light");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForURL(/saved=1/);
  t.equal(listItems("downloads")[0]?.file, address, "the list saves it");

  // A new upload from the same button.
  const manual = join(t.fixture.files, "manual.pdf");
  writeFileSync(manual, "%PDF-1.7 manual");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  const second = page.locator(".item").last();
  await second.getByLabel("Name").fill("Manual");
  await pickFiles(page, () => second.getByRole("button", { name: "Upload" }).click().then(() => page.locator("dialog.chooser[open]").getByRole("button", { name: "Upload" }).click()), [manual]);
  await t.eventually(async () => /-manual\.pdf$/.test(await second.getByRole("textbox", { name: "File" }).inputValue()), "an uploaded file fills the field");
  await second.getByRole("textbox", { name: "File" }).fill("https://example.com/elsewhere.zip");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForURL(/saved=1/);
  t.equal(listItems("downloads")[1]?.file, "https://example.com/elsewhere.zip", "a pasted link is kept as it is");

  // On the site: the bar has the field and its Upload.
  if (!listItems("press").length) replaceItems("press", [{ outlet: "The Wire", link: "", kit: "" }]);
  await t.go(page, "/n-admin/edit");
  await t.eventually(async () => (await page.frameLocator("[data-edit-frame]").locator("[data-cms-field]").count()) > 0, "the framed site becomes editable", 8000);
  await page.frameLocator("[data-edit-frame]").locator("[data-cms-item='press.0'] [data-cms=outlet]").click({ force: true });
  const tools = page.locator("[data-edit-tools]");
  await tools.getByRole("button", { name: "Upload kit" }).click();
  await page.locator("dialog.chooser[open]").getByRole("button", { name: /game-build-v2\.zip/ }).click();
  await t.eventually(async () => (await tools.getByRole("textbox", { name: "Kit" }).inputValue()) === address, "the bar's field holds the address");
  await page.locator("[data-edit-save]").click();
  await t.seeText(page.locator("[data-edit-state]"), "Saved");
  t.equal(listItems("press")[0]?.kit, address, "Edit site saves it");
});

scenario("media: find by name or use, and give an upload a name", async (t) => {
  const page = await t.page();
  const file = listMedia().find((entry) => entry.file === "game-build-v2.zip");
  await t.go(page, "/n-admin/media");
  const find = page.getByRole("search");
  await find.getByLabel("Find in Media").fill("game-build");
  await find.getByRole("button", { name: "Find" }).click();
  await page.waitForURL(/q=game-build/);
  t.equal(await cards(page).count(), 1, "one upload has that name");
  await t.seeText(card(page, file.name).locator(".media-uses"), "In Downloads", "it says where it is used");

  page.answers.push("Skate Game (Mac)");
  await card(page, file.name).getByRole("button", { name: "Name" }).click();
  await page.waitForURL(/renamed=1/);
  await t.seeText(page.getByRole("status").first(), "Renamed.");
  await t.seeText(card(page, file.name).locator(".media-name"), "Skate Game (Mac)", "the name it was uploaded under still finds it");

  await find.getByLabel("Find in Media").fill("skate");
  await find.getByRole("button", { name: "Find" }).click();
  await page.waitForURL(/q=skate/);
  await t.seeText(card(page, file.name).locator(".media-name"), "Skate Game (Mac)");
  const response = await page.request.get(new URL(`/n-admin/media/${file.name}`, page.url()).href);
  t.equal(response.headers()["content-disposition"], 'attachment; filename="skate-game-mac.zip"', "it downloads under the new name");

  page.answers.push(false);
  await card(page, file.name).getByRole("button", { name: "Rename" }).click();
  await t.seeText(card(page, file.name).locator(".media-name"), "Skate Game (Mac)", "Cancel changes nothing");

  await find.getByLabel("Find in Media").fill("downloads");
  await find.getByRole("button", { name: "Find" }).click();
  await page.waitForURL(/q=downloads/);
  t.check((await cards(page).count()) >= 1, "finds by where it is used");
  await find.getByLabel("Find in Media").fill("no such thing");
  await find.getByRole("button", { name: "Find" }).click();
  await t.seeText(page.locator(".empty-state"), 'Nothing matches "no such thing".');
  await t.fits(page, "Media with a search");
  if (t.browserName === "chrome") await t.shot(page, "media-search-desktop-light");
});

scenario("posts: tags from Settings show in the list and find the post", async (t) => {
  const page = await t.page();
  await newPost(t, page);
  await page.getByLabel("Title").fill("Tagged post");
  await page.locator(VISUAL).click();
  await page.keyboard.type("Words.");
  await openSettings(page);
  await page.getByLabel("Tags").fill("Games,  web dev, games");
  await t.fits(page, "editor settings with tags");
  if (t.browserName === "chrome") await t.shot(page, "editor-tags-desktop-light");
  await page.getByRole("button", { name: "Publish" }).click();
  await page.waitForURL(/saved=1/);
  t.equal(await page.getByLabel("Tags").inputValue(), "Games, web dev", "tags are tidied");
  t.check(cms.tags().some((tag) => tag.slug === "web-dev"), "the site sees the tag");
  t.equal(cms.posts({ tag: "games" })[0]?.title, "Tagged post", "and the post under it");

  await t.go(page, "/n-admin");
  const row = page.locator(".posts tr", { hasText: "Tagged post" });
  await t.visible(row.getByRole("link", { name: "web dev" }), "the list shows the tags");
  await row.getByRole("link", { name: "web dev" }).click();
  await page.waitForURL(/q=web/);
  t.equal(await page.locator(".posts tr").count(), 1, "a tag finds its posts");
  await t.fits(page, "posts with tags");
  if (t.browserName === "chrome") await t.shot(page, "posts-tags-desktop-light");
});
