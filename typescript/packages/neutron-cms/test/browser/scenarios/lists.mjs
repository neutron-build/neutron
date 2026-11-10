import { readFileSync } from "node:fs";
import { allTexts, listItems } from "../../../dist/store.js";
import { imageFile } from "../fixture.mjs";
import { PHONE, pickFiles, scenario, sendFiles } from "../harness.mjs";

const rows = (page) => page.locator(".rows > .item");
const save = async (page) => {
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForLoadState("load");
};

/** Through the image chooser when there are images already, else straight to the files. */
async function choose(page, click, paths) {
  await pickFiles(page, async () => {
    await click();
    const upload = page.locator("dialog.chooser[open]").getByRole("button", { name: "Upload" });
    if (await upload.isVisible().catch(() => false)) await upload.click();
  }, paths);
}

scenario("lists: add, reorder, remove, required fields and bad links", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/lists/links");
  t.equal(await rows(page).count(), 1, "the seeded link is there");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  t.check(await rows(page).last().locator("input").first().evaluate((el) => el === document.activeElement), "Add puts the cursor in the new row");
  await t.seeText(page.locator("[data-state]"), "Unsaved changes");
  await t.visible(page.getByRole("button", { name: "Discard changes" }), "Discard is offered");
  await rows(page).last().getByLabel("Label").fill("Docs");
  await save(page);
  await t.seeText(page.getByRole("alert"), "Row 2: add URL.", "a missing required field is named");
  t.equal(await rows(page).count(), 2, "what was typed is kept");
  await rows(page).last().getByLabel("URL").fill("not a link");
  await save(page);
  await t.seeText(page.getByRole("alert"), "isn't a link", "a bad link is refused");
  await rows(page).last().getByLabel("URL").fill("javascript:alert(1)");
  await save(page);
  await t.seeText(page.getByRole("alert"), "isn't a link", "a script link is refused");
  await rows(page).last().getByLabel("URL").fill("neutron.build/docs");
  await save(page);
  await t.seeText(page.getByRole("status"), "Saved");
  t.equal(listItems("links")[1].url, "https://neutron.build/docs", "a typed address is completed");

  await rows(page).first().getByRole("button", { name: "Move later" }).click();
  await save(page);
  t.equal(listItems("links").map((item) => item.label).join(","), "Docs,Home", "Move later reorders");

  await rows(page).first().getByRole("button", { name: "Remove" }).click();
  await save(page);
  t.equal(listItems("links").map((item) => item.label).join(","), "Home", "Remove removes");

  await rows(page).first().getByRole("button", { name: "Remove" }).click();
  page.answers.push(false);
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.waitForTimeout(200);
  t.equal(listItems("links").length, 1, "emptying a list asks first");
  page.answers.push(true);
  await save(page);
  t.equal(listItems("links").length, 0, "and empties it when confirmed");
  await t.fits(page, "an empty list");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await rows(page).last().getByLabel("Label").fill("Home");
  await rows(page).last().getByLabel("URL").fill("https://example.com");
  await save(page);
  t.equal(listItems("links").length, 1, "and fills again");
});

scenario("lists: a dated list keeps date order", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/lists/events");
  t.equal(await page.getByRole("button", { name: "Move later" }).count(), 0, "a sorted list has no move buttons");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await rows(page).last().getByLabel("Title").fill("Earlier");
  await rows(page).last().getByLabel("Date").fill("2029-03-04");
  await save(page);
  t.equal(listItems("events").map((item) => item.title).join(","), "Earlier,Launch", "saved in date order");
});

scenario("lists: a photo list fills by upload, drop and choosing", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/lists/gallery");
  await t.seeText(page.locator("[data-state]"), "Or drop photos on this page");
  const files = [1, 2, 3].map((n) => imageFile(t.fixture.files, `gallery-${n}.png`));
  await choose(page, () => page.getByRole("button", { name: "Add photos" }).click(), files);
  await t.eventually(async () => (await rows(page).count()) === 3, "three photos make three rows");
  await t.eventually(async () => !(await page.getByRole("button", { name: "Save", exact: true }).isDisabled()), "Save waits for the uploads");
  await t.eventually(async () => (await page.locator(".rows input[type=hidden]").evaluateAll((all) => all.every((el) => el.value.startsWith("/n-admin/media/")))), "each row has its image");
  await rows(page).nth(1).getByLabel("Caption").fill("The middle one");

  const dropped = imageFile(t.fixture.files, "gallery-drop.png");
  await sendFiles(page, "main", "dragover", [{ name: "x.png", data: readFileSync(dropped) }]);
  await sendFiles(page, "main", "drop", [{ name: "gallery-drop.png", data: readFileSync(dropped) }]);
  await t.eventually(async () => (await rows(page).count()) === 4, "a dropped photo adds a row");
  await t.eventually(async () => !(await page.getByRole("button", { name: "Save", exact: true }).isDisabled()), "the drop finishes uploading");

  // An image already uploaded, from the chooser.
  await page.getByRole("button", { name: "Add photos" }).click();
  const chooser = page.locator("dialog.chooser[open]");
  await t.visible(chooser, "the chooser opens once there are images");
  await t.fits(page, "image chooser");
  await chooser.getByRole("button", { name: "Use this image" }).first().click();
  await t.hidden(chooser, "choosing closes it");
  await t.eventually(async () => (await rows(page).count()) === 5, "and adds the row");

  await rows(page).first().getByRole("button", { name: "Move later" }).click();
  await save(page);
  await t.seeText(page.getByRole("status"), "Saved");
  const saved = listItems("gallery");
  t.equal(saved.length, 5, "all five are saved");
  t.check(saved.some((item) => item.caption === "The middle one"), "with the caption");

  // Replace on a photo.
  await choose(page, () => rows(page).first().getByRole("button", { name: "Replace" }).click({ force: true }), imageFile(t.fixture.files, "replacement.png"));
  await t.eventually(async () => (await rows(page).first().locator("input[type=hidden]").inputValue()) !== saved[0].image, "the replacement uploads");
  await save(page);
  t.check(listItems("gallery")[0].image !== saved[0].image, "Replace changes the photo");
});

scenario("lists: a list page on a phone", async (t) => {
  const page = await t.page({ size: PHONE });
  for (const path of ["/n-admin/lists/links", "/n-admin/lists/gallery", "/n-admin/lists/events", "/n-admin/lists/text"]) await t.go(page, path);
  await page.goto(`${t.origin}/n-admin/lists/links`);
  await page.getByRole("button", { name: "Add", exact: true }).tap();
  await rows(page).last().getByLabel("Label").fill("A label that is rather long for a narrow phone screen indeed");
  await t.fits(page, "a list row on a phone");
});

scenario("text page: words and an image field", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/lists/text");
  await page.getByLabel("Tagline").fill("A new tagline");
  await page.getByLabel("Intro").fill("Line one\nLine two");
  await choose(page, () => page.getByRole("button", { name: "Upload" }).click(), imageFile(t.fixture.files, "hero.png"));
  await t.eventually(async () => (await page.locator(".image-field input").inputValue()).startsWith("/n-admin/media/"), "the hero image uploads");
  await t.seeText(page.locator("[data-state]"), "Unsaved changes");
  await page.getByRole("button", { name: "Save" }).click();
  await page.waitForLoadState("load");
  await t.seeText(page.getByRole("status"), "Saved");
  const texts = allTexts();
  t.equal(texts.tagline, "A new tagline", "the tagline is saved");
  t.equal(texts.intro, "Line one\nLine two", "the intro keeps its lines");
  t.check(texts.hero.startsWith("/n-admin/media/"), "the image is saved");
  await t.visible(page.getByRole("link", { name: "History" }), "History is linked after a save");
});
