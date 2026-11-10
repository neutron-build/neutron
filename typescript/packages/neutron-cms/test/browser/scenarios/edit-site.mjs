import { allTexts, designPage, isLive, listItems } from "../../../dist/store.js";
import { imageFile } from "../fixture.mjs";
import { PHONE, scenario } from "../harness.mjs";
import { addImageByButton } from "./editor.mjs";

const site = (page) => page.frameLocator("[data-edit-frame]");
const saveButton = (page) => page.locator("[data-edit-save]");

/** Marks the framed page, so `ready` can tell when a new one has loaded. */
const mark = (page) => site(page).locator("body").evaluate(() => { window.cmsTestOld = true; });

/** Waits for a freshly loaded framed page to be bound for editing. */
async function ready(t, page) {
  await t.eventually(async () => {
    const frame = page.frames().find((one) => one !== page.mainFrame());
    return frame && !(await frame.evaluate(() => window.cmsTestOld === true)) && (await site(page).locator("[data-cms-field]").count()) > 0;
  }, "the framed site becomes editable", 8000);
}

async function typeInto(page, locator, text) {
  await locator.click({ force: true });
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.type(text);
}

async function saveEdits(t, page) {
  await mark(page);
  await saveButton(page).click();
  await t.seeText(page.locator("[data-edit-state]"), "Saved");
  await ready(t, page);
}

scenario("edit site: text, list fields, add, move, remove, Save and Discard", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/edit");
  await ready(t, page);
  await t.seeText(page.locator("[data-edit-state]"), "Click any text or image");
  t.check(await saveButton(page).isDisabled(), "Save waits for a change");

  await typeInto(page, site(page).locator("[data-cms-text=tagline]"), "Tagline typed in place");
  await t.seeText(page.locator("[data-edit-state]"), "Unsaved changes");
  t.check(!(await saveButton(page).isDisabled()), "a change enables Save");

  // A list item: its name in place, its link in the bar.
  await site(page).locator("[data-cms-item='team.0'] [data-cms=name]").click({ force: true });
  const tools = page.locator("[data-edit-tools]");
  await t.visible(tools.getByText("Team"), "the bar names the list");
  await tools.getByLabel("Site").fill("ada.dev");
  await t.fits(page, "edit site with the bar");
  await typeInto(page, site(page).locator("[data-cms-item='team.0'] [data-cms=name]"), "Ada L.");

  await tools.getByRole("button", { name: "Add another" }).click();
  await t.eventually(async () => (await site(page).locator("[data-cms-item]").count()) === 3, "Add puts a new item on the page");
  await page.keyboard.type("Linus");
  await tools.getByRole("button", { name: "Move earlier" }).click();
  await saveEdits(t, page);
  t.equal(listItems("team").map((item) => item.name).join(","), "Linus,Ada L.,Grace", "the new item is saved where it was moved");
  t.equal(listItems("team").find((item) => item.name === "Ada L.")?.site, "https://ada.dev", "the link from the bar is saved and completed");
  t.equal(allTexts().tagline, "Tagline typed in place", "the text is saved");

  await site(page).locator("[data-cms-item='team.2'] [data-cms=name]").click({ force: true });
  await tools.getByRole("button", { name: "Remove this" }).click();
  t.equal(await site(page).locator("[data-cms-item]").count(), 2, "Remove takes it off the page");
  page.answers.push(true);
  await mark(page);
  await page.locator("[data-edit-discard]").click();
  await ready(t, page);
  t.equal(await site(page).locator("[data-cms-item]").count(), 3, "Discard brings it back");
  t.equal(listItems("team").length, 3, "and nothing was saved");

  // An image is clicked to replace it.
  const photo = site(page).locator("[data-cms-item='team.1'] img[data-cms=photo]");
  await addImageByButton(t, page, imageFile(t.fixture.files, "team.png"), { click: () => photo.click({ force: true }) });
  await t.eventually(async () => !(await saveButton(page).isDisabled()), "the upload finishes");
  await saveEdits(t, page);
  t.check(listItems("team")[1].photo.startsWith("/n-admin/media/"), "the new photo is saved");
  await t.visible(page.locator("[data-edit-history]"), "History shows after a save");

  // Links to other sites are off while editing.
  await site(page).getByRole("link", { name: "Elsewhere" }).click({ force: true });
  await t.seeText(page.locator("[data-edit-state]"), "Links to other sites are off while editing.");
});

scenario("edit site: on a phone the bar wraps and the page fits", async (t) => {
  const page = await t.page({ size: PHONE });
  await t.go(page, "/n-admin/edit");
  await ready(t, page);
  await site(page).locator("[data-cms-item='team.0'] [data-cms=name]").tap({ force: true });
  await t.visible(page.locator("[data-edit-tools]").getByLabel("Site"), "the bar shows the item's link");
  await t.fits(page, "edit site on a phone");
  const frame = await page.locator("[data-edit-frame]").boundingBox();
  t.check(frame.height > 200, `the site keeps room on a phone (${Math.round(frame.height)}px)`);
});

scenario("edit site: a designed draft is edited and published from its bar", async (t) => {
  const visitor = await t.page({ owner: false });
  const before = await visitor.goto(`${t.origin}/portfolio`);
  t.equal(before.status(), 404, "a draft design is not on the site");

  const page = await t.page();
  await t.go(page, "/n-admin/pages");
  await page.getByRole("link", { name: "Portfolio" }).click();
  await t.seeText(page.locator(".edit-top .status"), "Draft");
  await ready(t, page);
  await typeInto(page, site(page).locator("[data-cms-text=intro]"), "Written on the draft");
  await page.getByRole("button", { name: "Publish" }).click();
  await t.seeText(page.locator(".edit-top .status"), "Live");
  t.equal(allTexts().intro, "Written on the draft", "Publish saved the edit first");
  t.check(isLive(designPage("portfolio")), "and published the page");
  const after = await visitor.goto(`${t.origin}/portfolio`);
  t.equal(after.status(), 200, "the page is on the site now");
  await t.seeText(visitor.locator("p"), "Written on the draft");
  await t.visible(page.getByRole("button", { name: "Unpublish" }), "Unpublish is offered");
});

scenario("edit site: the bar links to the list's own page, warning about unsaved changes", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/edit");
  await ready(t, page);
  await site(page).locator("[data-cms-item='team.0'] [data-cms=name]").click({ force: true });
  const link = page.locator("[data-edit-tools]").getByRole("link", { name: "Edit list" });
  await t.visible(link, "a list item offers its list's page");
  t.equal(await link.getAttribute("href"), "/n-admin/lists/team", "and links to it");

  await typeInto(page, site(page).locator("[data-cms-item='team.0'] [data-cms=name]"), "Changed, not saved");
  let asked = "";
  page.once("dialog", (dialog) => { asked = dialog.message(); });
  page.answers.push(false);
  await link.click();
  await t.eventually(() => asked.includes("unsaved changes"), "leaving with changes asks first");
  t.check(page.url().endsWith("/n-admin/edit"), "and staying keeps the page");

  page.answers.push(true);
  await link.click();
  await page.waitForURL(/\/n-admin\/lists\/team$/);
  await t.visible(page.getByRole("heading", { name: "Team" }), "going opens the list page");
});

scenario("edit site: an empty list marked on the page offers Add, and the first item is saved", async (t) => {
  const page = await t.page();
  await t.go(page, "/n-admin/edit");
  await ready(t, page);
  const addPress = site(page).getByRole("button", { name: "Add to Press" });
  await t.visible(addPress, "an empty marked list offers Add");
  if (t.browserName === "chrome") await t.shot(page, "edit-site-empty-list-desktop-light");
  await addPress.click();
  const tools = page.locator("[data-edit-tools]");
  await t.visible(tools.getByText("Press"), "the new item is selected in the bar");
  await page.keyboard.type("The Daily");
  await tools.getByLabel("Link").fill("https://daily.example.com");
  await saveEdits(t, page);
  t.equal(listItems("press").length, 1, "the first item is saved");
  t.equal(listItems("press")[0].outlet, "The Daily", "with what was typed in place");
  await t.visible(site(page).locator("[data-cms-item='press.0']"), "and the site shows it");
  t.equal(await site(page).getByRole("button", { name: "Add to Press" }).count(), 0, "a list with items has no empty Add");

  // A photo list asks for the image first.
  const addShot = site(page).getByRole("button", { name: "Add to Shots" });
  await addImageByButton(t, page, imageFile(t.fixture.files, "shot.png"), { click: () => addShot.click() });
  await t.eventually(async () => !(await saveButton(page).isDisabled()), "the photo is ready to save");
  await t.visible(tools.getByRole("link", { name: "All photos" }), "a photo list's bar links to All photos");
  await saveEdits(t, page);
  t.check(listItems("shots")[0]?.image.startsWith("/n-admin/media/"), "the first photo is saved");
});
