// A small harness: scenarios run one after another in one browser, each in fresh contexts, and every
// page fails the scenario on an uncaught error, a console error from the admin, or sideways scroll.
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { OWNER_PASSWORD } from "./fixture.mjs";

export const DESKTOP = { viewport: { width: 1280, height: 800 } };
export const PHONE = { viewport: { width: 390, height: 780 }, hasTouch: true, isMobile: true, deviceScaleFactor: 2 };

const scenarios = [];
export const scenario = (name, run) => scenarios.push({ name, run });
export const allScenarios = () => scenarios;

export class Failure extends Error {}

/** Everything a scenario uses: the browser, the site, checks, and the pages it opened. */
export class Run {
  constructor({ browser, browserName, fixture, shots }) {
    Object.assign(this, { browser, browserName, fixture, shots });
    this.assertions = 0;
    this.contexts = [];
    this.problems = [];
    this.ownerState = null;
    this.ownerPassword = OWNER_PASSWORD;
  }

  get origin() {
    return this.fixture.origin;
  }

  check(condition, message) {
    this.assertions += 1;
    if (!condition) throw new Failure(message);
  }

  equal(actual, expected, message) {
    this.assertions += 1;
    if (actual !== expected) throw new Failure(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }

  /** Waits for a condition evaluated in Node, such as a locator's text. */
  async eventually(probe, message, timeout = 5000) {
    this.assertions += 1;
    const until = Date.now() + timeout;
    let last;
    for (;;) {
      try {
        last = await probe();
        if (last) return last;
      } catch (error) {
        last = error.message;
      }
      if (Date.now() > until) throw new Failure(`${message} (last: ${JSON.stringify(last)})`);
      await new Promise((done) => setTimeout(done, 50));
    }
  }

  async seeText(locator, text, message = `shows "${text}"`) {
    await this.eventually(async () => (await locator.textContent())?.includes(text), message);
  }

  async visible(locator, message) {
    this.assertions += 1;
    try {
      await locator.first().waitFor({ state: "visible", timeout: 5000 });
    } catch {
      throw new Failure(message);
    }
  }

  async hidden(locator, message) {
    this.assertions += 1;
    try {
      await locator.first().waitFor({ state: "hidden", timeout: 5000 });
    } catch {
      throw new Failure(message);
    }
  }

  /** A context with the run's error checks. `owner: true` starts it logged in as the owner. */
  async context({ size = DESKTOP, dark = false, owner = true, state } = {}) {
    if (owner && !state && !this.ownerState) await this.logInOwner();
    const context = await this.browser.newContext({
      ...size,
      colorScheme: dark ? "dark" : "light",
      storageState: state ?? (owner ? this.ownerState : undefined),
      permissions: this.browserName === "chrome" ? ["clipboard-read", "clipboard-write"] : [],
    });
    // No network: players and other sites get a blank page.
    await context.route((url) => !url.href.startsWith(this.origin) && /^https?:/.test(url.protocol), (route) =>
      route.fulfill({ status: 200, contentType: route.request().resourceType() === "image" ? "image/png" : "text/html", body: route.request().resourceType() === "image" ? "" : "<!doctype html><title>Elsewhere</title>" }));
    context.on("page", (page) => this.watch(page));
    this.contexts.push(context);
    return context;
  }

  async page(options) {
    const context = await this.context(options);
    return context.newPage();
  }

  watch(page) {
    page.on("pageerror", (error) => this.problems.push(`Uncaught error on ${page.url()}: ${error.message}`));
    page.on("console", (message) => {
      if (message.type() !== "error") return;
      const where = message.location()?.url ?? "";
      // Missing images are the network's business; script and CSP errors are ours.
      if (/^Failed to load resource/.test(message.text())) return;
      if (where && !where.startsWith(`${this.origin}/n-admin`) && !where.startsWith("about:")) return;
      this.problems.push(`Console error on ${page.url()}: ${message.text()}`);
    });
    page.on("dialog", (dialog) => {
      // Confirms are answered by the scenario through `page.answer`; otherwise they are accepted.
      const answer = page.answers?.shift();
      if (answer === undefined) return dialog.accept();
      return answer ? dialog.accept() : dialog.dismiss();
    });
    page.answers = [];
  }

  async logInOwner() {
    const context = await this.browser.newContext(DESKTOP);
    const page = await context.newPage();
    this.watch(page);
    await page.goto(`${this.origin}/n-admin/login`);
    if (await page.locator("input[name=name]").count()) await page.locator("input[name=name]").fill("");
    await page.locator("input[name=password]").fill(this.ownerPassword);
    await page.getByRole("button", { name: "Log in" }).click();
    await page.waitForURL(`${this.origin}/n-admin`);
    this.ownerState = await context.storageState();
    await context.close();
  }

  /** Opens an admin path and checks the page fits the screen. */
  async go(page, path) {
    const response = await page.goto(`${this.origin}${path}`);
    this.check(response && response.status() < 400, `${path} answered ${response?.status()}`);
    await this.fits(page, path);
    return response;
  }

  async fits(page, what = page.url()) {
    // Fonts and late layout settle first.
    await page.evaluate(() => document.fonts?.ready);
    const { scroll, width } = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, width: innerWidth }));
    this.check(scroll <= width, `${what} scrolls sideways at ${width}px (${scroll}px wide)`);
  }

  async shot(page, name) {
    mkdirSync(this.shots, { recursive: true });
    await page.screenshot({ path: join(this.shots, `${name}.png`) });
  }

  /**
   * Presses Tab `times` times in an open modal dialog: focus must stay in it, or leave the document
   * for the browser's own controls as a native modal allows, but never reach the page behind it.
   */
  async trapped(page, dialog, times) {
    for (let n = 0; n < times; n++) {
      await page.keyboard.press("Tab");
      const inside = await dialog.evaluate((el) => el.contains(document.activeElement) || document.activeElement === document.body || !document.hasFocus());
      if (!inside) return false;
    }
    return true;
  }

  async closeAll() {
    for (const context of this.contexts.splice(0)) await context.close().catch(() => {});
  }
}

/** Puts files on the page as a paste or a drop, the way the clipboard and a drag would. */
export async function sendFiles(page, selector, kind, files, point) {
  const payload = files.map((file) => ({ name: file.name, type: "image/png", data: file.data.toString("base64") }));
  await page.evaluate(({ selector, kind, payload, point }) => {
    const target = document.querySelector(selector);
    const transfer = new DataTransfer();
    for (const file of payload) {
      const bytes = Uint8Array.from(atob(file.data), (c) => c.charCodeAt(0));
      transfer.items.add(new File([bytes], file.name, { type: file.type }));
    }
    const box = target.getBoundingClientRect();
    const at = point ?? { x: box.left + box.width / 2, y: box.top + Math.min(box.height / 2, 40) };
    const event = kind === "paste"
      ? new Event("paste", { bubbles: true, cancelable: true })
      : new Event(kind, { bubbles: true, cancelable: true });
    Object.defineProperty(event, kind === "paste" ? "clipboardData" : "dataTransfer", { value: transfer });
    if (kind !== "paste") {
      Object.defineProperty(event, "clientX", { value: at.x });
      Object.defineProperty(event, "clientY", { value: at.y });
    }
    target.dispatchEvent(event);
  }, { selector, kind, payload, point });
}

/** Pastes text the way a clipboard does. */
export async function pasteText(page, selector, text, html = "") {
  await page.evaluate(({ selector, text, html }) => {
    const transfer = new DataTransfer();
    transfer.setData("text/plain", text);
    if (html) transfer.setData("text/html", html);
    const event = new Event("paste", { bubbles: true, cancelable: true });
    Object.defineProperty(event, "clipboardData", { value: transfer });
    document.querySelector(selector).dispatchEvent(event);
  }, { selector, text, html });
}

/** Answers the file picker the next click opens. */
export async function pickFiles(page, click, paths) {
  const [chooser] = await Promise.all([page.waitForEvent("filechooser", { timeout: 5000 }), click()]);
  await chooser.setFiles(paths);
}
