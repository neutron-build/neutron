// Runs in the admin page and reaches into the framed site, so visitors never receive any of it.
// Uses `$`, `cms`, `uploadImage` and `pickImages` from admin.client.js.

const frame = $("[data-edit-frame]");
const tools = $("[data-edit-tools]");
const state = $("[data-edit-state]");
const saveButton = $("[data-edit-save]");
const discardButton = $("[data-edit-discard]");

const HINT = "Click any text or image to change it";

// Lives only in the frame, in memory: a constructed sheet touches neither the site's CSS nor its layout.
const PAGE_STYLES = `
  [data-cms-field]:hover { outline: 2px solid rgb(47 128 237 / .6); outline-offset: 2px; }
  [data-cms-field]:focus { outline: 2px solid #2f80ed; outline-offset: 2px; }
  [data-cms-selected] { outline: 1px dashed #2f80ed; outline-offset: 6px; }
  img[data-cms-field]:not([src]) { display: block; min-width: 120px; min-height: 90px; background: rgb(47 128 237 / .08); outline: 2px dashed rgb(47 128 237 / .6); cursor: pointer; }
  [contenteditable] { cursor: text; }
  [contenteditable]:empty { display: inline-block; min-width: 4em; }
  [contenteditable]:empty::before { content: attr(data-cms-placeholder); opacity: .5; }
  img[data-cms-field] { cursor: pointer; }
  img[data-cms-field]:not([src]) { min-width: 80px; min-height: 80px; background: rgb(128 128 128 / .3); }
`;

let data = null;
let page = null;
let selected = null;
let dirty = false;
let pending = 0;
let notice = "";
let bad = false;
let justSaved = false;
let touchedTexts = {};
let touchedLists = new Set();
const itemOf = new WeakMap();

function show() {
  saveButton.disabled = pending > 0 || !dirty;
  discardButton.disabled = !dirty;
  state.classList.toggle("bad", bad);
  state.textContent = notice || (pending ? `Uploading, ${pending} left` : dirty ? "Unsaved changes" : selected ? "" : HINT);
}

function say(message, isBad = false) {
  notice = message;
  bad = isBad;
  show();
}

function changed() {
  dirty = true;
  notice = "";
  bad = false;
  show();
}

function parseItem(el) {
  const value = el.getAttribute("data-cms-item");
  const at = value.lastIndexOf(".");
  return { key: value.slice(0, at), index: Number(value.slice(at + 1)) };
}

async function replaceImage(file, onDone, el) {
  const preview = el ? URL.createObjectURL(file) : null;
  const previous = el?.getAttribute("src");
  if (el) {
    el.removeAttribute("srcset");
    el.removeAttribute("sizes");
    el.src = preview;
  }
  pending += 1;
  changed();
  try {
    const { url, thumb } = await uploadImage(file);
    if (el) el.src = thumb;
    onDone(url);
    pending -= 1;
    show();
  } catch (error) {
    if (el && previous) el.src = previous;
    pending -= 1;
    say(`${file.name}: ${error.message}`, true);
  }
  if (preview) URL.revokeObjectURL(preview);
}

/** A field on the page: words are typed in place, an image is clicked. */
function bindField(el, field, write) {
  el.setAttribute("data-cms-field", "");
  if (field.type === "image") {
    el.title = `Click to replace this ${field.label.toLowerCase()}`;
    el.addEventListener("click", async (event) => {
      event.preventDefault();
      const [file] = await pickImages();
      if (file) replaceImage(file, write, el);
    });
    return;
  }
  el.setAttribute("contenteditable", "plaintext-only");
  el.setAttribute("data-cms-placeholder", field.label);
  el.spellcheck = false;
  el.addEventListener("input", () => {
    // The browser leaves a stray line break in an emptied field, which would hide the placeholder.
    if (!el.textContent.trim()) el.textContent = "";
    write(el.innerText.trim());
    changed();
  });
  el.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && field.type !== "textarea") event.preventDefault();
  });
}

const isInline = (field) => field.type === "text" || field.type === "textarea" || field.type === "image";

function boundFields(itemEl) {
  const inside = [...itemEl.querySelectorAll("[data-cms]"), ...(itemEl.matches("[data-cms]") ? [itemEl] : [])];
  return inside.filter((el) => el.closest("[data-cms-item]") === itemEl);
}

function bindItem(itemEl, key, item) {
  const list = data.lists[key];
  itemOf.set(itemEl, { key, item });
  for (const el of boundFields(itemEl)) {
    const name = el.getAttribute("data-cms");
    const field = list.fields[name];
    if (field && isInline(field)) {
      bindField(el, field, (value) => {
        item[name] = value;
        touchedLists.add(key);
      });
    }
  }
}

function rendered(key) {
  return [...page.querySelectorAll("[data-cms-item]")].filter((el) => itemOf.get(el)?.key === key);
}

/** Add, remove and move need every item on the page, once, side by side. A filtered list only edits fields. */
function decideArrangeable(key) {
  const list = data.lists[key];
  const elements = rendered(key);
  list.arrangeable = !list.sortBy && elements.length === list.items.length && new Set(elements.map((el) => el.parentElement)).size === 1;
}

function select(itemEl) {
  if (selected === itemEl) return;
  selected?.removeAttribute("data-cms-selected");
  selected = itemEl;
  selected?.setAttribute("data-cms-selected", "");
  drawTools();
  show();
}

function button(text, label, action, disabled = false) {
  const el = Object.assign(document.createElement("button"), { type: "button", className: "button", textContent: text, disabled });
  el.setAttribute("aria-label", label);
  el.addEventListener("click", action);
  return el;
}

function arrangeTools(key, list) {
  const touch = () => {
    touchedLists.add(key);
    changed();
    drawTools();
  };
  const siblings = rendered(key);
  const at = siblings.indexOf(selected);
  const next = siblings[at + 1] ?? siblings[at - 1];
  const sideways = next && Math.abs(next.getBoundingClientRect().top - selected.getBoundingClientRect().top) < 8;

  return [
    button(sideways ? "←" : "↑", "Move earlier", () => {
      siblings[at - 1].before(selected);
      touch();
    }, at === 0),
    button(sideways ? "→" : "↓", "Move later", () => {
      siblings[at + 1].after(selected);
      touch();
    }, at === siblings.length - 1),
    button("Add", "Add another", () => {
      const clone = selected.cloneNode(true);
      for (const el of [clone, ...clone.querySelectorAll("*")]) {
        for (const name of ["contenteditable", "data-cms-field", "data-cms-selected", "data-cms-placeholder", "title"]) el.removeAttribute(name);
      }
      for (const el of boundFields(clone)) {
        // An empty image, not a copy of the last one's description or a broken-image icon.
        if (el.tagName === "IMG") for (const name of ["src", "srcset", "sizes", "width", "height", "alt"]) el.removeAttribute(name);
        else el.textContent = "";
      }
      selected.after(clone);
      bindItem(clone, key, {});
      select(clone);
      touchedLists.add(key);
      changed();
      boundFields(clone).find((el) => el.isContentEditable)?.focus();
    }),
    button("Remove", "Remove this", () => {
      if (rendered(key).length === 1) return say(`This is the last one, so it stays. The ${list.label} list page can empty it.`, true);
      const going = selected;
      select(null);
      going.remove();
      touchedLists.add(key);
      changed();
    }),
  ];
}

function drawTools() {
  tools.textContent = "";
  tools.hidden = !selected;
  if (!selected) return;
  const { key, item } = itemOf.get(selected);
  const list = data.lists[key];

  tools.append(Object.assign(document.createElement("strong"), { textContent: list.label }));

  // Whatever the page doesn't show, such as a link's address, is edited here.
  const shown = new Set(boundFields(selected).map((el) => el.getAttribute("data-cms")));
  for (const [name, field] of Object.entries(list.fields)) {
    if (shown.has(name) && isInline(field)) continue;
    if (field.type === "image") {
      tools.append(button(`Change ${field.label.toLowerCase()}`, field.label, async () => {
        const [file] = await pickImages();
        if (file) {
          replaceImage(file, (url) => {
            item[name] = url;
            touchedLists.add(key);
          });
        }
      }));
      continue;
    }
    const input = Object.assign(document.createElement("input"), {
      className: "input",
      type: field.type === "date" ? "date" : "text",
      value: item[name] ?? "",
    });
    input.setAttribute("aria-label", field.label);
    input.spellcheck = false;
    input.addEventListener("input", () => {
      item[name] = input.value.trim();
      touchedLists.add(key);
      changed();
    });
    const label = Object.assign(document.createElement("label"), { className: "edit-field" });
    label.append(field.label, input);
    tools.append(label);
  }

  if (list.arrangeable) tools.append(...arrangeTools(key, list));
}

function bindPage() {
  page = frame.contentDocument;
  selected = null;
  drawTools();

  const sheet = new frame.contentWindow.CSSStyleSheet();
  sheet.replaceSync(PAGE_STYLES);
  page.adoptedStyleSheets = [...page.adoptedStyleSheets, sheet];

  for (const el of page.querySelectorAll("[data-cms-text]")) {
    const name = el.getAttribute("data-cms-text");
    const field = data.textFields[name];
    if (field) {
      bindField(el, field, (value) => {
        touchedTexts[name] = value;
      });
    }
  }
  for (const el of page.querySelectorAll("[data-cms-item]")) {
    const { key, index } = parseItem(el);
    const item = data.lists[key]?.items[index];
    if (item) bindItem(el, key, item);
  }
  Object.keys(data.lists).forEach(decideArrangeable);

  page.addEventListener("click", (event) => {
    const target = event.target;
    const itemEl = target.closest("[data-cms-item]");
    select(itemEl && itemOf.has(itemEl) ? itemEl : null);
    if (notice && !bad) say("");

    const link = target.closest("a[href]");
    if (!link) return;
    const internal = new URL(link.href).origin === location.origin;
    if (itemEl || target.closest("[data-cms-text]")) {
      event.preventDefault();
    } else if (!internal) {
      event.preventDefault();
      say("Links to other sites are off while editing.");
    } else if (dirty && !confirm("Leave this page? Your unsaved changes will be lost.")) {
      event.preventDefault();
    }
  }, true);
  page.addEventListener("submit", (event) => event.preventDefault(), true);
  page.addEventListener("keydown", shortcuts);
}

async function save(after) {
  if (saveButton.disabled) return after?.();
  page.activeElement?.blur?.();
  const lists = {};
  for (const key of touchedLists) {
    const list = data.lists[key];
    lists[key] = {
      revision: list.revision,
      items: list.arrangeable ? rendered(key).map((el) => itemOf.get(el).item) : list.items,
    };
  }
  say("Saving…");
  saveButton.disabled = true;
  try {
    const response = await cms("edit/save", JSON.stringify({ texts: touchedTexts, lists }));
    const result = await response.json().catch(() => ({}));
    if (response.status === 401) return say("Signed out. Log in again in another tab, then save. Nothing was lost.", true);
    if (!response.ok) return say(result.error ?? "Could not save", true);
    dirty = false;
    justSaved = true;
    $("[data-edit-history]")?.removeAttribute("hidden");
    if (after) return after();
    openPage(true);
  } catch {
    say("Offline. Nothing was lost. Try saving again.", true);
  }
}

function shortcuts(event) {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "s") {
    event.preventDefault();
    save();
  }
  if (event.key === "Escape") {
    page?.activeElement?.blur?.();
    select(null);
  }
}

// The content is read before the page that shows it, never after: a change landing in between then
// makes the save stale and refused, instead of lining edits up against items the page doesn't show.
let fresh = null;

async function readData() {
  let response;
  try {
    response = await fetch(`${ADMIN}/edit/data`, { headers: { "x-cms": "1" } });
  } catch {
    // Offline, or the page is being left; Safari reports the second as a failure too.
    say("Couldn't reach the site. Check the connection, then reload.", true);
    return null;
  }
  if (response.status === 401) {
    location.href = `${ADMIN}/login`;
    return null;
  }
  return response.json();
}

async function openPage(reload) {
  fresh = await readData();
  if (!fresh) return;
  if (reload) frame.contentWindow.location.reload();
  else frame.src = frame.dataset.src;
}

frame.addEventListener("load", async () => {
  if (!frame.getAttribute("src")) return;
  // A link followed inside the frame arrives without fresh content, so read it now.
  data = fresh ?? (await readData());
  fresh = null;
  if (!data) return;
  dirty = false;
  touchedTexts = {};
  touchedLists = new Set();
  try {
    if (!frame.contentDocument?.body) throw new Error("blocked");
    bindPage();
    // A page whose content comes from elsewhere, such as a README, has nothing to edit here.
    const editable = page.querySelector("[data-cms-field], [data-cms-item]");
    saveButton.hidden = discardButton.hidden = !editable;
    say(justSaved ? "Saved" : editable ? "" : "Nothing on this page is edited here.");
  } catch {
    say("This site doesn't allow being framed. Set X-Frame-Options to SAMEORIGIN and frame-ancestors to 'self'.", true);
  }
  justSaved = false;
});

saveButton.addEventListener("click", () => save());

discardButton.addEventListener("click", () => {
  if (!confirm("Discard your changes?")) return;
  dirty = false;
  openPage(true);
});

// Publishing a page saves its unsaved edits first, so what goes live is what's on screen.
const publishForm = document.querySelector(".publish-form");
publishForm?.addEventListener("submit", (event) => {
  if (!dirty && !pending) return;
  event.preventDefault();
  if (pending) return say("Wait for the images to finish uploading.", true);
  const submitter = event.submitter;
  save(() => {
    // The saved edits aren't changes any more, so leaving for the published page doesn't warn.
    dirty = false;
    publishForm.requestSubmit(submitter);
  });
});

openPage(false);

addEventListener("keydown", shortcuts);
addEventListener("beforeunload", (event) => {
  if (dirty || pending) event.preventDefault();
});
