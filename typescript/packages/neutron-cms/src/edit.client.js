// Runs in the admin page and reaches into the framed site, so visitors never receive any of it.
// Uses `$`, `cms`, `uploadImage` and `pickImages` from admin.client.js.

const frame = $("[data-edit-frame]");
const tools = $("[data-edit-tools]");
const state = $("[data-edit-state]");
const saveButton = $("[data-edit-save]");
const discardButton = $("[data-edit-discard]");

const ACCENT = "#2f80ed";

let data = null;
let page = null;
let selected = null;
let dirty = false;
let pending = 0;
let notice = "";
let touchedTexts = {};
let touchedLists = new Set();
const itemOf = new WeakMap();

function show() {
  saveButton.disabled = pending > 0 || !dirty;
  discardButton.disabled = !dirty;
  state.classList.toggle("bad", notice.startsWith("!"));
  state.textContent = notice.replace(/^!/, "") || (pending ? `Uploading, ${pending} left` : dirty ? "Unsaved changes" : "Click anything outlined to edit it");
}

function changed() {
  dirty = true;
  notice = "";
  show();
}

const parseItem = (el) => {
  const value = el.getAttribute("data-cms-item");
  const at = value.lastIndexOf(".");
  return { key: value.slice(0, at), index: Number(value.slice(at + 1)) };
};

function outline(el, color) {
  el.style.outline = color ? `2px solid ${color}` : "";
  el.style.outlineOffset = color ? "2px" : "";
}

function hoverable(el) {
  el.addEventListener("mouseenter", () => { if (el !== page.activeElement) outline(el, `${ACCENT}99`); });
  el.addEventListener("mouseleave", () => { if (el !== page.activeElement && el !== selected) outline(el, ""); });
}

/** A field on the page: words are typed in place, an image is clicked. */
function bindField(el, field, write) {
  if (field.type === "image") {
    el.dataset.cmsPick = "";
    el.style.cursor = "pointer";
    el.addEventListener("click", async (event) => {
      event.preventDefault();
      const [file] = await pickImages();
      if (!file) return;
      const preview = URL.createObjectURL(file);
      el.removeAttribute("srcset");
      el.removeAttribute("sizes");
      el.src = preview;
      pending += 1;
      changed();
      try {
        const { url, thumb } = await uploadImage(file);
        el.src = thumb;
        write(url);
      } catch (error) {
        notice = `!${file.name}: ${error.message}`;
      }
      URL.revokeObjectURL(preview);
      pending -= 1;
      show();
    });
  } else {
    el.setAttribute("contenteditable", "plaintext-only");
    el.spellcheck = true;
    el.style.minWidth = "1em";
    el.style.minHeight = "1em";
    el.style.cursor = "text";
    el.addEventListener("input", () => {
      write(el.innerText.trim());
      changed();
      place();
    });
    el.addEventListener("keydown", (event) => {
      if (event.key === "Escape") el.blur();
      if (event.key === "Enter" && field.type !== "textarea") event.preventDefault();
    });
    el.addEventListener("focus", () => outline(el, ACCENT));
    el.addEventListener("blur", () => outline(el, ""));
  }
  hoverable(el);
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
  if (selected) outline(selected, "");
  selected = itemEl;
  if (selected) outline(selected, ACCENT);
  drawTools();
}

function place() {
  if (!selected || !selected.isConnected) return;
  const box = selected.getBoundingClientRect();
  const height = tools.offsetHeight;
  const top = box.top - height - 8;
  tools.style.top = `${frame.offsetTop + Math.max(4, top < 0 ? box.bottom + 8 : top)}px`;
  tools.style.left = `${frame.offsetLeft + Math.max(4, Math.min(box.left, frame.clientWidth - tools.offsetWidth - 4))}px`;
}

function button(text, label, action, disabled = false) {
  const el = Object.assign(document.createElement("button"), { type: "button", className: "button", textContent: text, disabled });
  el.setAttribute("aria-label", label);
  el.addEventListener("click", action);
  return el;
}

function drawTools() {
  tools.textContent = "";
  tools.hidden = !selected;
  if (!selected) return;
  const { key, item } = itemOf.get(selected);
  const list = data.lists[key];

  const title = Object.assign(document.createElement("strong"), { textContent: list.label });
  tools.append(title);

  // Whatever the page doesn't show, such as a link's address, is edited here.
  const shown = new Set(boundFields(selected).map((el) => el.getAttribute("data-cms")));
  for (const [name, field] of Object.entries(list.fields)) {
    if (shown.has(name) && isInline(field)) continue;
    if (field.type === "image") {
      tools.append(button(`Change ${field.label.toLowerCase()}`, field.label, async () => {
        const [file] = await pickImages();
        if (!file) return;
        pending += 1;
        changed();
        try {
          item[name] = (await uploadImage(file)).url;
          touchedLists.add(key);
        } catch (error) {
          notice = `!${file.name}: ${error.message}`;
        }
        pending -= 1;
        show();
      }));
      continue;
    }
    const input = Object.assign(document.createElement("input"), {
      className: "input",
      type: field.type === "date" ? "date" : "text",
      value: item[name] ?? "",
      placeholder: field.label,
    });
    input.setAttribute("aria-label", field.label);
    input.addEventListener("input", () => {
      item[name] = input.value.trim();
      touchedLists.add(key);
      changed();
    });
    tools.append(input);
  }

  if (list.arrangeable) {
    const touch = () => {
      touchedLists.add(key);
      changed();
      place();
    };
    tools.append(button("←", "Move earlier", () => {
      const before = selected.previousElementSibling;
      if (before && itemOf.has(before)) before.before(selected);
      touch();
    }));
    tools.append(button("→", "Move later", () => {
      const after = selected.nextElementSibling;
      if (after && itemOf.has(after)) after.after(selected);
      touch();
    }));
    tools.append(button("Add", "Add another", () => {
      const clone = selected.cloneNode(true);
      for (const el of [clone, ...clone.querySelectorAll("*")]) {
        el.removeAttribute("contenteditable");
        el.removeAttribute("style");
      }
      for (const el of boundFields(clone)) {
        if (el.tagName === "IMG") {
          el.removeAttribute("srcset");
          el.removeAttribute("sizes");
          el.removeAttribute("src");
          el.style.minHeight = "4em";
        } else {
          el.textContent = "";
        }
      }
      selected.after(clone);
      bindItem(clone, key, {});
      select(clone);
      touch();
      const first = boundFields(clone).find((el) => el.isContentEditable);
      first?.focus();
    }));
    tools.append(button("Remove", "Remove this", () => {
      if (rendered(key).length === 1) {
        notice = `!This is the last one. Use the ${list.label} list page to empty it.`;
        return show();
      }
      const going = selected;
      select(null);
      going.remove();
      touch();
    }));
  }
  place();
}

function bindPage() {
  page = frame.contentDocument;
  selected = null;
  drawTools();

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

    const link = target.closest("a[href]");
    if (!link) return;
    const internal = new URL(link.href).origin === location.origin;
    if (itemEl || target.closest("[data-cms-text]") || !internal) {
      event.preventDefault();
      if (!itemEl && !internal) {
        notice = "Links to other sites are off while editing.";
        show();
      }
    } else if (dirty && !confirm("Leave this page? Your unsaved changes will be lost.")) {
      event.preventDefault();
    }
  }, true);
  page.addEventListener("submit", (event) => event.preventDefault(), true);
  page.addEventListener("scroll", place, true);
  frame.contentWindow.addEventListener("resize", place);
}

async function load() {
  const response = await fetch(`${ADMIN}/edit/data`, { headers: { "x-cms": "1" } });
  if (response.status === 401) {
    location.href = `${ADMIN}/login`;
    return;
  }
  data = await response.json();
  dirty = false;
  touchedTexts = {};
  touchedLists = new Set();
  notice = "";
  show();
}

frame.addEventListener("load", async () => {
  await load();
  try {
    if (!frame.contentDocument?.body) throw new Error("blocked");
    bindPage();
  } catch {
    notice = `!This site doesn't allow being framed. Set X-Frame-Options to SAMEORIGIN and frame-ancestors to 'self'.`;
    show();
  }
});

saveButton.addEventListener("click", async () => {
  page.activeElement?.blur?.();
  const lists = {};
  for (const key of touchedLists) {
    const list = data.lists[key];
    lists[key] = {
      revision: list.revision,
      items: list.arrangeable ? rendered(key).map((el) => itemOf.get(el).item) : list.items,
    };
  }
  notice = "Saving…";
  show();
  try {
    const response = await cms("edit/save", JSON.stringify({ texts: touchedTexts, lists }));
    const result = await response.json().catch(() => ({}));
    if (response.status === 401) notice = `!Signed out. Log in again in another tab, then save. Nothing was lost.`;
    else if (!response.ok) notice = `!${result.error ?? "Could not save"}`;
    else {
      dirty = false;
      frame.contentWindow.location.reload();
      return;
    }
  } catch {
    notice = "!Offline. Nothing was lost. Try saving again.";
  }
  show();
});

discardButton.addEventListener("click", () => {
  if (!confirm("Discard your changes?")) return;
  dirty = false;
  frame.contentWindow.location.reload();
});

addEventListener("beforeunload", (event) => {
  if (dirty || pending) event.preventDefault();
});
addEventListener("resize", place);
