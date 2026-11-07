const ADMIN = new URL(".", document.currentScript.src).pathname.replace(/\/$/, "");
const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const cms = (path, body) => fetch(`${ADMIN}/${path}`, { method: "POST", body, headers: { "x-cms": "1" } });

document.addEventListener("click", (event) => {
  const message = event.target.closest("[data-confirm]")?.dataset.confirm;
  if (message && !confirm(message)) event.preventDefault();
});

// Images are re-encoded here: phone photos shrink, and EXIF location data never leaves the device.

const MAX_SIDE = 2000;
const THUMB_WIDTH = 800; // images.ts decides the same on the server.

function encode(source, width, height, type) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext("2d");
  context.imageSmoothingQuality = "high";
  context.drawImage(source, 0, 0, width, height);
  return new Promise((resolve) => canvas.toBlob(resolve, type, 0.86));
}

async function prepareImage(file) {
  if (file.type === "image/gif") return { file };
  try {
    const bitmap = await createImageBitmap(file);
    const scale = Math.min(1, MAX_SIDE / Math.max(bitmap.width, bitmap.height));
    const width = Math.round(bitmap.width * scale);
    const height = Math.round(bitmap.height * scale);
    let full = await encode(bitmap, width, height, "image/webp");
    if (full?.type !== "image/webp") full = await encode(bitmap, width, height, "image/jpeg");
    const thumb = width > THUMB_WIDTH
      ? await encode(bitmap, THUMB_WIDTH, Math.round((height * THUMB_WIDTH) / width), full.type)
      : null;
    bitmap.close();
    return { file: full, thumb, width, height };
  } catch {
    return { file };
  }
}

async function uploadImage(picked) {
  const prepared = await prepareImage(picked);
  const form = new FormData();
  for (const [key, value] of Object.entries(prepared)) if (value) form.set(key, value);
  const response = await cms("upload", form);
  if (response.status === 401) throw new Error("Signed out. Log in again in another tab, then retry.");
  if (!response.ok) throw new Error((await response.json().catch(() => ({}))).error ?? "Upload failed");
  return response.json();
}

function pickImages(multiple = false) {
  return new Promise((resolve) => {
    const input = Object.assign(document.createElement("input"), { type: "file", accept: "image/*", multiple });
    input.addEventListener("change", () => resolve([...input.files]));
    input.click();
  });
}

const imageFiles = (list) => [...list].filter((file) => file.type.startsWith("image/"));

// The visual editor uploads through the same pipeline.
window.cmsUpload = uploadImage;

const editor = $("[data-editor]");
if (editor) startEditor(editor);

function startEditor(form) {
  const state = $("[data-state]");
  const revision = $("[name=revision]", form);
  const title = $("[name=title]", form);
  const body = $("[name=body]", form);
  const slug = $("[name=slug]");
  const local = $("[name=date_local]");
  const date = $("[name=date]", form);
  const publish = $("[data-publish]");
  const writing = $(".writing");
  const frame = $("[data-preview]");
  const storageKey = `n-admin:post:${form.dataset.editor}`;
  const isDraft = form.dataset.status === "draft";

  let dirty = false;
  let saving = false;
  let timer;

  const store = {
    read() {
      try { return JSON.parse(localStorage.getItem(storageKey)); } catch { return null; }
    },
    write() {
      try { localStorage.setItem(storageKey, JSON.stringify({ title: title.value, body: body.value })); } catch {}
    },
    clear() {
      try { localStorage.removeItem(storageKey); } catch {}
    },
  };

  // Words a reader sees: image addresses, link targets and Markdown marks don't count.
  const design = "design" in form.dataset;
  const words = () => {
    if (design) return "";
    const text = body.value
      .replace(/!\[[^\]]*\]\([^)]*\)/g, " ")
      .replace(/\]\([^)]*\)/g, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/[#*_>`~[\]|-]+/g, " ")
      .trim();
    const count = text ? text.split(/\s+/).length : 0;
    return `${count.toLocaleString()} ${count === 1 ? "word" : "words"}`;
  };

  function show(message, bad = false) {
    state.classList.toggle("bad", bad);
    state.textContent = message;
  }

  function signedOut() {
    show("Signed out. Your text is safe in this browser. ", true);
    state.append(Object.assign(document.createElement("a"), { href: `${ADMIN}/login`, target: "_blank", textContent: "Log in" }));
  }

  async function autosave() {
    if (!dirty || saving) return;
    saving = true;
    dirty = false;
    syncDate();
    const data = new FormData(form);
    data.set("intent", "save");
    show("Saving…");
    try {
      const response = await cms(`posts/${form.dataset.editor}`, data);
      const result = await response.json().catch(() => ({}));
      if (response.status === 401) {
        dirty = true;
        signedOut();
      } else if (response.status === 409 && result.error === "stale") {
        dirty = true;
        show("Changed elsewhere. Your text is safe in this browser. Reload to compare.", true);
      } else if (!response.ok || !result.ok) {
        dirty = true;
        show(result.message ?? "Could not save", true);
      } else {
        revision.value = result.revision;
        if (!dirty) store.clear();
        show(design ? "Saved" : `Saved, ${words()}`);
      }
    } catch {
      dirty = true;
      show("Offline. Your text is safe in this browser.", true);
    }
    saving = false;
    if (dirty && !state.classList.contains("bad")) schedule();
  }

  function schedule() {
    clearTimeout(timer);
    if (isDraft) timer = setTimeout(autosave, 1500);
  }

  function changed() {
    dirty = true;
    store.write();
    show(isDraft ? words() : design ? "Unsaved changes" : `Unsaved changes, ${words()}`);
    schedule();
  }

  // Where the browser can't size a textarea to its text, do it here, or long text would be cut off.
  const fits = CSS.supports("field-sizing", "content");
  function fit(area) {
    if (fits) return;
    const scroll = scrollY;
    area.style.height = "auto";
    area.style.height = `${area.scrollHeight}px`;
    scrollTo(0, scroll);
  }
  const fitAll = () => {
    fit(title);
    fit(body);
  };
  fitAll();
  addEventListener("resize", fitAll);
  body.addEventListener("cms:resize", () => fit(body));
  document.fonts?.ready.then(fitAll);

  title.addEventListener("input", () => {
    fit(title);
    changed();
  });
  body.addEventListener("input", () => {
    fit(body);
    changed();
  });

  // Settings slide over the page and leave when you go back to writing.

  const metaToggle = $("[data-toggle-meta]");
  const meta = $(".meta");
  function setMeta(open) {
    form.toggleAttribute("data-meta-open", open);
    metaToggle.setAttribute("aria-expanded", String(open));
  }
  metaToggle.addEventListener("click", () => setMeta(!form.hasAttribute("data-meta-open")));
  document.addEventListener("click", (event) => {
    if (form.hasAttribute("data-meta-open") && !meta.contains(event.target) && !metaToggle.contains(event.target)) setMeta(false);
  });
  addEventListener("keydown", (event) => {
    if (event.key === "Escape" && form.hasAttribute("data-meta-open")) {
      setMeta(false);
      metaToggle.focus();
    }
  });

  if (slug && !slug.disabled) {
    const fromTitle = (text) => text.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80);
    const syncSlug = () => { slug.placeholder = fromTitle(title.value) || "from-the-title"; };
    title.addEventListener("input", syncSlug);
    syncSlug();
  }

  // Dates are UTC on the server and local in this field.

  const pad = (n) => String(n).padStart(2, "0");
  const toLocal = (d) => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  if (date.value) local.value = toLocal(new Date(date.value));

  function syncDate() {
    date.value = local.value ? new Date(local.value).toISOString() : "";
  }

  function syncPublishLabel() {
    if (!publish) return;
    const future = local.value && new Date(local.value) > new Date();
    publish.textContent = future ? "Schedule" : "Publish";
  }

  const markEmpty = () => local.classList.toggle("empty", !local.value);
  markEmpty();
  local.addEventListener("input", () => {
    markEmpty();
    syncPublishLabel();
    changed();
  });
  syncPublishLabel();

  // Safari silently refuses to send a form with a half-typed date, so say what's wrong instead.
  form.addEventListener("submit", (event) => {
    if (local.validity.badInput && event.submitter?.value !== "delete") {
      event.preventDefault();
      setMeta(true);
      local.focus();
      show("Finish the publish date, or clear it.", true);
      return;
    }
    clearTimeout(timer);
    syncDate();
    dirty = false;
  });

  addEventListener("beforeunload", (event) => {
    if (dirty) event.preventDefault();
  });

  // A normal save lands on ?saved=1, which is the only proof the browser copy can go.

  const url = new URL(location.href);
  if (url.searchParams.has("saved")) {
    store.clear();
    url.searchParams.delete("saved");
    history.replaceState(null, "", url);
  }

  const copy = store.read();
  if (copy && (copy.title !== title.value || copy.body !== body.value)) {
    const banner = $("[data-restore]");
    banner.hidden = false;
    $("[data-restore-yes]", banner).addEventListener("click", () => {
      title.value = copy.title;
      body.value = copy.body;
      body.dispatchEvent(new Event("cms:replaced"));
      fitAll();
      banner.hidden = true;
      changed();
    });
    $("[data-restore-no]", banner).addEventListener("click", () => {
      store.clear();
      banner.hidden = true;
    });
  } else if (copy) {
    store.clear();
  }

  // Writing

  function insert(text, start = body.selectionStart, end = body.selectionEnd) {
    body.focus();
    body.setSelectionRange(start, end);
    if (!document.execCommand("insertText", false, text)) body.setRangeText(text, start, end, "end");
    changed();
  }

  function wrap(before, after = before) {
    const { selectionStart: start, selectionEnd: end } = body;
    insert(before + body.value.slice(start, end) + after, start, end);
    if (start === end) body.setSelectionRange(start + before.length, start + before.length);
  }

  async function addImages(files) {
    if (body.dataset.visual) {
      body.dispatchEvent(new CustomEvent("cms:images", { detail: files }));
      return;
    }
    for (const file of files) {
      const marker = `![Uploading ${file.name || "image"}…]()`;
      const lead = body.selectionStart > 0 && body.value[body.selectionStart - 1] !== "\n" ? "\n\n" : "";
      insert(`${lead}${marker}\n\n`);
      try {
        const { url } = await uploadImage(file);
        body.value = body.value.replace(marker, `![](${url})`);
        fit(body);
        changed();
      } catch (error) {
        body.value = body.value.replace(`${marker}\n\n`, "");
        changed();
        show(error.message, true);
      }
    }
  }

  body.addEventListener("paste", (event) => {
    const images = imageFiles(event.clipboardData.files);
    if (images.length) {
      event.preventDefault();
      addImages(images);
      return;
    }
    const pasted = event.clipboardData.getData("text").trim();
    const { selectionStart: start, selectionEnd: end } = body;
    if (start !== end && /^https?:\/\/\S+$/.test(pasted)) {
      event.preventDefault();
      insert(`[${body.value.slice(start, end)}](${pasted})`, start, end);
    }
  });

  writing.addEventListener("dragover", (event) => {
    event.preventDefault();
    writing.classList.add("dropping");
  });
  writing.addEventListener("dragleave", () => writing.classList.remove("dropping"));
  writing.addEventListener("drop", (event) => {
    if (event.defaultPrevented) return writing.classList.remove("dropping");
    event.preventDefault();
    writing.classList.remove("dropping");
    const images = imageFiles(event.dataTransfer.files);
    if (images.length) addImages(images);
  });

  $("[data-add-image]")?.addEventListener("click", async () => {
    addImages(await pickImages());
  });

  title.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      body.focus();
    }
  });

  addEventListener("keydown", (event) => {
    if (!(event.metaKey || event.ctrlKey)) return;
    const key = event.key.toLowerCase();
    if (key === "s") {
      event.preventDefault();
      if (isDraft) autosave();
      else $("[data-save]").click();
    } else if (document.activeElement === body && "bik".includes(key)) {
      event.preventDefault();
      if (key === "b") wrap("**");
      if (key === "i") wrap("*");
      if (key === "k") wrap("[", "]()");
    }
  });

  // Preview: the server's renderer, in a sandbox.

  $("[data-toggle-preview]")?.addEventListener("click", async (event) => {
    const button = event.currentTarget;
    const opening = frame.hidden;
    if (opening) {
      const data = new FormData();
      data.set("title", title.value);
      data.set("body", body.value);
      const response = await cms("preview", data);
      if (response.status === 401) return signedOut();
      frame.srcdoc = await response.text();
    }
    frame.hidden = !opening;
    writing.hidden = opening;
    button.textContent = opening ? "Edit" : "Preview";
    button.setAttribute("aria-pressed", String(opening));
    if (!opening) fitAll();
  });

  show(words());
}

// Site lists

$$("[data-list]").forEach((form) => {
  const rows = $(".rows", form);
  const template = $("template", form);
  const save = $("[type=submit]", form);
  const state = $("[data-state]", form);
  const photos = rows.classList.contains("photos");
  const arranged = !("sorted" in rows.dataset);

  let dirty = false;
  let pending = 0;
  let problem = "";
  let queue = Promise.resolve();
  let dragged = null;

  const discard = $("[data-discard]", form);
  discard.addEventListener("click", () => {
    if (!confirm("Discard your changes to this list?")) return;
    dirty = false;
    pending = 0;
    location.reload();
  });

  addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "s") {
      event.preventDefault();
      if (!pending) form.requestSubmit();
    }
  });

  function show() {
    save.disabled = pending > 0;
    discard.hidden = !dirty;
    form.classList.toggle("dirty", dirty);
    state.classList.toggle("bad", Boolean(problem));
    state.textContent = problem || (pending ? `Uploading, ${pending} left` : dirty ? "Unsaved changes" : state.textContent);
  }

  function changed() {
    dirty = true;
    problem = "";
    show();
  }

  function addRow() {
    rows.append(template.content.cloneNode(true));
    return rows.lastElementChild;
  }

  // One at a time, so a large batch cannot exhaust the browser's memory.
  function upload(field, file) {
    const image = $("img", field);
    const input = $("input", field);
    const previous = image.getAttribute("src");
    const preview = URL.createObjectURL(file);
    image.src = preview;
    pending += 1;
    dirty = true;
    show();
    queue = queue.then(async () => {
      try {
        const { url, thumb } = await uploadImage(file);
        input.value = url;
        image.src = thumb;
        $("button", field).textContent = "Replace";
      } catch (error) {
        problem = `${file.name}: ${error.message}`;
        if (input.value) image.src = previous;
        else field.closest(".item").remove();
      }
      URL.revokeObjectURL(preview);
      pending -= 1;
      show();
    });
  }

  const addPhotos = (files) => files.forEach((file) => upload($(".image-field", addRow()), file));

  form.addEventListener("input", changed);

  form.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-row]");
    if (!button) return;
    const row = button.closest(".item");
    const action = button.dataset.row;
    if (action === "up" && row.previousElementSibling) row.previousElementSibling.before(row);
    if (action === "down" && row.nextElementSibling) row.nextElementSibling.after(row);
    if (action === "remove") row.remove();
    if (action === "add") $("input:not([type=hidden]), textarea", addRow())?.focus();
    if (action === "add-photos") addPhotos(await pickImages(true));
    if (action === "upload") {
      const [file] = await pickImages();
      if (file) upload(button.closest(".image-field"), file);
    }
    if (["up", "down", "remove", "add"].includes(action)) changed();
  });

  form.addEventListener("submit", (event) => {
    if (pending) return event.preventDefault();
    if (!rows.children.length) {
      if (!confirm("Remove everything from this list?")) return event.preventDefault();
      $("[name=confirm_empty]", form).value = "1";
    }
    dirty = false;
  });

  addEventListener("beforeunload", (event) => {
    if (dirty || pending) event.preventDefault();
  });

  // Dragging a row. Typing and clicking inside one must still work.

  if (arranged) {
    rows.addEventListener("mousedown", (event) => {
      const row = event.target.closest(".item");
      if (row) row.draggable = !event.target.closest("input, textarea, button");
    });
    rows.addEventListener("dragstart", (event) => {
      dragged = event.target.closest(".item");
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", "");
      dragged.classList.add("dragging");
    });
    rows.addEventListener("dragover", (event) => {
      if (!dragged) return;
      event.preventDefault();
      const over = event.target.closest(".item");
      if (!over || over === dragged) return;
      const box = over.getBoundingClientRect();
      const past = photos ? event.clientX > box.left + box.width / 2 : event.clientY > box.top + box.height / 2;
      over[past ? "after" : "before"](dragged);
    });
    rows.addEventListener("dragend", () => {
      dragged.classList.remove("dragging");
      dragged = null;
      changed();
    });
  }

  // Dropping files anywhere on the page fills a photo list.

  if (photos) {
    const hasFiles = (event) => event.dataTransfer.types.includes("Files");
    addEventListener("dragover", (event) => {
      if (!hasFiles(event)) return;
      event.preventDefault();
      form.classList.add("dropping");
    });
    addEventListener("dragleave", (event) => {
      if (!event.relatedTarget) form.classList.remove("dropping");
    });
    addEventListener("drop", (event) => {
      if (!hasFiles(event)) return;
      event.preventDefault();
      form.classList.remove("dropping");
      addPhotos(imageFiles(event.dataTransfer.files));
    });
  }
});
