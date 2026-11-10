// The visual editor. It writes Markdown into the form's body field on every change, so saving,
// autosave, preview and the browser copy all work as they do with the plain Markdown field.
import { EditorState, NodeSelection, Plugin, TextSelection } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { keymap } from "prosemirror-keymap";
import { history, undo, redo } from "prosemirror-history";
import { baseKeymap, chainCommands, exitCode, lift, setBlockType, toggleMark, wrapIn } from "prosemirror-commands";
import { InputRule, inputRules, textblockTypeInputRule, wrappingInputRule } from "prosemirror-inputrules";
import { liftListItem, sinkListItem, splitListItem, wrapInList } from "prosemirror-schema-list";
import { dropCursor } from "prosemirror-dropcursor";
import { gapCursor } from "prosemirror-gapcursor";
import { Marked } from "marked";
import { isSafeUrl, schema } from "./editor/schema.js";
import { fromMarkdown, toMarkdown } from "./editor/markdown.js";

const { nodes, marks } = schema;

function start(body) {
  const form = body.form;
  const title = form.querySelector("[name=title]");
  const state = document.querySelector("[data-state]");
  const note = document.querySelector("[data-markdown-note]");
  const toggle = document.querySelector("[data-toggle-markdown]");
  const PREFER = "n-admin:markdown";

  // The site's own parser decides what the text means, and a round trip that changes the page is refused.
  const marked = new Marked({ gfm: true, breaks: true });
  const decoder = document.createElement("textarea");
  const decode = (text) => (text.includes("&") ? ((decoder.innerHTML = text), decoder.value) : text);
  // Compared as the browser reads it, so &mdash; and an em dash count as the same text.
  const normal = document.createElement("template");
  const rendered = (markdown) => {
    normal.innerHTML = marked.parse(markdown);
    return normal.innerHTML.replace(/>\s+</g, "><").trim();
  };
  function read(markdown) {
    try {
      const doc = fromMarkdown(markdown, (source) => marked.lexer(source), decode);
      return rendered(toMarkdown(doc)) === rendered(markdown) ? doc : null;
    } catch {
      return null;
    }
  }

  const host = document.createElement("div");
  host.className = "visual";
  body.after(host);

  const panel = new Panel();
  let view = null;

  function stateFor(doc) {
    return EditorState.create({ doc, plugins: plugins(panel) });
  }

  // Images whose upload finished, by the temporary address they were shown under, so an undo and
  // redo across the upload brings back the saved image, not a dead preview.
  const uploaded = new Map();
  let uploading = 0;
  const countUploads = () => (body.dataset.uploading = String(uploading));

  function writeBack(doc) {
    // The Markdown field is the one being edited now; the hidden visual copy mustn't overwrite it.
    if (!body.dataset.visual) return;
    body.value = toMarkdown(doc);
    body.dispatchEvent(new Event("input", { bubbles: true }));
  }

  function showVisual() {
    const doc = read(body.value);
    if (!doc) return false;
    if (view) view.updateState(stateFor(doc));
    else {
      view = new EditorView(host, {
        state: stateFor(doc),
        attributes: () => ({ class: `prose${isEmpty(view?.state.doc) ? " empty" : ""}`, "aria-label": "Body", "data-placeholder": "Start writing, or paste an image" }),
        nodeViews: { image: (node, v, getPos) => new ImageView(node, v, getPos), raw: (node, v, getPos) => new RawView(node, v, getPos) },
        dispatchTransaction(tr) {
          const next = view.state.apply(tr);
          view.updateState(next);
          if (tr.docChanged) {
            const stale = [];
            next.doc.descendants((node, pos) => {
              if (node.type === nodes.image && uploaded.has(node.attrs.src)) stale.push(pos);
            });
            if (stale.length) {
              const fix = view.state.tr.setMeta("addToHistory", false);
              for (const pos of stale) fix.setNodeMarkup(pos, null, { ...next.doc.nodeAt(pos).attrs, src: uploaded.get(next.doc.nodeAt(pos).attrs.src) });
              return view.dispatch(fix);
            }
            writeBack(next.doc);
          }
          panel.update(view);
        },
        handlePaste: (v, event) => pasted(v, event),
        handleDrop: (v, event) => dropped(v, event),
        handleDOMEvents: { blur: () => { setTimeout(() => { if (!host.contains(document.activeElement) && !panel.el.contains(document.activeElement)) panel.hide(); }); return false; } },
      });
      panel.attach(view);
    }
    host.hidden = false;
    body.hidden = true;
    body.dataset.visual = "1";
    note.hidden = true;
    toggle.textContent = "Edit as Markdown";
    return true;
  }

  function showMarkdown(reason) {
    host.hidden = true;
    body.hidden = false;
    delete body.dataset.visual;
    panel.hide();
    toggle.textContent = "Edit visually";
    note.hidden = !reason;
    if (reason) note.textContent = reason;
    body.dispatchEvent(new Event("cms:resize"));
  }

  const UNREADABLE = "This text uses Markdown the visual editor can't show without changing it, so it stays as Markdown.";

  toggle.addEventListener("click", () => {
    if (uploading) {
      fail("Wait for the images to finish uploading.");
      return;
    }
    if (body.hidden) {
      showMarkdown();
      try { localStorage.setItem(PREFER, "1"); } catch {}
      body.focus();
    } else if (showVisual()) {
      try { localStorage.removeItem(PREFER); } catch {}
      view.focus();
    } else showMarkdown(UNREADABLE);
  });

  let prefersMarkdown = false;
  try { prefersMarkdown = localStorage.getItem(PREFER) === "1"; } catch {}
  if (prefersMarkdown) showMarkdown();
  else if (!showVisual()) showMarkdown(UNREADABLE);

  // The plain field changed under us: restored from this browser's copy, or edited as Markdown.
  body.addEventListener("cms:replaced", () => {
    if (body.dataset.visual && !showVisual()) showMarkdown(UNREADABLE);
  });

  body.addEventListener("cms:images", (event) => {
    if (view) insertImages(view, event.detail, null);
  });

  title.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && view && body.dataset.visual) {
      event.preventDefault();
      view.focus();
      view.dispatch(view.state.tr.setSelection(TextSelection.atStart(view.state.doc)));
    }
  });

  function fail(message) {
    state.classList.add("bad");
    state.textContent = message;
  }

  // Images: shown at once from the file, saved into the text once uploaded.

  // An image already uploaded, chosen from the ones here.
  body.addEventListener("cms:image-url", (event) => {
    if (view) placeImage(view, event.detail, null);
  });

  function placeImage(v, src, at) {
    const image = nodes.image.create({ src, alt: "" });
    const { state: s } = v;
    let tr = s.tr;
    const $at = at == null ? s.selection.$from : s.doc.resolve(at);
    const parent = $at.parent;
    if (parent.type === nodes.paragraph && parent.content.size === 0 && $at.depth > 0) {
      tr = tr.replaceWith($at.before(), $at.after(), nodes.paragraph.create(null, image));
    } else {
      const after = $at.depth > 0 ? $at.after(1) : $at.pos;
      tr = tr.insert(after, nodes.paragraph.create(null, image));
    }
    v.dispatch(tr.scrollIntoView());
  }

  function insertImages(v, files, at) {
    for (const file of files) {
      const local = URL.createObjectURL(file);
      placeImage(v, local, at);
      uploading += 1;
      countUploads();
      window.cmsUpload(file).then(
        ({ url }) => {
          uploaded.set(local, url);
          replaceImage(v, local, (attrs) => ({ ...attrs, src: url }));
        },
        (error) => {
          replaceImage(v, local, null);
          fail(error.message);
        },
      ).finally(() => {
        uploading -= 1;
        countUploads();
        // Written out once more, so the saved text has the image even if it arrived while hidden.
        if (view) writeBack(view.state.doc);
      });
    }
  }

  function replaceImage(v, src, change) {
    let found = null;
    v.state.doc.descendants((node, pos) => {
      if (found == null && node.type === nodes.image && node.attrs.src === src) found = pos;
    });
    if (found == null) return;
    const node = v.state.doc.nodeAt(found);
    const tr = change ? v.state.tr.setNodeMarkup(found, null, change(node.attrs)) : v.state.tr.delete(found, found + 1);
    v.dispatch(tr.setMeta("addToHistory", false));
  }

  const imageFiles = (list) => [...(list ?? [])].filter((file) => file.type.startsWith("image/"));

  function pasted(v, event) {
    const images = imageFiles(event.clipboardData?.files);
    if (images.length) {
      insertImages(v, images, null);
      return true;
    }
    const text = event.clipboardData?.getData("text/plain").trim() ?? "";
    const { from, to, empty } = v.state.selection;
    if (!empty && /^(https?:\/\/|mailto:)\S+$/.test(text) && isSafeUrl(text)) {
      v.dispatch(v.state.tr.addMark(from, to, marks.link.create({ href: text })));
      return true;
    }
    // Text copied from another Markdown editor arrives as Markdown, so read it as such.
    if (!event.clipboardData?.getData("text/html") && /(^|\n)(#{1,6} |[-*] |\d+\. |> |```)|\*\*\S|\]\(/.test(text)) {
      const doc = read(text);
      if (doc) {
        v.dispatch(v.state.tr.replaceSelection(doc.slice(0, doc.content.size, true)).scrollIntoView());
        return true;
      }
    }
    return false;
  }

  function dropped(v, event) {
    const images = imageFiles(event.dataTransfer?.files);
    if (!images.length) return false;
    event.preventDefault();
    const at = v.posAtCoords({ left: event.clientX, top: event.clientY });
    insertImages(v, images, at ? at.pos : null);
    return true;
  }
}

const isEmpty = (doc) => !doc || (doc.childCount === 1 && doc.firstChild.type === nodes.paragraph && doc.firstChild.content.size === 0);

// Typing Markdown still works: "## " makes a heading, "- " a list, **bold** turns bold, and so on.

function markRule(pattern, type) {
  return new InputRule(pattern, (state, match, start, end) => {
    const from = start + (match[1] ?? "").length;
    const text = schema.text(match[2], type.create().addToSet(state.doc.resolve(from).marks()));
    return state.tr.replaceWith(from, end, text).removeStoredMark(type);
  });
}

function plugins(panel) {
  const heading = (level) => setBlockType(nodes.heading, { level });
  return [
    inputRules({
      rules: [
        wrappingInputRule(/^\s*>\s$/, nodes.blockquote),
        wrappingInputRule(/^(\d+)\.\s$/, nodes.ordered_list, (match) => ({ order: Number(match[1]) }), (match, node) => node.childCount + node.attrs.order === Number(match[1])),
        wrappingInputRule(/^\s*([-+*])\s$/, nodes.bullet_list),
        textblockTypeInputRule(/^```([\w-]*)\s$/, nodes.code_block, (match) => ({ params: match[1] || "" })),
        textblockTypeInputRule(/^(#{1,4})\s$/, nodes.heading, (match) => ({ level: match[1].length })),
        new InputRule(/^(?:---|\*\*\*|___)$/, (state, match, start, end) => {
          const $start = state.doc.resolve(start);
          if ($start.parent.type !== nodes.paragraph || $start.parent.content.size !== match[0].length - 1) return null;
          return state.tr.replaceRangeWith($start.before(), $start.after(), nodes.horizontal_rule.create())
            .insert($start.before() + 1, nodes.paragraph.create());
        }),
        markRule(/(^|[^*\w])\*\*([^*\s](?:[^*]*[^*\s])?)\*\*$/, marks.strong),
        markRule(/(^|[^*\w])\*([^*\s](?:[^*]*[^*\s])?)\*$/, marks.em),
        markRule(/(^|[^_\w])_([^_\s](?:[^_]*[^_\s])?)_$/, marks.em),
        markRule(/(^|[^`])`([^`]+)`$/, marks.code),
        markRule(/(^|[^~])~~([^~\s](?:[^~]*[^~\s])?)~~$/, marks.strike),
      ],
    }),
    keymap({
      "Mod-z": undo,
      "Shift-Mod-z": redo,
      "Mod-y": redo,
      "Mod-b": toggleMark(marks.strong),
      "Mod-i": toggleMark(marks.em),
      "Mod-e": toggleMark(marks.code),
      "Mod-Shift-x": toggleMark(marks.strike),
      "Mod-k": (state, dispatch, view) => {
        if (state.selection.empty && !linkAt(state)) return false;
        if (view) panel.editLink(view);
        return true;
      },
      "Mod-Alt-0": setBlockType(nodes.paragraph),
      "Mod-Alt-1": heading(1),
      "Mod-Alt-2": heading(2),
      "Mod-Alt-3": heading(3),
      "Shift-Enter": chainCommands(exitCode, (state, dispatch) => {
        // A line break inside a heading would split it on the site.
        if (state.selection.$from.parent.type === nodes.heading) return false;
        dispatch?.(state.tr.replaceSelectionWith(nodes.hard_break.create()).scrollIntoView());
        return true;
      }),
      Enter: splitListItem(nodes.list_item),
      Tab: sinkListItem(nodes.list_item),
      "Shift-Tab": liftListItem(nodes.list_item),
      // Backspace at the start of a heading or quote undoes it before joining lines.
      Backspace: (state, dispatch) => {
        const { $cursor } = state.selection;
        if (!$cursor || $cursor.parentOffset > 0) return false;
        if ($cursor.parent.type === nodes.heading || $cursor.parent.type === nodes.code_block) return setBlockType(nodes.paragraph)(state, dispatch);
        if ($cursor.node(-1)?.type === nodes.blockquote && $cursor.index(-1) === 0) return lift(state, dispatch);
        return false;
      },
      Escape: (state, dispatch, view) => {
        if (!panel.visible) return false;
        panel.hide();
        view?.focus();
        return true;
      },
    }),
    keymap(baseKeymap),
    history(),
    dropCursor({ color: "var(--accent)", width: 2 }),
    gapCursor(),
    new Plugin({ view: () => ({ update: (view) => panel.update(view) }) }),
  ];
}

// Selection toolbar, link editor and image details share one small floating panel.

function linkAt(state, pos = state.selection.from) {
  const $pos = state.doc.resolve(pos);
  const mark = marks.link.isInSet($pos.nodeAfter?.marks ?? []) ?? marks.link.isInSet($pos.nodeBefore?.marks ?? []);
  if (!mark) return null;
  const start = $pos.start();
  let from = null;
  let to = null;
  let found = null;
  $pos.parent.forEach((child, offset) => {
    const a = start + offset;
    if (!mark.isInSet(child.marks)) {
      from = to = null;
      return;
    }
    if (from === null || a !== to) from = a;
    to = a + child.nodeSize;
    if (from <= pos && pos <= to) found = { from, to, href: mark.attrs.href };
  });
  return found;
}

const ICONS = {
  strong: '<path d="M7 5h6a3.5 3.5 0 0 1 0 7H7zM7 12h7a3.5 3.5 0 0 1 0 7H7z"/>',
  em: '<path d="M10 5h8M6 19h8M14 5l-4 14"/>',
  strike: '<path d="M5 12h14M16 7a4 3 0 0 0-8 1c0 4 8 2 8 6a4 3 0 0 1-8 1"/>',
  code: '<path d="M9 8l-4 4 4 4M15 8l4 4-4 4"/>',
  link: '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
  h2: '<path d="M4 6v12M12 6v12M4 12h8M15 10a2.5 2.5 0 1 1 4 2l-4 6h5"/>',
  h3: '<path d="M4 6v12M12 6v12M4 12h8M15 8h5l-3 4a3 3 0 1 1-2 5"/>',
  quote: '<path d="M5 6v12M9 9h10M9 13h10M9 17h6"/>',
  bullets: '<path d="M10 7h10M10 12h10M10 17h10"/><circle cx="5" cy="7" r="1"/><circle cx="5" cy="12" r="1"/><circle cx="5" cy="17" r="1"/>',
  numbers: '<path d="M10 7h10M10 12h10M10 17h10M4 6l1.5-1v4M4 15.5a1.2 1.2 0 1 1 2 1L4 19h2.5"/>',
};
const icon = (name) => `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name]}</svg>`;

function inside(state, type) {
  const { $from } = state.selection;
  for (let d = $from.depth; d > 0; d--) if ($from.node(d).type === type) return true;
  return false;
}

class Panel {
  constructor() {
    this.el = document.createElement("div");
    this.el.className = "float-panel";
    this.el.hidden = true;
    this.el.setAttribute("role", "toolbar");
    document.body.append(this.el);
    this.mode = null;
    this.visible = false;
    this.el.addEventListener("mousedown", (event) => {
      if (!event.target.closest("input")) event.preventDefault();
    });
    addEventListener("scroll", () => this.visible && this.place(), { passive: true });
    addEventListener("resize", () => this.visible && this.place());
  }

  attach(view) {
    this.view = view;
  }

  hide() {
    this.el.hidden = true;
    this.visible = false;
    this.mode = null;
  }

  update(view) {
    this.view = view;
    if (this.mode === "link-edit" || this.mode === "alt-edit") return this.place();
    if (!view.hasFocus() && !this.el.contains(document.activeElement)) return this.hide();
    const { state } = view;
    const sel = state.selection;
    if (sel instanceof NodeSelection && sel.node.type === nodes.image) return this.showImage(sel);
    if (!sel.empty && !(sel instanceof NodeSelection)) return this.showFormat();
    const link = sel.empty ? linkAt(state) : null;
    if (link) return this.showLink(link);
    this.hide();
  }

  button(name, label, active, run) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `float-button${active ? " active" : ""}`;
    button.innerHTML = ICONS[name] ? icon(name) : "";
    if (!ICONS[name]) button.textContent = label;
    button.setAttribute("aria-label", label);
    button.title = label;
    button.setAttribute("aria-pressed", String(Boolean(active)));
    button.addEventListener("click", () => run());
    return button;
  }

  render(children, mode) {
    this.mode = mode;
    this.el.replaceChildren(...children);
    this.el.hidden = false;
    this.visible = true;
    this.place();
  }

  run(command) {
    const { view } = this;
    command(view.state, view.dispatch, view);
    view.focus();
  }

  showFormat() {
    const { state } = this.view;
    const { from, to, $from } = state.selection;
    const has = (type) => state.doc.rangeHasMark(from, to, type);
    const block = $from.parent;
    const isHeading = (level) => block.type === nodes.heading && block.attrs.level === level;
    const toggleHeading = (level) => (isHeading(level) ? setBlockType(nodes.paragraph) : setBlockType(nodes.heading, { level }));
    const listToggle = (type) => (inside(state, type) ? liftListItem(nodes.list_item) : wrapInList(type));
    const sep = () => Object.assign(document.createElement("span"), { className: "float-sep" });
    this.render([
      this.button("strong", "Bold", has(marks.strong), () => this.run(toggleMark(marks.strong))),
      this.button("em", "Italic", has(marks.em), () => this.run(toggleMark(marks.em))),
      this.button("strike", "Strikethrough", has(marks.strike), () => this.run(toggleMark(marks.strike))),
      this.button("code", "Code", has(marks.code), () => this.run(toggleMark(marks.code))),
      this.button("link", "Link", has(marks.link), () => this.editLink(this.view)),
      sep(),
      this.button("h2", "Heading", isHeading(2), () => this.run(toggleHeading(2))),
      this.button("h3", "Subheading", isHeading(3), () => this.run(toggleHeading(3))),
      this.button("quote", "Quote", inside(state, nodes.blockquote), () => this.run(inside(state, nodes.blockquote) ? lift : wrapIn(nodes.blockquote))),
      this.button("bullets", "Bulleted list", inside(state, nodes.bullet_list), () => this.run(listToggle(nodes.bullet_list))),
      this.button("numbers", "Numbered list", inside(state, nodes.ordered_list), () => this.run(listToggle(nodes.ordered_list))),
    ], "format");
  }

  showLink(link) {
    const open = Object.assign(document.createElement("a"), { className: "float-href", href: link.href, target: "_blank", rel: "noreferrer", textContent: link.href });
    this.render([
      open,
      this.button("", "Edit", false, () => this.editLink(this.view)),
      this.button("", "Remove", false, () => this.run((state, dispatch) => {
        dispatch(state.tr.removeMark(link.from, link.to, marks.link));
        return true;
      })),
    ], "link");
  }

  editLink(view) {
    this.view = view;
    const { state } = view;
    const existing = linkAt(state, state.selection.from);
    const range = state.selection.empty && existing ? existing : { from: state.selection.from, to: state.selection.to };
    const input = Object.assign(document.createElement("input"), { className: "float-input", type: "text", value: existing?.href ?? "", placeholder: "Paste or type a link", spellcheck: false });
    input.setAttribute("aria-label", "Link address");
    input.addEventListener("input", () => input.setCustomValidity(""));
    const apply = () => {
      // Spaces would end a Markdown link early, so they're encoded.
      let href = input.value.trim().replace(/\s/g, "%20");
      if (href && !/^([a-z][\w+.-]*:|\/|#|\?)/i.test(href)) href = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(href) ? `mailto:${href}` : `https://${href}`;
      if (href && !isSafeUrl(href)) {
        input.setCustomValidity("Use a web address, an email address or a path on this site.");
        input.reportValidity();
        return;
      }
      const tr = view.state.tr.removeMark(range.from, range.to, marks.link);
      if (href) tr.addMark(range.from, range.to, marks.link.create({ href }));
      this.mode = null;
      view.dispatch(tr.setSelection(TextSelection.create(tr.doc, range.to)));
      view.focus();
    };
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        apply();
      } else if (event.key === "Escape") {
        event.preventDefault();
        this.mode = null;
        view.focus();
        this.update(view);
      }
    });
    this.render([input, this.button("", "Apply", false, apply)], "link-edit");
    this.anchor = range;
    input.focus();
    input.select();
  }

  showImage(sel) {
    const pos = sel.from;
    const node = sel.node;
    const input = Object.assign(document.createElement("input"), { className: "float-input", type: "text", value: node.attrs.alt, placeholder: "Describe this image", spellcheck: true });
    input.title = "For people who can't see it, and for search engines";
    input.setAttribute("aria-label", "Image description");
    const save = () => {
      const current = this.view.state.doc.nodeAt(pos);
      if (current?.type === nodes.image && current.attrs.alt !== input.value) {
        const tr = this.view.state.tr.setNodeMarkup(pos, null, { ...current.attrs, alt: input.value });
        this.view.dispatch(tr.setSelection(NodeSelection.create(tr.doc, pos)));
      }
    };
    input.addEventListener("focus", () => { this.mode = "alt-edit"; });
    input.addEventListener("input", save);
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === "Escape") {
        event.preventDefault();
        this.mode = null;
        this.view.focus();
        this.update(this.view);
      }
    });
    input.addEventListener("blur", () => {
      setTimeout(() => {
        if (this.mode === "alt-edit" && !this.el.contains(document.activeElement)) {
          this.mode = null;
          if (!this.view.hasFocus()) this.hide();
        }
      });
    });
    const remove = this.button("", "Remove", false, () => this.run((state, dispatch) => {
      dispatch(state.tr.delete(pos, pos + 1));
      return true;
    }));
    this.render([input, remove], "image");
  }

  place() {
    if (!this.visible || !this.view) return;
    const { view } = this;
    const { state } = view;
    let rect;
    if (state.selection instanceof NodeSelection) {
      const dom = view.nodeDOM(state.selection.from);
      rect = dom?.getBoundingClientRect();
    }
    if (!rect) {
      const range = (this.mode === "link-edit" && this.anchor) || state.selection;
      const a = view.coordsAtPos(Math.min(range.from, state.doc.content.size));
      const b = view.coordsAtPos(Math.min(range.to, state.doc.content.size));
      rect = { left: Math.min(a.left, b.left), right: Math.max(a.right, b.right), top: Math.min(a.top, b.top), bottom: Math.max(a.bottom, b.bottom) };
    }
    const box = this.el.getBoundingClientRect();
    const bar = 56 + 8;
    const isImage = this.mode === "image";
    let top = isImage ? rect.bottom - box.height - 12 : rect.top - box.height - 8;
    if (!isImage && top < bar) top = rect.bottom + 8;
    if (isImage && top < bar) top = Math.min(innerHeight - box.height - 8, bar);
    const centre = (rect.left + rect.right) / 2;
    const left = Math.max(8, Math.min(innerWidth - box.width - 8, centre - box.width / 2));
    this.el.style.top = `${Math.round(top)}px`;
    this.el.style.left = `${Math.round(left)}px`;
  }
}

// An image shows its small copy while editing, and asks for a description until it has one.

const SIZED = /-(\d+)x(\d+)\.(webp|jpe?g|png|avif)$/i;

// A click on an image means "this image": its description takes the typing, which would otherwise
// replace the image. Arrowing onto it leaves the cursor in the text. The focus moves once the click
// is over, or the editor would take it back when the button comes up.
const focusDescription = () => {
  const field = () => document.querySelector(".float-panel:not([hidden]) .float-input");
  if (field()) field().focus();
  else setTimeout(() => field()?.focus());
};

class ImageView {
  constructor(node, view, getPos) {
    this.dom = document.createElement("span");
    this.dom.className = "image";
    this.img = document.createElement("img");
    this.img.draggable = false;
    this.img.addEventListener("click", focusDescription);
    this.dom.append(this.img);
    this.badge = Object.assign(document.createElement("span"), { className: "alt-badge", textContent: "Add description" });
    this.badge.addEventListener("mousedown", (event) => {
      event.preventDefault();
      view.dispatch(view.state.tr.setSelection(NodeSelection.create(view.state.doc, getPos())));
      view.focus();
      requestAnimationFrame(() => document.querySelector(".float-panel .float-input")?.focus());
    });
    this.dom.append(this.badge);
    this.update(node);
  }

  update(node) {
    if (node.type !== nodes.image) return false;
    const { src, alt } = node.attrs;
    const size = src.match(SIZED);
    const display = size && Number(size[1]) > 800 ? src.replace(SIZED, (m) => m.replace(/(\.\w+)$/, "-t$1")) : src;
    if (this.img.getAttribute("src") !== display) {
      this.img.src = display;
      if (display !== src) this.img.onerror = () => { this.img.onerror = null; this.img.src = src; };
    }
    if (size) {
      this.img.width = Number(size[1]);
      this.img.height = Number(size[2]);
    }
    this.img.alt = alt;
    this.dom.classList.toggle("uploading", src.startsWith("blob:"));
    this.badge.hidden = Boolean(alt) || src.startsWith("blob:");
    return true;
  }

  selectNode() {
    this.dom.classList.add("selected");
  }

  deselectNode() {
    this.dom.classList.remove("selected");
  }

  stopEvent(event) {
    return this.badge.contains(event.target);
  }

  ignoreMutation() {
    return true;
  }
}

// HTML and other Markdown the editor doesn't draw are edited as source, in place.

class RawView {
  constructor(node, view, getPos) {
    this.view = view;
    this.getPos = getPos;
    this.dom = document.createElement("div");
    this.dom.className = "raw";
    this.dom.contentEditable = "false";
    this.label = Object.assign(document.createElement("span"), { className: "raw-label" });
    this.area = Object.assign(document.createElement("textarea"), { spellcheck: false, rows: 1 });
    this.area.setAttribute("aria-label", "Source");
    this.dom.append(this.label, this.area);
    this.area.addEventListener("input", () => {
      this.fit();
      const pos = this.getPos();
      const current = this.view.state.doc.nodeAt(pos);
      if (current) this.view.dispatch(this.view.state.tr.setNodeMarkup(pos, null, { source: this.area.value }));
    });
    this.area.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z") {
        event.stopPropagation();
      }
    });
    this.update(node);
  }

  fit() {
    this.area.style.height = "auto";
    this.area.style.height = `${this.area.scrollHeight}px`;
  }

  update(node) {
    if (node.type !== nodes.raw) return false;
    const { source } = node.attrs;
    this.label.textContent = /^\s*</.test(source) ? "HTML" : "Markdown";
    if (this.area.value !== source) {
      this.area.value = source;
      requestAnimationFrame(() => this.fit());
    }
    return true;
  }

  selectNode() {
    this.dom.classList.add("selected");
  }

  deselectNode() {
    this.dom.classList.remove("selected");
  }

  stopEvent(event) {
    return event.target === this.area;
  }

  ignoreMutation() {
    return true;
  }
}

const body = document.querySelector("[data-editor] [name=body]");
if (body) start(body);
